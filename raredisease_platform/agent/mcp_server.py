from __future__ import annotations

import asyncio
import html
import json
import logging
import re
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

import httpx
from mcp.server.fastmcp import Context, FastMCP

# Path injection must happen AFTER the future import, but BEFORE importing local modules.
# This assumes this file is located at: raredisease_platform/agent/mcp_server.py
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from raredisease_platform.agent.openclaw_tools import evidence_query  # noqa: E402


BROKER_BASE_URL = "http://127.0.0.1:8000"
LOG_PATH = REPO_ROOT / "mcp_runtime.log"
JOBS_DIR = REPO_ROOT / "mcp_jobs"
UI_RUNS_DIR = REPO_ROOT / "ui_runs"


# ---------------------------------------------------------------------------
# Logging / server setup
# ---------------------------------------------------------------------------

def configure_logging() -> logging.Logger:
    logger = logging.getLogger("rare_disease_mcp")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d %(levelname)s [%(process)d] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Important: stdio MCP uses stdout as the protocol channel.
    # Do not print normal logs to stdout. stderr is safe.
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.INFO)
    stderr_handler.setFormatter(formatter)
    logger.addHandler(stderr_handler)

    logger.info("============================================================")
    logger.info("Starting rare-disease-evidence MCP server")
    logger.info("REPO_ROOT=%s", REPO_ROOT)
    logger.info("LOG_PATH=%s", LOG_PATH)
    logger.info("JOBS_DIR=%s", JOBS_DIR)
    logger.info("UI_RUNS_DIR=%s", UI_RUNS_DIR)
    logger.info("BROKER_BASE_URL=%s", BROKER_BASE_URL)
    logger.info("python=%s", sys.executable)
    logger.info("argv=%s", sys.argv)
    logger.info("============================================================")
    return logger


logger = configure_logging()
JOBS_DIR.mkdir(exist_ok=True)
UI_RUNS_DIR.mkdir(exist_ok=True)

mcp = FastMCP("rare-disease-evidence")
JOBS: Dict[str, Dict[str, Any]] = {}


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def _now() -> float:
    return time.time()


def _json_size_bytes(obj: Any) -> int:
    try:
        return len(json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8"))
    except Exception:
        return -1


def _clean_text(value: Any, *, max_chars: Optional[int] = None) -> str:
    if value is None:
        return ""

    text = html.unescape(str(value))
    text = re.sub(r"<[^>]+>", " ", text)
    text = " ".join(text.split())

    if max_chars is not None and max_chars > 0 and len(text) > max_chars:
        return text[: max_chars - 3].rstrip() + "..."

    return text


def _remove_empty(value: Any) -> Any:
    if isinstance(value, dict):
        cleaned = {k: _remove_empty(v) for k, v in value.items()}
        return {
            k: v
            for k, v in cleaned.items()
            if v not in (None, "", [], {})
        }

    if isinstance(value, list):
        cleaned_list = [_remove_empty(v) for v in value]
        return [v for v in cleaned_list if v not in (None, "", [], {})]

    return value


def _write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    tmp_path.replace(path)


def _read_json_file(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("Failed to read JSON file: %s", path)
        return None


def _safe_job_id(job_id: str) -> str:
    return "".join(ch for ch in str(job_id) if ch.isalnum() or ch in "-_")


def _job_path(job_id: str) -> Path:
    return JOBS_DIR / f"{_safe_job_id(job_id)}.json"


def _full_result_path(job_id: str) -> Path:
    return JOBS_DIR / f"{_safe_job_id(job_id)}.full.json"


def _write_job(job_id: str, job: Dict[str, Any]) -> None:
    job["updated_at"] = _now()
    JOBS[job_id] = job
    _write_json_atomic(_job_path(job_id), job)


def _read_job(job_id: str) -> Optional[Dict[str, Any]]:
    if job_id in JOBS:
        return JOBS[job_id]

    path = _job_path(job_id)
    if not path.exists():
        return None

    try:
        job = json.loads(path.read_text(encoding="utf-8"))
        JOBS[job_id] = job
        return job
    except Exception:
        logger.exception("Failed to read job file for job_id=%s", job_id)
        return None


def _safe_ui_run_path(path_or_name: str) -> Path:
    """
    Resolve a UI run file safely.

    Accepts:
    - "20260518_213248.agent.json"
    - "ui_runs/20260518_213248.agent.json"
    - "ui_runs\\20260518_213248.agent.json"

    Rejects paths outside REPO_ROOT/ui_runs.
    """
    raw = str(path_or_name).strip().replace("\\", "/")
    name = Path(raw).name

    if not name.endswith(".json"):
        raise ValueError("Only .json files are allowed.")

    path = (UI_RUNS_DIR / name).resolve()
    allowed_root = UI_RUNS_DIR.resolve()

    if allowed_root not in path.parents:
        raise ValueError("Resolved path is outside ui_runs.")

    return path


# ---------------------------------------------------------------------------
# Abstract-pack extraction
# ---------------------------------------------------------------------------

def _article_from_agent_record(
    article: Dict[str, Any],
    *,
    paper_index: int,
    max_abstract_chars: int,
) -> Dict[str, Any]:
    return _remove_empty(
        {
            "id": f"P{paper_index}",
            "title": _clean_text(article.get("title")),
            "year": article.get("year"),
            "journal": _clean_text(article.get("journal")),
            "pmid": article.get("pmid"),
            "pmcid": article.get("pmcid"),
            "doi": article.get("doi"),
            "abstract": _clean_text(
                article.get("abstract_excerpt") or article.get("abstract"),
                max_chars=max_abstract_chars,
            ),
        }
    )


def _article_from_full_broker_record(
    article: Dict[str, Any],
    *,
    paper_index: int,
    max_abstract_chars: int,
) -> Dict[str, Any]:
    return _remove_empty(
        {
            "id": f"P{paper_index}",
            "title": _clean_text(article.get("title")),
            "year": article.get("year"),
            "journal": _clean_text(article.get("journal")),
            "pmid": article.get("pmid"),
            "pmcid": article.get("pmcid"),
            "doi": article.get("doi"),
            "abstract": _clean_text(article.get("abstract"), max_chars=max_abstract_chars),
        }
    )


def _abstract_pack_from_agent_payload(
    agent_payload: Dict[str, Any],
    *,
    offset: int = 0,
    max_results: int = 10,
    max_abstract_chars: int = 900,
    include_trace_warnings: bool = True,
) -> Dict[str, Any]:
    """
    Convert a UI-generated .agent.json payload into a tiny LLM-facing abstract pack.

    This function supports batching via offset. If offset=20 and max_results=10,
    returned papers are P21-P30.
    """
    offset = max(0, int(offset))
    max_results = max(1, min(int(max_results), 100))
    max_abstract_chars = max(100, min(int(max_abstract_chars), 3000))

    query_summary = agent_payload.get("query_summary") or {}
    raw_results = agent_payload.get("top_literature_results") or []
    end = offset + max_results

    papers: List[Dict[str, Any]] = []
    for local_index, article in enumerate(raw_results[offset:end], start=1):
        if not isinstance(article, dict):
            continue
        absolute_index = offset + local_index
        papers.append(
            _article_from_agent_record(
                article,
                paper_index=absolute_index,
                max_abstract_chars=max_abstract_chars,
            )
        )

    trace_summary = agent_payload.get("trace_summary") or {}
    warnings: List[str] = []
    if include_trace_warnings:
        for key in ("pipeline_warnings", "normalization_warnings"):
            values = trace_summary.get(key) or []
            if isinstance(values, list):
                warnings.extend(str(v) for v in values if v)

    return _remove_empty(
        {
            "response_profile": "abstract_pack",
            "source": "ui_runs.agent_json",
            "literature_result_count": query_summary.get("literature_result_count"),
            "available_paper_count": len(raw_results),
            "offset": offset,
            "requested_paper_count": max_results,
            "returned_paper_count": len(papers),
            "paper_id_range": f"P{offset + 1}-P{offset + len(papers)}" if papers else None,
            "papers": papers,
            "warnings": warnings[:5],
            "citation_instructions": (
                "When making claims from a paper, cite the local paper id like [P1], [P2]. "
                "Use PMID/DOI fields for verification when available."
            ),
        }
    )


def _abstract_pack_from_full_result(
    full_result: Dict[str, Any],
    *,
    offset: int = 0,
    max_results: int = 10,
    max_abstract_chars: int = 900,
) -> Dict[str, Any]:
    offset = max(0, int(offset))
    max_results = max(1, min(int(max_results), 100))
    max_abstract_chars = max(100, min(int(max_abstract_chars), 3000))

    literature_results = full_result.get("literature_results") or []
    end = offset + max_results

    papers: List[Dict[str, Any]] = []
    for local_index, article in enumerate(literature_results[offset:end], start=1):
        if not isinstance(article, dict):
            continue
        absolute_index = offset + local_index
        papers.append(
            _article_from_full_broker_record(
                article,
                paper_index=absolute_index,
                max_abstract_chars=max_abstract_chars,
            )
        )

    trace = full_result.get("trace") or {}

    return _remove_empty(
        {
            "response_profile": "abstract_pack",
            "source": "mcp_job.full_json",
            "literature_result_count": len(literature_results),
            "available_paper_count": len(literature_results),
            "offset": offset,
            "requested_paper_count": max_results,
            "returned_paper_count": len(papers),
            "paper_id_range": f"P{offset + 1}-P{offset + len(papers)}" if papers else None,
            "papers": papers,
            "warnings": (trace.get("warnings") or [])[:5],
            "citation_instructions": (
                "When making claims from a paper, cite the local paper id like [P1], [P2]. "
                "Use PMID/DOI fields for verification when available."
            ),
        }
    )


def _make_batch_plan(
    *,
    total_available: int,
    papers_to_process: int,
    batch_size: int,
) -> List[Dict[str, int]]:
    papers_to_process = max(1, min(int(papers_to_process), int(total_available)))
    batch_size = max(1, min(int(batch_size), 100))

    batches = []
    for offset in range(0, papers_to_process, batch_size):
        count = min(batch_size, papers_to_process - offset)
        batches.append(
            {
                "batch_number": len(batches) + 1,
                "offset": offset,
                "max_results": count,
                "first_paper_id": offset + 1,
                "last_paper_id": offset + count,
            }
        )

    return batches


# ---------------------------------------------------------------------------
# Background evidence query jobs
# ---------------------------------------------------------------------------

async def _run_evidence_job(
    *,
    job_id: str,
    raw_query: str,
    expected_entity_types: Optional[List[str]],
    literature_keywords: Optional[str],
    literature_filters: Dict[str, Any],
    include_structured_evidence: bool,
    requested_evidence_types: Optional[List[str]],
    deep_search: bool,
    timeout_seconds: float,
) -> None:
    start = time.perf_counter()

    job = _read_job(job_id) or {}
    job.update(
        {
            "job_id": job_id,
            "status": "running",
            "started_at": _now(),
            "elapsed_seconds": 0.0,
            "broker_base_url": BROKER_BASE_URL,
            "message": "Broker query is running.",
        }
    )
    _write_job(job_id, job)

    logger.info(
        "[%s] JOB START raw_query=%r expected_entity_types=%r literature_keywords=%r "
        "filters=%r include_structured_evidence=%r requested_evidence_types=%r "
        "deep_search=%r timeout=%.1f",
        job_id,
        raw_query,
        expected_entity_types,
        literature_keywords,
        literature_filters,
        include_structured_evidence,
        requested_evidence_types,
        deep_search,
        timeout_seconds,
    )

    try:
        result = await asyncio.wait_for(
            evidence_query(
                raw_query=raw_query,
                expected_entity_types=expected_entity_types,
                literature_keywords=literature_keywords,
                literature_filters=literature_filters,
                include_structured_evidence=include_structured_evidence,
                requested_evidence_types=requested_evidence_types,
                deep_search=deep_search,
                broker_base_url=BROKER_BASE_URL,
                timeout_seconds=timeout_seconds,
            ),
            timeout=timeout_seconds,
        )

        elapsed = time.perf_counter() - start
        full_path = _full_result_path(job_id)
        _write_json_atomic(full_path, result)

        literature_results = result.get("literature_results") or []

        job.update(
            {
                "status": "completed",
                "completed_at": _now(),
                "elapsed_seconds": round(elapsed, 3),
                "message": "Broker query completed.",
                "result_summary": {
                    "literature_result_count": len(literature_results),
                    "full_result_size_bytes": _json_size_bytes(result),
                },
                "debug": {
                    "log_path": str(LOG_PATH),
                    "job_path": str(_job_path(job_id)),
                    "full_result_path": str(full_path),
                },
            }
        )
        _write_job(job_id, job)

        logger.info(
            "[%s] JOB DONE elapsed=%.3fs literature_results=%s full_size=%s",
            job_id,
            elapsed,
            len(literature_results),
            _json_size_bytes(result),
        )

    except asyncio.TimeoutError:
        elapsed = time.perf_counter() - start
        job.update(
            {
                "status": "failed",
                "completed_at": _now(),
                "elapsed_seconds": round(elapsed, 3),
                "message": f"Broker query timed out after {timeout_seconds:.1f} seconds.",
                "error_type": "TimeoutError",
                "error": f"Timed out after {timeout_seconds:.1f} seconds",
                "debug": {
                    "log_path": str(LOG_PATH),
                    "job_path": str(_job_path(job_id)),
                },
            }
        )
        _write_job(job_id, job)
        logger.error("[%s] JOB TIMEOUT elapsed=%.3fs timeout=%.1fs", job_id, elapsed, timeout_seconds)

    except Exception as exc:
        elapsed = time.perf_counter() - start
        tb = traceback.format_exc()

        job.update(
            {
                "status": "failed",
                "completed_at": _now(),
                "elapsed_seconds": round(elapsed, 3),
                "message": "Broker query failed.",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "traceback": tb[-4000:],
                "debug": {
                    "log_path": str(LOG_PATH),
                    "job_path": str(_job_path(job_id)),
                },
            }
        )
        _write_job(job_id, job)

        logger.error(
            "[%s] JOB ERROR elapsed=%.3fs %s: %s\n%s",
            job_id,
            elapsed,
            type(exc).__name__,
            exc,
            tb,
        )


# ---------------------------------------------------------------------------
# MCP tools: health
# ---------------------------------------------------------------------------

@mcp.tool(name="evidence_ping")
async def evidence_ping(ctx: Optional[Context] = None) -> Dict[str, Any]:
    """Fast MCP smoke test."""
    call_id = str(uuid4())
    logger.info("[%s] evidence_ping called", call_id)

    if ctx is not None:
        await ctx.info(f"[{call_id}] evidence_ping reached MCP server")

    return {
        "ok": True,
        "message": "rare-disease-evidence MCP server is reachable",
        "call_id": call_id,
    }


@mcp.tool(name="evidence_broker_health")
async def evidence_broker_health(ctx: Optional[Context] = None) -> Dict[str, Any]:
    """Check that the MCP server can reach the local broker."""
    call_id = str(uuid4())
    start = time.perf_counter()
    logger.info("[%s] evidence_broker_health START", call_id)

    if ctx is not None:
        await ctx.info(f"[{call_id}] checking broker health")

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(f"{BROKER_BASE_URL}/")

        elapsed = time.perf_counter() - start
        logger.info(
            "[%s] evidence_broker_health DONE status=%s elapsed=%.3fs",
            call_id,
            response.status_code,
            elapsed,
        )

        return {
            "ok": response.is_success,
            "status_code": response.status_code,
            "elapsed_seconds": round(elapsed, 3),
            "broker_base_url": BROKER_BASE_URL,
            "body": response.text[:500],
        }

    except Exception as exc:
        elapsed = time.perf_counter() - start
        logger.exception("[%s] evidence_broker_health ERROR elapsed=%.3fs", call_id, elapsed)
        return {
            "ok": False,
            "elapsed_seconds": round(elapsed, 3),
            "error_type": type(exc).__name__,
            "error": str(exc),
            "broker_base_url": BROKER_BASE_URL,
        }


# ---------------------------------------------------------------------------
# MCP tools: UI payload reading and batching
# ---------------------------------------------------------------------------

@mcp.tool(name="evidence_read_agent_payload")
async def evidence_read_agent_payload_tool(
    path_or_name: str,
    offset: int = 0,
    max_results: int = 10,
    max_abstract_chars: int = 900,
    include_trace_warnings: bool = True,
) -> Dict[str, Any]:
    """Read ui_runs/*.agent.json and return one abstract batch."""
    try:
        path = _safe_ui_run_path(path_or_name)
    except Exception as exc:
        return {
            "ok": False,
            "error_type": type(exc).__name__,
            "error": str(exc),
            "hint": "Pass a filename like 20260518_213248.agent.json or ui_runs/20260518_213248.agent.json.",
        }

    if not path.exists():
        return {
            "ok": False,
            "error_type": "FileNotFoundError",
            "error": f"Could not find UI payload file: {path}",
            "available_files": [
                p.name
                for p in sorted(
                    UI_RUNS_DIR.glob("*.agent.json"),
                    key=lambda p: p.stat().st_mtime,
                    reverse=True,
                )[:10]
            ],
        }

    payload = _read_json_file(path)
    if payload is None:
        return {
            "ok": False,
            "error_type": "JSONDecodeError",
            "error": f"Could not parse UI payload file: {path}",
            "path": str(path),
        }

    abstract_pack = _abstract_pack_from_agent_payload(
        payload,
        offset=offset,
        max_results=max_results,
        max_abstract_chars=max_abstract_chars,
        include_trace_warnings=include_trace_warnings,
    )

    return {
        "ok": True,
        "path": str(path),
        "result": abstract_pack,
        "debug": {
            "original_size_bytes": _json_size_bytes(payload),
            "returned_size_bytes": _json_size_bytes(abstract_pack),
        },
    }


@mcp.tool(name="evidence_plan_agent_payload_batches")
async def evidence_plan_agent_payload_batches_tool(
    path_or_name: str,
    papers_to_process: int = 100,
    batch_size: int = 20,
    max_abstract_chars: int = 700,
) -> Dict[str, Any]:
    """Return a batch plan for a saved ui_runs/*.agent.json file."""
    try:
        path = _safe_ui_run_path(path_or_name)
    except Exception as exc:
        return {
            "ok": False,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }

    if not path.exists():
        return {
            "ok": False,
            "error_type": "FileNotFoundError",
            "error": f"Could not find UI payload file: {path}",
        }

    payload = _read_json_file(path)
    if payload is None:
        return {
            "ok": False,
            "error_type": "JSONDecodeError",
            "error": f"Could not parse UI payload file: {path}",
        }

    available = len(payload.get("top_literature_results") or [])
    batches = _make_batch_plan(
        total_available=available,
        papers_to_process=papers_to_process,
        batch_size=batch_size,
    )

    return {
        "ok": True,
        "path": str(path),
        "available_paper_count": available,
        "papers_to_process": min(papers_to_process, available),
        "batch_size": batch_size,
        "max_abstract_chars": max_abstract_chars,
        "batch_count": len(batches),
        "batches": batches,
        "usage": (
            "Call evidence_read_agent_payload once per batch using the returned offset "
            "and max_results values. Then synthesize the batch summaries."
        ),
    }


@mcp.tool(name="evidence_list_ui_payloads")
async def evidence_list_ui_payloads_tool(limit: int = 10) -> Dict[str, Any]:
    """List recent UI-generated *.agent.json files."""
    limit = max(1, min(int(limit), 50))

    files = []
    for path in sorted(UI_RUNS_DIR.glob("*.agent.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:limit]:
        files.append(
            {
                "name": path.name,
                "path": str(path),
                "size_bytes": path.stat().st_size,
                "modified_at": path.stat().st_mtime,
            }
        )

    return {
        "ok": True,
        "ui_runs_dir": str(UI_RUNS_DIR),
        "files": files,
    }


# ---------------------------------------------------------------------------
# MCP tools: optional background broker jobs
# ---------------------------------------------------------------------------

@mcp.tool(name="evidence_query_start")
async def evidence_query_start_tool(
    raw_query: str,
    expected_entity_types: Optional[List[str]] = None,
    literature_keywords: Optional[str] = None,
    literature_filters: Optional[Dict[str, Any]] = None,
    include_structured_evidence: bool = False,
    requested_evidence_types: Optional[List[str]] = None,
    deep_search: bool = False,
    timeout_seconds: float = 300.0,
    ctx: Optional[Context] = None,
) -> Dict[str, Any]:
    """Start a long broker query in the background and return a job_id."""
    job_id = str(uuid4())
    filters = dict(literature_filters or {})
    timeout_seconds = max(float(timeout_seconds), 300.0)

    if deep_search:
        filters.setdefault("retmax", 10)
        include_structured_evidence = True
    else:
        # Do not silently cap retmax here; the UI may intentionally request large harvests.
        filters.setdefault("retmax", 10)

    job = {
        "job_id": job_id,
        "status": "queued",
        "created_at": _now(),
        "updated_at": _now(),
        "elapsed_seconds": 0.0,
        "message": "Evidence query job queued.",
        "broker_base_url": BROKER_BASE_URL,
        "request": {
            "raw_query": raw_query,
            "expected_entity_types": expected_entity_types,
            "literature_keywords": literature_keywords,
            "literature_filters": filters,
            "include_structured_evidence": include_structured_evidence,
            "requested_evidence_types": requested_evidence_types,
            "deep_search": deep_search,
            "timeout_seconds": timeout_seconds,
        },
        "debug": {
            "log_path": str(LOG_PATH),
            "job_path": str(_job_path(job_id)),
            "full_result_path": str(_full_result_path(job_id)),
        },
    }
    _write_job(job_id, job)

    logger.info("[%s] JOB QUEUED", job_id)

    asyncio.create_task(
        _run_evidence_job(
            job_id=job_id,
            raw_query=raw_query,
            expected_entity_types=expected_entity_types,
            literature_keywords=literature_keywords,
            literature_filters=filters,
            include_structured_evidence=include_structured_evidence,
            requested_evidence_types=requested_evidence_types,
            deep_search=deep_search,
            timeout_seconds=timeout_seconds,
        )
    )

    if ctx is not None:
        await ctx.info(f"[{job_id}] evidence query started in background")

    return {
        "ok": True,
        "job_id": job_id,
        "status": "queued",
        "message": "Evidence query started. Poll evidence_query_status with this job_id.",
    }


@mcp.tool(name="evidence_query_status")
async def evidence_query_status_tool(job_id: str, include_debug: bool = False) -> Dict[str, Any]:
    """Check the status of a background broker job."""
    job = _read_job(job_id)

    if job is None:
        response: Dict[str, Any] = {
            "ok": False,
            "job_id": job_id,
            "status": "not_found",
            "message": "No such evidence query job was found.",
        }
        if include_debug:
            response["debug"] = {"jobs_dir": str(JOBS_DIR), "log_path": str(LOG_PATH)}
        return response

    if job.get("status") == "running":
        started_at = job.get("started_at")
        if isinstance(started_at, (int, float)):
            job["elapsed_seconds"] = round(_now() - started_at, 3)
            _write_job(job_id, job)

    response = {
        "ok": True,
        "job_id": job_id,
        "status": job.get("status"),
        "elapsed_seconds": job.get("elapsed_seconds"),
        "message": job.get("message"),
        "result_summary": job.get("result_summary"),
        "error_type": job.get("error_type"),
        "error": job.get("error"),
    }

    if include_debug:
        response["debug"] = job.get("debug")

    return response


@mcp.tool(name="evidence_query_summary")
async def evidence_query_summary_tool(
    job_id: str,
    offset: int = 0,
    max_results: int = 10,
    max_abstract_chars: int = 900,
) -> Dict[str, Any]:
    """Return one abstract batch from a completed background broker job."""
    job = _read_job(job_id)

    if job is None:
        return {
            "ok": False,
            "job_id": job_id,
            "status": "not_found",
            "message": "No such evidence query job was found.",
        }

    if job.get("status") != "completed":
        return {
            "ok": False,
            "job_id": job_id,
            "status": job.get("status"),
            "elapsed_seconds": job.get("elapsed_seconds"),
            "message": "Job is not completed yet. Call evidence_query_status again later.",
            "error_type": job.get("error_type"),
            "error": job.get("error"),
        }

    full_path = Path((job.get("debug") or {}).get("full_result_path") or _full_result_path(job_id))
    full_result = _read_json_file(full_path)

    if full_result is None:
        return {
            "ok": False,
            "job_id": job_id,
            "status": "completed",
            "error": f"Could not read full result file: {full_path}",
        }

    abstract_pack = _abstract_pack_from_full_result(
        full_result,
        offset=offset,
        max_results=max_results,
        max_abstract_chars=max_abstract_chars,
    )

    return {
        "ok": True,
        "job_id": job_id,
        "status": "completed",
        "elapsed_seconds": job.get("elapsed_seconds"),
        "result": abstract_pack,
        "debug": {
            "returned_size_bytes": _json_size_bytes(abstract_pack),
            "full_result_path": str(full_path),
        },
    }


@mcp.tool(name="evidence_query_jobs")
async def evidence_query_jobs_tool(limit: int = 10) -> Dict[str, Any]:
    """List recent background broker jobs."""
    limit = max(1, min(int(limit), 50))
    jobs: List[Dict[str, Any]] = []

    for path in sorted(JOBS_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        if path.name.endswith(".full.json") or path.name.endswith(".compact.json"):
            continue

        try:
            job = json.loads(path.read_text(encoding="utf-8"))
            jobs.append(
                {
                    "job_id": job.get("job_id"),
                    "status": job.get("status"),
                    "elapsed_seconds": job.get("elapsed_seconds"),
                    "message": job.get("message"),
                    "result_summary": job.get("result_summary"),
                    "created_at": job.get("created_at"),
                    "updated_at": job.get("updated_at"),
                }
            )
            if len(jobs) >= limit:
                break
        except Exception:
            logger.exception("Failed to load job listing from %s", path)

    return {
        "ok": True,
        "jobs_dir": str(JOBS_DIR),
        "count": len(jobs),
        "jobs": jobs,
    }


if __name__ == "__main__":
    try:
        logger.info("Calling mcp.run()")
        mcp.run()
    except KeyboardInterrupt:
        logger.info("MCP server stopped by KeyboardInterrupt")
        raise
    except Exception:
        logger.exception("MCP server crashed")
        raise
