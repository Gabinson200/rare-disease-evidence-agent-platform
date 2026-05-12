from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

import httpx
from mcp.server.fastmcp import Context, FastMCP

# Path injection must happen AFTER the future import, but BEFORE importing local modules.
# This assumes this file is located at:
# raredisease_platform/agent/mcp_server.py
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from raredisease_platform.agent.openclaw_tools import evidence_query  # noqa: E402


BROKER_BASE_URL = "http://127.0.0.1:8000"
LOG_PATH = REPO_ROOT / "mcp_runtime.log"
JOBS_DIR = REPO_ROOT / "mcp_jobs"


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
    logger.info("BROKER_BASE_URL=%s", BROKER_BASE_URL)
    logger.info("python=%s", sys.executable)
    logger.info("argv=%s", sys.argv)
    logger.info("============================================================")

    return logger


logger = configure_logging()
JOBS_DIR.mkdir(exist_ok=True)

mcp = FastMCP("rare-disease-evidence")

# In-memory cache for job state. Job files are also written to disk so completed
# results can be inspected after the tool call returns.
JOBS: Dict[str, Dict[str, Any]] = {}


def _now() -> float:
    return time.time()


def _json_size_bytes(obj: Any) -> int:
    try:
        return len(json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8"))
    except Exception:
        return -1


def _job_path(job_id: str) -> Path:
    safe_job_id = "".join(ch for ch in job_id if ch.isalnum() or ch in "-_")
    return JOBS_DIR / f"{safe_job_id}.json"


def _write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    tmp_path.replace(path)


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


def _compact_result(result: Dict[str, Any], max_literature_results: int = 3) -> Dict[str, Any]:
    """
    Return a smaller payload for OpenClaw and other LLM runtimes.

    Inspector can display huge JSON, but agent runtimes can become slow or noisy
    when a tool returns deeply nested provenance / raw records. This keeps the
    useful fields and trims large internals.
    """
    if not isinstance(result, dict):
        return {"raw_result": result}

    compact = dict(result)

    literature_results = compact.get("literature_results")
    if isinstance(literature_results, list):
        compact["literature_results"] = literature_results[:max_literature_results]

    for article in compact.get("literature_results", []) or []:
        if not isinstance(article, dict):
            continue

        provenance = article.get("provenance")
        if isinstance(provenance, dict):
            raw_record = provenance.get("raw_record")
            if isinstance(raw_record, dict):
                provenance["raw_record"] = {
                    "esearch_term": raw_record.get("esearch_term"),
                    "entity_validation": raw_record.get("entity_validation"),
                    "scoring_adjustments": raw_record.get("scoring_adjustments"),
                }

    graph = compact.get("evidence_graph")
    if isinstance(graph, dict):
        if isinstance(graph.get("nodes"), list):
            graph["nodes"] = graph["nodes"][:10]
        if isinstance(graph.get("edges"), list):
            graph["edges"] = graph["edges"][:10]
        if isinstance(graph.get("ranked_summaries"), list):
            graph["ranked_summaries"] = graph["ranked_summaries"][:5]

    return compact


async def _safe_client_progress(
    ctx: Optional[Context],
    *,
    call_id: str,
    elapsed: float,
    timeout_seconds: float,
    message: str,
) -> None:
    if ctx is None:
        return

    try:
        progress = min(elapsed, timeout_seconds)
        await ctx.info(f"[{call_id}] {message} elapsed={elapsed:.1f}s")
        await ctx.report_progress(
            progress=progress,
            total=timeout_seconds,
            message=message,
        )
    except Exception as exc:
        logger.debug("[%s] Progress notification failed: %s", call_id, exc)


async def _run_with_logging(
    *,
    tool_name: str,
    call_id: str,
    coro,
    timeout_seconds: float,
    ctx: Optional[Context] = None,
    heartbeat_seconds: float = 10.0,
) -> Dict[str, Any]:
    """
    Run a coroutine with logging, optional MCP progress messages, and a timeout.

    This is useful for Inspector and clients that can tolerate long-running calls.
    OpenClaw may still have client-side watchdog behavior, so use the job tools
    for the most reliable OpenClaw flow.
    """
    start = time.perf_counter()
    timeout_seconds = float(timeout_seconds)
    logger.info("[%s] START tool=%s timeout=%.1fs", call_id, tool_name, timeout_seconds)

    task = asyncio.create_task(coro)

    await _safe_client_progress(
        ctx,
        call_id=call_id,
        elapsed=0.0,
        timeout_seconds=timeout_seconds,
        message=f"{tool_name} started",
    )

    try:
        while True:
            elapsed = time.perf_counter() - start
            remaining = timeout_seconds - elapsed

            if remaining <= 0:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

                logger.error(
                    "[%s] TIMEOUT tool=%s elapsed=%.3fs timeout=%.1fs",
                    call_id,
                    tool_name,
                    elapsed,
                    timeout_seconds,
                )

                return {
                    "ok": False,
                    "call_id": call_id,
                    "tool_name": tool_name,
                    "elapsed_seconds": round(elapsed, 3),
                    "error_type": "TimeoutError",
                    "error": f"MCP tool timed out after {timeout_seconds:.1f} seconds",
                    "broker_base_url": BROKER_BASE_URL,
                    "debug": {
                        "log_path": str(LOG_PATH),
                    },
                }

            done, _pending = await asyncio.wait(
                {task},
                timeout=min(heartbeat_seconds, remaining),
                return_when=asyncio.FIRST_COMPLETED,
            )

            if task in done:
                result = await task
                elapsed = time.perf_counter() - start

                original_size = _json_size_bytes(result)
                compact = _compact_result(result)
                compact_size = _json_size_bytes(compact)

                logger.info(
                    "[%s] DONE tool=%s elapsed=%.3fs original_size=%s compact_size=%s keys=%s",
                    call_id,
                    tool_name,
                    elapsed,
                    original_size,
                    compact_size,
                    list(result.keys()) if isinstance(result, dict) else type(result).__name__,
                )

                await _safe_client_progress(
                    ctx,
                    call_id=call_id,
                    elapsed=elapsed,
                    timeout_seconds=timeout_seconds,
                    message=f"{tool_name} completed",
                )

                return {
                    "ok": True,
                    "call_id": call_id,
                    "tool_name": tool_name,
                    "elapsed_seconds": round(elapsed, 3),
                    "broker_base_url": BROKER_BASE_URL,
                    "result": compact,
                    "debug": {
                        "original_size_bytes": original_size,
                        "compact_size_bytes": compact_size,
                        "log_path": str(LOG_PATH),
                    },
                }

            elapsed = time.perf_counter() - start
            logger.info(
                "[%s] HEARTBEAT tool=%s elapsed=%.1fs timeout=%.1fs",
                call_id,
                tool_name,
                elapsed,
                timeout_seconds,
            )

            await _safe_client_progress(
                ctx,
                call_id=call_id,
                elapsed=elapsed,
                timeout_seconds=timeout_seconds,
                message=f"{tool_name} still waiting for broker response",
            )

    except Exception as exc:
        elapsed = time.perf_counter() - start
        tb = traceback.format_exc()

        logger.error(
            "[%s] ERROR tool=%s elapsed=%.3fs error=%s: %s\n%s",
            call_id,
            tool_name,
            elapsed,
            type(exc).__name__,
            exc,
            tb,
        )

        try:
            if ctx is not None:
                await ctx.error(f"[{call_id}] {tool_name} failed: {type(exc).__name__}: {exc}")
        except Exception:
            pass

        return {
            "ok": False,
            "call_id": call_id,
            "tool_name": tool_name,
            "elapsed_seconds": round(elapsed, 3),
            "error_type": type(exc).__name__,
            "error": str(exc),
            "traceback": tb[-4000:],
            "broker_base_url": BROKER_BASE_URL,
            "debug": {
                "log_path": str(LOG_PATH),
            },
        }


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
    """
    Run a broker query in the MCP server background loop and store the result.

    This avoids keeping a single MCP request open for 60+ seconds.
    """
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
        compact = _compact_result(result)

        job.update(
            {
                "status": "completed",
                "completed_at": _now(),
                "elapsed_seconds": round(elapsed, 3),
                "message": "Broker query completed.",
                "result": compact,
                "debug": {
                    "log_path": str(LOG_PATH),
                    "job_path": str(_job_path(job_id)),
                    "result_size_bytes": _json_size_bytes(compact),
                },
            }
        )
        _write_job(job_id, job)

        logger.info("[%s] JOB DONE elapsed=%.3fs", job_id, elapsed)

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


@mcp.tool(name="evidence_ping")
async def evidence_ping(ctx: Optional[Context] = None) -> Dict[str, Any]:
    """
    Fast no-broker MCP smoke test.

    Use this to prove OpenClaw can call this MCP server at all.
    """
    call_id = str(uuid4())
    logger.info("[%s] evidence_ping called", call_id)

    if ctx is not None:
        await ctx.info(f"[{call_id}] evidence_ping reached MCP server")

    return {
        "ok": True,
        "call_id": call_id,
        "message": "rare-disease-evidence MCP server is reachable",
        "repo_root": str(REPO_ROOT),
        "log_path": str(LOG_PATH),
        "python": sys.executable,
    }


@mcp.tool(name="evidence_broker_health")
async def evidence_broker_health(ctx: Optional[Context] = None) -> Dict[str, Any]:
    """
    Fast broker connectivity test.

    Use this to prove the MCP server can reach the FastAPI broker.
    """
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
            "call_id": call_id,
            "status_code": response.status_code,
            "elapsed_seconds": round(elapsed, 3),
            "broker_base_url": BROKER_BASE_URL,
            "body": response.text[:1000],
            "log_path": str(LOG_PATH),
        }

    except Exception as exc:
        elapsed = time.perf_counter() - start
        logger.exception("[%s] evidence_broker_health ERROR elapsed=%.3fs", call_id, elapsed)

        return {
            "ok": False,
            "call_id": call_id,
            "elapsed_seconds": round(elapsed, 3),
            "error_type": type(exc).__name__,
            "error": str(exc),
            "broker_base_url": BROKER_BASE_URL,
            "log_path": str(LOG_PATH),
        }


@mcp.tool(name="evidence_query_fast")
async def evidence_query_fast_tool(
    raw_query: str,
    expected_entity_types: Optional[List[str]] = None,
    literature_keywords: Optional[str] = None,
    timeout_seconds: float = 300.0,
    ctx: Optional[Context] = None,
) -> Dict[str, Any]:
    """
    Direct fast-ish evidence query.

    This is useful in MCP Inspector after raising Inspector timeouts.
    For OpenClaw, prefer evidence_query_start / evidence_query_status /
    evidence_query_result.
    """
    call_id = str(uuid4())
    timeout_seconds = max(float(timeout_seconds), 300.0)

    logger.info(
        "[%s] evidence_query_fast args raw_query=%r expected_entity_types=%r "
        "literature_keywords=%r timeout_seconds=%.1f",
        call_id,
        raw_query,
        expected_entity_types,
        literature_keywords,
        timeout_seconds,
    )

    return await _run_with_logging(
        tool_name="evidence_query_fast",
        call_id=call_id,
        timeout_seconds=timeout_seconds,
        ctx=ctx,
        heartbeat_seconds=10.0,
        coro=evidence_query(
            raw_query=raw_query,
            expected_entity_types=expected_entity_types,
            literature_keywords=literature_keywords,
            literature_filters={
                "retmax": 1,
            },
            include_structured_evidence=False,
            requested_evidence_types=["genes", "diseases", "variants"],
            deep_search=False,
            broker_base_url=BROKER_BASE_URL,
            timeout_seconds=timeout_seconds,
        ),
    )


@mcp.tool(name="evidence_query")
async def evidence_query_tool(
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
    """
    Direct evidence query with progress heartbeats.

    This can work in MCP Inspector when client timeouts are raised. For OpenClaw,
    prefer the job-based tools because no single MCP request stays open.
    """
    call_id = str(uuid4())
    filters = dict(literature_filters or {})
    timeout_seconds = max(float(timeout_seconds), 300.0)

    if deep_search:
        filters.setdefault("retmax", 5)
        include_structured_evidence = True
    else:
        filters["retmax"] = min(int(filters.get("retmax", 3)), 3)
        include_structured_evidence = False

    logger.info(
        "[%s] evidence_query args raw_query=%r expected_entity_types=%r literature_keywords=%r "
        "literature_filters=%r include_structured_evidence=%r requested_evidence_types=%r "
        "deep_search=%r timeout_seconds=%.1f",
        call_id,
        raw_query,
        expected_entity_types,
        literature_keywords,
        filters,
        include_structured_evidence,
        requested_evidence_types,
        deep_search,
        timeout_seconds,
    )

    return await _run_with_logging(
        tool_name="evidence_query",
        call_id=call_id,
        timeout_seconds=timeout_seconds,
        ctx=ctx,
        heartbeat_seconds=10.0,
        coro=evidence_query(
            raw_query=raw_query,
            expected_entity_types=expected_entity_types,
            literature_keywords=literature_keywords,
            literature_filters=filters,
            include_structured_evidence=include_structured_evidence,
            requested_evidence_types=requested_evidence_types,
            deep_search=deep_search,
            broker_base_url=BROKER_BASE_URL,
            timeout_seconds=timeout_seconds,
        ),
    )


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
    """
    Start a long evidence query in the background and return immediately.

    This is the recommended OpenClaw path because it avoids keeping one MCP
    request open for 60+ seconds.
    """
    job_id = str(uuid4())
    filters = dict(literature_filters or {})
    timeout_seconds = max(float(timeout_seconds), 300.0)

    if deep_search:
        filters.setdefault("retmax", 5)
        include_structured_evidence = True
    else:
        filters["retmax"] = min(int(filters.get("retmax", 3)), 3)
        include_structured_evidence = False

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
        "debug": {
            "log_path": str(LOG_PATH),
            "job_path": str(_job_path(job_id)),
        },
    }


@mcp.tool(name="evidence_query_status")
async def evidence_query_status_tool(job_id: str) -> Dict[str, Any]:
    """
    Check the status of a background evidence query job.

    This intentionally does not return the full result. Use evidence_query_result
    after status becomes completed.
    """
    job = _read_job(job_id)

    if job is None:
        return {
            "ok": False,
            "job_id": job_id,
            "status": "not_found",
            "message": "No such evidence query job was found.",
            "debug": {
                "jobs_dir": str(JOBS_DIR),
                "log_path": str(LOG_PATH),
            },
        }

    if job.get("status") == "running":
        started_at = job.get("started_at")
        if isinstance(started_at, (int, float)):
            job["elapsed_seconds"] = round(_now() - started_at, 3)
            _write_job(job_id, job)

    return {
        "ok": True,
        "job_id": job_id,
        "status": job.get("status"),
        "elapsed_seconds": job.get("elapsed_seconds"),
        "message": job.get("message"),
        "has_result": "result" in job,
        "error_type": job.get("error_type"),
        "error": job.get("error"),
        "debug": job.get("debug"),
    }


@mcp.tool(name="evidence_query_result")
async def evidence_query_result_tool(job_id: str) -> Dict[str, Any]:
    """
    Fetch the result of a completed background evidence query job.
    """
    job = _read_job(job_id)

    if job is None:
        return {
            "ok": False,
            "job_id": job_id,
            "status": "not_found",
            "message": "No such evidence query job was found.",
            "debug": {
                "jobs_dir": str(JOBS_DIR),
                "log_path": str(LOG_PATH),
            },
        }

    status = job.get("status")

    if status != "completed":
        return {
            "ok": False,
            "job_id": job_id,
            "status": status,
            "elapsed_seconds": job.get("elapsed_seconds"),
            "message": "Job is not completed yet. Call evidence_query_status again later.",
            "error_type": job.get("error_type"),
            "error": job.get("error"),
            "debug": job.get("debug"),
        }

    return {
        "ok": True,
        "job_id": job_id,
        "status": status,
        "elapsed_seconds": job.get("elapsed_seconds"),
        "result": job.get("result"),
        "debug": job.get("debug"),
    }


@mcp.tool(name="evidence_query_jobs")
async def evidence_query_jobs_tool(limit: int = 10) -> Dict[str, Any]:
    """
    List recent evidence query jobs.
    """
    limit = max(1, min(int(limit), 50))

    jobs: List[Dict[str, Any]] = []

    for path in sorted(JOBS_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:limit]:
        try:
            job = json.loads(path.read_text(encoding="utf-8"))
            jobs.append(
                {
                    "job_id": job.get("job_id"),
                    "status": job.get("status"),
                    "elapsed_seconds": job.get("elapsed_seconds"),
                    "message": job.get("message"),
                    "created_at": job.get("created_at"),
                    "updated_at": job.get("updated_at"),
                    "debug": job.get("debug"),
                }
            )
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