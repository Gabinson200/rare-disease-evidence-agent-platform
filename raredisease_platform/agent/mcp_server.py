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

# In-memory cache for job metadata. Full results are saved as separate files so
# OpenClaw does not receive thousands of lines unless explicitly requested.
JOBS: Dict[str, Dict[str, Any]] = {}


def _now() -> float:
    return time.time()


def _json_size_bytes(obj: Any) -> int:
    try:
        return len(json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8"))
    except Exception:
        return -1


def _safe_job_id(job_id: str) -> str:
    return "".join(ch for ch in str(job_id) if ch.isalnum() or ch in "-_")


def _job_path(job_id: str) -> Path:
    return JOBS_DIR / f"{_safe_job_id(job_id)}.json"


def _full_result_path(job_id: str) -> Path:
    return JOBS_DIR / f"{_safe_job_id(job_id)}.full.json"


def _compact_result_path(job_id: str) -> Path:
    return JOBS_DIR / f"{_safe_job_id(job_id)}.compact.json"


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


def _read_json_file(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("Failed to read JSON file: %s", path)
        return None


def _truncate_text(value: Optional[str], max_chars: int) -> Optional[str]:
    if not value:
        return None

    text = " ".join(str(value).split())

    if len(text) <= max_chars:
        return text

    return text[: max_chars - 3].rstrip() + "..."


def _first_present(mapping: Dict[str, Any], keys: List[str]) -> Optional[Any]:
    for key in keys:
        value = mapping.get(key)
        if value not in (None, "", [], {}):
            return value
    return None


def _brief_source_ids(source_ids: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    source_ids = source_ids or {}

    preferred_keys = [
        "orpha",
        "orphanet",
        "mondo",
        "hgnc",
        "entrez",
        "ensembl",
        "ensembl_gene_id",
        "hpo",
        "mesh",
        "medgen",
        "clinvar",
        "vcv",
        "rcv",
        "dbsnp",
        "pubchem",
        "nct",
    ]

    out: Dict[str, Any] = {}

    for key in preferred_keys:
        if key in source_ids and source_ids[key] not in (None, "", [], {}):
            out[key] = source_ids[key]

    # Cap the number of IDs so a weird connector cannot flood the LLM context.
    return dict(list(out.items())[:8])


def _brief_entity(entity: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "type": entity.get("entity_type"),
        "label": entity.get("preferred_label"),
        "ids": _brief_source_ids(entity.get("source_ids")),
        "confidence": entity.get("confidence"),
        "synonyms": (entity.get("synonyms") or [])[:3],
    }


def _brief_match_features(match_features: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    match_features = match_features or {}

    keep_keys = [
        "exact_disease_id",
        "exact_gene_id",
        "exact_phenotype_id",
        "exact_compound_id",
        "publication_type",
        "title_match_strength",
        "abstract_match_strength",
        "phenotype_overlap_strength",
        "mesh_topic_importance",
        "recency",
        "full_text_available",
        "source_trust_level",
    ]

    return {
        key: match_features.get(key)
        for key in keep_keys
        if match_features.get(key) not in (None, "", [], {})
    }


def _brief_entity_validation(raw_record: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    raw_record = raw_record or {}
    validation = raw_record.get("entity_validation") or {}

    out = {
        "required_groups": validation.get("required_groups"),
        "matched_groups": validation.get("matched_groups"),
        "missing_groups": validation.get("missing_groups"),
        "matched_terms": validation.get("matched_terms"),
    }

    return {
        key: value
        for key, value in out.items()
        if value not in (None, "", [], {})
    }


def _brief_scoring_adjustments(raw_record: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    raw_record = raw_record or {}
    adjustments = raw_record.get("scoring_adjustments") or []

    if not isinstance(adjustments, list):
        return []

    brief: List[Dict[str, Any]] = []

    for item in adjustments[:6]:
        if not isinstance(item, dict):
            continue

        brief.append(
            {
                "reason": item.get("reason"),
                "delta": item.get("delta"),
            }
        )

    return brief


def _brief_literature_result(
    article: Dict[str, Any],
    *,
    include_abstracts: bool,
    max_abstract_chars: int,
) -> Dict[str, Any]:
    provenance = article.get("provenance") or {}
    raw_record = provenance.get("raw_record") or {}

    brief = {
        "pmid": article.get("pmid"),
        "pmcid": article.get("pmcid"),
        "doi": article.get("doi"),
        "title": article.get("title"),
        "year": article.get("year"),
        "journal": article.get("journal"),
        "authors": (article.get("authors") or [])[:4],
        "score": article.get("score"),
        "match_features": _brief_match_features(article.get("match_features")),
        "entity_validation": _brief_entity_validation(raw_record),
        "scoring_adjustments": _brief_scoring_adjustments(raw_record),
        "pubmed_query": raw_record.get("esearch_term"),
    }

    if include_abstracts:
        brief["abstract_excerpt"] = _truncate_text(
            article.get("abstract"),
            max_abstract_chars,
        )

    return {
        key: value
        for key, value in brief.items()
        if value not in (None, "", [], {})
    }


def _trace_summary(result: Dict[str, Any]) -> Dict[str, Any]:
    trace = result.get("trace") or {}
    normalized_bundle = result.get("normalized_bundle") or {}
    normalization_trace = normalized_bundle.get("normalization_trace") or {}

    warnings: List[str] = []

    for item in normalization_trace.get("warnings") or []:
        warnings.append(str(item))

    for item in trace.get("warnings") or []:
        warnings.append(str(item))

    detected_candidates = normalization_trace.get("detected_candidates") or []
    dropped_candidates = normalization_trace.get("dropped_candidates") or []
    connector_calls = normalization_trace.get("connector_calls") or []
    threshold_decisions = normalization_trace.get("threshold_decisions") or []

    connector_summary: List[Dict[str, Any]] = []
    for call in connector_calls[:8]:
        if not isinstance(call, dict):
            continue

        connector_summary.append(
            {
                "connector": call.get("connector"),
                "surface_text": call.get("surface_text"),
                "status": call.get("status"),
                "records_returned": call.get("records_returned"),
                "error": call.get("error"),
            }
        )

    detected_summary: List[Dict[str, Any]] = []
    for candidate in detected_candidates[:8]:
        if not isinstance(candidate, dict):
            continue

        detected_summary.append(
            {
                "surface_text": candidate.get("surface_text"),
                "types": candidate.get("candidate_entity_types"),
                "strategy": candidate.get("strategy"),
            }
        )

    return {
        "warnings": warnings[:8],
        "detected_candidates": detected_summary,
        "connector_calls": connector_summary,
        "counts": {
            "detected_candidates": len(detected_candidates),
            "connector_calls": len(connector_calls),
            "threshold_decisions": len(threshold_decisions),
            "dropped_candidates": len(dropped_candidates),
        },
    }


def _graph_summary(result: Dict[str, Any]) -> Dict[str, Any]:
    graph = result.get("evidence_graph") or {}

    if not isinstance(graph, dict):
        return {}

    nodes = graph.get("nodes") or []
    edges = graph.get("edges") or []
    ranked_summaries = graph.get("ranked_summaries") or []

    return {
        "node_count": len(nodes),
        "edge_count": len(edges),
        "ranked_summaries": ranked_summaries[:3] if isinstance(ranked_summaries, list) else [],
    }


def _structured_evidence_summary(result: Dict[str, Any]) -> Dict[str, Any]:
    structured = result.get("structured_evidence") or {}

    if not isinstance(structured, dict):
        return {}

    counts: Dict[str, int] = {}

    for key, value in structured.items():
        if isinstance(value, list):
            counts[key] = len(value)
        elif value is not None:
            counts[key] = 1

    return counts


def _make_summary_text(
    *,
    entities: List[Dict[str, Any]],
    literature_results: List[Dict[str, Any]],
    trace_summary: Dict[str, Any],
) -> str:
    pieces: List[str] = []

    if entities:
        labels = []
        for entity in entities[:5]:
            label = entity.get("label")
            entity_type = entity.get("type")
            if label and entity_type:
                labels.append(f"{label} ({entity_type})")
            elif label:
                labels.append(str(label))

        if labels:
            pieces.append("Interpreted entities: " + ", ".join(labels) + ".")

    if literature_results:
        top = literature_results[0]
        title = top.get("title") or "Untitled result"
        pmid = top.get("pmid") or "unknown PMID"
        year = top.get("year") or "unknown year"
        journal = top.get("journal") or "unknown journal"
        score = top.get("score")

        if score is not None:
            pieces.append(
                f"Top result: {title} (PMID: {pmid}, {year}, {journal}, score: {score})."
            )
        else:
            pieces.append(
                f"Top result: {title} (PMID: {pmid}, {year}, {journal})."
            )
    else:
        pieces.append("No literature results were returned.")

    warnings = trace_summary.get("warnings") or []
    counts = trace_summary.get("counts") or {}

    if warnings:
        pieces.append("Trace warnings: " + "; ".join(warnings[:3]))
    elif counts.get("dropped_candidates"):
        pieces.append(
            f"Trace note: {counts.get('dropped_candidates')} low-confidence normalization candidate(s) were dropped."
        )
    else:
        pieces.append("No major trace warnings were present in the optimized payload.")

    return " ".join(pieces)


def _llm_optimized_result(
    result: Dict[str, Any],
    *,
    job_id: Optional[str] = None,
    elapsed_seconds: Optional[float] = None,
    max_results: int = 3,
    include_abstracts: bool = True,
    max_abstract_chars: int = 700,
) -> Dict[str, Any]:
    """
    Small payload intended to be fed into OpenClaw / LLM context.

    This preserves enough information for an agentic summary while avoiding the
    full raw broker response, full provenance, full traces, and full graph.
    """
    if not isinstance(result, dict):
        return {
            "ok": False,
            "response_profile": "llm_optimized",
            "error": "Result was not a dictionary.",
            "raw_result_type": type(result).__name__,
        }

    max_results = max(1, min(int(max_results), 10))
    max_abstract_chars = max(0, min(int(max_abstract_chars), 2000))

    normalized_bundle = result.get("normalized_bundle") or {}
    entities_raw = normalized_bundle.get("entities") or []
    alternatives_raw = normalized_bundle.get("alternatives") or []
    articles_raw = result.get("literature_results") or []

    entities = [
        _brief_entity(entity)
        for entity in entities_raw[:8]
        if isinstance(entity, dict)
    ]

    alternatives = [
        _brief_entity(entity)
        for entity in alternatives_raw[:5]
        if isinstance(entity, dict)
    ]

    articles = [
        _brief_literature_result(
            article,
            include_abstracts=include_abstracts,
            max_abstract_chars=max_abstract_chars,
        )
        for article in articles_raw[:max_results]
        if isinstance(article, dict)
    ]

    trace = _trace_summary(result)

    return {
        "ok": True,
        "response_profile": "llm_optimized",
        "job_id": job_id,
        "elapsed_seconds": elapsed_seconds,
        "summary_text": _make_summary_text(
            entities=entities,
            literature_results=articles,
            trace_summary=trace,
        ),
        "interpreted_entities": entities,
        "alternative_entities": alternatives,
        "top_literature_results": articles,
        "result_counts": {
            "interpreted_entities": len(entities_raw),
            "alternative_entities": len(alternatives_raw),
            "literature_results": len(articles_raw),
        },
        "structured_evidence_counts": _structured_evidence_summary(result),
        "evidence_graph_summary": _graph_summary(result),
        "trace_summary": trace,
    }


def _compact_result(
    result: Dict[str, Any],
    *,
    max_literature_results: int = 5,
    include_abstracts: bool = True,
    max_abstract_chars: int = 1000,
) -> Dict[str, Any]:
    """
    Medium-size debug payload.

    This is intentionally larger than the LLM-optimized profile but still avoids
    dumping raw PubMed records and full traces.
    """
    if not isinstance(result, dict):
        return {"raw_result": result}

    max_literature_results = max(1, min(int(max_literature_results), 25))

    normalized_bundle = result.get("normalized_bundle") or {}
    entities_raw = normalized_bundle.get("entities") or []
    alternatives_raw = normalized_bundle.get("alternatives") or []
    articles_raw = result.get("literature_results") or []

    compact = {
        "response_profile": "compact_debug",
        "normalized_bundle": {
            "entities": [
                _brief_entity(entity)
                for entity in entities_raw
                if isinstance(entity, dict)
            ],
            "alternatives": [
                _brief_entity(entity)
                for entity in alternatives_raw[:10]
                if isinstance(entity, dict)
            ],
        },
        "literature_results": [
            _brief_literature_result(
                article,
                include_abstracts=include_abstracts,
                max_abstract_chars=max_abstract_chars,
            )
            for article in articles_raw[:max_literature_results]
            if isinstance(article, dict)
        ],
        "structured_evidence_counts": _structured_evidence_summary(result),
        "evidence_graph_summary": _graph_summary(result),
        "trace_summary": _trace_summary(result),
        "result_counts": {
            "interpreted_entities": len(entities_raw),
            "alternative_entities": len(alternatives_raw),
            "literature_results": len(articles_raw),
        },
    }

    return compact


def _load_full_result_or_fallback(job: Dict[str, Any], job_id: str) -> Dict[str, Any]:
    debug = job.get("debug") or {}

    # New jobs store full result here.
    full_result_path = debug.get("full_result_path")
    if full_result_path:
        loaded = _read_json_file(Path(full_result_path))
        if loaded is not None:
            return loaded

    # Fallback by convention.
    loaded = _read_json_file(_full_result_path(job_id))
    if loaded is not None:
        return loaded

    # Older jobs may have a compact or full-ish result embedded directly.
    result = job.get("result")
    if isinstance(result, dict):
        return result

    return {}


def _build_result_response(
    *,
    job_id: Optional[str],
    elapsed_seconds: Optional[float],
    result: Dict[str, Any],
    response_profile: str,
    max_results: int,
    include_abstracts: bool,
    max_abstract_chars: int,
) -> Dict[str, Any]:
    profile = (response_profile or "summary").strip().lower()

    if profile in {"summary", "llm", "llm_optimized", "optimized"}:
        optimized = _llm_optimized_result(
            result,
            job_id=job_id,
            elapsed_seconds=elapsed_seconds,
            max_results=max_results,
            include_abstracts=include_abstracts,
            max_abstract_chars=max_abstract_chars,
        )

        return {
            "ok": True,
            "response_profile": "llm_optimized",
            "result": optimized,
            "debug": {
                "returned_size_bytes": _json_size_bytes(optimized),
            },
        }

    if profile in {"compact", "debug"}:
        compact = _compact_result(
            result,
            max_literature_results=max_results,
            include_abstracts=include_abstracts,
            max_abstract_chars=max_abstract_chars,
        )

        return {
            "ok": True,
            "response_profile": "compact_debug",
            "result": compact,
            "debug": {
                "returned_size_bytes": _json_size_bytes(compact),
            },
        }

    if profile == "full":
        return {
            "ok": True,
            "response_profile": "full",
            "warning": "Full result can be very large and expensive to send to an LLM. Use only for debugging.",
            "result": result,
            "debug": {
                "returned_size_bytes": _json_size_bytes(result),
            },
        }

    return {
        "ok": False,
        "error": f"Unknown response_profile: {response_profile}",
        "valid_response_profiles": ["summary", "compact", "full"],
    }


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
    response_profile: str = "summary",
    max_results: int = 3,
    include_abstracts: bool = True,
    max_abstract_chars: int = 700,
) -> Dict[str, Any]:
    """
    Run a coroutine with logging, optional MCP progress messages, and a timeout.

    Direct tools now default to an LLM-optimized payload instead of returning the
    full broker JSON.
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
                payload = _build_result_response(
                    job_id=None,
                    elapsed_seconds=round(elapsed, 3),
                    result=result,
                    response_profile=response_profile,
                    max_results=max_results,
                    include_abstracts=include_abstracts,
                    max_abstract_chars=max_abstract_chars,
                )
                returned_size = _json_size_bytes(payload)

                logger.info(
                    "[%s] DONE tool=%s elapsed=%.3fs original_size=%s returned_size=%s profile=%s",
                    call_id,
                    tool_name,
                    elapsed,
                    original_size,
                    returned_size,
                    response_profile,
                )

                await _safe_client_progress(
                    ctx,
                    call_id=call_id,
                    elapsed=elapsed,
                    timeout_seconds=timeout_seconds,
                    message=f"{tool_name} completed",
                )

                payload.update(
                    {
                        "call_id": call_id,
                        "tool_name": tool_name,
                        "elapsed_seconds": round(elapsed, 3),
                        "broker_base_url": BROKER_BASE_URL,
                    }
                )
                payload.setdefault("debug", {})
                payload["debug"].update(
                    {
                        "original_size_bytes": original_size,
                        "returned_size_bytes": returned_size,
                        "log_path": str(LOG_PATH),
                    }
                )

                return payload

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

    Full output is stored on disk for verification.
    The job metadata stores only the LLM-optimized result.
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

        summary_result = _llm_optimized_result(
            result,
            job_id=job_id,
            elapsed_seconds=round(elapsed, 3),
            max_results=3,
            include_abstracts=True,
            max_abstract_chars=700,
        )

        compact_result = _compact_result(
            result,
            max_literature_results=5,
            include_abstracts=True,
            max_abstract_chars=1000,
        )

        full_path = _full_result_path(job_id)
        compact_path = _compact_result_path(job_id)

        # Keep full result for future verification and debugging, but do not
        # return it to OpenClaw by default.
        _write_json_atomic(full_path, result)
        _write_json_atomic(compact_path, compact_result)

        full_size = _json_size_bytes(result)
        compact_size = _json_size_bytes(compact_result)
        summary_size = _json_size_bytes(summary_result)

        job.update(
            {
                "status": "completed",
                "completed_at": _now(),
                "elapsed_seconds": round(elapsed, 3),
                "message": "Broker query completed.",
                "result": summary_result,
                "debug": {
                    "log_path": str(LOG_PATH),
                    "job_path": str(_job_path(job_id)),
                    "full_result_path": str(full_path),
                    "compact_result_path": str(compact_path),
                    "summary_size_bytes": summary_size,
                    "compact_size_bytes": compact_size,
                    "full_size_bytes": full_size,
                },
            }
        )
        _write_job(job_id, job)

        logger.info(
            "[%s] JOB DONE elapsed=%.3fs summary_size=%s compact_size=%s full_size=%s",
            job_id,
            elapsed,
            summary_size,
            compact_size,
            full_size,
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


@mcp.tool(name="evidence_ping")
async def evidence_ping(ctx: Optional[Context] = None) -> Dict[str, Any]:
    """
    Fast no-broker smoke test.
    """
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
    """
    Fast broker connectivity test.
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


@mcp.tool(name="evidence_query_fast")
async def evidence_query_fast_tool(
    raw_query: str,
    expected_entity_types: Optional[List[str]] = None,
    literature_keywords: Optional[str] = None,
    timeout_seconds: float = 300.0,
    response_profile: str = "summary",
    max_results: int = 3,
    include_abstracts: bool = True,
    max_abstract_chars: int = 700,
    ctx: Optional[Context] = None,
) -> Dict[str, Any]:
    """
    Direct evidence query.

    Defaults to a small LLM-optimized result. Use response_profile="full" only
    for debugging because it can be very large.
    """
    call_id = str(uuid4())
    timeout_seconds = max(float(timeout_seconds), 300.0)

    logger.info(
        "[%s] evidence_query_fast args raw_query=%r expected_entity_types=%r "
        "literature_keywords=%r timeout_seconds=%.1f response_profile=%r",
        call_id,
        raw_query,
        expected_entity_types,
        literature_keywords,
        timeout_seconds,
        response_profile,
    )

    return await _run_with_logging(
        tool_name="evidence_query_fast",
        call_id=call_id,
        timeout_seconds=timeout_seconds,
        ctx=ctx,
        heartbeat_seconds=10.0,
        response_profile=response_profile,
        max_results=max_results,
        include_abstracts=include_abstracts,
        max_abstract_chars=max_abstract_chars,
        coro=evidence_query(
            raw_query=raw_query,
            expected_entity_types=expected_entity_types,
            literature_keywords=literature_keywords,
            literature_filters={"retmax": 1},
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
    response_profile: str = "summary",
    max_results: int = 3,
    include_abstracts: bool = True,
    max_abstract_chars: int = 700,
    ctx: Optional[Context] = None,
) -> Dict[str, Any]:
    """
    Direct configurable evidence query.

    Defaults to a small LLM-optimized result. Use response_profile="compact" or
    "full" only when debugging.
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
        "deep_search=%r timeout_seconds=%.1f response_profile=%r",
        call_id,
        raw_query,
        expected_entity_types,
        literature_keywords,
        filters,
        include_structured_evidence,
        requested_evidence_types,
        deep_search,
        timeout_seconds,
        response_profile,
    )

    return await _run_with_logging(
        tool_name="evidence_query",
        call_id=call_id,
        timeout_seconds=timeout_seconds,
        ctx=ctx,
        heartbeat_seconds=10.0,
        response_profile=response_profile,
        max_results=max_results,
        include_abstracts=include_abstracts,
        max_abstract_chars=max_abstract_chars,
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

    This is the recommended OpenClaw path.
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
            "full_result_path": str(_full_result_path(job_id)),
            "compact_result_path": str(_compact_result_path(job_id)),
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

    # Keep this tiny. OpenClaw only needs the job_id.
    return {
        "ok": True,
        "job_id": job_id,
        "status": "queued",
        "message": "Evidence query started. Poll evidence_query_status with this job_id.",
    }


@mcp.tool(name="evidence_query_status")
async def evidence_query_status_tool(
    job_id: str,
    include_debug: bool = False,
) -> Dict[str, Any]:
    """
    Check the status of a background evidence query job.
    """
    job = _read_job(job_id)

    if job is None:
        response = {
            "ok": False,
            "job_id": job_id,
            "status": "not_found",
            "message": "No such evidence query job was found.",
        }

        if include_debug:
            response["debug"] = {
                "jobs_dir": str(JOBS_DIR),
                "log_path": str(LOG_PATH),
            }

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
        "has_result": "result" in job,
        "error_type": job.get("error_type"),
        "error": job.get("error"),
    }

    if include_debug:
        response["debug"] = job.get("debug")

    return response


@mcp.tool(name="evidence_query_result")
async def evidence_query_result_tool(
    job_id: str,
    response_profile: str = "summary",
    max_results: int = 3,
    include_abstracts: bool = True,
    max_abstract_chars: int = 700,
    include_debug: bool = False,
) -> Dict[str, Any]:
    """
    Fetch the result of a completed background evidence query job.

    response_profile:
    - "summary": default LLM-optimized payload
    - "compact": medium debug payload
    - "full": full broker JSON; expensive, use only for debugging
    """
    job = _read_job(job_id)

    if job is None:
        response = {
            "ok": False,
            "job_id": job_id,
            "status": "not_found",
            "message": "No such evidence query job was found.",
        }

        if include_debug:
            response["debug"] = {
                "jobs_dir": str(JOBS_DIR),
                "log_path": str(LOG_PATH),
            }

        return response

    status = job.get("status")

    if status != "completed":
        response = {
            "ok": False,
            "job_id": job_id,
            "status": status,
            "elapsed_seconds": job.get("elapsed_seconds"),
            "message": "Job is not completed yet. Call evidence_query_status again later.",
            "error_type": job.get("error_type"),
            "error": job.get("error"),
        }

        if include_debug:
            response["debug"] = job.get("debug")

        return response

    response_profile_clean = (response_profile or "summary").strip().lower()

    if response_profile_clean in {"summary", "llm", "llm_optimized", "optimized"}:
        existing = job.get("result")

        # New jobs already store the small LLM-optimized payload in job["result"].
        # Regenerate only if caller asks for non-default options.
        if (
            isinstance(existing, dict)
            and existing.get("response_profile") == "llm_optimized"
            and int(max_results) == 3
            and bool(include_abstracts) is True
            and int(max_abstract_chars) == 700
        ):
            payload = existing
        else:
            full_result = _load_full_result_or_fallback(job, job_id)
            payload = _llm_optimized_result(
                full_result,
                job_id=job_id,
                elapsed_seconds=job.get("elapsed_seconds"),
                max_results=max_results,
                include_abstracts=include_abstracts,
                max_abstract_chars=max_abstract_chars,
            )

        response = {
            "ok": True,
            "job_id": job_id,
            "status": "completed",
            "elapsed_seconds": job.get("elapsed_seconds"),
            "response_profile": "summary",
            "result": payload,
        }

        if include_debug:
            response["debug"] = {
                **(job.get("debug") or {}),
                "returned_size_bytes": _json_size_bytes(response),
            }

        return response

    full_result = _load_full_result_or_fallback(job, job_id)

    formatted = _build_result_response(
        job_id=job_id,
        elapsed_seconds=job.get("elapsed_seconds"),
        result=full_result,
        response_profile=response_profile_clean,
        max_results=max_results,
        include_abstracts=include_abstracts,
        max_abstract_chars=max_abstract_chars,
    )

    response = {
        "ok": formatted.get("ok"),
        "job_id": job_id,
        "status": "completed",
        "elapsed_seconds": job.get("elapsed_seconds"),
        "response_profile": formatted.get("response_profile"),
        "result": formatted.get("result"),
    }

    if formatted.get("warning"):
        response["warning"] = formatted.get("warning")

    if formatted.get("error"):
        response["error"] = formatted.get("error")
        response["valid_response_profiles"] = formatted.get("valid_response_profiles")

    if include_debug:
        response["debug"] = {
            **(job.get("debug") or {}),
            **(formatted.get("debug") or {}),
            "returned_size_bytes": _json_size_bytes(response),
        }

    return response


@mcp.tool(name="evidence_query_summary")
async def evidence_query_summary_tool(
    job_id: str,
    max_results: int = 3,
    include_abstracts: bool = True,
    max_abstract_chars: int = 700,
) -> Dict[str, Any]:
    """
    Return only the LLM-optimized result for a completed job.

    This is the cheapest tool to use from OpenClaw after a job completes.
    """
    return await evidence_query_result_tool(
        job_id=job_id,
        response_profile="summary",
        max_results=max_results,
        include_abstracts=include_abstracts,
        max_abstract_chars=max_abstract_chars,
        include_debug=False,
    )


@mcp.tool(name="evidence_query_jobs")
async def evidence_query_jobs_tool(
    limit: int = 10,
    include_debug: bool = False,
) -> Dict[str, Any]:
    """
    List recent evidence query jobs.
    """
    limit = max(1, min(int(limit), 50))

    jobs: List[Dict[str, Any]] = []

    for path in sorted(JOBS_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:limit]:
        # Skip full/compact result files; this tool should list only job metadata files.
        if path.name.endswith(".full.json") or path.name.endswith(".compact.json"):
            continue

        try:
            job = json.loads(path.read_text(encoding="utf-8"))

            item = {
                "job_id": job.get("job_id"),
                "status": job.get("status"),
                "elapsed_seconds": job.get("elapsed_seconds"),
                "message": job.get("message"),
                "created_at": job.get("created_at"),
                "updated_at": job.get("updated_at"),
            }

            if include_debug:
                item["debug"] = job.get("debug")

            jobs.append(item)

            if len(jobs) >= limit:
                break

        except Exception:
            logger.exception("Failed to load job listing from %s", path)

    return {
        "ok": True,
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