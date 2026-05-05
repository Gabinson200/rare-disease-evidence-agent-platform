"""Tool functions intended for OpenClaw/MCP-style integration.

The first exposed agent tool should be evidence_query. It wraps the broker's
/evidence/query endpoint and keeps the LLM from directly querying raw biomedical
sources.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from .client import EvidenceBrokerClient


async def evidence_query(
    raw_query: str,
    expected_entity_types: Optional[list[str]] = None,
    literature_keywords: Optional[str] = None,
    literature_filters: Optional[Dict[str, Any]] = None,
    include_structured_evidence: bool = False,
    requested_evidence_types: Optional[list[str]] = None,
    structured_filters: Optional[Dict[str, Any]] = None,
    scoring_profile: Optional[str] = None,
    deep_search: bool = False,
    broker_base_url: str = "http://127.0.0.1:8000",
    timeout_seconds: float = 180.0,
) -> Dict[str, Any]:
    """Run the full broker evidence workflow.

    Default mode is fast and interactive:
    - include_structured_evidence = False
    - retmax <= 3

    Set deep_search=True for a slower, more complete evidence pass.
    """
    filters = dict(literature_filters or {})

    if deep_search:
        include_structured_evidence = True
        filters.setdefault("retmax", 10)
    else:
        include_structured_evidence = False
        filters["retmax"] = min(int(filters.get("retmax", 3)), 3)

    payload = {
        "raw_query": raw_query,
        "expected_entity_types": expected_entity_types,
        "literature_keywords": literature_keywords,
        "literature_filters": filters,
        "include_structured_evidence": include_structured_evidence,
        "requested_evidence_types": requested_evidence_types,
        "structured_filters": structured_filters,
        "scoring_profile": scoring_profile,
    }

    payload = {key: value for key, value in payload.items() if value is not None}

    client = EvidenceBrokerClient(
        base_url=broker_base_url,
        timeout_seconds=timeout_seconds,
    )
    return await client.evidence_query(payload)
