
"""Tool functions intended for OpenClaw/MCP-style integration."""

from __future__ import annotations

from typing import Any, Dict, Optional

from .client import EvidenceBrokerClient


async def evidence_query(
    raw_query: str,
    expected_entity_types: Optional[list[str]] = None,
    literature_keywords: Optional[str] = None,
    literature_filters: Optional[Dict[str, Any]] = None,
    include_structured_evidence: bool = True,
    requested_evidence_types: Optional[list[str]] = None,
    structured_filters: Optional[Dict[str, Any]] = None,
    scoring_profile: Optional[str] = None,
    broker_base_url: str = "http://127.0.0.1:8000",
) -> Dict[str, Any]:
    """Run the full broker evidence workflow.

    This should be the primary tool exposed to OpenClaw.
    """
    client = EvidenceBrokerClient(base_url=broker_base_url)

    payload = {
        "raw_query": raw_query,
        "expected_entity_types": expected_entity_types,
        "literature_keywords": literature_keywords,
        "literature_filters": literature_filters,
        "include_structured_evidence": include_structured_evidence,
        "requested_evidence_types": requested_evidence_types,
        "structured_filters": structured_filters,
        "scoring_profile": scoring_profile,
    }

    payload = {key: value for key, value in payload.items() if value is not None}

    return await client.evidence_query(payload)
