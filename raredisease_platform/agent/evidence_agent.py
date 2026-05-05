"""Constrained evidence retrieval agent.

This is not a general autonomous biomedical search agent. It is a thin
planning + tool-calling + formatting layer around the broker's /evidence/query
endpoint.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from .client import EvidenceBrokerClient
from .formatter import EvidenceResponseFormatter
from .planner import EvidenceQueryPlanner


@dataclass
class EvidenceAgentConfig:
    broker_base_url: str = "http://127.0.0.1:8000"
    timeout_seconds: float = 180.0
    default_retmax: int = 10
    include_structured_evidence: bool = True
    return_raw_payload: bool = False


class EvidenceAgent:
    """First-pass rare disease evidence agent."""

    def __init__(
        self,
        config: Optional[EvidenceAgentConfig] = None,
        planner: Optional[EvidenceQueryPlanner] = None,
        client: Optional[EvidenceBrokerClient] = None,
        formatter: Optional[EvidenceResponseFormatter] = None,
    ) -> None:
        self.config = config or EvidenceAgentConfig()
        self.planner = planner or EvidenceQueryPlanner()
        self.client = client or EvidenceBrokerClient(
            base_url=self.config.broker_base_url,
            timeout_seconds=self.config.timeout_seconds,
        )
        self.formatter = formatter or EvidenceResponseFormatter()

    async def answer(self, user_query: str) -> Dict[str, Any]:
        """Plan, call the broker, and format an answer."""
        request_payload = self.planner.plan(
            user_query,
            include_structured_evidence=self.config.include_structured_evidence,
            retmax=self.config.default_retmax,
        )

        evidence_payload = await self.client.evidence_query(request_payload)
        answer_text = self.formatter.format(evidence_payload)

        result = {
            "user_query": user_query,
            "planned_request": request_payload,
            "answer": answer_text,
        }

        if self.config.return_raw_payload:
            result["evidence_payload"] = evidence_payload

        return result
