"""HTTP client used by the agent layer to call the broker API."""

from __future__ import annotations

from typing import Any, Dict, Optional

import httpx


class EvidenceBrokerClient:
    """Small async client for the broker's agent-facing endpoints."""

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8000",
        timeout_seconds: float = 180.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = httpx.Timeout(
            connect=15.0,
            read=timeout_seconds,
            write=30.0,
            pool=15.0,
        )

    async def evidence_query(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Call POST /evidence/query."""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/evidence/query",
                json=payload,
            )
            response.raise_for_status()
            return response.json()

    async def normalize_entities(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Call POST /normalize."""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/normalize",
                json=payload,
            )
            response.raise_for_status()
            return response.json()

    async def normalize_gene(self, raw_gene: str) -> Dict[str, Any]:
        """Call POST /normalize/gene."""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/normalize/gene",
                json={"raw_gene": raw_gene},
            )
            response.raise_for_status()
            return response.json()

    async def crosswalk_gene_identifier(
        self,
        identifier: str,
        namespace: str = "hgnc_id",
    ) -> Dict[str, str]:
        """Call POST /genes/crosswalk."""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/genes/crosswalk",
                json={
                    "identifier": identifier,
                    "namespace": namespace,
                },
            )
            response.raise_for_status()
            return response.json()
