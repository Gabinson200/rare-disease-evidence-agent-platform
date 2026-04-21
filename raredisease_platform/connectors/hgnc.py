from __future__ import annotations

"""Connector for HGNC gene normalization.

The HUGO Gene Nomenclature Committee (HGNC) provides authoritative gene
symbols and identifiers via its REST API. This connector uses HGNC as
the normalization authority for human genes and returns internal
``NormalizedEntity`` objects.

Official API docs:
https://www.genenames.org/help/rest/
"""

from typing import Any, Dict, List, Optional
from urllib.parse import quote

import httpx

from ..models import EntityType, NormalizedEntity
from .base import BaseConnector


class HGNCConnector(BaseConnector):
    """Connector for HGNC gene normalization and gene ID crosswalks."""

    name = "hgnc"
    base_url = "https://rest.genenames.org"
    accept_header = {"Accept": "application/json"}

    SEARCH_ONLY_FIELDS = {
        "alias_symbol",
        "alias_name",
        "prev_symbol",
        "prev_name",
        "name",
        "symbol",
    }
    FETCHABLE_FIELDS = {
        "hgnc_id",
        "symbol",
        "entrez_id",
        "ensembl_gene_id",
        "omim_id",
        "status",
        "alias_symbol",
        "alias_name",
        "prev_symbol",
        "prev_name",
        "name",
    }

    async def _get_json(self, path: str) -> Dict[str, Any]:
        async with httpx.AsyncClient(
            timeout=20.0,
            headers=self.accept_header,
        ) as client:
            response = await client.get(f"{self.base_url}{path}")
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _encode(term: str) -> str:
        return quote(term.strip(), safe="")

    @staticmethod
    def _strip_hgnc_prefix(identifier: str) -> str:
        text = identifier.strip()
        if text.upper().startswith("HGNC:"):
            return text.split(":", 1)[1]
        return text

    @staticmethod
    def _as_list(value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(v) for v in value if v not in (None, "")]
        return [str(value)]

    def _pick_best_source_ids(self, doc: Dict[str, Any]) -> Dict[str, str]:
        source_ids: Dict[str, str] = {}

        if doc.get("hgnc_id"):
            source_ids["hgnc"] = str(doc["hgnc_id"])
        if doc.get("entrez_id"):
            source_ids["entrez"] = str(doc["entrez_id"])
        if doc.get("ensembl_gene_id"):
            source_ids["ensembl"] = str(doc["ensembl_gene_id"])

        omim_ids = self._as_list(doc.get("omim_id"))
        if omim_ids:
            source_ids["omim"] = omim_ids[0]

        uniprot_ids = self._as_list(doc.get("uniprot_ids") or doc.get("uniprot_id"))
        if uniprot_ids:
            source_ids["uniprot"] = uniprot_ids[0]

        return source_ids

    def _build_synonyms(self, doc: Dict[str, Any]) -> List[str]:
        values: List[str] = []
        for field in ("alias_symbol", "prev_symbol", "alias_name", "prev_name"):
            values.extend(self._as_list(doc.get(field)))

        preferred_symbol = str(doc.get("symbol") or "").strip().lower()
        preferred_name = str(doc.get("name") or "").strip().lower()

        deduped: List[str] = []
        seen = {preferred_symbol, preferred_name}
        for value in values:
            key = value.strip().lower()
            if key and key not in seen:
                deduped.append(value)
                seen.add(key)
        return deduped

    def _confidence(self, match_type: str, doc: Dict[str, Any]) -> float:
        base = {
            "exact_hgnc_id": 0.995,
            "exact_symbol": 0.99,
            "entrez_id": 0.985,
            "ensembl_gene_id": 0.985,
            "alias_symbol": 0.92,
            "prev_symbol": 0.88,
            "name": 0.84,
            "alias_name": 0.80,
            "prev_name": 0.76,
            "broad_search": 0.60,
        }.get(match_type, 0.50)

        status = str(doc.get("status") or "").lower()
        if "withdrawn" in status:
            base -= 0.20

        return max(0.0, min(1.0, base))

    def _doc_to_entity(
        self,
        doc: Dict[str, Any],
        match_type: str,
        query_text: str,
    ) -> NormalizedEntity:
        hgnc_id = str(doc.get("hgnc_id") or "")
        fetch_url = None
        if hgnc_id:
            fetch_url = f"{self.base_url}/fetch/hgnc_id/{self._strip_hgnc_prefix(hgnc_id)}"

        return NormalizedEntity(
            entity_type=EntityType.gene,
            preferred_label=str(doc.get("symbol") or ""),
            source_ids=self._pick_best_source_ids(doc),
            synonyms=self._build_synonyms(doc),
            description=doc.get("name"),
            confidence=self._confidence(match_type, doc),
            provenance={
                "source": self.name,
                "method": match_type,
                "query_text": query_text,
                "status": doc.get("status"),
                "url": fetch_url,
            },
        )

    async def _fetch_docs(self, field: str, term: str) -> List[Dict[str, Any]]:
        term = term.strip()
        if not term:
            return []

        if field == "hgnc_id":
            term = self._strip_hgnc_prefix(term)

        payload = await self._get_json(f"/fetch/{field}/{self._encode(term)}")
        return payload.get("response", {}).get("docs", [])

    async def _search_docs(self, field: str, term: str) -> List[Dict[str, Any]]:
        term = term.strip()
        if not term:
            return []

        payload = await self._get_json(f"/search/{field}/{self._encode(term)}")
        hits = payload.get("response", {}).get("docs", [])

        hydrated: List[Dict[str, Any]] = []
        for hit in hits:
            hgnc_id = hit.get("hgnc_id")
            if not hgnc_id:
                continue
            docs = await self._fetch_docs("hgnc_id", str(hgnc_id))
            hydrated.extend(docs)

        return hydrated

    async def fetch_by_hgnc_id(self, hgnc_id: str) -> Optional[Dict[str, Any]]:
        docs = await self._fetch_docs("hgnc_id", hgnc_id)
        return docs[0] if docs else None

    async def fetch_by_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        docs = await self._fetch_docs("symbol", symbol.upper())
        return docs[0] if docs else None

    async def fetch_by_alias(self, alias: str) -> List[Dict[str, Any]]:
        docs: List[Dict[str, Any]] = []
        docs.extend(await self._search_docs("alias_symbol", alias))
        docs.extend(await self._search_docs("prev_symbol", alias))
        docs.extend(await self._search_docs("alias_name", alias))
        docs.extend(await self._search_docs("prev_name", alias))
        return docs

    async def fetch_by_name(self, name: str) -> List[Dict[str, Any]]:
        return await self._search_docs("name", name)

    async def fetch_by_id(self, identifier: str) -> Any:
        return await self.fetch_by_hgnc_id(identifier)

    async def crosswalk_ids(
        self,
        identifier: str,
        namespace: str = "hgnc_id",
    ) -> Dict[str, str]:
        if namespace == "hgnc":
            namespace = "hgnc_id"

        if namespace not in self.FETCHABLE_FIELDS and namespace not in self.SEARCH_ONLY_FIELDS:
            raise ValueError(f"Unsupported HGNC namespace: {namespace}")

        if namespace in self.FETCHABLE_FIELDS:
            docs = await self._fetch_docs(namespace, identifier)
        else:
            docs = await self._search_docs(namespace, identifier)

        if not docs:
            return {}

        return self._pick_best_source_ids(docs[0])

    async def crosswalk(self, source_id: str) -> Dict[str, str]:
        if source_id.upper().startswith("HGNC:"):
            return await self.crosswalk_ids(source_id, "hgnc_id")
        if source_id.upper().startswith("ENSG"):
            return await self.crosswalk_ids(source_id, "ensembl_gene_id")
        if source_id.isdigit():
            by_hgnc = await self.crosswalk_ids(source_id, "hgnc_id")
            if by_hgnc:
                return by_hgnc
            return await self.crosswalk_ids(source_id, "entrez_id")
        return await self.crosswalk_ids(source_id, "symbol")

    async def normalize(self, text: str) -> List[Dict[str, Any]]:
        query = text.strip()
        if not query:
            return []

        candidates: List[tuple[str, Dict[str, Any]]] = []
        seen_hgnc_ids: set[str] = set()

        def add_docs(match_type: str, docs: List[Dict[str, Any]]) -> None:
            for doc in docs:
                hgnc_id = str(doc.get("hgnc_id") or "")
                if not hgnc_id or hgnc_id in seen_hgnc_ids:
                    continue
                seen_hgnc_ids.add(hgnc_id)
                candidates.append((match_type, doc))

        if query.upper().startswith("HGNC:") or query.isdigit():
            add_docs("exact_hgnc_id", await self._fetch_docs("hgnc_id", query))

        add_docs("exact_symbol", await self._fetch_docs("symbol", query.upper()))

        if query.upper().startswith("ENSG"):
            add_docs("ensembl_gene_id", await self._fetch_docs("ensembl_gene_id", query))
        elif query.isdigit():
            add_docs("entrez_id", await self._fetch_docs("entrez_id", query))

        if not candidates:
            add_docs("alias_symbol", await self._search_docs("alias_symbol", query))
            add_docs("prev_symbol", await self._search_docs("prev_symbol", query))
            add_docs("name", await self._search_docs("name", query))
            add_docs("alias_name", await self._search_docs("alias_name", query))
            add_docs("prev_name", await self._search_docs("prev_name", query))

        if not candidates:
            payload = await self._get_json(f"/search/{self._encode(query)}")
            for hit in payload.get("response", {}).get("docs", []):
                hgnc_id = hit.get("hgnc_id")
                if not hgnc_id:
                    continue
                docs = await self._fetch_docs("hgnc_id", str(hgnc_id))
                add_docs("broad_search", docs)

        entities = [
            self._doc_to_entity(doc, match_type, query).model_dump()
            for match_type, doc in candidates
        ]
        entities.sort(key=lambda item: item["confidence"], reverse=True)
        return entities

    async def search(self, query: Dict[str, Any]) -> Any:
        text = (query.get("text") or query.get("query") or "").strip()
        field = (query.get("field") or "").strip()

        if not text:
            return []

        if field:
            if field in self.FETCHABLE_FIELDS:
                return await self._fetch_docs(field, text)
            if field in self.SEARCH_ONLY_FIELDS:
                return await self._search_docs(field, text)
            raise ValueError(f"Unsupported HGNC field: {field}")

        return await self.normalize(text)

    async def health_check(self) -> bool:
        try:
            await self._get_json("/info")
            return True
        except Exception:
            return False

    async def rate_limit_policy(self) -> Dict[str, Any]:
        return {
            "source": self.name,
            "requests_per_second": 10,
            "notes": "HGNC requests clients to stay at or below 10 requests per second.",
        }
