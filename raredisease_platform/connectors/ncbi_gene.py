"""Connector for NCBI Gene metadata enrichment plus disease-link proposals via MedGen."""

from __future__ import annotations

import os
import re
from typing import Any, Dict, Iterable, List, Optional

import httpx

from ..models import EntityType, NormalizedEntity
from .base import BaseConnector


class NCBIGeneConnector(BaseConnector):
    """NCBI Gene connector for enrichment, not primary gene normalization."""

    name = "ncbi_gene"

    GENE_ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    GENE_ESUMMARY_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"

    MEDGEN_ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    MEDGEN_ESUMMARY_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"

    ENTREZ_ID_RE = re.compile(r"^\d+$")
    CUI_RE = re.compile(r"\bCN?\d{4,}\b", re.IGNORECASE)
    MONDO_RE = re.compile(r"\bMONDO[:_]\d+\b", re.IGNORECASE)
    ORPHA_RE = re.compile(r"\bORPHA[:_ ]?\d+\b", re.IGNORECASE)
    MESH_RE = re.compile(r"\bD\d{6,}\b")
    OMIM_RE = re.compile(r"\bOMIM[:_ ]?\d+\b", re.IGNORECASE)

    def __init__(self) -> None:
        self.ncbi_tool = os.getenv("NCBI_TOOL", "rare-disease-evidence-platform")
        self.ncbi_email = os.getenv("NCBI_EMAIL")
        self.ncbi_api_key = os.getenv("NCBI_API_KEY")

    def _base_params(self) -> Dict[str, str]:
        params = {"tool": self.ncbi_tool}
        if self.ncbi_email:
            params["email"] = self.ncbi_email
        if self.ncbi_api_key:
            params["api_key"] = self.ncbi_api_key
        return params

    @staticmethod
    def _unique_preserve_order(items: Iterable[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for item in items:
            cleaned = str(item).strip()
            if not cleaned:
                continue
            key = cleaned.lower()
            if key not in seen:
                seen.add(key)
                out.append(cleaned)
        return out

    def _collect_strings(self, value: Any) -> List[str]:
        out: List[str] = []
        if isinstance(value, str):
            out.append(value)
        elif isinstance(value, dict):
            for child in value.values():
                out.extend(self._collect_strings(child))
        elif isinstance(value, list):
            for child in value:
                out.extend(self._collect_strings(child))
        return out

    @staticmethod
    def _first_present(data: Dict[str, Any], *keys: str) -> Optional[str]:
        for key in keys:
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    @staticmethod
    def _normalize_mondo_id(value: str) -> str:
        return value.strip().upper().replace("_", ":")

    async def _request_json(
        self,
        url: str,
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        async with httpx.AsyncClient(
            timeout=20.0,
            headers={"User-Agent": "rare-disease-evidence-platform/0.1"},
        ) as client:
            response = await client.get(url, params=params)
        response.raise_for_status()
        return response.json()

    async def _gene_esearch(self, term: str, retmax: int = 5) -> List[str]:
        params = {
            **self._base_params(),
            "db": "gene",
            "term": term,
            "retmode": "json",
            "retmax": int(retmax),
            "sort": "relevance",
        }
        payload = await self._request_json(self.GENE_ESEARCH_URL, params)
        return [str(uid) for uid in (payload.get("esearchresult", {}).get("idlist", []) or [])]

    async def _gene_esummary(self, ids: List[str]) -> List[Dict[str, Any]]:
        if not ids:
            return []

        params = {
            **self._base_params(),
            "db": "gene",
            "id": ",".join(ids),
            "retmode": "json",
        }
        payload = await self._request_json(self.GENE_ESUMMARY_URL, params)

        result = payload.get("result", {}) or {}
        uids = result.get("uids", []) or ids

        docs: List[Dict[str, Any]] = []
        for uid in uids:
            doc = result.get(str(uid))
            if isinstance(doc, dict):
                copied = dict(doc)
                copied["_uid"] = str(uid)
                docs.append(copied)
        return docs

    async def _medgen_esearch(self, term: str, retmax: int = 10) -> List[str]:
        params = {
            **self._base_params(),
            "db": "medgen",
            "term": term,
            "retmode": "json",
            "retmax": int(retmax),
            "sort": "relevance",
        }
        payload = await self._request_json(self.MEDGEN_ESEARCH_URL, params)
        return [str(uid) for uid in (payload.get("esearchresult", {}).get("idlist", []) or [])]

    async def _medgen_esummary(self, ids: List[str]) -> List[Dict[str, Any]]:
        if not ids:
            return []

        params = {
            **self._base_params(),
            "db": "medgen",
            "id": ",".join(ids),
            "retmode": "json",
        }
        payload = await self._request_json(self.MEDGEN_ESUMMARY_URL, params)

        result = payload.get("result", {}) or {}
        uids = result.get("uids", []) or ids

        docs: List[Dict[str, Any]] = []
        for uid in uids:
            doc = result.get(str(uid))
            if isinstance(doc, dict):
                copied = dict(doc)
                copied["_uid"] = str(uid)
                docs.append(copied)
        return docs

    def _parse_synonyms(self, doc: Dict[str, Any]) -> List[str]:
        synonyms: List[str] = []

        aliases = self._first_present(doc, "otheraliases", "OtherAliases")
        if aliases:
            for part in aliases.replace(";", ",").split(","):
                cleaned = part.strip()
                if cleaned:
                    synonyms.append(cleaned)

        designations = self._first_present(doc, "otherdesignations", "OtherDesignations")
        if designations:
            for part in designations.split("|"):
                cleaned = part.strip()
                if cleaned:
                    synonyms.append(cleaned)

        preferred = self._first_present(doc, "name", "description", "nomenclaturesymbol")
        return [s for s in self._unique_preserve_order(synonyms) if s.lower() != str(preferred or "").lower()]

    def _extract_medgen_source_ids(self, doc: Dict[str, Any]) -> Dict[str, str]:
        source_ids: Dict[str, str] = {}

        uid = str(doc.get("_uid") or "").strip()
        if uid:
            source_ids["medgen_uid"] = uid

        for text in self._collect_strings(doc):
            upper = text.upper()

            if "medgen" not in source_ids:
                match = self.CUI_RE.search(upper)
                if match:
                    source_ids["medgen"] = match.group(0)

            if "mondo" not in source_ids:
                match = self.MONDO_RE.search(upper)
                if match:
                    source_ids["mondo"] = self._normalize_mondo_id(match.group(0))

            if "orpha" not in source_ids:
                match = self.ORPHA_RE.search(upper)
                if match:
                    source_ids["orpha"] = (
                        match.group(0)
                        .replace("ORPHA:", "")
                        .replace("ORPHA_", "")
                        .replace("ORPHA ", "")
                        .strip()
                    )

            if "mesh" not in source_ids:
                match = self.MESH_RE.search(upper)
                if match:
                    source_ids["mesh"] = match.group(0)

            if "omim" not in source_ids:
                match = self.OMIM_RE.search(upper)
                if match:
                    source_ids["omim"] = (
                        match.group(0)
                        .replace("OMIM:", "")
                        .replace("OMIM_", "")
                        .replace("OMIM ", "")
                        .strip()
                    )

        return source_ids

    def _medgen_doc_to_disease(self, doc: Dict[str, Any], *, query_text: str, rank: int = 0) -> Dict[str, Any]:
        title = self._first_present(doc, "title", "Title") or "Disease candidate"
        source_ids = self._extract_medgen_source_ids(doc)
        confidence = max(0.60, 0.86 - min(0.18, 0.04 * rank))

        return NormalizedEntity(
            entity_type=EntityType.disease,
            preferred_label=title,
            source_ids=source_ids,
            synonyms=[],
            description=None,
            confidence=confidence,
            provenance={
                "source": "medgen",
                "method": "gene_to_disease_search",
                "query_text": query_text,
                "medgen_uid": doc.get("_uid"),
            },
        ).model_dump()

    async def _fetch_disease_links(self, gene_symbol: str, max_candidates: int = 8) -> List[Dict[str, Any]]:
        # MedGen docs explicitly support queries like LMNB1[gene].
        term = f"{gene_symbol}[gene]"
        ids = await self._medgen_esearch(term, retmax=max_candidates)
        docs = await self._medgen_esummary(ids)

        results: List[Dict[str, Any]] = []
        for rank, doc in enumerate(docs):
            results.append(self._medgen_doc_to_disease(doc, query_text=term, rank=rank))
        return results

    def _doc_to_entity(
        self,
        doc: Dict[str, Any],
        *,
        query_text: str,
        rank: int = 0,
    ) -> NormalizedEntity:
        entrez = str(doc.get("_uid") or "").strip()
        symbol = self._first_present(doc, "name", "nomenclaturesymbol") or query_text.strip()
        description = self._first_present(doc, "description", "summary", "nomenclaturename")

        source_ids: Dict[str, str] = {}
        if entrez:
            source_ids["entrez"] = entrez

        synonyms = self._parse_synonyms(doc)

        confidence = 0.98 if rank == 0 else max(0.75, 0.98 - 0.05 * rank)

        return NormalizedEntity(
            entity_type=EntityType.gene,
            preferred_label=symbol,
            source_ids=source_ids,
            synonyms=synonyms,
            description=description,
            confidence=confidence,
            provenance={
                "source": self.name,
                "method": "ncbi_gene_eutils",
                "query_text": query_text,
                "chromosome": self._first_present(doc, "chromosome"),
                "map_location": self._first_present(doc, "maplocation"),
                "gene_type": self._first_present(doc, "genetype"),
                "summary": self._first_present(doc, "summary"),
                "nomenclature_name": self._first_present(doc, "nomenclaturename"),
                "url": f"https://www.ncbi.nlm.nih.gov/gene/{entrez}" if entrez else None,
            },
        )

    async def fetch_by_id(self, identifier: str) -> Any:
        raw = identifier.strip()
        if not raw:
            return None

        if self.ENTREZ_ID_RE.fullmatch(raw):
            docs = await self._gene_esummary([raw])
            if not docs:
                return None
            doc = docs[0]
            entity = self._doc_to_entity(doc, query_text=raw)
            disease_links = await self._fetch_disease_links(entity.preferred_label, max_candidates=6)
            entity.provenance = dict(entity.provenance or {})
            entity.provenance["disease_links"] = disease_links
            return entity.model_dump()

        # Symbol search fallback, restricted to human by taxonomy.
        ids = await self._gene_esearch(f'{raw} AND 9606[Taxonomy ID]', retmax=3)
        docs = await self._gene_esummary(ids)
        if not docs:
            return None

        entity = self._doc_to_entity(docs[0], query_text=raw)
        disease_links = await self._fetch_disease_links(entity.preferred_label, max_candidates=6)
        entity.provenance = dict(entity.provenance or {})
        entity.provenance["disease_links"] = disease_links
        return entity.model_dump()

    async def normalize(self, text: str) -> List[Dict[str, Any]]:
        raw = text.strip()
        if not raw:
            return []

        record = await self.fetch_by_id(raw)
        return [record] if record else []

    async def crosswalk(self, source_id: str) -> Dict[str, str]:
        record = await self.fetch_by_id(source_id)
        if not record:
            return {}
        return record.get("source_ids", {}) or {}

    async def search(self, query: Dict[str, Any]) -> Any:
        filters = query.get("filters") or {}
        gene_ids = [str(x) for x in (query.get("gene_ids") or []) if str(x).strip()]
        gene_terms = [str(x) for x in (query.get("gene_terms") or []) if str(x).strip()]

        records: List[Dict[str, Any]] = []

        # Prefer Entrez IDs from HGNC-normalized entities.
        if gene_ids:
            for gene_id in gene_ids[: int(filters.get("retmax", 10))]:
                record = await self.fetch_by_id(gene_id)
                if record:
                    records.append(record)
            return records

        # Fall back to symbols/terms.
        limit = int(filters.get("retmax", 10))
        for term in gene_terms[:limit]:
            record = await self.fetch_by_id(term)
            if record:
                records.append(record)

        return records

    async def health_check(self) -> bool:
        try:
            ids = await self._gene_esearch("TP53 AND 9606[Taxonomy ID]", retmax=1)
            return bool(ids)
        except Exception:
            return False

    async def rate_limit_policy(self) -> Dict[str, Any]:
        return {
            "source": self.name,
            "notes": "Uses NCBI Gene E-utilities for metadata enrichment and MedGen for gene->disease candidate links.",
        }
