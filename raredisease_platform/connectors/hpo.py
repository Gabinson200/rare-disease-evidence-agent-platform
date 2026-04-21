"""Connector for phenotype normalization and phenotype-first disease candidate retrieval.

This connector uses:
- NLM Clinical Tables HPO API for HPO term normalization
- NCBI MedGen (E-utilities) to propose disease candidates from normalized phenotypes
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, Iterable, List, Optional

import httpx

from ..models import EntityType, NormalizedEntity
from .base import BaseConnector


class HPOConnector(BaseConnector):
    """Connector for Human Phenotype Ontology term normalization."""

    name = "hpo"
    HPO_SEARCH_URL = "https://clinicaltables.nlm.nih.gov/api/hpo/v3/search"

    MEDGEN_ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    MEDGEN_ESUMMARY_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"

    HPO_ID_RE = re.compile(r"^HP:\d{7}$", re.IGNORECASE)
    CUI_RE = re.compile(r"\bCN?\d{4,}\b", re.IGNORECASE)
    MONDO_RE = re.compile(r"\bMONDO[:_]\d+\b", re.IGNORECASE)
    ORPHA_RE = re.compile(r"\bORPHA[:_ ]?\d+\b", re.IGNORECASE)
    MESH_RE = re.compile(r"\bD\d{6,}\b")
    OMIM_RE = re.compile(r"\bOMIM[:_ ]?\d+\b", re.IGNORECASE)

    def __init__(self) -> None:
        self.ncbi_tool = os.getenv("NCBI_TOOL", "rare-disease-evidence-platform")
        self.ncbi_email = os.getenv("NCBI_EMAIL")
        self.ncbi_api_key = os.getenv("NCBI_API_KEY")

    # -------------------------------------------------------------------------
    # Generic helpers
    # -------------------------------------------------------------------------

    def _ncbi_base_params(self) -> Dict[str, str]:
        params = {
            "tool": self.ncbi_tool,
        }
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

    @staticmethod
    def _is_hpo_id(value: str) -> bool:
        return bool(HPOConnector.HPO_ID_RE.match(value.strip()))

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

    # -------------------------------------------------------------------------
    # HPO Clinical Tables API
    # -------------------------------------------------------------------------

    async def _search_api(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        async with httpx.AsyncClient(
            timeout=20.0,
            headers={"User-Agent": "rare-disease-evidence-platform/0.1"},
        ) as client:
            response = await client.get(self.HPO_SEARCH_URL, params=params)

        if response.status_code == 404:
            return []

        response.raise_for_status()
        payload = response.json()

        # Expected shape:
        # [
        #   total_count,
        #   [codes...],
        #   {extra_field: [values...]},
        #   [[display_fields...], ...],
        #   [optional code systems]
        # ]
        if not isinstance(payload, list) or len(payload) < 4:
            return []

        codes = payload[1] if isinstance(payload[1], list) else []
        extras = payload[2] if isinstance(payload[2], dict) else {}
        display_rows = payload[3] if isinstance(payload[3], list) else []

        records: List[Dict[str, Any]] = []
        for idx, code in enumerate(codes):
            row: Dict[str, Any] = {"id": code}

            if idx < len(display_rows) and isinstance(display_rows[idx], list):
                display = display_rows[idx]
                if display:
                    row["name"] = display[0]

            for field_name, field_values in extras.items():
                if isinstance(field_values, list) and idx < len(field_values):
                    row[field_name] = field_values[idx]

            records.append(row)

        return records

    async def _search_terms(
        self,
        text: str,
        *,
        count: int = 10,
    ) -> List[Dict[str, Any]]:
        params = {
            "terms": text,
            "count": count,
            "df": "name",
            "ef": "definition,alt_id,synonym,is_a,xref,is_obsolete,replaced_by,consider",
        }
        return await self._search_api(params)

    async def _fetch_exact_by_id(self, hpo_id: str) -> Optional[Dict[str, Any]]:
        params = {
            "terms": hpo_id,
            "q": f'id:"{hpo_id.upper()}"',
            "count": 1,
            "df": "name",
            "ef": "definition,alt_id,synonym,is_a,xref,is_obsolete,replaced_by,consider",
        }
        rows = await self._search_api(params)
        return rows[0] if rows else None

    def _extract_synonyms(self, row: Dict[str, Any]) -> List[str]:
        raw_synonyms = row.get("synonym") or []
        synonyms: List[str] = []

        if isinstance(raw_synonyms, list):
            for item in raw_synonyms:
                if isinstance(item, str):
                    synonyms.append(item)
                elif isinstance(item, dict):
                    term = item.get("term")
                    if isinstance(term, str) and term.strip():
                        synonyms.append(term.strip())
        elif isinstance(raw_synonyms, str):
            synonyms.append(raw_synonyms)

        preferred_label = str(row.get("name") or "").strip().lower()
        return [
            s for s in self._unique_preserve_order(synonyms)
            if s.lower() != preferred_label
        ]

    def _extract_parent_terms(self, row: Dict[str, Any]) -> List[Dict[str, str]]:
        parents = row.get("is_a") or []
        out: List[Dict[str, str]] = []

        if isinstance(parents, list):
            for parent in parents:
                if isinstance(parent, dict):
                    parent_id = parent.get("id")
                    parent_name = parent.get("name")
                    if isinstance(parent_id, str) and isinstance(parent_name, str):
                        out.append({"id": parent_id, "name": parent_name})

        return out

    def _extract_source_ids(self, row: Dict[str, Any]) -> Dict[str, str]:
        source_ids: Dict[str, str] = {}

        hpo_id = row.get("id")
        if isinstance(hpo_id, str) and hpo_id.strip():
            source_ids["hpo"] = hpo_id.strip().upper()

        xrefs = row.get("xref") or []
        if isinstance(xrefs, list):
            for xref in xrefs:
                if not isinstance(xref, dict):
                    continue
                xref_id = xref.get("id")
                if not isinstance(xref_id, str):
                    continue

                xref_id = xref_id.strip()
                upper = xref_id.upper()

                if upper.startswith("MESH:") and "mesh" not in source_ids:
                    source_ids["mesh"] = xref_id.split(":", 1)[1]
                elif upper.startswith("MSH:") and "mesh" not in source_ids:
                    source_ids["mesh"] = xref_id.split(":", 1)[1]
                elif upper.startswith("UMLS:") and "umls" not in source_ids:
                    source_ids["umls"] = xref_id.split(":", 1)[1]
                elif upper.startswith("MEDGEN:") and "medgen" not in source_ids:
                    source_ids["medgen"] = xref_id.split(":", 1)[1]

        return source_ids

    def _description(self, row: Dict[str, Any]) -> Optional[str]:
        definition = row.get("definition")
        if isinstance(definition, str) and definition.strip():
            return definition.strip()
        return None

    def _match_type(self, query: str, row: Dict[str, Any]) -> str:
        q = query.strip().lower()
        name = str(row.get("name") or "").strip().lower()

        if self._is_hpo_id(query) and str(row.get("id") or "").strip().upper() == query.strip().upper():
            return "exact_hpo_id"

        if name == q:
            return "exact_name"

        for synonym in self._extract_synonyms(row):
            if synonym.lower() == q:
                return "exact_synonym"

        alt_ids = row.get("alt_id") or []
        if isinstance(alt_ids, list):
            for alt_id in alt_ids:
                if isinstance(alt_id, str) and alt_id.strip().upper() == query.strip().upper():
                    return "alt_id"

        return "approximate_name"

    def _confidence(self, match_type: str, row: Dict[str, Any], rank: int = 0) -> float:
        base = {
            "exact_hpo_id": 0.995,
            "exact_name": 0.99,
            "exact_synonym": 0.93,
            "alt_id": 0.90,
            "approximate_name": 0.80,
        }.get(match_type, 0.70)

        is_obsolete = bool(row.get("is_obsolete"))
        if is_obsolete:
            base -= 0.20

        if rank > 0:
            base -= min(0.15, 0.03 * rank)

        return max(0.0, min(1.0, base))

    def _row_to_entity(
        self,
        row: Dict[str, Any],
        *,
        query_text: str,
        rank: int = 0,
    ) -> NormalizedEntity:
        match_type = self._match_type(query_text, row)
        synonyms = self._extract_synonyms(row)
        source_ids = self._extract_source_ids(row)
        parents = self._extract_parent_terms(row)

        return NormalizedEntity(
            entity_type=EntityType.phenotype,
            preferred_label=str(row.get("name") or query_text).strip(),
            source_ids=source_ids,
            synonyms=synonyms,
            description=self._description(row),
            confidence=self._confidence(match_type, row, rank=rank),
            provenance={
                "source": self.name,
                "method": "clinicaltables_hpo_api",
                "match_type": match_type,
                "query_text": query_text,
                "parents": parents,
                "is_obsolete": row.get("is_obsolete"),
                "replaced_by": row.get("replaced_by"),
                "consider": row.get("consider"),
                "url": f"{self.HPO_SEARCH_URL}?terms={query_text}",
            },
        )

    # -------------------------------------------------------------------------
    # MedGen phenotype-first disease candidate retrieval
    # -------------------------------------------------------------------------

    async def _medgen_esearch(self, term: str, retmax: int = 10) -> List[str]:
        params = {
            **self._ncbi_base_params(),
            "db": "medgen",
            "term": term,
            "retmode": "json",
            "retmax": int(retmax),
            "sort": "relevance",
        }

        async with httpx.AsyncClient(
            timeout=20.0,
            headers={"User-Agent": "rare-disease-evidence-platform/0.1"},
        ) as client:
            response = await client.get(self.MEDGEN_ESEARCH_URL, params=params)
            response.raise_for_status()
            payload = response.json()

        return [str(uid) for uid in (payload.get("esearchresult", {}).get("idlist", []) or [])]

    async def _medgen_esummary(self, ids: List[str]) -> List[Dict[str, Any]]:
        if not ids:
            return []

        params = {
            **self._ncbi_base_params(),
            "db": "medgen",
            "id": ",".join(ids),
            "retmode": "json",
        }

        async with httpx.AsyncClient(
            timeout=20.0,
            headers={"User-Agent": "rare-disease-evidence-platform/0.1"},
        ) as client:
            response = await client.get(self.MEDGEN_ESUMMARY_URL, params=params)
            response.raise_for_status()
            payload = response.json()

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

    def _extract_medgen_source_ids(self, doc: Dict[str, Any]) -> Dict[str, str]:
        source_ids: Dict[str, str] = {}

        uid = str(doc.get("_uid") or "").strip()
        if uid:
            source_ids["medgen_uid"] = uid

        for text in self._collect_strings(doc):
            text_upper = text.upper()

            if "medgen" not in source_ids:
                match = self.CUI_RE.search(text_upper)
                if match:
                    source_ids["medgen"] = match.group(0)

            if "mondo" not in source_ids:
                match = self.MONDO_RE.search(text_upper)
                if match:
                    source_ids["mondo"] = self._normalize_mondo_id(match.group(0))

            if "orpha" not in source_ids:
                match = self.ORPHA_RE.search(text_upper)
                if match:
                    source_ids["orpha"] = (
                        match.group(0)
                        .replace("ORPHA:", "")
                        .replace("ORPHA_", "")
                        .replace("ORPHA ", "")
                        .strip()
                    )

            if "mesh" not in source_ids:
                match = self.MESH_RE.search(text_upper)
                if match:
                    source_ids["mesh"] = match.group(0)

            if "omim" not in source_ids:
                match = self.OMIM_RE.search(text_upper)
                if match:
                    source_ids["omim"] = (
                        match.group(0)
                        .replace("OMIM:", "")
                        .replace("OMIM_", "")
                        .replace("OMIM ", "")
                        .strip()
                    )

        return source_ids

    def _medgen_doc_to_disease_entity(
        self,
        doc: Dict[str, Any],
        *,
        matched_phenotypes: List[str],
        support_count: int,
        max_support: int,
    ) -> NormalizedEntity:
        title = self._first_present(doc, "title", "Title") or "Disease candidate"
        source_ids = self._extract_medgen_source_ids(doc)

        confidence = 0.72 + 0.06 * max(0, support_count - 1)
        if max_support > 0:
            confidence += 0.06 * (support_count / max_support)
        confidence = min(0.95, confidence)

        return NormalizedEntity(
            entity_type=EntityType.disease,
            preferred_label=title,
            source_ids=source_ids,
            synonyms=[],
            description=None,
            confidence=confidence,
            provenance={
                "source": "medgen",
                "method": "phenotype_first_retrieval",
                "matched_phenotypes": matched_phenotypes,
                "support_count": support_count,
                "medgen_uid": doc.get("_uid"),
                "raw_title": title,
            },
        )

    async def propose_disease_candidates(
        self,
        phenotype_entities: List[NormalizedEntity],
        *,
        max_candidates: int = 10,
        max_terms_per_phenotype: int = 3,
    ) -> List[Dict[str, Any]]:
        """
        Use normalized HPO phenotypes to propose disease candidates through MedGen.

        MedGen supports queries like:
        "short stature[clinical features]"
        """
        if not phenotype_entities:
            return []

        candidate_support: Dict[str, Dict[str, Any]] = {}

        for phenotype in phenotype_entities:
            phenotype_terms = self._unique_preserve_order(
                [phenotype.preferred_label] + list((phenotype.synonyms or [])[: max_terms_per_phenotype - 1])
            )

            for term in phenotype_terms[:max_terms_per_phenotype]:
                medgen_query = f'{term}[clinical features]'

                try:
                    ids = await self._medgen_esearch(medgen_query, retmax=12)
                    docs = await self._medgen_esummary(ids)
                except Exception:
                    continue

                for doc in docs:
                    source_ids = self._extract_medgen_source_ids(doc)
                    key = (
                        source_ids.get("medgen")
                        or source_ids.get("mondo")
                        or source_ids.get("orpha")
                        or str(doc.get("_uid") or "")
                        or (self._first_present(doc, "title", "Title") or "").lower()
                    )

                    if not key:
                        continue

                    entry = candidate_support.setdefault(
                        key,
                        {
                            "doc": doc,
                            "matched_phenotypes": [],
                        },
                    )

                    if phenotype.preferred_label not in entry["matched_phenotypes"]:
                        entry["matched_phenotypes"].append(phenotype.preferred_label)

        if not candidate_support:
            return []

        max_support = max(len(v["matched_phenotypes"]) for v in candidate_support.values())

        entities: List[NormalizedEntity] = []
        for entry in candidate_support.values():
            matched = entry["matched_phenotypes"]
            entity = self._medgen_doc_to_disease_entity(
                entry["doc"],
                matched_phenotypes=matched,
                support_count=len(matched),
                max_support=max_support,
            )
            entities.append(entity)

        entities.sort(key=lambda e: e.confidence, reverse=True)
        return [entity.model_dump() for entity in entities[:max_candidates]]

    # -------------------------------------------------------------------------
    # BaseConnector interface
    # -------------------------------------------------------------------------

    async def fetch_by_id(self, identifier: str) -> Any:
        hpo_id = identifier.strip().upper()
        if not self._is_hpo_id(hpo_id):
            return None

        row = await self._fetch_exact_by_id(hpo_id)
        if not row:
            return None

        return self._row_to_entity(row, query_text=hpo_id).model_dump()

    async def normalize(self, text: str) -> List[Dict[str, Any]]:
        query = text.strip()
        if not query:
            return []

        if self._is_hpo_id(query):
            row = await self._fetch_exact_by_id(query.strip().upper())
            if not row:
                return []
            return [self._row_to_entity(row, query_text=query).model_dump()]

        rows = await self._search_terms(query, count=10)
        if not rows:
            return []

        def sort_key(item: Dict[str, Any]) -> tuple[int, str]:
            match_type = self._match_type(query, item)
            priority = {
                "exact_name": 0,
                "exact_synonym": 1,
                "alt_id": 2,
                "approximate_name": 3,
            }.get(match_type, 4)
            return (priority, str(item.get("name") or "").lower())

        rows = sorted(rows, key=sort_key)

        entities: List[Dict[str, Any]] = []
        for rank, row in enumerate(rows[:5]):
            entities.append(self._row_to_entity(row, query_text=query, rank=rank).model_dump())

        return entities

    async def crosswalk(self, source_id: str) -> Dict[str, str]:
        entity = await self.fetch_by_id(source_id)
        if not entity:
            return {}
        return entity.get("source_ids", {}) or {}

    async def search(self, query: Dict[str, Any]) -> Any:
        text = (
            query.get("text")
            or query.get("query")
            or query.get("identifier")
            or ""
        ).strip()

        if not text:
            return []

        return await self.normalize(text)

    async def health_check(self) -> bool:
        try:
            rows = await self._search_terms("seizure", count=1)
            return len(rows) > 0
        except Exception:
            return False

    async def rate_limit_policy(self) -> Dict[str, Any]:
        return {
            "source": self.name,
            "notes": "Uses the NLM Clinical Tables HPO API for normalization and MedGen E-utilities for phenotype-first disease candidate retrieval.",
        }
