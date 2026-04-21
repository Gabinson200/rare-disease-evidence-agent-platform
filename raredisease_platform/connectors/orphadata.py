"""Connector for rare disease normalization plus MONDO / MeSH / MedGen enrichment.

This implementation uses the public ORPHAcodes API for disease normalization,
then enriches the resolved disease entity with best-effort crosswalks to:

- MONDO
- MeSH
- MedGen

Notes:
- ORPHAcodes API is used as the public Orphanet-facing normalization layer.
- MeSH uses the NLM RDF Lookup API for exact descriptor matching.
- MedGen uses NCBI E-utilities (esearch + esummary).
- MONDO uses EBI OLS as a best-effort lookup/enrichment layer.
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote

import httpx

from ..models import EntityType, NormalizedEntity
from .base import BaseConnector


class OrphadataConnector(BaseConnector):
    """Disease normalization connector backed by ORPHAcodes plus crosswalk enrichment."""

    name = "orphadata"

    ORPHA_BASE_URL = "https://api.orphacode.org"
    DEFAULT_LANG = "EN"

    MEDGEN_ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    MEDGEN_ESUMMARY_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"

    MESH_LOOKUP_URL = "https://id.nlm.nih.gov/mesh/lookup/descriptor"

    OLS_SEARCH_URL = "https://www.ebi.ac.uk/ols4/api/search"

    CUI_RE = re.compile(r"\bCN?\d{4,}\b", re.IGNORECASE)
    MONDO_RE = re.compile(r"\bMONDO[:_]\d+\b", re.IGNORECASE)
    ORPHA_RE = re.compile(r"\bORPHA[:_ ]?\d+\b", re.IGNORECASE)
    MESH_RE = re.compile(r"\bD\d{6,}\b")

    def __init__(self) -> None:
        self.api_key = os.getenv("ORPHACODE_API_KEY", "token")
        self.default_lang = os.getenv("ORPHACODE_LANG", self.DEFAULT_LANG).upper()

        self.ncbi_tool = os.getenv("NCBI_TOOL", "rare-disease-evidence-platform")
        self.ncbi_email = os.getenv("NCBI_EMAIL")
        self.ncbi_api_key = os.getenv("NCBI_API_KEY")

    # -------------------------------------------------------------------------
    # Generic helpers
    # -------------------------------------------------------------------------

    def _orpha_headers(self) -> Dict[str, str]:
        return {
            "Accept": "application/json",
            "User-Agent": "rare-disease-evidence-platform/0.1",
            "apiKey": self.api_key,
        }

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
    def _clean_orpha_code(value: str) -> str:
        text = str(value).strip()
        text = text.replace("ORPHA:", "").replace("Orphanet:", "").strip()
        return text

    @staticmethod
    def _encode(value: str) -> str:
        return quote(value.strip(), safe="")

    @staticmethod
    def _as_list(value: Any) -> List[Any]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]

    @staticmethod
    def _coerce_strings(value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        if isinstance(value, list):
            return [str(v) for v in value if v is not None]
        return [str(value)]

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
    def _quote_term(term: str) -> str:
        escaped = term.replace("\\", "\\\\").replace('"', '\\"').strip()
        return f'"{escaped}"'

    def _normalize_mondo_id(self, value: str) -> str:
        text = value.strip().upper().replace("_", ":")
        return text

    # -------------------------------------------------------------------------
    # ORPHAcodes API
    # -------------------------------------------------------------------------

    async def _orpha_get_json(self, path: str) -> Optional[Any]:
        async with httpx.AsyncClient(timeout=20.0, headers=self._orpha_headers()) as client:
            response = await client.get(f"{self.ORPHA_BASE_URL}{path}")

        if response.status_code == 404:
            return None

        response.raise_for_status()
        return response.json()

    async def _get_property(
        self,
        orpha_code: str,
        field_name: str,
        *,
        lang: Optional[str] = None,
    ) -> Optional[Any]:
        use_lang = (lang or self.default_lang).upper()
        path = f"/{use_lang}/ClinicalEntity/orphacode/{orpha_code}/{field_name}"
        return await self._orpha_get_json(path)

    async def _approximate_name_search(
        self,
        query: str,
        *,
        lang: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        use_lang = (lang or self.default_lang).upper()
        path = f"/{use_lang}/ClinicalEntity/ApproximateName/{self._encode(query)}"
        payload = await self._orpha_get_json(path)
        if payload is None:
            return []

        if isinstance(payload, list):
            candidates = payload
        elif isinstance(payload, dict):
            if isinstance(payload.get("entities"), list):
                candidates = payload["entities"]
            elif isinstance(payload.get("results"), list):
                candidates = payload["results"]
            else:
                candidates = [payload]
        else:
            candidates = []

        return [c for c in candidates if isinstance(c, dict)]

    def _extract_orpha_code_from_candidate(self, candidate: Dict[str, Any]) -> Optional[str]:
        for key in ("ORPHAcode", "orpha_code", "orphaCode", "Orphacode", "code", "id"):
            value = candidate.get(key)
            if value is not None:
                cleaned = self._clean_orpha_code(str(value))
                if cleaned:
                    return cleaned
        return None

    def _extract_name_from_payload(self, payload: Any) -> Optional[str]:
        if payload is None:
            return None
        if isinstance(payload, str):
            return payload.strip() or None
        if isinstance(payload, dict):
            for key in ("Preferred term", "PreferredTerm", "Name", "name", "label"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        return None

    def _extract_definition_from_payload(self, payload: Any) -> Optional[str]:
        if payload is None:
            return None
        if isinstance(payload, str):
            return payload.strip() or None
        if isinstance(payload, dict):
            for key in ("Definition", "definition", "Text", "text"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        return None

    def _extract_synonyms_from_payload(self, payload: Any) -> List[str]:
        synonyms: List[str] = []

        if payload is None:
            return []

        if isinstance(payload, str):
            synonyms.append(payload)
        elif isinstance(payload, list):
            for item in payload:
                if isinstance(item, str):
                    synonyms.append(item)
                elif isinstance(item, dict):
                    for key in ("Synonym", "synonym", "label", "value", "Name"):
                        value = item.get(key)
                        if isinstance(value, str) and value.strip():
                            synonyms.append(value.strip())
        elif isinstance(payload, dict):
            for key in ("Synonym", "synonym", "Synonyms", "synonyms"):
                value = payload.get(key)
                if isinstance(value, str):
                    synonyms.append(value)
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, str):
                            synonyms.append(item)
                        elif isinstance(item, dict):
                            for subkey in ("label", "value", "Name"):
                                subval = item.get(subkey)
                                if isinstance(subval, str) and subval.strip():
                                    synonyms.append(subval.strip())

        return self._unique_preserve_order(synonyms)

    def _extract_simple_field(self, payload: Any, *keys: str) -> Optional[str]:
        if payload is None:
            return None
        if isinstance(payload, str):
            return payload.strip() or None
        if isinstance(payload, dict):
            for key in keys:
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        return None

    def _extract_list_of_strings(self, payload: Any, *keys: str) -> List[str]:
        out: List[str] = []
        if payload is None:
            return out

        if isinstance(payload, str):
            out.append(payload)
        elif isinstance(payload, list):
            for item in payload:
                if isinstance(item, str):
                    out.append(item)
                elif isinstance(item, dict):
                    for key in keys:
                        value = item.get(key)
                        if isinstance(value, str) and value.strip():
                            out.append(value.strip())
        elif isinstance(payload, dict):
            for key in keys:
                value = payload.get(key)
                if isinstance(value, str):
                    out.append(value)
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, str):
                            out.append(item)
                        elif isinstance(item, dict):
                            for subkey in ("label", "value", "code", "id"):
                                subval = item.get(subkey)
                                if isinstance(subval, str) and subval.strip():
                                    out.append(subval.strip())

        return self._unique_preserve_order(out)

    # -------------------------------------------------------------------------
    # MedGen enrichment
    # -------------------------------------------------------------------------

    async def _medgen_esearch(self, term: str, retmax: int = 5) -> List[str]:
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

    def _extract_medgen_cui(self, doc: Dict[str, Any]) -> Optional[str]:
        for key in ("conceptid", "concept_id", "cui", "ConceptId"):
            value = doc.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

        for text in self._collect_strings(doc):
            match = self.CUI_RE.search(text)
            if match:
                return match.group(0).upper()

        return None

    def _extract_mondo_ids_from_doc(self, doc: Dict[str, Any]) -> List[str]:
        mondo_ids: List[str] = []
        for text in self._collect_strings(doc):
            for match in self.MONDO_RE.findall(text):
                mondo_ids.append(self._normalize_mondo_id(match))
        return self._unique_preserve_order(mondo_ids)

    def _score_medgen_doc(
        self,
        doc: Dict[str, Any],
        *,
        preferred_label: str,
        synonyms: List[str],
        orpha_code: Optional[str],
    ) -> int:
        score = 0
        title = self._first_present(doc, "title", "Title") or ""
        title_lower = title.lower().strip()

        if title_lower == preferred_label.lower().strip():
            score += 10

        for syn in synonyms[:5]:
            if title_lower == syn.lower().strip():
                score += 6

        strings = " | ".join(self._collect_strings(doc)).lower()

        if preferred_label.lower() in strings:
            score += 4

        for syn in synonyms[:5]:
            if syn.lower() in strings:
                score += 2

        if orpha_code:
            if f"orpha:{orpha_code}".lower() in strings or f"orpha {orpha_code}".lower() in strings:
                score += 8
            elif orpha_code in strings:
                score += 2

        return score

    async def _find_medgen_crosswalk(
        self,
        *,
        preferred_label: str,
        synonyms: List[str],
        orpha_code: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        search_plans: List[str] = []

        if orpha_code:
            search_plans.append(f'"ORPHA:{orpha_code}"[Source ID]')
            search_plans.append(f'"{orpha_code}"[Source ID]')

        search_plans.append(f'"{preferred_label}"[title]')
        for syn in synonyms[:4]:
            search_plans.append(f'"{syn}"[title]')

        best_doc: Optional[Dict[str, Any]] = None
        best_score = -1

        for term in self._unique_preserve_order(search_plans):
            try:
                ids = await self._medgen_esearch(term, retmax=5)
                docs = await self._medgen_esummary(ids)
            except Exception:
                continue

            for doc in docs:
                score = self._score_medgen_doc(
                    doc,
                    preferred_label=preferred_label,
                    synonyms=synonyms,
                    orpha_code=orpha_code,
                )
                if score > best_score:
                    best_score = score
                    best_doc = doc

            if best_score >= 10:
                break

        if not best_doc:
            return None

        uid = str(best_doc.get("_uid") or "")
        cui = self._extract_medgen_cui(best_doc)
        mondo_candidates = self._extract_mondo_ids_from_doc(best_doc)

        return {
            "uid": uid or None,
            "cui": cui,
            "title": self._first_present(best_doc, "title", "Title"),
            "mondo_candidates": mondo_candidates,
            "raw_record": best_doc,
        }

    # -------------------------------------------------------------------------
    # MeSH enrichment
    # -------------------------------------------------------------------------

    async def _mesh_lookup_exact(self, label: str) -> List[Dict[str, Any]]:
        params = {
            "label": label,
            "match": "exact",
            "limit": 10,
        }

        async with httpx.AsyncClient(
            timeout=20.0,
            headers={"User-Agent": "rare-disease-evidence-platform/0.1"},
        ) as client:
            response = await client.get(self.MESH_LOOKUP_URL, params=params)

        if response.status_code == 404:
            return []

        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, list) else []

    def _mesh_id_from_resource(self, resource: str) -> Optional[str]:
        if not resource:
            return None
        candidate = resource.rstrip("/").split("/")[-1]
        if self.MESH_RE.fullmatch(candidate):
            return candidate
        return None

    async def _find_mesh_crosswalk(
        self,
        *,
        preferred_label: str,
        synonyms: List[str],
    ) -> Optional[Dict[str, Any]]:
        terms = self._unique_preserve_order([preferred_label] + synonyms[:5])

        for term in terms:
            try:
                results = await self._mesh_lookup_exact(term)
            except Exception:
                continue

            for result in results:
                if not isinstance(result, dict):
                    continue
                label = str(result.get("label") or "").strip()
                resource = str(result.get("resource") or "").strip()
                mesh_id = self._mesh_id_from_resource(resource)

                if not mesh_id:
                    continue

                if label.lower() == term.lower().strip():
                    return {
                        "mesh": mesh_id,
                        "label": label,
                        "resource": resource,
                    }

        return None

    # -------------------------------------------------------------------------
    # MONDO enrichment
    # -------------------------------------------------------------------------

    async def _ols_search_mondo(self, query_text: str, *, exact: bool = True, rows: int = 10) -> List[Dict[str, Any]]:
        params = {
            "q": query_text,
            "ontology": "mondo",
            "type": "class",
            "rows": int(rows),
            "exact": str(exact).lower(),
        }

        async with httpx.AsyncClient(
            timeout=20.0,
            headers={"User-Agent": "rare-disease-evidence-platform/0.1"},
        ) as client:
            response = await client.get(self.OLS_SEARCH_URL, params=params)

        if response.status_code == 404:
            return []

        response.raise_for_status()
        payload = response.json()

        # OLS search commonly returns a Solr-style response object.
        docs = payload.get("response", {}).get("docs", [])
        return docs if isinstance(docs, list) else []

    def _extract_mondo_id_from_ols_doc(self, doc: Dict[str, Any]) -> Optional[str]:
        for key in ("obo_id", "short_form", "shortForm", "id"):
            value = doc.get(key)
            if isinstance(value, str) and value.strip():
                text = value.strip()
                if text.upper().startswith("MONDO:") or text.upper().startswith("MONDO_"):
                    return self._normalize_mondo_id(text)

        for text in self._collect_strings(doc):
            match = self.MONDO_RE.search(text)
            if match:
                return self._normalize_mondo_id(match.group(0))

        return None

    def _score_mondo_doc(
        self,
        doc: Dict[str, Any],
        *,
        preferred_label: str,
        synonyms: List[str],
        orpha_code: Optional[str],
        mesh_id: Optional[str],
        medgen_cui: Optional[str],
    ) -> int:
        score = 0
        label = self._first_present(doc, "label", "name") or ""
        label_lower = label.lower().strip()

        if label_lower == preferred_label.lower().strip():
            score += 10

        for syn in synonyms[:5]:
            if label_lower == syn.lower().strip():
                score += 5

        strings = " | ".join(self._collect_strings(doc)).lower()

        if preferred_label.lower() in strings:
            score += 3

        if orpha_code:
            if f"orphanet:{orpha_code}".lower() in strings or f"orpha:{orpha_code}".lower() in strings:
                score += 8
            elif orpha_code in strings and "orpha" in strings:
                score += 4

        if mesh_id and mesh_id.lower() in strings:
            score += 6

        if medgen_cui and medgen_cui.lower() in strings:
            score += 5

        return score

    async def _find_mondo_crosswalk(
        self,
        *,
        preferred_label: str,
        synonyms: List[str],
        orpha_code: Optional[str],
        mesh_id: Optional[str],
        medgen_cui: Optional[str],
        medgen_mondo_candidates: List[str],
    ) -> Optional[Dict[str, Any]]:
        # If MedGen already exposed MONDO IDs, trust that first.
        if medgen_mondo_candidates:
            return {
                "mondo": medgen_mondo_candidates[0],
                "label": None,
                "method": "medgen_summary",
            }

        search_terms: List[str] = []

        if orpha_code:
            search_terms.append(f"Orphanet:{orpha_code}")
            search_terms.append(f"ORPHA:{orpha_code}")

        search_terms.append(preferred_label)
        search_terms.extend(synonyms[:4])

        best_doc: Optional[Dict[str, Any]] = None
        best_score = -1

        for idx, search_term in enumerate(self._unique_preserve_order(search_terms)):
            try:
                docs = await self._ols_search_mondo(search_term, exact=(idx < 2), rows=10)
                if not docs and idx >= 2:
                    docs = await self._ols_search_mondo(search_term, exact=False, rows=10)
            except Exception:
                continue

            for doc in docs:
                mondo_id = self._extract_mondo_id_from_ols_doc(doc)
                if not mondo_id:
                    continue

                score = self._score_mondo_doc(
                    doc,
                    preferred_label=preferred_label,
                    synonyms=synonyms,
                    orpha_code=orpha_code,
                    mesh_id=mesh_id,
                    medgen_cui=medgen_cui,
                )
                if score > best_score:
                    best_score = score
                    best_doc = doc

            if best_score >= 10:
                break

        if not best_doc:
            return None

        return {
            "mondo": self._extract_mondo_id_from_ols_doc(best_doc),
            "label": self._first_present(best_doc, "label", "name"),
            "method": "ols_search",
            "raw_record": best_doc,
        }

    # -------------------------------------------------------------------------
    # Crosswalk orchestrator
    # -------------------------------------------------------------------------

    async def _enrich_crosswalks(self, entity: NormalizedEntity) -> NormalizedEntity:
        if entity.entity_type != EntityType.disease:
            return entity

        preferred_label = entity.preferred_label
        synonyms = entity.synonyms or []
        orpha_code = (entity.source_ids or {}).get("orpha")

        mesh = await self._find_mesh_crosswalk(
            preferred_label=preferred_label,
            synonyms=synonyms,
        )

        medgen = await self._find_medgen_crosswalk(
            preferred_label=preferred_label,
            synonyms=synonyms,
            orpha_code=orpha_code,
        )

        mondo = await self._find_mondo_crosswalk(
            preferred_label=preferred_label,
            synonyms=synonyms,
            orpha_code=orpha_code,
            mesh_id=(mesh or {}).get("mesh"),
            medgen_cui=(medgen or {}).get("cui"),
            medgen_mondo_candidates=(medgen or {}).get("mondo_candidates", []),
        )

        updated_source_ids = dict(entity.source_ids or {})
        if mesh and mesh.get("mesh") and "mesh" not in updated_source_ids:
            updated_source_ids["mesh"] = str(mesh["mesh"])

        if medgen:
            if medgen.get("uid") and "medgen_uid" not in updated_source_ids:
                updated_source_ids["medgen_uid"] = str(medgen["uid"])
            if medgen.get("cui") and "medgen" not in updated_source_ids:
                updated_source_ids["medgen"] = str(medgen["cui"])

        if mondo and mondo.get("mondo") and "mondo" not in updated_source_ids:
            updated_source_ids["mondo"] = str(mondo["mondo"])

        provenance = dict(entity.provenance or {})
        provenance["crosswalks"] = {
            "mesh": mesh,
            "medgen": {
                "uid": (medgen or {}).get("uid"),
                "cui": (medgen or {}).get("cui"),
                "title": (medgen or {}).get("title"),
                "mondo_candidates": (medgen or {}).get("mondo_candidates", []),
            } if medgen else None,
            "mondo": mondo,
        }

        entity.source_ids = updated_source_ids
        entity.provenance = provenance
        return entity

    # -------------------------------------------------------------------------
    # Entity construction
    # -------------------------------------------------------------------------

    async def _hydrate_orpha_code(
        self,
        orpha_code: str,
        *,
        query_text: str,
        match_type: str,
        rank: int = 0,
        lang: Optional[str] = None,
    ) -> Optional[NormalizedEntity]:
        code = self._clean_orpha_code(orpha_code)
        if not code:
            return None

        name_payload = await self._get_property(code, "Name", lang=lang)
        if name_payload is None:
            return None

        definition_payload = await self._get_property(code, "Definition", lang=lang)
        synonym_payload = await self._get_property(code, "Synonym", lang=lang)
        classification_payload = await self._get_property(code, "Classification", lang=lang)

        status_payload = await self._get_property(code, "Status", lang=lang)
        typology_payload = await self._get_property(code, "Typology", lang=lang)
        target_payload = await self._get_property(code, "TargetORPHAcode", lang=lang)
        omim_payload = await self._get_property(code, "OMIM", lang=lang)
        icd10_payload = await self._get_property(code, "ICD10", lang=lang)

        preferred_label = self._extract_name_from_payload(name_payload) or f"ORPHA:{code}"
        definition = self._extract_definition_from_payload(definition_payload)
        synonyms = self._extract_synonyms_from_payload(synonym_payload)

        status = self._extract_simple_field(status_payload, "Status", "status", "value")
        typology = self._extract_simple_field(typology_payload, "Typology", "typology", "value")
        target_orpha = self._extract_simple_field(
            target_payload,
            "TargetORPHAcode",
            "target",
            "orpha_code",
            "ORPHAcode",
            "value",
        )

        omim_values = self._extract_list_of_strings(omim_payload, "OMIM", "omim", "code", "value")
        icd10_values = self._extract_list_of_strings(icd10_payload, "ICD10", "icd10", "code", "value")

        source_ids: Dict[str, str] = {"orpha": code}
        if omim_values:
            source_ids["omim"] = omim_values[0]
        if icd10_values:
            source_ids["icd10"] = icd10_values[0]

        confidence = {
            "exact_orpha_code": 0.995,
            "exact_name": 0.99,
            "exact_synonym": 0.94,
            "approximate_name": 0.82,
        }.get(match_type, 0.75)

        if rank > 0:
            confidence = max(0.55, confidence - min(0.18, 0.04 * rank))

        if status and "inactive" in status.lower():
            confidence = max(0.40, confidence - 0.15)

        entity = NormalizedEntity(
            entity_type=EntityType.disease,
            preferred_label=preferred_label,
            source_ids=source_ids,
            synonyms=[s for s in synonyms if s.lower() != preferred_label.lower()],
            description=definition,
            confidence=confidence,
            provenance={
                "source": self.name,
                "method": "orphacodes_api",
                "match_type": match_type,
                "query_text": query_text,
                "status": status,
                "typology": typology,
                "target_orpha": target_orpha,
                "classification": classification_payload,
                "url": f"{self.ORPHA_BASE_URL}/{(lang or self.default_lang).upper()}/ClinicalEntity/orphacode/{code}/Name",
            },
        )

        return await self._enrich_crosswalks(entity)

    # -------------------------------------------------------------------------
    # BaseConnector interface
    # -------------------------------------------------------------------------

    async def fetch_by_id(self, identifier: str) -> Any:
        code = self._clean_orpha_code(identifier)
        entity = await self._hydrate_orpha_code(
            code,
            query_text=identifier,
            match_type="exact_orpha_code",
        )
        return entity.model_dump() if entity else None

    async def crosswalk(self, source_id: str) -> Dict[str, str]:
        entity = await self.fetch_by_id(source_id)
        if not entity:
            return {}
        return entity.get("source_ids", {}) or {}

    async def normalize(self, text: str) -> List[Dict[str, Any]]:
        query = text.strip()
        if not query:
            return []

        cleaned = self._clean_orpha_code(query)

        # 1) Exact ORPHA code path
        if cleaned.isdigit():
            entity = await self._hydrate_orpha_code(
                cleaned,
                query_text=query,
                match_type="exact_orpha_code",
            )
            return [entity.model_dump()] if entity else []

        # 2) Approximate/public name search path
        candidates = await self._approximate_name_search(query)
        if not candidates:
            return []

        exact_name_matches: List[tuple[int, Dict[str, Any]]] = []
        synonym_matches: List[tuple[int, Dict[str, Any]]] = []
        fuzzy_matches: List[tuple[int, Dict[str, Any]]] = []

        lower_query = query.lower().strip()

        for idx, candidate in enumerate(candidates):
            preferred = ""
            for key in ("Preferred term", "PreferredTerm", "Name", "label", "name"):
                value = candidate.get(key)
                if isinstance(value, str) and value.strip():
                    preferred = value.strip()
                    break

            synonyms = []
            syn_val = candidate.get("Synonym") or candidate.get("synonym") or candidate.get("synonyms")
            if isinstance(syn_val, list):
                synonyms = [str(s).strip() for s in syn_val if str(s).strip()]
            elif isinstance(syn_val, str):
                synonyms = [syn_val.strip()]

            if preferred.lower() == lower_query:
                exact_name_matches.append((idx, candidate))
            elif any(s.lower() == lower_query for s in synonyms):
                synonym_matches.append((idx, candidate))
            else:
                fuzzy_matches.append((idx, candidate))

        ordered_candidates = exact_name_matches + synonym_matches + fuzzy_matches

        results: List[Dict[str, Any]] = []
        for out_rank, (_orig_idx, candidate) in enumerate(ordered_candidates[:5]):
            code = self._extract_orpha_code_from_candidate(candidate)
            if not code:
                continue

            if out_rank < len(exact_name_matches):
                match_type = "exact_name"
            elif out_rank < len(exact_name_matches) + len(synonym_matches):
                match_type = "exact_synonym"
            else:
                match_type = "approximate_name"

            entity = await self._hydrate_orpha_code(
                code,
                query_text=query,
                match_type=match_type,
                rank=out_rank,
            )
            if entity:
                results.append(entity.model_dump())

        return results

    async def search(self, query: Dict[str, Any]) -> Any:
        """
        Minimal search interface for disease lookup / normalization.
        Supports:
        - {"text": "..."}
        - {"query": "..."}
        - {"identifier": "ORPHA:558"}
        """
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
            payload = await self._get_property("558", "Definition", lang="EN")
            return payload is not None
        except Exception:
            return False

    async def rate_limit_policy(self) -> Dict[str, Any]:
        return {
            "source": self.name,
            "notes": "Uses ORPHAcodes as the primary disease normalizer, then enriches with MedGen, MeSH, and best-effort MONDO crosswalks. Prefer caching repeated disease lookups.",
        }
