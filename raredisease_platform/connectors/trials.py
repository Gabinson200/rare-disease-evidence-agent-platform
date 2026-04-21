"""Connector for live ClinicalTrials.gov study retrieval using API v2."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

import httpx

from ..models import EntityType, NormalizedEntity
from .base import BaseConnector


class ClinicalTrialsConnector(BaseConnector):
    """Connector for ClinicalTrials.gov trial normalization and study search."""

    name = "clinicaltrials"
    BASE_URL = "https://clinicaltrials.gov/api/v2/studies"
    NCT_RE = re.compile(r"^NCT\d{8}$", re.IGNORECASE)
    MAX_PAGE_SIZE = 1000

    @staticmethod
    def _is_nct_id(value: str) -> bool:
        return bool(ClinicalTrialsConnector.NCT_RE.match(value.strip()))

    @staticmethod
    def _coerce_list(value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        if isinstance(value, list):
            out: List[str] = []
            for item in value:
                if item is None:
                    continue
                out.append(str(item))
            return out
        return [str(value)]

    @staticmethod
    def _unique_preserve_order(items: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for item in items:
            cleaned = item.strip()
            if not cleaned:
                continue
            key = cleaned.lower()
            if key not in seen:
                seen.add(key)
                out.append(cleaned)
        return out

    @staticmethod
    def _quote_term(term: str) -> str:
        escaped = term.replace("\\", "\\\\").replace('"', '\\"').strip()
        return f'"{escaped}"'

    @staticmethod
    def _normalize_enum_like(value: str) -> str:
        return value.strip().upper().replace(" ", "_").replace("-", "_")

    def _combine_terms(self, terms: List[str]) -> Optional[str]:
        terms = self._unique_preserve_order([t for t in terms if t and t.strip()])
        if not terms:
            return None
        if len(terms) == 1:
            return self._quote_term(terms[0])
        return "(" + " OR ".join(self._quote_term(t) for t in terms) + ")"

    async def _get_json(
        self,
        path: str = "",
        params: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        url = self.BASE_URL if not path else f"{self.BASE_URL}/{path.lstrip('/')}"
        async with httpx.AsyncClient(
            timeout=20.0,
            headers={"User-Agent": "rare-disease-evidence-platform/0.1"},
        ) as client:
            response = await client.get(url, params=params)

        if response.status_code == 404:
            return None

        response.raise_for_status()
        return response.json()

    def _extract_terms(self, query: Dict[str, Any], key: str) -> List[str]:
        return self._unique_preserve_order(self._coerce_list(query.get(key)))

    def _extract_nct_ids_from_query(self, query: Dict[str, Any]) -> List[str]:
        candidates: List[str] = []
        for key in ("trial_ids", "nct_ids", "article_ids"):
            for value in self._coerce_list(query.get(key)):
                if self._is_nct_id(value):
                    candidates.append(value.upper())

        identifier = str(query.get("identifier") or "").strip()
        if self._is_nct_id(identifier):
            candidates.append(identifier.upper())

        text = str(query.get("text") or query.get("query") or "").strip()
        if self._is_nct_id(text):
            candidates.append(text.upper())

        return self._unique_preserve_order(candidates)

    def _extract_filter_payload(self, query: Dict[str, Any]) -> Dict[str, Any]:
        filters = query.get("filters") or {}
        return filters if isinstance(filters, dict) else {}

    def _study_to_entity(
        self,
        study: Dict[str, Any],
        *,
        match_type: str,
        query_text: str,
        rank: int = 0,
    ) -> NormalizedEntity:
        protocol = study.get("protocolSection", {}) or {}
        identification = protocol.get("identificationModule", {}) or {}
        status_module = protocol.get("statusModule", {}) or {}
        design_module = protocol.get("designModule", {}) or {}
        description_module = protocol.get("descriptionModule", {}) or {}
        conditions_module = protocol.get("conditionsModule", {}) or {}
        arms_module = protocol.get("armsInterventionsModule", {}) or {}
        sponsor_module = protocol.get("sponsorCollaboratorsModule", {}) or {}
        eligibility_module = protocol.get("eligibilityModule", {}) or {}

        nct_id = str(identification.get("nctId") or "")
        brief_title = str(identification.get("briefTitle") or "").strip()
        official_title = str(identification.get("officialTitle") or "").strip()
        acronym = str(identification.get("acronym") or "").strip()
        brief_summary = description_module.get("briefSummary")

        overall_status = status_module.get("overallStatus")
        study_type = design_module.get("studyType")
        phases = design_module.get("phases") or []
        sex = eligibility_module.get("sex")
        std_ages = eligibility_module.get("stdAges") or []
        sponsor = (sponsor_module.get("leadSponsor") or {}).get("name")

        conditions = conditions_module.get("conditions") or []
        interventions = arms_module.get("interventions") or []

        synonyms: List[str] = []
        for candidate in (official_title, acronym):
            if candidate and candidate.lower() != brief_title.lower():
                synonyms.append(candidate)

        confidence = {
            "exact_nct_id": 0.995,
            "disease_search": 0.88,
            "intervention_search": 0.88,
            "combined_search": 0.92,
            "keyword_search": 0.75,
        }.get(match_type, 0.70)

        if rank > 0:
            confidence = max(0.55, confidence - min(0.15, 0.03 * rank))

        return NormalizedEntity(
            entity_type=EntityType.trial,
            preferred_label=brief_title or official_title or nct_id or "Clinical trial",
            source_ids={"nct": nct_id} if nct_id else {},
            synonyms=synonyms,
            description=str(brief_summary) if brief_summary else None,
            confidence=confidence,
            provenance={
                "source": self.name,
                "method": "clinicaltrials_api_v2",
                "match_type": match_type,
                "query_text": query_text,
                "overall_status": overall_status,
                "study_type": study_type,
                "phases": phases,
                "sex": sex,
                "std_ages": std_ages,
                "conditions": conditions,
                "intervention_names": [
                    i.get("name") for i in interventions if isinstance(i, dict) and i.get("name")
                ],
                "sponsor": sponsor,
                "url": f"https://clinicaltrials.gov/study/{nct_id}" if nct_id else None,
            },
        )

    def _matches_filters(self, study: Dict[str, Any], filters: Dict[str, Any]) -> bool:
        if not filters:
            return True

        protocol = study.get("protocolSection", {}) or {}
        status_module = protocol.get("statusModule", {}) or {}
        design_module = protocol.get("designModule", {}) or {}
        eligibility_module = protocol.get("eligibilityModule", {}) or {}
        sponsor_module = protocol.get("sponsorCollaboratorsModule", {}) or {}
        locations_module = protocol.get("contactsLocationsModule", {}) or {}

        overall_status = str(status_module.get("overallStatus") or "").strip()
        phases = [str(p) for p in (design_module.get("phases") or [])]
        sex = str(eligibility_module.get("sex") or "").strip()
        std_ages = [str(a) for a in (eligibility_module.get("stdAges") or [])]
        sponsor = str((sponsor_module.get("leadSponsor") or {}).get("name") or "").strip()

        if filters.get("recruiting_status"):
            wanted_status = self._normalize_enum_like(str(filters["recruiting_status"]))
            if self._normalize_enum_like(overall_status) != wanted_status:
                return False

        if filters.get("phase"):
            wanted_phase = self._normalize_enum_like(str(filters["phase"]))
            normalized_phases = {self._normalize_enum_like(p) for p in phases}
            if wanted_phase not in normalized_phases:
                return False

        if filters.get("sex"):
            wanted_sex = self._normalize_enum_like(str(filters["sex"]))
            normalized_sex = self._normalize_enum_like(sex) if sex else ""
            if wanted_sex and normalized_sex and wanted_sex != normalized_sex:
                return False

        if filters.get("age_group"):
            wanted_age = self._normalize_enum_like(str(filters["age_group"]))
            normalized_ages = {self._normalize_enum_like(a) for a in std_ages}
            if wanted_age not in normalized_ages:
                return False

        if filters.get("sponsor"):
            sponsor_query = str(filters["sponsor"]).strip().lower()
            if sponsor_query and sponsor_query not in sponsor.lower():
                return False

        if filters.get("country"):
            wanted_country = str(filters["country"]).strip().lower()
            locations = locations_module.get("locations") or []
            countries = {
                str(loc.get("country") or "").strip().lower()
                for loc in locations
                if isinstance(loc, dict) and loc.get("country")
            }
            if wanted_country and wanted_country not in countries:
                return False

        if filters.get("date_updated_from"):
            # Prefer lastUpdatePostDateStruct.date when present.
            last_update_struct = status_module.get("lastUpdatePostDateStruct") or {}
            last_update = str(last_update_struct.get("date") or "").strip()
            if last_update and str(filters["date_updated_from"]).strip():
                if last_update < str(filters["date_updated_from"]).strip():
                    return False

        return True

    async def fetch_by_id(self, identifier: str) -> Any:
        nct_id = identifier.strip().upper()
        if not self._is_nct_id(nct_id):
            return None

        payload = await self._get_json(nct_id)
        if not payload:
            return None

        # The single-study endpoint returns one study object.
        return payload

    async def normalize(self, text: str) -> List[Dict[str, Any]]:
        raw = text.strip().upper()
        if not self._is_nct_id(raw):
            return []

        study = await self.fetch_by_id(raw)
        if not study:
            return []

        entity = self._study_to_entity(
            study,
            match_type="exact_nct_id",
            query_text=text,
        )
        return [entity.model_dump()]

    async def crosswalk(self, source_id: str) -> Dict[str, str]:
        nct_id = source_id.strip().upper()
        if not self._is_nct_id(nct_id):
            return {}
        return {"nct": nct_id}

    async def search(self, query: Dict[str, Any]) -> Any:
        """
        Search ClinicalTrials.gov studies using disease, intervention, keyword, ID,
        sponsor, and location-aware query parameters.

        Expected query keys can include:
        - disease_terms
        - compound_terms
        - gene_terms
        - phenotype_terms
        - keywords
        - trial_ids / nct_ids / identifier
        - filters: recruiting_status, phase, sex, age_group, country, sponsor, date_updated_from, retmax
        """
        filters = self._extract_filter_payload(query)

        # Exact-ID path first.
        nct_ids = self._extract_nct_ids_from_query(query)
        if nct_ids:
            entities: List[Dict[str, Any]] = []
            for rank, nct_id in enumerate(nct_ids[:10]):
                study = await self.fetch_by_id(nct_id)
                if not study:
                    continue
                entity = self._study_to_entity(
                    study,
                    match_type="exact_nct_id",
                    query_text=nct_id,
                    rank=rank,
                )
                entities.append(entity.model_dump())
            return entities

        disease_terms = self._extract_terms(query, "disease_terms")
        compound_terms = self._extract_terms(query, "compound_terms")
        gene_terms = self._extract_terms(query, "gene_terms")
        phenotype_terms = self._extract_terms(query, "phenotype_terms")
        keyword_terms = self._coerce_list(query.get("keywords"))

        params: Dict[str, Any] = {
            "pageSize": min(
                max(int(filters.get("retmax", 10)), 1),
                self.MAX_PAGE_SIZE,
            )
        }

        cond_clause = self._combine_terms(disease_terms)
        intr_clause = self._combine_terms(compound_terms)

        general_terms: List[str] = []
        general_terms.extend(gene_terms)
        general_terms.extend(phenotype_terms)
        general_terms.extend(keyword_terms)
        term_clause = self._combine_terms(self._unique_preserve_order(general_terms))

        if cond_clause:
            params["query.cond"] = cond_clause
        if intr_clause:
            params["query.intr"] = intr_clause
        if term_clause:
            params["query.term"] = term_clause

        if filters.get("sponsor"):
            params["query.spons"] = str(filters["sponsor"]).strip()
        if filters.get("country"):
            params["query.locn"] = str(filters["country"]).strip()

        # If there is nothing meaningful to search, return no results.
        if len(params) == 1:  # pageSize only
            return []

        payload = await self._get_json(params=params)
        if not payload:
            return []

        studies = payload.get("studies", []) or []

        if filters:
            studies = [study for study in studies if self._matches_filters(study, filters)]

        match_type = "keyword_search"
        if cond_clause and intr_clause:
            match_type = "combined_search"
        elif cond_clause:
            match_type = "disease_search"
        elif intr_clause:
            match_type = "intervention_search"

        query_text_parts = []
        for key in ("query.cond", "query.intr", "query.term", "query.spons", "query.locn"):
            if params.get(key):
                query_text_parts.append(f"{key}={params[key]}")
        query_text = " | ".join(query_text_parts)

        entities: List[Dict[str, Any]] = []
        for rank, study in enumerate(studies):
            entity = self._study_to_entity(
                study,
                match_type=match_type,
                query_text=query_text,
                rank=rank,
            )
            entities.append(entity.model_dump())

        return entities

    async def health_check(self) -> bool:
        try:
            payload = await self._get_json(params={"pageSize": 1})
            return payload is not None and "studies" in payload
        except Exception:
            return False

    async def rate_limit_policy(self) -> Dict[str, Any]:
        return {
            "source": self.name,
            "notes": "Use moderate concurrency and prefer caching repeated study and search requests.",
        }
