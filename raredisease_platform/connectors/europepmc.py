import datetime
import logging
import re
from typing import Any, Dict, List, Optional

import httpx

from ..models import LiteratureMatchFeatures, LiteratureProvenance, LiteratureResult
from .base import BaseConnector

logger = logging.getLogger(__name__)


class EuropePMCConnector(BaseConnector):
    """Connector for Europe PMC literature search."""

    name = "europepmc"
    SEARCH_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"

    def _tokenize(self, text: Optional[str]) -> List[str]:
        if not text:
            return []
        return re.findall(r"[a-z0-9]+", text.lower())

    def _parse_year(self, value: Optional[str]) -> Optional[int]:
        if not value:
            return None
        match = re.search(r"\b(19|20)\d{2}\b", value)
        return int(match.group(0)) if match else None

    def _build_query(self, query: Dict[str, Any]) -> Optional[str]:
        filters = query.get("filters") or {}
        keywords = (query.get("keywords") or "").strip()
        if not keywords:
            return None

        parts: List[str] = [keywords]

        if filters.get("case_reports_only"):
            parts.append('PUB_TYPE:"case-report"')
        if filters.get("reviews_only"):
            parts.append('PUB_TYPE:"review"')
        if filters.get("trials_only"):
            parts.append('PUB_TYPE:"clinical trial"')
        if filters.get("title_only"):
            parts = [f'TITLE:"{keywords}"']

        date_from = filters.get("date_from") or filters.get("mindate")
        date_to = filters.get("date_to") or filters.get("maxdate")
        if date_from:
            parts.append(f'FIRST_PDATE:[{str(date_from).replace("/", "-")} TO *]')
        if date_to:
            start = str(date_from).replace("/", "-") if date_from else "*"
            parts = [p for p in parts if not p.startswith("FIRST_PDATE:")]
            parts.append(f'FIRST_PDATE:[{start} TO {str(date_to).replace("/", "-")}]')

        return " AND ".join(parts)

    def _score_record(self, title: str, abstract: Optional[str], has_full_text: bool, year: Optional[int], pub_type: Optional[str]) -> Dict[str, Any]:
        query_tokens = set(self._tokenize(title))
        title_tokens = set(self._tokenize(title))
        abstract_tokens = set(self._tokenize(abstract))

        title_match_strength = 1.0 if title_tokens else 0.0
        abstract_match_strength = 1.0 if abstract_tokens else 0.0

        current_year = datetime.datetime.utcnow().year
        if year is None:
            recency = 0.3
        else:
            age = max(0, current_year - year)
            recency = max(0.0, 1.0 - (age / 15.0))

        score = (
            0.35 * title_match_strength
            + 0.20 * abstract_match_strength
            + 0.15 * (1.0 if pub_type else 0.0)
            + 0.15 * recency
            + 0.15 * (1.0 if has_full_text else 0.0)
        )

        return {
            "title_match_strength": round(title_match_strength, 4),
            "abstract_match_strength": round(abstract_match_strength, 4),
            "publication_type": pub_type,
            "recency": round(recency, 4),
            "full_text_available": has_full_text,
            "source_trust_level": 0.85,
            "score": round(score, 4),
        }

    async def search(self, query: Dict[str, Any]) -> List[LiteratureResult]:
        epmc_query = self._build_query(query)
        if not epmc_query:
            return []

        filters = query.get("filters") or {}
        retmax = int(filters.get("retmax", 10))
        params = {
            "query": epmc_query,
            "format": "json",
            "pageSize": retmax,
            "resultType": "core",
        }

        async with httpx.AsyncClient(timeout=20.0) as client:
            try:
                response = await client.get(self.SEARCH_URL, params=params)
                response.raise_for_status()
                payload = response.json()
            except httpx.HTTPError as exc:
                logger.warning("Europe PMC HTTP error for query '%s': %s", epmc_query, exc)
                return []
            except Exception as exc:
                logger.exception("Europe PMC search failure for query '%s': %s", epmc_query, exc)
                return []

        results_raw = payload.get("resultList", {}).get("result", []) or []
        results: List[LiteratureResult] = []
        now = datetime.datetime.utcnow().isoformat() + "Z"

        keyword_tokens = set(self._tokenize(query.get("keywords") or ""))

        for record in results_raw:
            title = record.get("title") or ""
            abstract = record.get("abstractText")
            title_tokens = set(self._tokenize(title))
            abstract_tokens = set(self._tokenize(abstract))
            if keyword_tokens:
                title_match_strength = len(keyword_tokens & title_tokens) / len(keyword_tokens)
                abstract_match_strength = len(keyword_tokens & abstract_tokens) / len(keyword_tokens)
            else:
                title_match_strength = 0.0
                abstract_match_strength = 0.0

            year = self._parse_year(record.get("firstPublicationDate") or record.get("pubYear"))
            has_full_text = bool(record.get("hasPDF") == "Y" or record.get("isOpenAccess") == "Y" or record.get("fullTextUrlList"))
            pub_type = record.get("pubType")
            current_year = datetime.datetime.utcnow().year
            recency = max(0.0, 1.0 - (max(0, current_year - year) / 15.0)) if year else 0.3
            score = round(
                0.35 * title_match_strength
                + 0.20 * abstract_match_strength
                + 0.10 * (1.0 if pub_type else 0.0)
                + 0.15 * recency
                + 0.10 * (1.0 if has_full_text else 0.0)
                + 0.10 * (1.0 if abstract else 0.0),
                4,
            )

            authors = []
            author_str = record.get("authorString")
            if author_str:
                authors = [a.strip() for a in author_str.split(",") if a.strip()]

            result = LiteratureResult(
                pmid=record.get("pmid"),
                pmcid=record.get("pmcid"),
                doi=record.get("doi"),
                title=title,
                abstract=abstract,
                year=year,
                journal=record.get("journalTitle"),
                authors=authors,
                match_features=LiteratureMatchFeatures(
                    exact_disease_id=False,
                    exact_gene_id=False,
                    title_match_strength=round(title_match_strength, 4),
                    abstract_match_strength=round(abstract_match_strength, 4),
                    publication_type=pub_type,
                    recency=round(recency, 4),
                    full_text_available=has_full_text,
                    source_trust_level=0.85,
                ),
                score=score,
                provenance=LiteratureProvenance(
                    source=self.name,
                    retrieved_at=now,
                    raw_record=record,
                ),
            )
            results.append(result)

        results.sort(key=lambda x: x.score, reverse=True)
        return results

    async def normalize(self, text: str) -> List[Dict[str, Any]]:
        return []
