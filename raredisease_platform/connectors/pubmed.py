import datetime
import logging
import os
import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

import httpx

from ..models import LiteratureResult, LiteratureProvenance, LiteratureMatchFeatures
from .base import BaseConnector

logger = logging.getLogger(__name__)


class PubMedConnector(BaseConnector):
    name = "pubmed"

    ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    ESUMMARY_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
    EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

    LANGUAGE_MAP = {
        "english": "eng",
        "eng": "eng",
        "french": "fre",
        "fre": "fre",
        "german": "ger",
        "ger": "ger",
        "spanish": "spa",
        "spa": "spa",
    }

    def __init__(self) -> None:
        self.tool = os.getenv("NCBI_TOOL", "rare-disease-evidence-platform")
        self.email = os.getenv("NCBI_EMAIL")
        self.api_key = os.getenv("NCBI_API_KEY")

    def _base_params(self) -> Dict[str, str]:
        params = {
            "db": "pubmed",
            "tool": self.tool,
        }
        if self.email:
            params["email"] = self.email
        if self.api_key:
            params["api_key"] = self.api_key
        return params

    def _normalize_date(self, value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        return value.replace("-", "/")

    def _coerce_languages(self, filters: Dict[str, Any]) -> List[str]:
        languages = filters.get("languages")
        if not languages and filters.get("language") is not None:
            languages = filters["language"]

        if languages is None:
            return []
        if isinstance(languages, str):
            return [languages]
        return list(languages)

    async def _request_eutils(
        self,
        client: httpx.AsyncClient,
        url: str,
        params: Dict[str, Any],
        *,
        use_post: bool = False,
    ) -> httpx.Response:
        if use_post:
            response = await client.post(url, data=params)
        else:
            response = await client.get(url, params=params)
        response.raise_for_status()
        return response

    def _build_esearch_params(self, query: Dict[str, Any]) -> Dict[str, Any]:
        filters = query.get("filters") or {}
        params: Dict[str, Any] = {
            **self._base_params(),
            "retmode": "json",
            "retstart": int(filters.get("retstart", 0)),
            "retmax": int(filters.get("retmax", 10)),
            "sort": filters.get("sort", "relevance"),
        }

        term_parts: List[str] = []
        keywords = (query.get("keywords") or "").strip()

        if keywords:
            if filters.get("title_only"):
                term_parts.append(f"({keywords})[Title]")
            elif filters.get("field"):
                term_parts.append(f"({keywords})[{filters['field']}]")
            else:
                term_parts.append(f"({keywords})")

        for pub_type in filters.get("publication_types") or []:
            term_parts.append(f"\"{pub_type}\"[Publication Type]")

        if filters.get("case_reports_only"):
            term_parts.append("\"Case Reports\"[Publication Type]")

        if filters.get("reviews_only"):
            term_parts.append("\"Review\"[Publication Type]")

        if filters.get("trials_only"):
            term_parts.append("\"Clinical Trial\"[Publication Type]")

        for lang in self._coerce_languages(filters):
            normalized_lang = self.LANGUAGE_MAP.get(str(lang).lower(), str(lang).lower())
            term_parts.append(f"{normalized_lang}[Language]")

        datetype = filters.get("datetype") or "pdat"
        reldate = filters.get("reldate")

        mindate = self._normalize_date(filters.get("mindate") or filters.get("date_from"))
        maxdate = self._normalize_date(filters.get("maxdate") or filters.get("date_to"))

        if reldate is not None:
            params["reldate"] = int(reldate)
            params["datetype"] = datetype
        elif mindate and maxdate:
            params["mindate"] = mindate
            params["maxdate"] = maxdate
            params["datetype"] = datetype

        params["term"] = " AND ".join(term_parts) if term_parts else "rare disease"
        return params
    
    def _tokenize(self, text: Optional[str]) -> List[str]:
        if not text:
            return []
        return re.findall(r"[a-z0-9]+", text.lower())

    def _parse_year(self, pubdate: Optional[str]) -> Optional[int]:
        if not pubdate:
            return None
        match = re.search(r"\b(19|20)\d{2}\b", pubdate)
        if match:
            return int(match.group(0))
        return None

    def _extract_pmcid(self, article_ids: List[Dict[str, Any]]) -> Optional[str]:
        for article_id in article_ids:
            idtype = article_id.get("idtype")
            value = article_id.get("value")
            if not value:
                continue

            if idtype == "pmc":
                return value.strip()

            if idtype == "pmcid":
                cleaned = value.replace("pmc-id:", "").replace("PMC", "PMC").replace(";", "").strip()
                return cleaned

        return None

    async def _fetch_abstracts(
        self,
        client: httpx.AsyncClient,
        id_list: List[str],
    ) -> Dict[str, Optional[str]]:
        params = {
            **self._base_params(),
            "id": ",".join(id_list),
            "retmode": "xml",
        }

        response = await self._request_eutils(
            client,
            self.EFETCH_URL,
            params,
            use_post=len(id_list) > 200,
        )

        root = ET.fromstring(response.text)
        abstracts_by_pmid: Dict[str, Optional[str]] = {}

        for article in root.findall(".//PubmedArticle"):
            pmid_el = article.find(".//MedlineCitation/PMID")
            if pmid_el is None or not pmid_el.text:
                continue

            pmid = pmid_el.text.strip()
            abstract_chunks: List[str] = []

            for abstract_node in article.findall(".//Article/Abstract/AbstractText"):
                text = "".join(abstract_node.itertext()).strip()
                if not text:
                    continue

                label = abstract_node.attrib.get("Label") or abstract_node.attrib.get("NlmCategory")
                if label and not text.lower().startswith(label.lower()):
                    abstract_chunks.append(f"{label}: {text}")
                else:
                    abstract_chunks.append(text)

            abstracts_by_pmid[pmid] = "\n\n".join(abstract_chunks) if abstract_chunks else None

        return abstracts_by_pmid
    
    def _score_record(
        self,
        record: Dict[str, Any],
        abstract: Optional[str],
        pmcid: Optional[str],
        query: Dict[str, Any],
    ) -> Dict[str, Any]:
        filters = query.get("filters") or {}
        keywords = query.get("keywords") or ""

        query_tokens = set(self._tokenize(keywords))
        title_tokens = set(self._tokenize(record.get("title")))
        abstract_tokens = set(self._tokenize(abstract))

        if query_tokens:
            title_match_strength = len(query_tokens & title_tokens) / len(query_tokens)
            abstract_match_strength = len(query_tokens & abstract_tokens) / len(query_tokens)
        else:
            title_match_strength = 0.0
            abstract_match_strength = 0.0

        pubtypes = record.get("pubtype", []) or []
        pubtypes_lower = {p.lower() for p in pubtypes}

        requested_case_reports = bool(filters.get("case_reports_only")) or any(
            "case" in str(p).lower() for p in (filters.get("publication_types") or [])
        )
        has_case_report_type = "case reports" in pubtypes_lower

        if requested_case_reports and has_case_report_type:
            publication_type_score = 1.0
        elif has_case_report_type:
            publication_type_score = 0.4
        else:
            publication_type_score = 0.0

        year = self._parse_year(record.get("pubdate"))
        current_year = datetime.datetime.utcnow().year
        if year is None:
            recency = 0.3
        else:
            age = max(0, current_year - year)
            recency = max(0.0, 1.0 - (age / 15.0))

        full_text_available = bool(pmcid)
        source_trust_level = 0.9  # PubMed-indexed result

        score = (
            0.35 * title_match_strength
            + 0.20 * abstract_match_strength
            + 0.15 * publication_type_score
            + 0.10 * recency
            + 0.10 * (1.0 if full_text_available else 0.0)
            + 0.10 * (1.0 if abstract else 0.0)
        )

        return {
            "title_match_strength": round(title_match_strength, 4),
            "abstract_match_strength": round(abstract_match_strength, 4),
            "publication_type": ", ".join(pubtypes) if pubtypes else None,
            "recency": round(recency, 4),
            "full_text_available": full_text_available,
            "source_trust_level": source_trust_level,
            "score": round(score, 4),
        }

    async def search(self, query: Dict[str, Any]) -> List[LiteratureResult]:
        esearch_params = self._build_esearch_params(query)
        term = esearch_params["term"]

        headers = {
            "User-Agent": "rare-disease-evidence-platform/0.1"
        }

        async with httpx.AsyncClient(timeout=20.0, headers=headers) as client:
            try:
                esearch_resp = await self._request_eutils(
                    client,
                    self.ESEARCH_URL,
                    esearch_params,
                    use_post=len(term) > 500,
                )
                esearch_data = esearch_resp.json()

                id_list = esearch_data.get("esearchresult", {}).get("idlist", [])
                if not id_list:
                    return []

                esummary_params = {
                    **self._base_params(),
                    "id": ",".join(id_list),
                    "retmode": "json",
                }

                esummary_resp = await self._request_eutils(
                    client,
                    self.ESUMMARY_URL,
                    esummary_params,
                    use_post=len(id_list) > 200,
                )
                esummary_data = esummary_resp.json()

            except httpx.TimeoutException:
                logger.warning("PubMed request timed out for term: %s", term)
                return []
            except httpx.HTTPError as exc:
                logger.warning("PubMed HTTP error for term '%s': %s", term, exc)
                return []
            except Exception as exc:
                logger.exception("Unexpected PubMed search failure for term '%s': %s", term, exc)
                return []

            try:
                abstracts_by_pmid = await self._fetch_abstracts(client, id_list)
            except Exception as exc:
                logger.warning("PubMed abstract fetch failed for term '%s': %s", term, exc)
                abstracts_by_pmid = {}

        results: List[LiteratureResult] = []
        now = datetime.datetime.utcnow().isoformat() + "Z"

        for pmid in id_list:
            record = esummary_data.get("result", {}).get(pmid)
            if not record:
                continue

            authors = []
            for author in record.get("authors", []):
                name = author.get("name")
                if name:
                    authors.append(name)

            article_ids = record.get("articleids", []) or []
            doi = None
            for article_id in article_ids:
                if article_id.get("idtype") == "doi":
                    doi = article_id.get("value")
                    break

            pmcid = self._extract_pmcid(article_ids)
            abstract = abstracts_by_pmid.get(pmid)
            year = self._parse_year(record.get("pubdate"))

            scoring = self._score_record(
                record=record,
                abstract=abstract,
                pmcid=pmcid,
                query=query,
            )

            result = LiteratureResult(
                pmid=pmid,
                pmcid=pmcid,
                doi=doi,
                title=record.get("title", ""),
                abstract=abstract,
                year=year,
                journal=record.get("fulljournalname"),
                authors=authors,
                match_features=LiteratureMatchFeatures(
                    exact_disease_id=False,
                    exact_gene_id=False,
                    phenotype_overlap_strength=None,
                    mesh_topic_importance=None,
                    title_match_strength=scoring["title_match_strength"],
                    abstract_match_strength=scoring["abstract_match_strength"],
                    publication_type=scoring["publication_type"],
                    recency=scoring["recency"],
                    full_text_available=scoring["full_text_available"],
                    source_trust_level=scoring["source_trust_level"],
                ),
                score=scoring["score"],
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
