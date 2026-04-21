import datetime
import logging
import re
from typing import Any, Dict, Iterable, List, Optional

import httpx

from ..models import LiteratureMatchFeatures, LiteratureProvenance, LiteratureResult
from .base import BaseConnector

logger = logging.getLogger(__name__)


class EuropePMCConnector(BaseConnector):
    """Connector for Europe PMC literature search."""

    name = "europepmc"
    SEARCH_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"

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

    def _tokenize(self, text: Optional[str]) -> List[str]:
        if not text:
            return []
        return re.findall(r"[a-z0-9]+", text.lower())

    def _parse_year(self, value: Optional[str]) -> Optional[int]:
        if not value:
            return None
        match = re.search(r"\b(19|20)\d{2}\b", value)
        return int(match.group(0)) if match else None

    def _quote(self, value: str) -> str:
        escaped = value.replace("\\", "\\\\").replace('"', '\\"').strip()
        return f'"{escaped}"'

    def _unique_preserve_order(self, items: Iterable[str]) -> List[str]:
        seen = set()
        ordered: List[str] = []
        for item in items:
            cleaned = item.strip()
            if not cleaned:
                continue
            key = cleaned.lower()
            if key not in seen:
                seen.add(key)
                ordered.append(cleaned)
        return ordered

    def _normalize_string_list(self, value: Any) -> List[str]:
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

    def _extract_terms(
        self,
        query: Dict[str, Any],
        *,
        terms_key: str,
        entities_key: str,
        include_synonyms: bool = True,
        synonym_limit_per_entity: int = 4,
    ) -> List[str]:
        """
        Pull usable text terms from either:
        - explicit term lists, e.g. query['gene_terms']
        - richer entity objects, e.g. query['gene_entities']
        """
        terms: List[str] = []

        # Explicit plain-text terms win first.
        terms.extend(self._normalize_string_list(query.get(terms_key)))

        # Optional richer entity payloads.
        entities = query.get(entities_key) or []
        if not isinstance(entities, list):
            entities = [entities]

        for entity in entities:
            if isinstance(entity, str):
                terms.append(entity)
                continue

            if not isinstance(entity, dict):
                continue

            preferred = entity.get("preferred_label")
            if preferred:
                terms.append(str(preferred))

            if include_synonyms:
                synonyms = entity.get("synonyms") or []
                if isinstance(synonyms, list):
                    for synonym in synonyms[:synonym_limit_per_entity]:
                        if synonym:
                            terms.append(str(synonym))

        return self._unique_preserve_order(terms)

    def _extract_id_fallback_terms(
        self,
        query: Dict[str, Any],
    ) -> Dict[str, List[str]]:
        """
        Use raw IDs only when they have some chance of being text-searchable in Europe PMC.

        Good candidates:
        - Ensembl gene IDs (ENSG...)
        - rsIDs
        - PMCID / DOI / PMID-like article identifiers

        Poor candidates:
        - HGNC IDs
        - ORPHA / MONDO / HPO / PubChem CIDs
        """
        gene_fallbacks: List[str] = []
        variant_fallbacks: List[str] = []
        article_fallbacks: List[str] = []

        for gid in self._normalize_string_list(query.get("gene_ids")):
            upper = gid.upper().strip()
            if upper.startswith("ENSG"):
                gene_fallbacks.append(upper)

        for vid in self._normalize_string_list(query.get("variant_ids")):
            upper = vid.upper().strip()
            if upper.startswith("RS"):
                variant_fallbacks.append(upper)

        for aid in self._normalize_string_list(query.get("article_ids")):
            cleaned = aid.strip()
            if cleaned:
                article_fallbacks.append(cleaned)

        return {
            "gene_fallbacks": self._unique_preserve_order(gene_fallbacks),
            "variant_fallbacks": self._unique_preserve_order(variant_fallbacks),
            "article_fallbacks": self._unique_preserve_order(article_fallbacks),
        }

    def _build_or_group(self, clauses: List[str]) -> Optional[str]:
        clauses = [c for c in clauses if c]
        if not clauses:
            return None
        if len(clauses) == 1:
            return clauses[0]
        return "(" + " OR ".join(clauses) + ")"

    def _build_article_id_group(self, article_ids: List[str]) -> Optional[str]:
        clauses: List[str] = []
        for article_id in article_ids:
            value = article_id.strip()
            upper = value.upper()

            if upper.startswith("PMC"):
                clauses.append(f"PMCID:{upper}")
            elif value.lower().startswith("10."):
                clauses.append(f"DOI:{value}")
            elif value.isdigit():
                # Europe PMC docs: PMID should use EXT_ID with SRC:MED for uniqueness.
                clauses.append(f"(EXT_ID:{value} AND SRC:MED)")
            else:
                clauses.append(self._quote(value))

        return self._build_or_group(clauses)

    def _build_query(self, query: Dict[str, Any]) -> Optional[str]:
        filters = query.get("filters") or {}
        clauses: List[str] = []

        keywords = (query.get("keywords") or "").strip()
        if keywords:
            if filters.get("title_only"):
                clauses.append(f"TITLE:{self._quote(keywords)}")
            else:
                # Keep free-text keywords available for broad retrieval.
                clauses.append(f"({keywords})")

        # Prefer normalized terms / labels when available.
        disease_terms = self._extract_terms(
            query,
            terms_key="disease_terms",
            entities_key="disease_entities",
            include_synonyms=True,
            synonym_limit_per_entity=4,
        )
        gene_terms = self._extract_terms(
            query,
            terms_key="gene_terms",
            entities_key="gene_entities",
            include_synonyms=True,
            synonym_limit_per_entity=3,
        )
        phenotype_terms = self._extract_terms(
            query,
            terms_key="phenotype_terms",
            entities_key="phenotype_entities",
            include_synonyms=True,
            synonym_limit_per_entity=3,
        )
        compound_terms = self._extract_terms(
            query,
            terms_key="compound_terms",
            entities_key="compound_entities",
            include_synonyms=True,
            synonym_limit_per_entity=3,
        )

        # Limited ID fallbacks.
        fallback_terms = self._extract_id_fallback_terms(query)

        if disease_terms:
            disease_group: List[str] = []
            for term in disease_terms:
                disease_group.append(f"DISEASE:{self._quote(term)}")
                disease_group.append(f"KW:{self._quote(term)}")
            group = self._build_or_group(self._unique_preserve_order(disease_group))
            if group:
                clauses.append(group)

        if gene_terms or fallback_terms["gene_fallbacks"]:
            gene_group: List[str] = []
            for term in gene_terms:
                # Europe PMC documents GENE_PROTEIN as a mined term field.
                gene_group.append(f"GENE_PROTEIN:{self._quote(term)}")
                gene_group.append(f"KW:{self._quote(term)}")
            for fallback in fallback_terms["gene_fallbacks"]:
                gene_group.append(self._quote(fallback))
            group = self._build_or_group(self._unique_preserve_order(gene_group))
            if group:
                clauses.append(group)

        if phenotype_terms:
            phenotype_group: List[str] = []
            for term in phenotype_terms:
                # No dedicated phenotype field documented in the search syntax reference,
                # so use keyword + phrase fallback.
                phenotype_group.append(f"KW:{self._quote(term)}")
                phenotype_group.append(self._quote(term))
            group = self._build_or_group(self._unique_preserve_order(phenotype_group))
            if group:
                clauses.append(group)

        if compound_terms:
            compound_group: List[str] = []
            for term in compound_terms:
                compound_group.append(f"CHEM:{self._quote(term)}")
                compound_group.append(f"KW:{self._quote(term)}")
            group = self._build_or_group(self._unique_preserve_order(compound_group))
            if group:
                clauses.append(group)

        if fallback_terms["variant_fallbacks"]:
            variant_group = self._build_or_group(
                [self._quote(v) for v in fallback_terms["variant_fallbacks"]]
            )
            if variant_group:
                clauses.append(variant_group)

        article_id_group = self._build_article_id_group(fallback_terms["article_fallbacks"])
        if article_id_group:
            clauses.append(article_id_group)

        # Filters supported by documented Europe PMC search fields.
        if filters.get("case_reports_only"):
            clauses.append('PUB_TYPE:"case-report"')
        if filters.get("reviews_only"):
            clauses.append('PUB_TYPE:"review"')
        if filters.get("trials_only"):
            clauses.append('PUB_TYPE:"clinical trial"')

        languages = filters.get("languages")
        if not languages and filters.get("language") is not None:
            languages = filters["language"]

        if languages is not None:
            if isinstance(languages, str):
                languages = [languages]
            for lang in languages:
                normalized_lang = self.LANGUAGE_MAP.get(str(lang).lower(), str(lang).lower())
                clauses.append(f"LANG:{normalized_lang}")

        if filters.get("abstract_required"):
            clauses.append("HAS_ABSTRACT:y")

        if filters.get("free_full_text_only"):
            clauses.append("HAS_FREE_FULLTEXT:y")
        elif filters.get("full_text_available"):
            clauses.append("HAS_FT:y")

        date_from = filters.get("date_from") or filters.get("mindate")
        date_to = filters.get("date_to") or filters.get("maxdate")
        if date_from or date_to:
            start = str(date_from).replace("/", "-") if date_from else "*"
            end = str(date_to).replace("/", "-") if date_to else "*"
            clauses.append(f"FIRST_PDATE:[{start} TO {end}]")

        clauses = [c for c in clauses if c]
        if not clauses:
            return None

        return " AND ".join(clauses)

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
            has_full_text = bool(
                record.get("hasPDF") == "Y"
                or record.get("isOpenAccess") == "Y"
                or record.get("fullTextUrlList")
            )
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

            authors: List[str] = []
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
