"""Connector for ClinVar variant normalization and lookup via NCBI E-utilities."""

from __future__ import annotations

import os
import re
from typing import Any, Dict, Iterable, List, Optional

import httpx

from ..models import EntityType, NormalizedEntity
from .base import BaseConnector


class ClinVarConnector(BaseConnector):
    """Connector for ClinVar-backed variant normalization and search."""

    name = "clinvar"

    ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    ESUMMARY_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"

    ACCESSION_RE = re.compile(r"\b(?:VCV|RCV|SCV)\d{9}(?:\.\d+)?\b", re.IGNORECASE)
    RSID_RE = re.compile(r"\brs\d+\b", re.IGNORECASE)
    HGVS_HINT_RE = re.compile(
        r"([A-Z]{2}_\d+(?:\.\d+)?(?::|\([A-Za-z0-9_-]+\):)[cgmnrp]\.)|([cgmnrp]\.)",
        re.IGNORECASE,
    )

    def __init__(self) -> None:
        self.tool = os.getenv("NCBI_TOOL", "rare-disease-evidence-platform")
        self.email = os.getenv("NCBI_EMAIL")
        self.api_key = os.getenv("NCBI_API_KEY")

    def _base_params(self) -> Dict[str, str]:
        params = {
            "db": "clinvar",
            "tool": self.tool,
        }
        if self.email:
            params["email"] = self.email
        if self.api_key:
            params["api_key"] = self.api_key
        return params

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

    @staticmethod
    def _quote_term(term: str) -> str:
        escaped = term.replace("\\", "\\\\").replace('"', '\\"').strip()
        return f'"{escaped}"'

    @staticmethod
    def _coerce_list(value: Any) -> List[str]:
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
            cleaned = item.strip()
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
    def _first_present(doc: Dict[str, Any], *keys: str) -> Optional[str]:
        for key in keys:
            value = doc.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    def _looks_like_accession(self, value: str) -> bool:
        return bool(self.ACCESSION_RE.search(value.strip()))

    def _looks_like_rsid(self, value: str) -> bool:
        return bool(self.RSID_RE.search(value.strip()))

    def _looks_like_hgvs(self, value: str) -> bool:
        return bool(self.HGVS_HINT_RE.search(value.strip()))

    async def _esearch(self, term: str, retmax: int = 3) -> List[str]:
        params = {
            **self._base_params(),
            "term": term,
            "retmode": "json",
            "retmax": int(retmax),
            "sort": "relevance",
        }

        async with httpx.AsyncClient(
            timeout=20.0,
            headers={"User-Agent": "rare-disease-evidence-platform/0.1"},
        ) as client:
            response = await self._request_eutils(
                client,
                self.ESEARCH_URL,
                params,
                use_post=len(term) > 500,
            )
            payload = response.json()

        idlist = payload.get("esearchresult", {}).get("idlist", []) or []
        return [str(uid) for uid in idlist]

    async def _esummary(self, ids: List[str]) -> List[Dict[str, Any]]:
        if not ids:
            return []

        params = {
            **self._base_params(),
            "id": ",".join(ids),
            "retmode": "json",
        }

        async with httpx.AsyncClient(
            timeout=20.0,
            headers={"User-Agent": "rare-disease-evidence-platform/0.1"},
        ) as client:
            response = await self._request_eutils(
                client,
                self.ESUMMARY_URL,
                params,
                use_post=len(ids) > 200,
            )
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

    def _extract_accessions(self, doc: Dict[str, Any]) -> List[str]:
        matches: List[str] = []

        explicit = self._first_present(doc, "accession", "accession_version")
        if explicit:
            matches.append(explicit.upper())

        for string_value in self._collect_strings(doc):
            for match in self.ACCESSION_RE.findall(string_value):
                matches.append(match.upper())

        return self._unique_preserve_order(matches)

    def _extract_rsids(self, doc: Dict[str, Any]) -> List[str]:
        matches: List[str] = []
        for string_value in self._collect_strings(doc):
            for match in self.RSID_RE.findall(string_value):
                matches.append(match.lower())
        return self._unique_preserve_order(matches)

    def _extract_conditions(self, doc: Dict[str, Any]) -> List[str]:
        conditions: List[str] = []

        trait_set = doc.get("trait_set") or doc.get("traits") or []
        if isinstance(trait_set, list):
            for trait in trait_set:
                if isinstance(trait, dict):
                    for key in ("trait_name", "name", "preferred_name", "element_value"):
                        value = trait.get(key)
                        if isinstance(value, str) and value.strip():
                            conditions.append(value.strip())

        # Fallback: scan nested data for likely condition-bearing strings.
        if not conditions:
            for string_value in self._collect_strings(trait_set):
                if len(string_value.strip()) > 2:
                    conditions.append(string_value.strip())

        return self._unique_preserve_order(conditions)[:8]

    def _extract_gene_symbols(self, doc: Dict[str, Any]) -> List[str]:
        genes: List[str] = []

        gene_sort = self._first_present(doc, "gene_sort")
        if gene_sort:
            genes.append(gene_sort)

        gene_list = doc.get("genes") or []
        if isinstance(gene_list, list):
            for gene in gene_list:
                if isinstance(gene, dict):
                    for key in ("symbol", "gene_symbol", "name"):
                        value = gene.get(key)
                        if isinstance(value, str) and value.strip():
                            genes.append(value.strip())

        return self._unique_preserve_order(genes)

    @staticmethod
    def _variation_id_from_vcv(accession: str) -> Optional[str]:
        """
        ClinVar docs note that VCV accessions are constructed from the Variation ID
        by prefixing VCV and zero-padding to 9 digits.
        """
        accession_no_version = accession.split(".", 1)[0].upper()
        if not accession_no_version.startswith("VCV"):
            return None

        digits = accession_no_version[3:]
        if not digits.isdigit():
            return None

        return str(int(digits))

    def _classification_text(self, doc: Dict[str, Any]) -> Optional[str]:
        return self._first_present(
            doc,
            "germline_classification",
            "clinical_significance",
            "classification",
            "germline_description",
        )

    def _review_text(self, doc: Dict[str, Any]) -> Optional[str]:
        return self._first_present(
            doc,
            "review_status",
            "reviewstatus",
        )

    def _build_source_ids(
        self,
        uid: str,
        accessions: List[str],
        rsids: List[str],
    ) -> Dict[str, str]:
        source_ids: Dict[str, str] = {}

        variation_id: Optional[str] = None
        vcv = next((a for a in accessions if a.startswith("VCV")), None)
        rcv = next((a for a in accessions if a.startswith("RCV")), None)
        scv = next((a for a in accessions if a.startswith("SCV")), None)

        if vcv:
            variation_id = self._variation_id_from_vcv(vcv)

        if variation_id:
            source_ids["clinvar"] = variation_id
        elif uid:
            source_ids["clinvar_uid"] = uid

        if vcv:
            source_ids["vcv"] = vcv
        if rcv:
            source_ids["rcv"] = rcv
        if scv:
            source_ids["scv"] = scv
        if rsids:
            source_ids["dbsnp"] = rsids[0]

        return source_ids

    def _confidence(self, match_type: str, rank: int = 0) -> float:
        base = {
            "exact_variation_id": 0.995,
            "exact_accession": 0.995,
            "exact_rsid": 0.985,
            "exact_hgvs": 0.975,
            "text_search": 0.78,
            "combined_search": 0.82,
        }.get(match_type, 0.70)

        if rank > 0:
            base -= min(0.18, 0.04 * rank)

        return max(0.0, min(1.0, base))

    def _summary_to_entity(
        self,
        doc: Dict[str, Any],
        *,
        query_text: str,
        match_type: str,
        rank: int = 0,
    ) -> NormalizedEntity:
        uid = str(doc.get("_uid") or "")
        accessions = self._extract_accessions(doc)
        rsids = self._extract_rsids(doc)
        conditions = self._extract_conditions(doc)
        gene_symbols = self._extract_gene_symbols(doc)

        title = self._first_present(doc, "title", "variation_name", "obj_name")
        if not title:
            title = accessions[0] if accessions else query_text.strip()

        classification = self._classification_text(doc)
        review_status = self._review_text(doc)

        description_parts: List[str] = []
        if classification:
            description_parts.append(f"Classification: {classification}")
        if review_status:
            description_parts.append(f"Review status: {review_status}")
        if conditions:
            description_parts.append("Conditions: " + "; ".join(conditions[:3]))
        description = " | ".join(description_parts) if description_parts else None

        synonyms: List[str] = []
        seen = {title.strip().lower()}

        for accession in accessions:
            key = accession.lower()
            if key not in seen:
                synonyms.append(accession)
                seen.add(key)

        for rsid in rsids:
            key = rsid.lower()
            if key not in seen:
                synonyms.append(rsid)
                seen.add(key)

        for gene_symbol in gene_symbols[:3]:
            key = gene_symbol.lower()
            if key not in seen:
                synonyms.append(gene_symbol)
                seen.add(key)

        if query_text.strip():
            key = query_text.strip().lower()
            if key not in seen:
                synonyms.append(query_text.strip())

        source_ids = self._build_source_ids(uid, accessions, rsids)

        clinvar_web_url = None
        if "clinvar" in source_ids:
            clinvar_web_url = f"https://www.ncbi.nlm.nih.gov/clinvar/variation/{source_ids['clinvar']}/"

        return NormalizedEntity(
            entity_type=EntityType.variant,
            preferred_label=title,
            source_ids=source_ids,
            synonyms=synonyms[:12],
            description=description,
            confidence=self._confidence(match_type, rank=rank),
            provenance={
                "source": self.name,
                "method": "clinvar_eutils",
                "match_type": match_type,
                "query_text": query_text,
                "uid": uid,
                "classification": classification,
                "review_status": review_status,
                "conditions": conditions,
                "url": clinvar_web_url,
            },
        )

    def _build_search_term(self, query: Dict[str, Any]) -> Optional[str]:
        filters = query.get("filters") or {}

        variant_terms = self._coerce_list(query.get("variant_ids"))
        gene_terms = self._coerce_list(query.get("gene_terms"))
        disease_terms = self._coerce_list(query.get("disease_terms"))
        phenotype_terms = self._coerce_list(query.get("phenotype_terms"))
        keywords = self._coerce_list(query.get("keywords"))

        text = str(query.get("text") or query.get("query") or "").strip()
        if text:
            variant_terms.append(text)

        parts: List[str] = []

        if variant_terms:
            variants = self._unique_preserve_order(variant_terms)
            parts.append("(" + " OR ".join(self._quote_term(v) for v in variants) + ")")

        if gene_terms:
            genes = self._unique_preserve_order(gene_terms)
            parts.append("(" + " OR ".join(self._quote_term(g) for g in genes) + ")")

        disease_like_terms = self._unique_preserve_order(disease_terms + phenotype_terms)
        if disease_like_terms:
            parts.append("(" + " OR ".join(self._quote_term(d) for d in disease_like_terms) + ")")

        if keywords:
            for kw in self._unique_preserve_order(keywords):
                parts.append(self._quote_term(kw))

        # These are not field-qualified because the ClinVar programmatic docs guarantee
        # website-style query syntax, but do not provide a concise field guide on the
        # programmatic-access page itself.
        if filters.get("clinvar_significance"):
            parts.append(self._quote_term(str(filters["clinvar_significance"])))

        if filters.get("variant_review_status"):
            parts.append(self._quote_term(str(filters["variant_review_status"])))

        if not parts:
            return None

        return " AND ".join(parts)

    def _matches_filters(self, entity: NormalizedEntity, filters: Dict[str, Any]) -> bool:
        if not filters:
            return True

        description = (entity.description or "").lower()
        provenance = entity.provenance or {}

        if filters.get("clinvar_significance"):
            wanted = str(filters["clinvar_significance"]).strip().lower()
            classification = str(provenance.get("classification") or "").strip().lower()
            if wanted and wanted not in classification and wanted not in description:
                return False

        if filters.get("variant_review_status"):
            wanted = str(filters["variant_review_status"]).strip().lower()
            review_status = str(provenance.get("review_status") or "").strip().lower()
            if wanted and wanted not in review_status and wanted not in description:
                return False

        return True

    async def fetch_by_id(self, identifier: str) -> Any:
        raw = identifier.strip()
        if not raw:
            return None

        # Numeric ID may already be the ClinVar UID / variation-like identifier.
        if raw.isdigit():
            docs = await self._esummary([raw])
            if docs:
                return docs[0]

        ids = await self._esearch(raw, retmax=1)
        docs = await self._esummary(ids)
        return docs[0] if docs else None

    async def normalize(self, text: str) -> List[Dict[str, Any]]:
        query = text.strip()
        if not query:
            return []

        match_type = "text_search"

        if query.isdigit():
            docs = await self._esummary([query])
            if docs:
                entities = [
                    self._summary_to_entity(
                        doc,
                        query_text=query,
                        match_type="exact_variation_id",
                        rank=rank,
                    ).model_dump()
                    for rank, doc in enumerate(docs[:3])
                ]
                return entities

        if self._looks_like_accession(query):
            match_type = "exact_accession"
        elif self._looks_like_rsid(query):
            match_type = "exact_rsid"
        elif self._looks_like_hgvs(query):
            match_type = "exact_hgvs"

        ids = await self._esearch(query, retmax=3)
        docs = await self._esummary(ids)

        entities = [
            self._summary_to_entity(
                doc,
                query_text=query,
                match_type=match_type,
                rank=rank,
            ).model_dump()
            for rank, doc in enumerate(docs)
        ]
        return entities

    async def crosswalk(self, source_id: str) -> Dict[str, str]:
        doc = await self.fetch_by_id(source_id)
        if not doc:
            return {}

        uid = str(doc.get("_uid") or "")
        accessions = self._extract_accessions(doc)
        rsids = self._extract_rsids(doc)

        return self._build_source_ids(uid, accessions, rsids)

    async def search(self, query: Dict[str, Any]) -> Any:
        filters = query.get("filters") or {}
        term = self._build_search_term(query)
        if not term:
            return []

        retmax = int(filters.get("retmax", 10))
        ids = await self._esearch(term, retmax=retmax)
        docs = await self._esummary(ids)

        match_type = "combined_search"
        if not (
            query.get("gene_terms")
            or query.get("disease_terms")
            or query.get("phenotype_terms")
            or query.get("variant_ids")
        ):
            match_type = "text_search"

        entities: List[Dict[str, Any]] = []
        for rank, doc in enumerate(docs):
            entity = self._summary_to_entity(
                doc,
                query_text=term,
                match_type=match_type,
                rank=rank,
            )
            if self._matches_filters(entity, filters):
                entities.append(entity.model_dump())

        return entities

    async def health_check(self) -> bool:
        try:
            ids = await self._esearch("rs113488022", retmax=1)
            return bool(ids)
        except Exception:
            return False

    async def rate_limit_policy(self) -> Dict[str, Any]:
        return {
            "source": self.name,
            "notes": "Use moderate concurrency and prefer caching repeated ClinVar E-utilities queries.",
        }
