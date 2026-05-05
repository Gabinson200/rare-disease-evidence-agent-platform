"""Rule-based query planner for the first agent version.

This planner converts user text into the broker's EvidenceQueryRequest shape.
It is deliberately conservative. The broker still performs actual normalization,
source querying, joining, ranking, and tracing.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


class EvidenceQueryPlanner:
    """Map natural-language user requests to /evidence/query payloads."""

    CASE_REPORT_PATTERNS = [
        r"\bcase report\b",
        r"\bcase reports\b",
        r"\bcase series\b",
    ]

    REVIEW_PATTERNS = [
        r"\breview\b",
        r"\bsystematic review\b",
        r"\bliterature review\b",
    ]

    TRIAL_PATTERNS = [
        r"\bclinical trial\b",
        r"\btrial\b",
        r"\btrials\b",
        r"\brecruiting\b",
    ]

    COMPOUND_CONTEXT_PATTERNS = [
        r"\bdrug\b",
        r"\bcompound\b",
        r"\bintervention\b",
        r"\btreatment\b",
        r"\btherapy\b",
        r"\btreated with\b",
    ]

    PHENOTYPE_CONTEXT_PATTERNS = [
        r"\bsymptom\b",
        r"\bsymptoms\b",
        r"\bphenotype\b",
        r"\bphenotypes\b",
        r"\bfeatures\b",
        r"\bclinical features\b",
        r"\bheterotopic ossification\b",
        r"\bseizures\b",
        r"\bweakness\b",
        r"\bshort stature\b",
        r"\bdevelopmental delay\b",
    ]

    VARIANT_CONTEXT_PATTERNS = [
        r"\bvariant\b",
        r"\bvariants\b",
        r"\bmutation\b",
        r"\bmutations\b",
        r"\bpathogenic\b",
        r"\bclinvar\b",
        r"\brs\d+\b",
        r"\b(?:c|g|m|n|p|r)\.[A-Za-z0-9_>*?+\-delinsdup]+\b",
    ]

    GENE_ID_PATTERNS = [
        r"\bHGNC:\d+\b",
        r"\bENSG\d{8,}\b",
    ]

    TRIAL_ID_PATTERNS = [
        r"\bNCT\d{8}\b",
    ]

    GENE_SYMBOL_PATTERN = r"\b[A-Z][A-Z0-9-]{1,10}\b"

    COMMON_NON_GENE_TOKENS = {
        "AND",
        "OR",
        "THE",
        "FOR",
        "WITH",
        "CASE",
        "CASES",
        "REPORT",
        "REPORTS",
        "REVIEW",
        "REVIEWS",
        "RARE",
        "DISEASE",
        "DISEASES",
        "GENE",
        "GENES",
        "DRUG",
        "DRUGS",
        "TRIAL",
        "TRIALS",
        "SHOW",
        "FIND",
        "ME",
        "IN",
        "OF",
        "ON",
        "TO",
        "BY",
        "FROM",
    }

    def plan(
        self,
        user_query: str,
        *,
        include_structured_evidence: Optional[bool] = None,
        retmax: int = 10,
    ) -> Dict[str, Any]:
        """Create a broker /evidence/query payload from a user query."""
        query = user_query.strip()
        lower_query = query.lower()

        expected_entity_types = self._infer_expected_entity_types(query)
        literature_keywords = self._infer_literature_keywords(lower_query)
        literature_filters = self._infer_literature_filters(lower_query, retmax=retmax)
        requested_evidence_types = self._infer_requested_evidence_types(
            expected_entity_types,
            lower_query,
        )

        if include_structured_evidence is None:
            include_structured_evidence = True

        return {
            "raw_query": query,
            "expected_entity_types": expected_entity_types or None,
            "literature_keywords": literature_keywords,
            "literature_filters": literature_filters,
            "include_structured_evidence": include_structured_evidence,
            "requested_evidence_types": requested_evidence_types or None,
        }

    def _infer_expected_entity_types(self, query: str) -> List[str]:
        lower_query = query.lower()
        entity_types: List[str] = []

        if self._matches_any(query, self.GENE_ID_PATTERNS) or self._has_gene_like_symbol(query):
            entity_types.append("gene")

        if self._matches_any(query, self.VARIANT_CONTEXT_PATTERNS):
            entity_types.append("variant")

        if self._matches_any(query, self.COMPOUND_CONTEXT_PATTERNS):
            entity_types.append("compound")

        if self._matches_any(query, self.TRIAL_ID_PATTERNS) or self._matches_any(lower_query, self.TRIAL_PATTERNS):
            entity_types.append("trial")

        if self._matches_any(lower_query, self.PHENOTYPE_CONTEXT_PATTERNS):
            entity_types.append("phenotype")

        if self._looks_disease_or_rare_disease_query(lower_query, entity_types):
            entity_types.append("disease")

        # If the query includes a phrase plus a gene, disease is often the missing type.
        # Example: "case reports for fibrodysplasia ossificans progressiva involving ACVR1"
        if (
            "gene" in entity_types
            and "compound" not in entity_types
            and "phenotype" not in entity_types
            and "trial" not in entity_types
            and self._has_long_lowercase_phrase(query)
        ):
            entity_types.append("disease")

        return self._dedupe(entity_types)

    def _infer_literature_keywords(self, lower_query: str) -> Optional[str]:
        keywords: List[str] = []

        if self._matches_any(lower_query, self.CASE_REPORT_PATTERNS):
            keywords.append("case report")

        if self._matches_any(lower_query, self.REVIEW_PATTERNS):
            keywords.append("review")

        if self._matches_any(lower_query, self.TRIAL_PATTERNS):
            keywords.append("clinical trial")

        if "mechanism" in lower_query or "pathway" in lower_query:
            keywords.append("mechanism")

        if "rare disease" in lower_query:
            keywords.append("rare disease")

        return " ".join(self._dedupe(keywords)) or None

    def _infer_literature_filters(self, lower_query: str, *, retmax: int) -> Dict[str, Any]:
        filters: Dict[str, Any] = {
            "retmax": retmax,
        }

        if self._matches_any(lower_query, self.CASE_REPORT_PATTERNS):
            filters["case_reports_only"] = True

        if self._matches_any(lower_query, self.REVIEW_PATTERNS):
            filters["reviews_only"] = True

        if self._matches_any(lower_query, self.TRIAL_PATTERNS):
            filters["trials_only"] = True

        if "exact gene" in lower_query or "involving" in lower_query:
            filters["exact_gene_required"] = True

        if "exact disease" in lower_query:
            filters["exact_disease_required"] = True

        if "exact compound" in lower_query:
            filters["exact_compound_required"] = True

        if "case report" in lower_query and "involving" in lower_query:
            filters["exact_gene_required"] = True

        return filters

    def _infer_requested_evidence_types(
        self,
        expected_entity_types: List[str],
        lower_query: str,
    ) -> List[str]:
        requested: List[str] = []

        if "gene" in expected_entity_types:
            requested.extend(["genes", "diseases", "variants", "relationships"])

        if "disease" in expected_entity_types:
            requested.extend(["genes", "phenotypes", "variants", "trials", "relationships"])

        if "phenotype" in expected_entity_types:
            requested.extend(["phenotypes", "diseases", "genes", "relationships"])

        if "compound" in expected_entity_types:
            requested.extend(["compounds", "trials", "genes", "relationships"])

        if "variant" in expected_entity_types:
            requested.extend(["variants", "genes", "diseases", "relationships"])

        if "trial" in expected_entity_types or self._matches_any(lower_query, self.TRIAL_PATTERNS):
            requested.extend(["trials", "compounds", "diseases"])

        return self._dedupe(requested)

    def _looks_disease_or_rare_disease_query(
        self,
        lower_query: str,
        entity_types: List[str],
    ) -> bool:
        if "disease" in lower_query or "rare disease" in lower_query:
            return True

        if "case report" in lower_query and "compound" not in entity_types:
            return True

        if "orpha" in lower_query or "mondo" in lower_query:
            return True

        return False

    def _has_gene_like_symbol(self, query: str) -> bool:
        for match in re.finditer(self.GENE_SYMBOL_PATTERN, query):
            token = match.group(0)
            if token not in self.COMMON_NON_GENE_TOKENS:
                return True
        return False

    def _has_long_lowercase_phrase(self, query: str) -> bool:
        cleaned = re.sub(self.GENE_SYMBOL_PATTERN, " ", query)
        cleaned = re.sub(
            r"\b(case|cases|report|reports|review|find|show|me|for|about|with|involving|and|or|the|a|an|gene|compound|drug|trial)\b",
            " ",
            cleaned,
            flags=re.IGNORECASE,
        )
        words = [w for w in re.split(r"\s+", cleaned.strip()) if len(w) > 2]
        return len(words) >= 2

    def _matches_any(self, text: str, patterns: List[str]) -> bool:
        return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns)

    def _dedupe(self, items: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for item in items:
            if item not in seen:
                seen.add(item)
                out.append(item)
        return out
