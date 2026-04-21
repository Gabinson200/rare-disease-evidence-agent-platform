"""Connector for HGNC gene normalization.

The HUGO Gene Nomenclature Committee (HGNC) provides authoritative
gene symbols and identifiers.  This stub implements a small mapping
from gene symbols to HGNC IDs and returns :class:`NormalizedEntity`
objects.  Real implementations would call the HGNC REST service
(`https://rest.genenames.org`) to resolve symbols and aliases【400†L5-L12】.
"""

from typing import Any, Dict, List

from ..models import NormalizedEntity, EntityType
from .base import BaseConnector


class HGNCConnector(BaseConnector):
    name = "hgnc"

    async def normalize(self, text: str) -> List[Dict[str, Any]]:
        """Normalize a gene symbol into HGNC identifiers.

        The stub maps a handful of symbols to HGNC IDs.  Unknown
        symbols produce an empty list.  In a real scenario, fallback
        synonyms from the HGNC API could return multiple candidates with
        confidence values.
        """
        symbol = text.upper().strip()
        lookup: Dict[str, Dict[str, str]] = {
            "ACVR1": {"hgnc": "171", "entrez": "90", "ensembl": "ENSG00000115170"},
            "CFTR": {"hgnc": "1884", "entrez": "1080", "ensembl": "ENSG00000001626"},
        }
        results: List[Dict[str, Any]] = []
        if symbol in lookup:
            ids = lookup[symbol]
            entity = NormalizedEntity(
                entity_type=EntityType.gene,
                preferred_label=symbol,
                source_ids={"hgnc": ids["hgnc"], "entrez": ids["entrez"], "ensembl": ids["ensembl"]},
                synonyms=[],
                confidence=0.99,
                description=None,
                provenance={"source": self.name, "method": "stub_lookup"},
            )
            results.append(entity.dict())
        return results

    async def search(self, query: Dict[str, Any]) -> Any:
        raise NotImplementedError("HGNC does not support literature search")