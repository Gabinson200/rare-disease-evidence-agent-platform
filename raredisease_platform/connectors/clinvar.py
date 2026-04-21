"""Connector for ClinVar variant evidence.

ClinVar provides public records of variants and their clinical
interpretations.  This stub implements normalization of a limited set
of HGVS strings into variation IDs.  Searching ClinVar for evidence
about disease associations is not implemented here but could be
expanded in future iterations【400†L5-L12】.
"""

from typing import Any, Dict, List

from ..models import NormalizedEntity, EntityType
from .base import BaseConnector


class ClinVarConnector(BaseConnector):
    name = "clinvar"

    async def normalize(self, text: str) -> List[Dict[str, Any]]:
        variant = text.strip().lower()
        lookup: Dict[str, Dict[str, str]] = {
            "c.617g>a": {"clinvar": "12345", "dbsnp": "rs28937515"},
            "c.1521_1523delctt": {"clinvar": "17661", "dbsnp": "rs113993960"},
        }
        results: List[Dict[str, Any]] = []
        if variant in lookup:
            ids = lookup[variant]
            entity = NormalizedEntity(
                entity_type=EntityType.variant,
                preferred_label=variant,
                source_ids={"clinvar": ids["clinvar"], "dbsnp": ids["dbsnp"]},
                synonyms=[],
                confidence=0.95,
                description=None,
                provenance={"source": self.name, "method": "stub_lookup"},
            )
            results.append(entity.dict())
        return results

    async def search(self, query: Dict[str, Any]) -> Any:
        raise NotImplementedError("ClinVar search not implemented in this stub")