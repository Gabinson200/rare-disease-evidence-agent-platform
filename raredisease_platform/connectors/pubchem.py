"""Connector for PubChem compound normalization.

This stub matches a small set of compound names to PubChem CIDs and
InChIKeys.  It does not perform chemical similarity searches or
structure normalization.  Real implementations would query the PubChem
PUG REST API to resolve names and synonyms【400†L5-L12】.
"""

from typing import Any, Dict, List

from ..models import NormalizedEntity, EntityType
from .base import BaseConnector


class PubChemConnector(BaseConnector):
    name = "pubchem"

    async def normalize(self, text: str) -> List[Dict[str, Any]]:
        name = text.lower().strip()
        lookup: Dict[str, Dict[str, str]] = {
            "alpelisib": {"cid": "67300054", "inchikey": "URDVQCFDWDTKOM-UHFFFAOYSA-N"},
            "dexamethasone": {"cid": "5743", "inchikey": "YSFXBKZQMRNKDQ-UHFFFAOYSA-N"},
        }
        results: List[Dict[str, Any]] = []
        if name in lookup:
            ids = lookup[name]
            entity = NormalizedEntity(
                entity_type=EntityType.compound,
                preferred_label=name,
                source_ids={"pubchem": ids["cid"], "inchikey": ids["inchikey"]},
                synonyms=[],
                confidence=0.98,
                description=None,
                provenance={"source": self.name, "method": "stub_lookup"},
            )
            results.append(entity.dict())
        return results

    async def search(self, query: Dict[str, Any]) -> Any:
        raise NotImplementedError("PubChem search not implemented in this stub")