"""Connector for the Human Phenotype Ontology (HPO).

This stub normalizes phenotype descriptions into HPO terms by matching
known labels.  Real implementations would query the HPO API or
download the OBO file to resolve terms and synonyms【400†L5-L12】.
"""

from typing import Any, Dict, List

from ..models import NormalizedEntity, EntityType
from .base import BaseConnector


class HPOConnector(BaseConnector):
    name = "hpo"

    async def normalize(self, text: str) -> List[Dict[str, Any]]:
        term = text.lower().strip()
        lookup: Dict[str, Dict[str, str]] = {
            "short stature": {"hpo_id": "HP:0004322", "mesh": "D013106"},
            "hearing loss": {"hpo_id": "HP:0000365", "mesh": "D021578"},
        }
        results: List[Dict[str, Any]] = []
        if term in lookup:
            ids = lookup[term]
            entity = NormalizedEntity(
                entity_type=EntityType.phenotype,
                preferred_label=term,
                source_ids={"hpo": ids["hpo_id"], "mesh": ids["mesh"]},
                synonyms=[],
                confidence=0.99,
                description=None,
                provenance={"source": self.name, "method": "stub_lookup"},
            )
            results.append(entity.dict())
        return results

    async def search(self, query: Dict[str, Any]) -> Any:
        raise NotImplementedError("HPO is not used for literature search in this stub")