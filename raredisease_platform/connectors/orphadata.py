"""Connector for Orphadata / ORDO disease normalization.

The Orphadata API provides access to rare disease concepts, including
Orphanet disease codes and preferred terms.  This stub implements a
simple normalization routine that maps a small set of example disease
terms to static ORPHA identifiers.  In a full implementation, it would
perform HTTP requests to the Orphadata API, parse XML or JSON
responses, and assemble :class:`NormalizedEntity` objects with
provenance.【400†L5-L12】
"""

from typing import Any, Dict, List

from ..models import NormalizedEntity, EntityType
from .base import BaseConnector


class OrphadataConnector(BaseConnector):
    name = "orphadata"

    async def normalize(self, text: str) -> List[Dict[str, Any]]:
        """Normalize a disease name into ORPHA identifiers.

        The stub uses a small lookup table.  Unknown inputs return an
        empty list to indicate no confident match.  Real code would
        query Orphadata's API and may return multiple candidates with
        confidence scores【200†L18-L25】.
        """
        normalized: List[Dict[str, Any]] = []
        term = text.lower().strip()
        lookup: Dict[str, Dict[str, str]] = {
            "fibrodysplasia ossificans progressiva": {
                "preferred_label": "fibrodysplasia ossificans progressiva",
                "orpha": "337",
                "mondo": "MONDO:0007525",
                "synonyms": ["fop"],
            },
            "wilson disease": {
                "preferred_label": "wilson disease",
                "orpha": "905",
                "mondo": "MONDO:0009651",
                "synonyms": ["hepatolenticular degeneration"],
            },
        }
        if term in lookup:
            info = lookup[term]
            entity = NormalizedEntity(
                entity_type=EntityType.disease,
                preferred_label=info["preferred_label"],
                source_ids={"orpha": info["orpha"], "mondo": info["mondo"]},
                synonyms=info["synonyms"],
                confidence=0.99,
                description=None,
                provenance={"source": self.name, "method": "stub_lookup"},
            )
            normalized.append(entity.dict())
        return normalized

    async def search(self, query: Dict[str, Any]) -> Any:
        """Orphadata does not support arbitrary search of literature or evidence.

        This stub raises NotImplementedError to signal that the broker
        should not call ``search()`` on this connector.  Normalization
        should be used instead.
        """
        raise NotImplementedError("Orphadata does not support search")