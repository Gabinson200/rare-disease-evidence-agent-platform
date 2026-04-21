"""Connector for ClinicalTrials.gov intervention study retrieval.

This stub normalizes NCT IDs and returns placeholder trial records.  It
does not perform full search or filtering.  Real implementations
should query the ClinicalTrials.gov REST API (e.g. via the `clinicaltrials.gov` NIH service)
to fetch trial metadata by disease, compound, or gene【400†L5-L12】.
"""

from typing import Any, Dict, List

from ..models import NormalizedEntity, EntityType
from .base import BaseConnector


class ClinicalTrialsConnector(BaseConnector):
    name = "clinicaltrials"

    async def normalize(self, text: str) -> List[Dict[str, Any]]:
        # Accept trial identifiers and return a trial entity
        trial_id = text.strip().upper()
        if trial_id.startswith("NCT"):
            entity = NormalizedEntity(
                entity_type=EntityType.trial,
                preferred_label=trial_id,
                source_ids={"nct": trial_id},
                synonyms=[],
                confidence=0.9,
                description=None,
                provenance={"source": self.name, "method": "stub_lookup"},
            )
            return [entity.dict()]
        return []

    async def search(self, query: Dict[str, Any]) -> Any:
        raise NotImplementedError("ClinicalTrials search not implemented in this stub")