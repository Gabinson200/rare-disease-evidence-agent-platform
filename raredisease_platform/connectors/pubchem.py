"""Connector for PubChem compound normalization using PubChem PUG REST."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import httpx

from ..models import EntityType, NormalizedEntity
from .base import BaseConnector


class PubChemConnector(BaseConnector):
    """Connector for PubChem compound normalization and identifier lookup."""

    name = "pubchem"
    BASE_URL = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"

    INCHIKEY_RE = re.compile(r"^[A-Z]{14}-[A-Z]{10}-[A-Z]$")

    @staticmethod
    def _encode(value: str) -> str:
        return quote(value.strip(), safe="")

    @staticmethod
    def _is_inchikey(value: str) -> bool:
        return bool(PubChemConnector.INCHIKEY_RE.match(value.strip().upper()))

    async def _get_json(self, path: str) -> Optional[Dict[str, Any]]:
        """
        Return parsed JSON for a PubChem PUG REST path.

        Returns None on 404 / no-hit conditions, raises on other HTTP errors.
        """
        async with httpx.AsyncClient(
            timeout=20.0,
            headers={"User-Agent": "rare-disease-evidence-platform/0.1"},
        ) as client:
            response = await client.get(f"{self.BASE_URL}{path}")

        if response.status_code == 404:
            return None

        response.raise_for_status()
        return response.json()

    async def _resolve_name_to_cids(self, name: str) -> List[str]:
        payload = await self._get_json(
            f"/compound/name/{self._encode(name)}/cids/JSON"
        )
        if not payload:
            return []
        cids = payload.get("IdentifierList", {}).get("CID", []) or []
        return [str(cid) for cid in cids]

    async def _resolve_inchikey_to_cids(self, inchikey: str) -> List[str]:
        payload = await self._get_json(
            f"/compound/inchikey/{self._encode(inchikey.upper())}/cids/JSON"
        )
        if not payload:
            return []
        cids = payload.get("IdentifierList", {}).get("CID", []) or []
        return [str(cid) for cid in cids]

    async def _get_properties(self, cid: str) -> Dict[str, Any]:
        payload = await self._get_json(
            f"/compound/cid/{cid}/property/"
            "Title,IUPACName,InChIKey,CanonicalSMILES,MolecularFormula/JSON"
        )
        if not payload:
            return {}
        props = payload.get("PropertyTable", {}).get("Properties", []) or []
        return props[0] if props else {}

    async def _get_synonyms(self, cid: str) -> List[str]:
        payload = await self._get_json(f"/compound/cid/{cid}/synonyms/JSON")
        if not payload:
            return []
        info = payload.get("InformationList", {}).get("Information", []) or []
        if not info:
            return []
        synonyms = info[0].get("Synonym", []) or []
        return [str(s) for s in synonyms[:20]]

    def _build_source_ids(self, cid: str, props: Dict[str, Any]) -> Dict[str, str]:
        source_ids: Dict[str, str] = {"pubchem": str(cid)}

        inchikey = props.get("InChIKey")
        if inchikey:
            source_ids["inchikey"] = str(inchikey)

        smiles = props.get("CanonicalSMILES")
        if smiles:
            source_ids["smiles"] = str(smiles)

        return source_ids

    def _clean_synonyms(
        self,
        preferred_label: str,
        description: Optional[str],
        synonyms: List[str],
    ) -> List[str]:
        seen = {preferred_label.strip().lower()}
        if description:
            seen.add(description.strip().lower())

        cleaned: List[str] = []
        for synonym in synonyms:
            key = synonym.strip().lower()
            if not key or key in seen:
                continue
            cleaned.append(synonym)
            seen.add(key)
        return cleaned

    def _confidence(self, match_type: str, rank: int = 0) -> float:
        base = {
            "exact_cid": 0.995,
            "exact_inchikey": 0.99,
            "name_lookup": 0.97,
            "ambiguous_name_lookup": 0.90,
        }.get(match_type, 0.80)

        # If a name resolves to multiple candidates, confidence decays slightly by rank.
        if rank > 0:
            base -= min(0.15, 0.03 * rank)

        return max(0.0, min(1.0, base))

    def _cid_to_entity(
        self,
        cid: str,
        props: Dict[str, Any],
        synonyms: List[str],
        *,
        match_type: str,
        query_text: str,
        rank: int = 0,
    ) -> NormalizedEntity:
        preferred_label = (
            str(props.get("Title"))
            if props.get("Title")
            else (synonyms[0] if synonyms else query_text.strip())
        )
        description = props.get("IUPACName")
        cleaned_synonyms = self._clean_synonyms(preferred_label, description, synonyms)

        return NormalizedEntity(
            entity_type=EntityType.compound,
            preferred_label=preferred_label,
            source_ids=self._build_source_ids(cid, props),
            synonyms=cleaned_synonyms,
            description=str(description) if description else None,
            confidence=self._confidence(match_type, rank=rank),
            provenance={
                "source": self.name,
                "method": "pubchem_pug_rest",
                "match_type": match_type,
                "query_text": query_text,
                "cid": str(cid),
                "url": f"{self.BASE_URL}/compound/cid/{cid}/property/Title,IUPACName,InChIKey,CanonicalSMILES,MolecularFormula/JSON",
            },
        )

    async def fetch_by_id(self, identifier: str) -> Any:
        """
        Fetch a PubChem record by CID when possible.
        """
        raw = identifier.strip()
        cids: List[str]

        if raw.isdigit():
            cids = [raw]
        elif self._is_inchikey(raw):
            cids = await self._resolve_inchikey_to_cids(raw)
        else:
            cids = await self._resolve_name_to_cids(raw)

        if not cids:
            return None

        cid = cids[0]
        props = await self._get_properties(cid)
        synonyms = await self._get_synonyms(cid)

        return {
            "cid": cid,
            "properties": props,
            "synonyms": synonyms,
        }

    async def normalize(self, text: str) -> List[Dict[str, Any]]:
        """
        Normalize a free-text compound query into one or more PubChem-backed entities.

        Supported input styles:
        - PubChem CID, e.g. '5743'
        - InChIKey, e.g. 'YSFXBKZQMRNKDQ-UHFFFAOYSA-N'
        - compound name / synonym, e.g. 'dexamethasone'
        """
        query = text.strip()
        if not query:
            return []

        match_type: str
        cids: List[str]

        if query.isdigit():
            cids = [query]
            match_type = "exact_cid"
        elif self._is_inchikey(query):
            cids = await self._resolve_inchikey_to_cids(query)
            match_type = "exact_inchikey"
        else:
            cids = await self._resolve_name_to_cids(query)
            match_type = "name_lookup" if len(cids) <= 1 else "ambiguous_name_lookup"

        if not cids:
            return []

        # Keep only the top few candidates for ambiguous name lookups.
        cids = cids[:3]

        entities: List[Dict[str, Any]] = []
        for rank, cid in enumerate(cids):
            props = await self._get_properties(cid)
            synonyms = await self._get_synonyms(cid)

            entity = self._cid_to_entity(
                cid,
                props,
                synonyms,
                match_type=match_type,
                query_text=query,
                rank=rank,
            )
            entities.append(entity.model_dump())

        return entities

    async def crosswalk(self, source_id: str) -> Dict[str, str]:
        """
        Return identifier crosswalks for a PubChem-backed compound.
        """
        record = await self.fetch_by_id(source_id)
        if not record:
            return {}

        cid = str(record["cid"])
        props = record.get("properties", {}) or {}

        out: Dict[str, str] = {"pubchem": cid}

        inchikey = props.get("InChIKey")
        if inchikey:
            out["inchikey"] = str(inchikey)

        smiles = props.get("CanonicalSMILES")
        if smiles:
            out["smiles"] = str(smiles)

        return out

    async def search(self, query: Dict[str, Any]) -> Any:
        """
        Minimal search interface for compound lookup.

        Supported shapes:
        - {"text": "dexamethasone"}
        - {"query": "dexamethasone"}
        - {"identifier": "5743"}
        """
        text = (
            query.get("text")
            or query.get("query")
            or query.get("identifier")
            or ""
        ).strip()

        if not text:
            return []

        return await self.normalize(text)

    async def health_check(self) -> bool:
        try:
            payload = await self._get_json("/compound/cid/2244/property/Title/JSON")
            return payload is not None
        except Exception:
            return False

    async def rate_limit_policy(self) -> Dict[str, Any]:
        return {
            "source": self.name,
            "notes": "Use moderate request concurrency and prefer caching for repeated CID/property lookups.",
        }
