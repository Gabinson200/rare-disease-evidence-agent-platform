"""Broker service orchestrating normalization, search, joining, and ranking.

The :class:`Broker` class coordinates calls to source connectors based on
user intent.  It exposes high‑level methods corresponding to the tools
specified in section 11 of the specification: `normalize_entities`,
`search_literature`, `search_structured_evidence`,
`assemble_evidence_graph`, and `generate_dossier`【800†L1-L14】.  Each
method returns the appropriate Pydantic model so that FastAPI can
perform automatic validation and serialization.

In this minimal implementation, the broker executes connector calls
sequentially and returns stubbed results.  Production versions should
perform concurrent I/O (e.g. using ``asyncio.gather``), apply query
planning based on expected entity types, and merge results via
identifier crosswalks【200†L18-L25】.
"""

import asyncio
from typing import Dict, List, Optional, Any, Sequence

from .models import (
    EntityType,
    NormalizedEntity,
    NormalizationResponse,
    LiteratureResult,
    StructuredEvidenceResult,
    EvidenceGraph,
    Dossier,
    PubMedSearchFilters,
)
from .connectors import get_connector, CONNECTOR_REGISTRY


class Broker:
    """Core orchestrator for the evidence retrieval platform."""

    def __init__(self) -> None:
        # instantiate connectors lazily via registry
        self.connectors = CONNECTOR_REGISTRY

    async def normalize_entities(
        self,
        raw_query: str,
        expected_entity_types: Optional[List[EntityType]] = None,
        disambiguation_preferences: Optional[Dict[str, Any]] = None,
    ) -> NormalizationResponse:
        normalized_entities: List[NormalizedEntity] = []
        alternative_candidates: List[NormalizedEntity] = []

        connectors: Sequence[str]
        if expected_entity_types:
            connectors = []
            for etype in expected_entity_types:
                if etype == EntityType.disease:
                    connectors.append("orphadata")
                elif etype == EntityType.gene:
                    connectors.append("hgnc")
                elif etype == EntityType.phenotype:
                    connectors.append("hpo")
                elif etype == EntityType.variant:
                    connectors.append("clinvar")
                elif etype == EntityType.compound:
                    connectors.append("pubchem")
                elif etype == EntityType.trial:
                    connectors.append("clinicaltrials")
        else:
            connectors = [
                "orphadata",
                "hgnc",
                "hpo",
                "clinvar",
                "pubchem",
                "clinicaltrials",
            ]

        async def call_normalizer(name: str) -> List[Dict[str, Any]]:
            connector = get_connector(name)
            try:
                return await connector.normalize(raw_query)
            except NotImplementedError:
                return []

        tasks = [call_normalizer(name) for name in connectors]
        results = await asyncio.gather(*tasks)

        for connector_results in results:
            for record in connector_results:
                try:
                    entity = NormalizedEntity.model_validate(record)
                    normalized_entities.append(entity)
                except Exception:
                    try:
                        entity_alt = NormalizedEntity.model_validate(record)
                        alternative_candidates.append(entity_alt)
                    except Exception:
                        continue

        return NormalizationResponse(
            entities=normalized_entities,
            alternatives=alternative_candidates or None,
        )

    async def search_literature(
        self,
        disease_ids: Optional[List[str]] = None,
        gene_ids: Optional[List[str]] = None,
        phenotype_ids: Optional[List[str]] = None,
        compound_ids: Optional[List[str]] = None,
        keywords: Optional[str] = None,
        filters: Optional[PubMedSearchFilters | Dict[str, Any]] = None,
    ) -> List[LiteratureResult]:
        if isinstance(filters, PubMedSearchFilters):
            filter_payload = filters.model_dump(exclude_none=True)
        else:
            filter_payload = filters or {}

        query: Dict[str, Any] = {
            "disease_ids": disease_ids,
            "gene_ids": gene_ids,
            "phenotype_ids": phenotype_ids,
            "compound_ids": compound_ids,
            "keywords": keywords,
            "filters": filter_payload,
        }

        pubmed = get_connector("pubmed")
        return await pubmed.search(query)

    async def search_structured_evidence(
        self,
        normalized_bundle: NormalizationResponse,
        requested_evidence_types: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> StructuredEvidenceResult:
        """Retrieve non‑literature evidence from structured sources.

        This stub uses the normalized entities to organize them by type
        and returns them directly.  A full implementation would look up
        related genes, variants, phenotypes, compounds, and trials
        through each respective connector, apply crosswalks, and build
        relationship objects【800†L1-L14】.
        """
        # Organize normalized entities by type
        by_type: Dict[EntityType, List[NormalizedEntity]] = {}
        for entity in normalized_bundle.entities:
            by_type.setdefault(entity.entity_type, []).append(entity)

        result = StructuredEvidenceResult(
            genes=by_type.get(EntityType.gene),
            variants=by_type.get(EntityType.variant),
            phenotypes=by_type.get(EntityType.phenotype),
            compounds=by_type.get(EntityType.compound),
            trials=by_type.get(EntityType.trial),
            relationships=[],  # relationships are not generated in stub
        )
        return result

    async def assemble_evidence_graph(
        self,
        normalized_bundle: NormalizationResponse,
        literature_results: Sequence[LiteratureResult],
        structured_evidence_results: StructuredEvidenceResult,
        scoring_profile: Optional[str] = None,
    ) -> EvidenceGraph:
        """Merge and score evidence across sources into an evidence graph.

        The stub simply aggregates all normalized nodes from the
        structured evidence result and attaches no edges or rankings.  A
        production version would create nodes for articles, genes,
        variants, etc., connect them via relationships, and compute
        ranked summaries【800†L1-L14】.
        """
        nodes: List[NormalizedEntity] = []
        nodes.extend(structured_evidence_results.genes or [])
        nodes.extend(structured_evidence_results.variants or [])
        nodes.extend(structured_evidence_results.phenotypes or [])
        nodes.extend(structured_evidence_results.compounds or [])
        nodes.extend(structured_evidence_results.trials or [])
        # Add article nodes
        for art in literature_results:
            node = NormalizedEntity(
                entity_type=EntityType.article,
                preferred_label=art.title,
                source_ids={"pmid": art.pmid or ""},
                synonyms=[],
                description=art.abstract,
                confidence=art.score,
                provenance={"source": art.provenance.source},
            )
            nodes.append(node)
        graph = EvidenceGraph(
            nodes=nodes,
            edges=[],
            ranked_summaries=["Stub evidence graph with no edges"],
            explanation={"note": "No ranking applied in this stub"},
        )
        return graph

    async def generate_dossier(
        self,
        primary_entity: NormalizedEntity,
        scope: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        output_profile: Optional[str] = None,
    ) -> Dossier:
        normalized_bundle = NormalizationResponse(entities=[primary_entity])
        structured = await self.search_structured_evidence(normalized_bundle)

        literature = await self.search_literature(
            disease_ids=[primary_entity.source_ids["orpha"]]
            if primary_entity.entity_type == EntityType.disease and "orpha" in primary_entity.source_ids
            else None,
            gene_ids=[primary_entity.source_ids["hgnc"]]
            if primary_entity.entity_type == EntityType.gene and "hgnc" in primary_entity.source_ids
            else None,
            compound_ids=[primary_entity.source_ids["pubchem"]]
            if primary_entity.entity_type == EntityType.compound and "pubchem" in primary_entity.source_ids
            else None,
        )

        graph = await self.assemble_evidence_graph(
            normalized_bundle, literature, structured, scoring_profile=output_profile
        )

        summary = [
            f"Dossier for {primary_entity.preferred_label} (type: {primary_entity.entity_type})",
            "Number of evidence nodes: " + str(len(graph.nodes)),
        ]

        return Dossier(
            primary_entity=primary_entity,
            scope=scope,
            summary_blocks=summary,
            citation_references=[f"PMID:{art.pmid}" for art in literature if art.pmid],
            evidence_graph=graph,
        )
