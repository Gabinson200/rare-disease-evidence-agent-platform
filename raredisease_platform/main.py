from typing import Any, Dict, List, Optional

from fastapi import FastAPI

from .broker import Broker
from .models import (
    Dossier,
    EvidenceGraph,
    GeneCrosswalkRequest,
    GeneLookupRequest,
    LiteratureResult,
    LiteratureSearchRequest,
    NormalizationResponse,
    NormalizedEntity,
    NormalizeRequest,
    StructuredEvidenceResult,
)

app = FastAPI(
    title="Rare Disease Evidence Retrieval Platform",
    version="0.1",
    description="APIs for normalizing and retrieving biomedical evidence related to rare diseases.",
)

broker = Broker()


@app.get("/")
async def root():
    return {
        "message": "Rare Disease Evidence Retrieval Platform API",
        "docs": "/docs",
    }


@app.post("/normalize", response_model=NormalizationResponse)
async def normalize_entities(request: NormalizeRequest) -> NormalizationResponse:
    return await broker.normalize_entities(
        raw_query=request.raw_query,
        expected_entity_types=request.expected_entity_types,
        disambiguation_preferences=request.disambiguation_preferences,
    )


@app.post("/normalize/gene", response_model=NormalizationResponse)
async def normalize_gene(request: GeneLookupRequest) -> NormalizationResponse:
    return await broker.normalize_gene(raw_gene=request.raw_gene)


@app.post("/genes/crosswalk", response_model=Dict[str, str])
async def crosswalk_gene_identifier(request: GeneCrosswalkRequest) -> Dict[str, str]:
    return await broker.crosswalk_gene_identifier(
        identifier=request.identifier,
        namespace=request.namespace,
    )


@app.post("/search_literature", response_model=List[LiteratureResult])
async def search_literature(request: LiteratureSearchRequest) -> List[LiteratureResult]:
    return await broker.search_literature(
        disease_ids=request.disease_ids,
        gene_ids=request.gene_ids,
        phenotype_ids=request.phenotype_ids,
        compound_ids=request.compound_ids,
        keywords=request.keywords,
        filters=request.filters,
    )


@app.post("/search_structured", response_model=StructuredEvidenceResult)
async def search_structured_evidence(
    normalized_bundle: NormalizationResponse,
    requested_evidence_types: Optional[List[str]] = None,
    filters: Optional[Dict[str, Any]] = None,
) -> StructuredEvidenceResult:
    return await broker.search_structured_evidence(
        normalized_bundle=normalized_bundle,
        requested_evidence_types=requested_evidence_types,
        filters=filters,
    )


@app.post("/assemble_graph", response_model=EvidenceGraph)
async def assemble_evidence_graph(
    normalized_bundle: NormalizationResponse,
    literature_results: List[LiteratureResult],
    structured_evidence_results: StructuredEvidenceResult,
    scoring_profile: Optional[str] = None,
) -> EvidenceGraph:
    return await broker.assemble_evidence_graph(
        normalized_bundle=normalized_bundle,
        literature_results=literature_results,
        structured_evidence_results=structured_evidence_results,
        scoring_profile=scoring_profile,
    )


@app.post("/generate_dossier", response_model=Dossier)
async def generate_dossier(
    primary_entity: NormalizedEntity,
    scope: Optional[str] = None,
    filters: Optional[Dict[str, Any]] = None,
    output_profile: Optional[str] = None,
) -> Dossier:
    return await broker.generate_dossier(
        primary_entity=primary_entity,
        scope=scope,
        filters=filters,
        output_profile=output_profile,
    )
