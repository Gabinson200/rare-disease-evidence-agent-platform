from typing import Any, Dict, List, Optional

from fastapi import Body, FastAPI
from fastapi.middleware.cors import CORSMiddleware

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

APP_DESCRIPTION = """
APIs for normalizing and retrieving biomedical evidence related to rare diseases.

## What this service does

This API is designed to help an agent or application:

- normalize raw biomedical text into canonical entities such as diseases and genes
- search PubMed using normalized identifiers instead of raw keywords alone
- retrieve structured evidence objects for downstream graph assembly
- build a lightweight evidence graph and dossier object for summarization

## Recommended usage flow

For most workflows, use the endpoints in this order:

1. **POST `/normalize`** or **POST `/normalize/gene`**
2. **POST `/search_literature`**
3. **POST `/search_structured`**
4. **POST `/assemble_graph`**
5. **POST `/generate_dossier`**

## Quick examples

### Normalize a mixed biomedical query

```json
{
  "raw_query": "fibrodysplasia ossificans progressiva ACVR1",
  "expected_entity_types": ["disease", "gene"]
}
```

### Normalize a gene directly with HGNC

```json
{
  "raw_gene": "ACVR1"
}
```

### Crosswalk a gene identifier

```json
{
  "identifier": "HGNC:171",
  "namespace": "hgnc_id"
}
```

### Search PubMed

```json
{
  "disease_ids": ["ORPHA:337"],
  "gene_ids": ["HGNC:171"],
  "keywords": "case report",
  "filters": {
    "case_reports_only": true,
    "retmax": 5
  }
}
```
"""

app = FastAPI(
    title="Rare Disease Evidence Retrieval Platform",
    version="0.1",
    description=APP_DESCRIPTION,
    openapi_tags=[
        {"name": "normalization", "description": "Resolve raw text into canonical biomedical entities."},
        {"name": "genes", "description": "HGNC-backed gene-specific normalization and identifier crosswalk operations."},
        {"name": "literature", "description": "PubMed literature search over normalized disease, gene, phenotype, and compound identifiers."},
        {"name": "evidence", "description": "Structured evidence retrieval and evidence graph assembly."},
        {"name": "dossiers", "description": "Generate a dossier object around a primary entity for downstream summarization."},
    ],
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5500",
        "http://localhost:5500",
        "null",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

broker = Broker()


@app.get("/", summary="Service status")
async def root():
    return {
        "message": "Rare Disease Evidence Retrieval Platform API",
        "docs": "/docs",
    }


@app.post(
    "/normalize",
    response_model=NormalizationResponse,
    tags=["normalization"],
    summary="Normalize a general biomedical query",
    description=(
        "Resolve raw query text into one or more canonical entities. "
        "This is the broad entry point for mixed queries that may contain diseases, genes, "
        "phenotypes, variants, compounds, or trials.\n\n"
        "**Example use cases**\n"
        "- 'fibrodysplasia ossificans progressiva ACVR1'\n"
        "- 'case reports for CFTR'\n"
        "- 'rare disease with these symptoms'"
    ),
)
async def normalize_entities(
    request: NormalizeRequest = Body(
        ...,
        openapi_examples={
            "mixed_disease_gene": {
                "summary": "Mixed disease and gene query",
                "value": {
                    "raw_query": "fibrodysplasia ossificans progressiva ACVR1",
                    "expected_entity_types": ["disease", "gene"],
                },
            },
            "gene_only": {
                "summary": "Single gene query",
                "value": {
                    "raw_query": "CFTR",
                    "expected_entity_types": ["gene"],
                },
            },
        },
    )
) -> NormalizationResponse:
    return await broker.normalize_entities(
        raw_query=request.raw_query,
        expected_entity_types=request.expected_entity_types,
        disambiguation_preferences=request.disambiguation_preferences,
    )


@app.post(
    "/normalize/gene",
    response_model=NormalizationResponse,
    tags=["genes"],
    summary="Normalize a gene using HGNC",
    description=(
        "Resolve a raw gene string through the HGNC connector. "
        "This endpoint is useful when you already know the input should refer to a human gene.\n\n"
        "It can handle inputs such as approved symbols, HGNC IDs, Entrez IDs, Ensembl gene IDs, "
        "and some aliases or previous symbols."
    ),
)
async def normalize_gene(
    request: GeneLookupRequest = Body(
        ...,
        openapi_examples={
            "approved_symbol": {
                "summary": "Approved symbol",
                "value": {"raw_gene": "TP53"},
            },
            "hgnc_id": {
                "summary": "HGNC identifier",
                "value": {"raw_gene": "HGNC:171"},
            },
            "ensembl_id": {
                "summary": "Ensembl gene identifier",
                "value": {"raw_gene": "ENSG00000115170"},
            },
        },
    )
) -> NormalizationResponse:
    return await broker.normalize_gene(raw_gene=request.raw_gene)


@app.post(
    "/genes/crosswalk",
    response_model=Dict[str, str],
    tags=["genes"],
    summary="Crosswalk a gene identifier",
    description=(
        "Map one gene identifier namespace to others using HGNC-backed lookup.\n\n"
        "For example, you can provide an HGNC ID, approved symbol, Entrez ID, Ensembl gene ID, "
        "or OMIM ID and get back the corresponding identifier bundle when available."
    ),
)
async def crosswalk_gene_identifier(
    request: GeneCrosswalkRequest = Body(
        ...,
        openapi_examples={
            "from_hgnc": {
                "summary": "Crosswalk from HGNC ID",
                "value": {"identifier": "HGNC:171", "namespace": "hgnc_id"},
            },
            "from_symbol": {
                "summary": "Crosswalk from approved symbol",
                "value": {"identifier": "TP53", "namespace": "symbol"},
            },
            "from_entrez": {
                "summary": "Crosswalk from Entrez ID",
                "value": {"identifier": "90", "namespace": "entrez_id"},
            },
        },
    )
) -> Dict[str, str]:
    return await broker.crosswalk_gene_identifier(
        identifier=request.identifier,
        namespace=request.namespace,
    )

@app.post(
    "/search_literature",
    response_model=List[LiteratureResult],
    tags=["literature"],
    summary="Search biomedical literature",
    description=(
        "Search PubMed and Europe PMC using normalized identifiers, optional keyword constraints, "
        "and optional normalized entity bundles.\n\n"
        "If `normalized_bundle` is provided, the broker can derive disease, gene, phenotype, and "
        "compound search terms from canonical entities and their synonyms before querying Europe PMC."
    ),
)
async def search_literature(
    request: LiteratureSearchRequest = Body(
        ...,
        openapi_examples={
            "ids_and_keywords": {
                "summary": "Search using IDs and keywords",
                "value": {
                    "disease_ids": ["ORPHA:337"],
                    "gene_ids": ["HGNC:171"],
                    "keywords": "case report",
                    "filters": {
                        "case_reports_only": True,
                        "retmax": 5,
                    },
                },
            },
            "with_normalized_bundle": {
                "summary": "Search using a normalized bundle",
                "value": {
                    "keywords": "case report",
                    "filters": {
                        "case_reports_only": True,
                        "retmax": 5,
                    },
                    "normalized_bundle": {
                        "entities": [
                            {
                                "entity_type": "disease",
                                "preferred_label": "fibrodysplasia ossificans progressiva",
                                "source_ids": {
                                    "orpha": "337",
                                    "mondo": "MONDO:0007525",
                                },
                                "synonyms": ["FOP"],
                                "description": None,
                                "confidence": 0.99,
                                "provenance": {
                                    "source": "orphadata",
                                    "method": "stub_lookup",
                                },
                            },
                            {
                                "entity_type": "gene",
                                "preferred_label": "ACVR1",
                                "source_ids": {
                                    "hgnc": "HGNC:171",
                                    "entrez": "90",
                                },
                                "synonyms": [],
                                "description": "activin A receptor type 1",
                                "confidence": 0.99,
                                "provenance": {
                                    "source": "hgnc",
                                    "method": "exact_symbol",
                                },
                            },
                        ]
                    },
                },
            },
        },
    )
) -> List[LiteratureResult]:
    return await broker.search_literature(
        disease_ids=request.disease_ids,
        gene_ids=request.gene_ids,
        phenotype_ids=request.phenotype_ids,
        compound_ids=request.compound_ids,
        keywords=request.keywords,
        filters=request.filters,
        normalized_bundle=request.normalized_bundle,
    )


@app.post(
    "/search_structured",
    response_model=StructuredEvidenceResult,
    tags=["evidence"],
    summary="Retrieve structured evidence objects",
    description=(
        "Take a normalized entity bundle and organize or expand non-literature evidence.\n\n"
        "In the current implementation this is still lightweight and mostly groups entities by type, "
        "but it is intended to become the broker entry point for structured evidence retrieval "
        "from genes, variants, phenotypes, compounds, and trials.\n\n"
        "**Inputs**\n"
        "- `normalized_bundle`: a `NormalizationResponse`\n"
        "- `requested_evidence_types`: optional list such as `['genes', 'variants']`\n"
        "- `filters`: optional free-form filter dictionary"
    ),
)
async def search_structured_evidence(
    normalized_bundle: NormalizationResponse = Body(...),
    requested_evidence_types: Optional[List[str]] = Body(default=None),
    filters: Optional[Dict[str, Any]] = Body(default=None),
) -> StructuredEvidenceResult:
    return await broker.search_structured_evidence(
        normalized_bundle=normalized_bundle,
        requested_evidence_types=requested_evidence_types,
        filters=filters,
    )


@app.post(
    "/assemble_graph",
    response_model=EvidenceGraph,
    tags=["evidence"],
    summary="Assemble an evidence graph",
    description=(
        "Merge normalized entities, literature results, and structured evidence into a single evidence graph object.\n\n"
        "In the current implementation the graph is still a stub, but this endpoint defines the shape "
        "that downstream ranking and summarization will use.\n\n"
        "**Inputs**\n"
        "- `normalized_bundle`: a `NormalizationResponse`\n"
        "- `literature_results`: a list of `LiteratureResult` objects\n"
        "- `structured_evidence_results`: a `StructuredEvidenceResult` object\n"
        "- `scoring_profile`: optional scoring profile name"
    ),
)
async def assemble_evidence_graph(
    normalized_bundle: NormalizationResponse = Body(...),
    literature_results: List[LiteratureResult] = Body(...),
    structured_evidence_results: StructuredEvidenceResult = Body(...),
    scoring_profile: Optional[str] = Body(default=None),
) -> EvidenceGraph:
    return await broker.assemble_evidence_graph(
        normalized_bundle=normalized_bundle,
        literature_results=literature_results,
        structured_evidence_results=structured_evidence_results,
        scoring_profile=scoring_profile,
    )


@app.post(
    "/generate_dossier",
    response_model=Dossier,
    tags=["dossiers"],
    summary="Generate a dossier for a primary entity",
    description=(
        "Create a dossier object around a primary normalized entity.\n\n"
        "This endpoint is intended for downstream summarization and reporting. "
        "The broker currently assembles a lightweight dossier using the current search and graph stubs.\n\n"
        "**Inputs**\n"
        "- `primary_entity`: a normalized entity object\n"
        "- `scope`: optional scope string\n"
        "- `filters`: optional filter dictionary\n"
        "- `output_profile`: optional output profile string"
    ),
)
async def generate_dossier(
    primary_entity: NormalizedEntity = Body(...),
    scope: Optional[str] = Body(default=None),
    filters: Optional[Dict[str, Any]] = Body(default=None),
    output_profile: Optional[str] = Body(default=None),
) -> Dossier:
    return await broker.generate_dossier(
        primary_entity=primary_entity,
        scope=scope,
        filters=filters,
        output_profile=output_profile,
    )
