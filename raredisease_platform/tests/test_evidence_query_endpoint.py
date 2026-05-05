import pytest
from httpx import ASGITransport, AsyncClient

from raredisease_platform.main import app, broker
from raredisease_platform.models import (
    EntityType,
    EvidenceGraph,
    LiteratureProvenance,
    LiteratureResult,
    NormalizationResponse,
    NormalizedEntity,
    StructuredEvidenceResult,
)


@pytest.mark.asyncio
async def test_evidence_query_endpoint_runs_full_pipeline(monkeypatch):
    disease = NormalizedEntity(
        entity_type=EntityType.disease,
        preferred_label="fibrodysplasia ossificans progressiva",
        source_ids={"orpha": "337"},
        synonyms=["FOP"],
        confidence=0.99,
        provenance={"source": "orphadata", "method": "exact_name"},
    )

    gene = NormalizedEntity(
        entity_type=EntityType.gene,
        preferred_label="ACVR1",
        source_ids={"hgnc": "HGNC:171", "entrez": "90"},
        synonyms=["ALK2"],
        description="activin A receptor type 1",
        confidence=0.99,
        provenance={"source": "hgnc", "method": "exact_symbol"},
    )

    normalized_bundle = NormalizationResponse(
        entities=[disease, gene],
        alternatives=None,
        normalization_trace={
            "source": "test_normalization",
            "detected_candidates": [
                {"surface_text": "fibrodysplasia ossificans progressiva"},
                {"surface_text": "ACVR1"},
            ],
        },
    )

    literature_result = LiteratureResult(
        pmid="12345678",
        title="Fibrodysplasia ossificans progressiva involving ACVR1: a case report",
        abstract="This case report discusses ACVR1 in fibrodysplasia ossificans progressiva.",
        year=2024,
        journal="Example Journal",
        authors=["Example A"],
        score=0.95,
        provenance=LiteratureProvenance(
            source="pubmed",
            raw_record={
                "esearch_term": '"fibrodysplasia ossificans progressiva"[Title/Abstract] AND "ACVR1"[Title/Abstract]'
            },
        ),
    )

    structured = StructuredEvidenceResult(
        diseases=[disease],
        genes=[gene],
        relationships=[
            {
                "relationship_type": "gene_associated_with_disease",
                "source": "test",
                "confidence": 0.95,
            }
        ],
    )

    graph = EvidenceGraph(
        nodes=[disease, gene],
        edges=[
            {
                "relationship_type": "gene_associated_with_disease",
                "source": "test",
                "confidence": 0.95,
            }
        ],
        ranked_summaries=["ACVR1 is associated with fibrodysplasia ossificans progressiva."],
        explanation={"source": "test_graph"},
    )

    async def fake_normalize_entities(*args, **kwargs):
        return normalized_bundle

    async def fake_search_literature(*args, **kwargs):
        assert kwargs["normalized_bundle"] == normalized_bundle
        assert kwargs["keywords"] == "case report"
        assert kwargs["filters"].case_reports_only is True
        return [literature_result]

    async def fake_search_structured_evidence(*args, **kwargs):
        assert kwargs["normalized_bundle"] == normalized_bundle
        assert kwargs["requested_evidence_types"] == ["genes", "variants", "relationships"]
        return structured

    async def fake_assemble_evidence_graph(*args, **kwargs):
        assert kwargs["normalized_bundle"] == normalized_bundle
        assert kwargs["literature_results"] == [literature_result]
        assert kwargs["structured_evidence_results"] == structured
        return graph

    monkeypatch.setattr(broker, "normalize_entities", fake_normalize_entities)
    monkeypatch.setattr(broker, "search_literature", fake_search_literature)
    monkeypatch.setattr(broker, "search_structured_evidence", fake_search_structured_evidence)
    monkeypatch.setattr(broker, "assemble_evidence_graph", fake_assemble_evidence_graph)

    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/evidence/query",
            json={
                "raw_query": "case reports for fibrodysplasia ossificans progressiva involving ACVR1",
                "expected_entity_types": ["disease", "gene"],
                "literature_keywords": "case report",
                "literature_filters": {
                    "case_reports_only": True,
                    "exact_disease_required": True,
                    "exact_gene_required": True,
                    "retmax": 10,
                },
                "include_structured_evidence": True,
                "requested_evidence_types": ["genes", "variants", "relationships"],
            },
        )

    assert response.status_code == 200
    payload = response.json()

    assert payload["normalized_bundle"]["entities"][0]["preferred_label"] == "fibrodysplasia ossificans progressiva"
    assert payload["normalized_bundle"]["entities"][1]["preferred_label"] == "ACVR1"
    assert payload["literature_results"][0]["pmid"] == "12345678"
    assert payload["structured_evidence"]["relationships"][0]["relationship_type"] == "gene_associated_with_disease"
    assert payload["evidence_graph"]["edges"][0]["relationship_type"] == "gene_associated_with_disease"

    assert payload["trace"]["source"] == "broker.query_evidence"
    assert [step["step"] for step in payload["trace"]["steps"]] == [
        "normalize_entities",
        "search_literature",
        "search_structured_evidence",
        "assemble_evidence_graph",
    ]
