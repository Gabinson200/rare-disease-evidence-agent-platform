import pytest

from raredisease_platform.broker import Broker
from raredisease_platform.models import EntityType, NormalizedEntity


class FakeConnector:
    def __init__(self, name, responses):
        self.name = name
        self.responses = responses

    async def normalize(self, text: str):
        return self.responses.get(text, [])


@pytest.mark.asyncio
async def test_detect_candidate_spans_for_disease_gene_query():
    broker = Broker()

    candidates = broker._detect_candidate_spans(
        raw_query="case reports for fibrodysplasia ossificans progressiva involving ACVR1",
        expected_entity_types=[EntityType.disease, EntityType.gene],
    )

    surfaces = {candidate["surface_text"] for candidate in candidates}

    assert "ACVR1" in surfaces
    assert "fibrodysplasia ossificans progressiva" in surfaces

    acvr1 = next(c for c in candidates if c["surface_text"] == "ACVR1")
    assert EntityType.gene in acvr1["entity_types"]

    disease = next(
        c for c in candidates
        if c["surface_text"] == "fibrodysplasia ossificans progressiva"
    )
    assert EntityType.disease in disease["entity_types"]


@pytest.mark.asyncio
async def test_normalize_entities_uses_candidate_routing_and_trace(monkeypatch):
    broker = Broker()

    disease_entity = NormalizedEntity(
        entity_type=EntityType.disease,
        preferred_label="fibrodysplasia ossificans progressiva",
        source_ids={"orpha": "337"},
        synonyms=["FOP"],
        confidence=0.99,
        provenance={"source": "orphadata", "method": "exact_name"},
    ).model_dump()

    gene_entity = NormalizedEntity(
        entity_type=EntityType.gene,
        preferred_label="ACVR1",
        source_ids={"hgnc": "HGNC:171", "entrez": "90"},
        synonyms=[],
        confidence=0.99,
        provenance={"source": "hgnc", "method": "exact_symbol"},
    ).model_dump()

    connectors = {
        "orphadata": FakeConnector(
            "orphadata",
            {"fibrodysplasia ossificans progressiva": [disease_entity]},
        ),
        "hgnc": FakeConnector(
            "hgnc",
            {"ACVR1": [gene_entity]},
        ),
    }

    def fake_get_connector(name):
        return connectors[name]

    monkeypatch.setattr("raredisease_platform.broker.get_connector", fake_get_connector)

    response = await broker.normalize_entities(
        raw_query="case reports for fibrodysplasia ossificans progressiva involving ACVR1",
        expected_entity_types=[EntityType.disease, EntityType.gene],
    )

    labels = {entity.preferred_label for entity in response.entities}

    assert "fibrodysplasia ossificans progressiva" in labels
    assert "ACVR1" in labels
    assert response.alternatives is None

    assert response.normalization_trace is not None
    assert response.normalization_trace["final_entity_count"] == 2
    assert len(response.normalization_trace["detected_candidates"]) >= 2
    assert len(response.normalization_trace["connector_calls"]) == 2

    for entity in response.entities:
        assert entity.provenance is not None
        assert "normalization_span" in entity.provenance


@pytest.mark.asyncio
async def test_normalize_entities_moves_weak_matches_to_alternatives(monkeypatch):
    broker = Broker()

    weak_gene_entity = NormalizedEntity(
        entity_type=EntityType.gene,
        preferred_label="WEAK1",
        source_ids={"hgnc": "HGNC:999999"},
        synonyms=[],
        confidence=0.72,
        provenance={"source": "hgnc", "method": "broad_search"},
    ).model_dump()

    connectors = {
        "hgnc": FakeConnector(
            "hgnc",
            {"WEAK1": [weak_gene_entity]},
        ),
    }

    def fake_get_connector(name):
        return connectors[name]

    monkeypatch.setattr("raredisease_platform.broker.get_connector", fake_get_connector)

    response = await broker.normalize_entities(
        raw_query="WEAK1",
        expected_entity_types=[EntityType.gene],
    )

    assert response.entities == []
    assert response.alternatives is not None
    assert response.alternatives[0].preferred_label == "WEAK1"
    assert response.normalization_trace is not None
    assert response.normalization_trace["alternative_candidate_count"] == 1

@pytest.mark.asyncio
async def test_detect_candidate_spans_splits_compound_from_gene_symbol():
    broker = Broker()

    candidates = broker._detect_candidate_spans(
        raw_query="compound aspirin ACVR1",
        expected_entity_types=[EntityType.compound, EntityType.gene],
    )

    compound_candidates = [
        candidate for candidate in candidates
        if EntityType.compound in candidate["entity_types"]
    ]
    gene_candidates = [
        candidate for candidate in candidates
        if EntityType.gene in candidate["entity_types"]
    ]

    assert any(candidate["surface_text"].lower() == "aspirin" for candidate in compound_candidates)
    assert any(candidate["surface_text"] == "ACVR1" for candidate in gene_candidates)

    assert not any(
        candidate["surface_text"].lower() == "aspirin acvr1"
        for candidate in compound_candidates
    )
