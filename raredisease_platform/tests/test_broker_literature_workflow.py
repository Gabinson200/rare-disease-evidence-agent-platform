import pytest

from raredisease_platform.broker import Broker
from raredisease_platform.models import (
    EntityType,
    LiteratureProvenance,
    LiteratureResult,
    NormalizedEntity,
    NormalizationResponse,
)

class FakeNormalizerConnector:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    async def normalize(self, text: str):
        self.calls.append(text)
        return self.responses.get(text, [])


class CapturingLiteratureConnector:
    def __init__(self, source_name, results=None):
        self.source_name = source_name
        self.results = results or []
        self.queries = []

    async def search(self, query):
        self.queries.append(query)
        return self.results


@pytest.mark.asyncio
async def test_search_literature_hydrates_direct_ids_before_pubmed_search(monkeypatch):
    broker = Broker()

    disease_entity = NormalizedEntity(
        entity_type=EntityType.disease,
        preferred_label="fibrodysplasia ossificans progressiva",
        source_ids={
            "orpha": "337",
            "mondo": "MONDO:0007525",
        },
        synonyms=["FOP"],
        description=None,
        confidence=0.99,
        provenance={
            "source": "orphadata",
            "method": "exact_orpha_id",
        },
    ).model_dump()

    gene_entity = NormalizedEntity(
        entity_type=EntityType.gene,
        preferred_label="ACVR1",
        source_ids={
            "hgnc": "HGNC:171",
            "entrez": "90",
            "ensembl": "ENSG00000115170",
        },
        synonyms=["ALK2"],
        description="activin A receptor type 1",
        confidence=0.99,
        provenance={
            "source": "hgnc",
            "method": "exact_hgnc_id",
        },
    ).model_dump()

    pubmed_result = LiteratureResult(
        pmid="12345678",
        title="Fibrodysplasia ossificans progressiva involving ACVR1: a case report",
        abstract="This case report discusses ACVR1 in fibrodysplasia ossificans progressiva.",
        year=2024,
        journal="Example Journal",
        authors=["Example A"],
        score=0.95,
        provenance=LiteratureProvenance(
            source="pubmed",
            raw_record={},
        ),
    )

    orphadata = FakeNormalizerConnector(
        {
            "ORPHA:337": [disease_entity],
        }
    )
    hgnc = FakeNormalizerConnector(
        {
            "HGNC:171": [gene_entity],
        }
    )
    pubmed = CapturingLiteratureConnector("pubmed", [pubmed_result])
    europepmc = CapturingLiteratureConnector("europepmc", [])

    connectors = {
        "orphadata": orphadata,
        "hgnc": hgnc,
        "pubmed": pubmed,
        "europepmc": europepmc,
    }

    def fake_get_connector(name):
        return connectors[name]

    monkeypatch.setattr("raredisease_platform.broker.get_connector", fake_get_connector)

    results = await broker.search_literature(
        disease_ids=["ORPHA:337"],
        gene_ids=["HGNC:171"],
        keywords="case report",
        filters={
            "case_reports_only": True,
            "exact_disease_required": True,
            "exact_gene_required": True,
            "retmax": 5,
        },
    )

    assert len(results) == 1
    assert results[0].pmid == "12345678"

    assert orphadata.calls == ["ORPHA:337"]
    assert hgnc.calls == ["HGNC:171"]

    assert len(pubmed.queries) == 1
    pubmed_query = pubmed.queries[0]

    assert "fibrodysplasia ossificans progressiva" in pubmed_query["disease_terms"]
    assert "FOP" in pubmed_query["disease_terms"]
    assert "ACVR1" in pubmed_query["gene_terms"]
    assert "ALK2" in pubmed_query["gene_terms"]

    assert pubmed_query["keywords"] == "case report"
    assert pubmed_query["filters"]["case_reports_only"] is True
    assert pubmed_query["filters"]["exact_disease_required"] is True
    assert pubmed_query["filters"]["exact_gene_required"] is True

    assert pubmed_query["direct_id_hydration_trace"] is not None
    assert pubmed_query["direct_id_hydration_trace"]["final_entity_count"] == 2


@pytest.mark.asyncio
async def test_search_literature_merges_normalized_bundle_with_direct_ids(monkeypatch):
    broker = Broker()

    disease_entity = NormalizedEntity(
        entity_type=EntityType.disease,
        preferred_label="fibrodysplasia ossificans progressiva",
        source_ids={"orpha": "337"},
        synonyms=["FOP"],
        confidence=0.99,
        provenance={"source": "orphadata", "method": "exact_name"},
    )

    gene_entity = NormalizedEntity(
        entity_type=EntityType.gene,
        preferred_label="ACVR1",
        source_ids={"hgnc": "HGNC:171"},
        synonyms=["ALK2"],
        confidence=0.99,
        provenance={"source": "hgnc", "method": "exact_hgnc_id"},
    ).model_dump()

    hgnc = FakeNormalizerConnector(
        {
            "HGNC:171": [gene_entity],
        }
    )
    pubmed = CapturingLiteratureConnector("pubmed", [])
    europepmc = CapturingLiteratureConnector("europepmc", [])

    connectors = {
        "hgnc": hgnc,
        "pubmed": pubmed,
        "europepmc": europepmc,
    }

    def fake_get_connector(name):
        return connectors[name]

    monkeypatch.setattr("raredisease_platform.broker.get_connector", fake_get_connector)

    await broker.search_literature(
        gene_ids=["HGNC:171"],
        keywords="case report",
        normalized_bundle=NormalizationResponse(
            entities=[disease_entity],
            alternatives=None,
            normalization_trace={"source": "test_bundle"},
        )
    )

    assert len(pubmed.queries) == 1
    pubmed_query = pubmed.queries[0]

    assert "fibrodysplasia ossificans progressiva" in pubmed_query["disease_terms"]
    assert "ACVR1" in pubmed_query["gene_terms"]
