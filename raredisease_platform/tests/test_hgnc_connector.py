import pytest

from raredisease_platform.connectors.hgnc import HGNCConnector


ACVR1_DOC = {
    "hgnc_id": "HGNC:171",
    "symbol": "ACVR1",
    "name": "activin A receptor type 1",
    "status": "Approved",
    "entrez_id": "90",
    "ensembl_gene_id": "ENSG00000115170",
    "alias_symbol": ["ALK2"],
    "prev_symbol": [],
    "alias_name": [],
    "prev_name": [],
}


@pytest.mark.asyncio
async def test_hgnc_normalize_exact_symbol(monkeypatch):
    connector = HGNCConnector()

    async def fake_fetch_docs(field, term):
        if field == "symbol" and term == "ACVR1":
            return [ACVR1_DOC]
        return []

    async def fake_search_docs(field, term):
        return []

    monkeypatch.setattr(connector, "_fetch_docs", fake_fetch_docs)
    monkeypatch.setattr(connector, "_search_docs", fake_search_docs)

    records = await connector.normalize("ACVR1")

    assert len(records) == 1
    record = records[0]

    assert record["entity_type"] == "gene"
    assert record["preferred_label"] == "ACVR1"
    assert record["source_ids"]["hgnc"] == "HGNC:171"
    assert record["source_ids"]["entrez"] == "90"
    assert record["source_ids"]["ensembl"] == "ENSG00000115170"
    assert record["confidence"] >= 0.98
    assert record["provenance"]["method"] == "exact_symbol"


@pytest.mark.asyncio
async def test_hgnc_normalize_alias_symbol(monkeypatch):
    connector = HGNCConnector()

    async def fake_fetch_docs(field, term):
        return []

    async def fake_search_docs(field, term):
        if field == "alias_symbol" and term == "ALK2":
            return [ACVR1_DOC]
        return []

    monkeypatch.setattr(connector, "_fetch_docs", fake_fetch_docs)
    monkeypatch.setattr(connector, "_search_docs", fake_search_docs)

    records = await connector.normalize("ALK2")

    assert len(records) == 1
    record = records[0]

    assert record["preferred_label"] == "ACVR1"
    assert record["confidence"] == pytest.approx(0.92)
    assert record["provenance"]["method"] == "alias_symbol"


@pytest.mark.asyncio
async def test_hgnc_crosswalk_ids(monkeypatch):
    connector = HGNCConnector()

    async def fake_fetch_docs(field, term):
        if field == "hgnc_id" and term in {"HGNC:171", "171"}:
            return [ACVR1_DOC]
        return []

    async def fake_search_docs(field, term):
        return []

    monkeypatch.setattr(connector, "_fetch_docs", fake_fetch_docs)
    monkeypatch.setattr(connector, "_search_docs", fake_search_docs)

    crosswalk = await connector.crosswalk_ids("HGNC:171", namespace="hgnc_id")

    assert crosswalk["hgnc"] == "HGNC:171"
    assert crosswalk["entrez"] == "90"
    assert crosswalk["ensembl"] == "ENSG00000115170"
