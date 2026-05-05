from raredisease_platform.connectors.pubmed import PubMedConnector


def test_pubmed_query_builder_uses_normalized_entity_terms():
    connector = PubMedConnector()

    params = connector._build_esearch_params(
        {
            "disease_terms": ["fibrodysplasia ossificans progressiva", "FOP"],
            "gene_terms": ["ACVR1"],
            "phenotype_terms": [],
            "compound_terms": [],
            "keywords": "case report",
            "filters": {
                "case_reports_only": True,
                "retmax": 5,
            },
        }
    )

    term = params["term"]

    assert '"fibrodysplasia ossificans progressiva"[Title/Abstract]' in term
    assert '"fibrodysplasia ossificans progressiva"[MeSH Terms]' in term
    assert '"FOP"[Title/Abstract]' in term
    assert '"ACVR1"[Title/Abstract]' in term
    assert '"Case Reports"[Publication Type]' in term
    assert "(case report)" in term
    assert " AND " in term
    assert params["retmax"] == 5


def test_pubmed_query_builder_keeps_keyword_fallback():
    connector = PubMedConnector()

    params = connector._build_esearch_params(
        {
            "keywords": "progeria case report",
            "filters": {
                "retmax": 3,
            },
        }
    )

    assert params["term"] == "(progeria case report)"
    assert params["retmax"] == 3


def test_pubmed_query_builder_falls_back_to_rare_disease_when_empty():
    connector = PubMedConnector()

    params = connector._build_esearch_params({})

    assert params["term"] == "rare disease"
