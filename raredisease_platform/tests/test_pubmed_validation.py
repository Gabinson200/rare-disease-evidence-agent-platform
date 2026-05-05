from raredisease_platform.connectors.pubmed import PubMedConnector


def test_pubmed_validation_detects_required_entity_matches():
    connector = PubMedConnector()

    record = {
        "title": "Fibrodysplasia ossificans progressiva caused by ACVR1 mutation",
        "pubtype": ["Case Reports"],
        "pubdate": "2022",
    }

    validation = connector._validate_entity_terms(
        record=record,
        abstract="This case report discusses ACVR1 in fibrodysplasia ossificans progressiva.",
        query={
            "disease_terms": ["fibrodysplasia ossificans progressiva"],
            "gene_terms": ["ACVR1"],
            "phenotype_terms": [],
            "compound_terms": [],
        },
    )

    assert validation["exact_disease_match"] is True
    assert validation["exact_gene_match"] is True
    assert validation["missing_groups"] == []
    assert "disease" in validation["matched_groups"]
    assert "gene" in validation["matched_groups"]


def test_pubmed_validation_marks_missing_entity_groups():
    connector = PubMedConnector()

    record = {
        "title": "A generic case report about unrelated symptoms",
        "pubtype": ["Case Reports"],
        "pubdate": "2022",
    }

    validation = connector._validate_entity_terms(
        record=record,
        abstract="This abstract does not mention the required disease or gene.",
        query={
            "disease_terms": ["fibrodysplasia ossificans progressiva"],
            "gene_terms": ["ACVR1"],
            "phenotype_terms": [],
            "compound_terms": [],
        },
    )

    assert validation["exact_disease_match"] is False
    assert validation["exact_gene_match"] is False
    assert "disease" in validation["missing_groups"]
    assert "gene" in validation["missing_groups"]


def test_pubmed_scoring_penalizes_missing_required_entities():
    connector = PubMedConnector()

    record = {
        "title": "A generic case report",
        "pubtype": ["Case Reports"],
        "pubdate": "2024",
    }

    validation = {
        "matched_groups": [],
        "missing_groups": ["disease", "gene"],
    }

    scoring = connector._score_record(
        record=record,
        abstract="A generic abstract.",
        pmcid=None,
        query={
            "keywords": "case report",
            "filters": {
                "case_reports_only": True,
                "exact_disease_required": True,
                "exact_gene_required": True,
            },
        },
        validation=validation,
    )

    assert scoring["entity_missing_penalty"] > 0.36
    assert 0.0 <= scoring["score"] <= 1.0
