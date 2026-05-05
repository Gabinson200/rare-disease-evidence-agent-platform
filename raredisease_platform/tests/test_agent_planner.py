from raredisease_platform.agent.planner import EvidenceQueryPlanner


def test_planner_handles_disease_gene_case_report_query():
    planner = EvidenceQueryPlanner()

    payload = planner.plan(
        "case reports for fibrodysplasia ossificans progressiva involving ACVR1",
        retmax=5,
    )

    assert payload["raw_query"] == "case reports for fibrodysplasia ossificans progressiva involving ACVR1"
    assert "disease" in payload["expected_entity_types"]
    assert "gene" in payload["expected_entity_types"]
    assert payload["literature_keywords"] == "case report"
    assert payload["literature_filters"]["case_reports_only"] is True
    assert payload["literature_filters"]["exact_gene_required"] is True
    assert payload["literature_filters"]["retmax"] == 5
    assert payload["include_structured_evidence"] is True


def test_planner_handles_gene_first_query():
    planner = EvidenceQueryPlanner()

    payload = planner.plan("what rare diseases are associated with ACVR1?")

    assert "gene" in payload["expected_entity_types"]
    assert "disease" in payload["expected_entity_types"]
    assert payload["literature_keywords"] == "rare disease"
    assert "genes" in payload["requested_evidence_types"]


def test_planner_handles_compound_gene_query():
    planner = EvidenceQueryPlanner()

    payload = planner.plan("compound aspirin ACVR1")

    assert "compound" in payload["expected_entity_types"]
    assert "gene" in payload["expected_entity_types"]
    assert "compounds" in payload["requested_evidence_types"]
    assert "genes" in payload["requested_evidence_types"]


def test_planner_handles_phenotype_gene_query():
    planner = EvidenceQueryPlanner()

    payload = planner.plan("heterotopic ossification ACVR1")

    assert "phenotype" in payload["expected_entity_types"]
    assert "gene" in payload["expected_entity_types"]
    assert "phenotypes" in payload["requested_evidence_types"]
