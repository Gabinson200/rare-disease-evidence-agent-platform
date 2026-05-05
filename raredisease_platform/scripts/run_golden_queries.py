"""
Run golden-query evaluations against a local broker server.

Usage:

    python -m uvicorn raredisease_platform.main:app --reload

Then in another terminal:

    python scripts/run_golden_queries.py

Useful examples:

    python scripts/run_golden_queries.py --fast --case gene_only_acvr1 --timeout 180
    python scripts/run_golden_queries.py --case compound_gene_aspirin_acvr1 --timeout 240
    python scripts/run_golden_queries.py --timeout 240

Outputs:

    reports/golden_query_results.jsonl
    reports/golden_query_summary.md
"""

from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx


GOLDEN_QUERIES: List[Dict[str, Any]] = [
    {
        "id": "fop_acvr1_case_reports",
        "description": "Disease + gene case-report query should normalize FOP disease and ACVR1 gene.",
        "request": {
            "raw_query": "case reports for fibrodysplasia ossificans progressiva involving ACVR1",
            "expected_entity_types": ["disease", "gene"],
            "literature_keywords": "case report",
            "literature_filters": {
                "case_reports_only": True,
                "exact_disease_required": True,
                "exact_gene_required": True,
                "retmax": 5,
            },
            "include_structured_evidence": True,
            "requested_evidence_types": ["genes", "variants", "relationships"],
        },
        "expect": {
            "required_entity_labels": [
                "fibrodysplasia ossificans progressiva",
                "ACVR1",
            ],
            "required_entity_types": ["disease", "gene"],
        },
    },
    {
        "id": "gene_only_acvr1",
        "description": "Gene-only query should normalize ACVR1 through HGNC and retrieve gene-centered evidence.",
        "request": {
            "raw_query": "ACVR1",
            "expected_entity_types": ["gene"],
            "literature_keywords": "rare disease",
            "literature_filters": {
                "retmax": 5,
            },
            "include_structured_evidence": True,
            "requested_evidence_types": ["genes", "diseases", "variants"],
        },
        "expect": {
            "required_entity_labels": ["ACVR1"],
            "required_entity_types": ["gene"],
        },
    },
    {
        "id": "hgnc_id_acvr1",
        "description": "Direct HGNC ID should resolve to ACVR1.",
        "request": {
            "raw_query": "HGNC:171",
            "expected_entity_types": ["gene"],
            "literature_keywords": "rare disease",
            "literature_filters": {
                "retmax": 5,
            },
            "include_structured_evidence": True,
            "requested_evidence_types": ["genes", "diseases", "variants"],
        },
        "expect": {
            "required_entity_labels": ["ACVR1"],
            "required_entity_types": ["gene"],
        },
    },
    {
        "id": "ensembl_acvr1",
        "description": "Ensembl ID should route to HGNC and resolve to ACVR1 when HGNC crosswalk is available.",
        "request": {
            "raw_query": "ENSG00000115170",
            "expected_entity_types": ["gene"],
            "literature_keywords": "rare disease",
            "literature_filters": {
                "retmax": 5,
            },
            "include_structured_evidence": False,
        },
        "expect": {
            "required_entity_labels": ["ACVR1"],
            "required_entity_types": ["gene"],
        },
    },
    {
        "id": "fop_acvr1_ambiguous_abbreviation",
        "description": "FOP + ACVR1 should expose abbreviation ambiguity rather than silently assuming disease from acronym only.",
        "request": {
            "raw_query": "FOP ACVR1",
            "expected_entity_types": ["disease", "gene"],
            "literature_keywords": "case report",
            "literature_filters": {
                "case_reports_only": True,
                "retmax": 5,
            },
            "include_structured_evidence": False,
        },
        "expect": {
            "required_entity_labels": ["ACVR1"],
            "required_entity_types": ["gene"],
            "allow_alternatives": True,
        },
    },
    {
        "id": "heterotopic_ossification_acvr1",
        "description": "Phenotype-ish phrase plus gene should avoid overconfident disease mapping unless supported.",
        "request": {
            "raw_query": "heterotopic ossification ACVR1",
            "expected_entity_types": ["phenotype", "gene"],
            "literature_keywords": None,
            "literature_filters": {
                "retmax": 5,
            },
            "include_structured_evidence": True,
            "requested_evidence_types": ["genes", "phenotypes", "diseases"],
        },
        "expect": {
            "required_entity_labels": ["ACVR1"],
            "required_entity_types": ["gene"],
            "allow_alternatives": True,
        },
    },
    {
        "id": "compound_gene_aspirin_acvr1",
        "description": "Compound + gene query should route aspirin-like compound context and ACVR1 gene separately.",
        "request": {
            "raw_query": "compound aspirin ACVR1",
            "expected_entity_types": ["compound", "gene"],
            "literature_keywords": None,
            "literature_filters": {
                "retmax": 5,
            },
            "include_structured_evidence": True,
            "requested_evidence_types": ["genes", "compounds", "trials"],
        },
        "expect": {
            "required_entity_labels": ["ACVR1"],
            "required_entity_types": ["gene", "compound"],
            "allow_alternatives": True,
        },
    },
]


def normalized_string(value: Any) -> str:
    return str(value or "").strip().lower()


def get_normalized_bundle(payload: Dict[str, Any]) -> Dict[str, Any]:
    bundle = payload.get("normalized_bundle", {})
    return bundle if isinstance(bundle, dict) else {}


def get_entities(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    entities = get_normalized_bundle(payload).get("entities", []) or []
    return [entity for entity in entities if isinstance(entity, dict)]


def get_alternatives(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    alternatives = get_normalized_bundle(payload).get("alternatives", []) or []
    return [entity for entity in alternatives if isinstance(entity, dict)]


def get_normalization_trace(payload: Dict[str, Any]) -> Dict[str, Any]:
    trace = get_normalized_bundle(payload).get("normalization_trace", {}) or {}
    return trace if isinstance(trace, dict) else {}


def entity_label_set(entities: List[Dict[str, Any]]) -> set[str]:
    return {
        normalized_string(entity.get("preferred_label"))
        for entity in entities
        if entity.get("preferred_label")
    }


def entity_type_set(entities: List[Dict[str, Any]]) -> set[str]:
    return {
        normalized_string(entity.get("entity_type"))
        for entity in entities
        if entity.get("entity_type")
    }


def evaluate_payload(case: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    entities = get_entities(payload)
    alternatives = get_alternatives(payload)

    labels = entity_label_set(entities)
    entity_types = entity_type_set(entities)

    expected = case.get("expect", {}) or {}
    required_labels = [
        normalized_string(label)
        for label in expected.get("required_entity_labels", [])
    ]
    required_types = [
        normalized_string(entity_type)
        for entity_type in expected.get("required_entity_types", [])
    ]

    missing_labels = [
        label for label in required_labels
        if label not in labels
    ]
    missing_types = [
        entity_type for entity_type in required_types
        if entity_type not in entity_types
    ]

    literature_results = payload.get("literature_results", []) or []
    trace = payload.get("trace", {}) or {}
    normalization_trace = get_normalization_trace(payload)

    detected_candidates = normalization_trace.get("detected_candidates", []) or []
    connector_calls = normalization_trace.get("connector_calls", []) or []

    passed = not missing_labels and not missing_types

    return {
        "id": case["id"],
        "passed": passed,
        "missing_labels": missing_labels,
        "missing_types": missing_types,
        "entity_labels": sorted(labels),
        "entity_types": sorted(entity_types),
        "alternative_count": len(alternatives),
        "literature_result_count": len(literature_results),
        "pipeline_steps": [step.get("step") for step in trace.get("steps", [])],
        "warnings": trace.get("warnings", []) or [],
        "detected_candidates": detected_candidates,
        "connector_calls": connector_calls,
    }


def make_failure_evaluation(
    case: Dict[str, Any],
    warning: str,
    *,
    status_code: Optional[int] = None,
) -> Dict[str, Any]:
    return {
        "id": case["id"],
        "passed": False,
        "missing_labels": [],
        "missing_types": [],
        "entity_labels": [],
        "entity_types": [],
        "alternative_count": 0,
        "literature_result_count": 0,
        "pipeline_steps": [],
        "warnings": [warning if status_code is None else f"HTTP {status_code}: {warning}"],
        "detected_candidates": [],
        "connector_calls": [],
    }


def compact_connector_calls(connector_calls: List[Dict[str, Any]]) -> str:
    compact = []
    for call in connector_calls:
        if not isinstance(call, dict):
            continue
        surface = call.get("surface_text", "?")
        connector = call.get("connector", "?")
        status = call.get("status", "?")
        returned = call.get("records_returned", "?")
        compact.append(f"{surface}->{connector}:{status}/{returned}")
    return "; ".join(compact) or "-"


def compact_candidates(candidates: List[Dict[str, Any]]) -> str:
    compact = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        surface = candidate.get("surface_text", "?")
        types = candidate.get("candidate_entity_types", []) or []
        if isinstance(types, list):
            type_text = ",".join(str(t) for t in types)
        else:
            type_text = str(types)
        compact.append(f"{surface} [{type_text}]")
    return "; ".join(compact) or "-"


def write_summary(results: List[Dict[str, Any]], output_path: Path) -> None:
    passed = sum(1 for result in results if result["evaluation"]["passed"])
    total = len(results)

    lines = [
        "# Golden Query Evaluation Summary",
        "",
        f"Passed: **{passed}/{total}**",
        "",
        "| Query ID | Pass | Entities | Entity types | Literature results | Missing labels | Missing types |",
        "|---|---:|---|---|---:|---|---|",
    ]

    for result in results:
        evaluation = result["evaluation"]
        lines.append(
            "| {id} | {passed} | {entities} | {types} | {lit_count} | {missing_labels} | {missing_types} |".format(
                id=evaluation["id"],
                passed="yes" if evaluation["passed"] else "no",
                entities=", ".join(evaluation["entity_labels"]) or "-",
                types=", ".join(evaluation["entity_types"]) or "-",
                lit_count=evaluation["literature_result_count"],
                missing_labels=", ".join(evaluation["missing_labels"]) or "-",
                missing_types=", ".join(evaluation["missing_types"]) or "-",
            )
        )

    lines.append("")
    lines.append("## Debug details")
    lines.append("")

    for result in results:
        evaluation = result["evaluation"]
        lines.append(f"### {evaluation['id']}")
        lines.append("")
        lines.append(f"- Pass: {'yes' if evaluation['passed'] else 'no'}")
        lines.append(f"- Pipeline steps: {', '.join(evaluation['pipeline_steps']) or '-'}")
        lines.append(f"- Detected candidates: {compact_candidates(evaluation['detected_candidates'])}")
        lines.append(f"- Connector calls: {compact_connector_calls(evaluation['connector_calls'])}")
        if evaluation["warnings"]:
            lines.append(f"- Warnings: {'; '.join(str(w) for w in evaluation['warnings'])}")
        else:
            lines.append("- Warnings: -")
        lines.append("")

    lines.append("## Notes")
    lines.append("")
    lines.append("- Failures should be inspected before changing agent behavior.")
    lines.append("- Ambiguous acronym queries may pass if the gene is resolved and ambiguity is exposed through alternatives/trace.")
    lines.append("- Literature result counts can vary because live biomedical APIs change over time.")

    output_path.write_text("\n".join(lines), encoding="utf-8")


def select_cases(case_id: Optional[str]) -> List[Dict[str, Any]]:
    cases = copy.deepcopy(GOLDEN_QUERIES)

    if not case_id:
        return cases

    selected = [case for case in cases if case["id"] == case_id]
    if not selected:
        available = ", ".join(case["id"] for case in GOLDEN_QUERIES)
        raise SystemExit(f"No golden query found with id: {case_id}. Available: {available}")

    return selected


def apply_fast_mode(cases: List[Dict[str, Any]]) -> None:
    for case in cases:
        request = case["request"]
        request["include_structured_evidence"] = False

        filters = request.setdefault("literature_filters", {})
        filters["retmax"] = min(int(filters.get("retmax", 5)), 1)


def run_case(
    client: httpx.Client,
    base_url: str,
    case: Dict[str, Any],
) -> Dict[str, Any]:
    print(f"\nRunning {case['id']}...")
    print(f"  {case['description']}")

    result: Dict[str, Any] = {
        "id": case["id"],
        "description": case["description"],
        "request": case["request"],
    }

    try:
        response = client.post(
            f"{base_url}/evidence/query",
            json=case["request"],
        )
        result["status_code"] = response.status_code

        if response.status_code == 200:
            payload = response.json()
            result["payload"] = payload
            result["evaluation"] = evaluate_payload(case, payload)
        else:
            result["payload"] = response.text
            result["evaluation"] = make_failure_evaluation(
                case,
                response.text,
                status_code=response.status_code,
            )

    except httpx.TimeoutException as exc:
        result["status_code"] = None
        result["payload"] = None
        result["evaluation"] = make_failure_evaluation(
            case,
            f"timeout: {type(exc).__name__}: {exc}",
        )

    except httpx.HTTPError as exc:
        result["status_code"] = None
        result["payload"] = None
        result["evaluation"] = make_failure_evaluation(
            case,
            f"http_error: {type(exc).__name__}: {exc}",
        )

    print(f"  {'PASS' if result['evaluation']['passed'] else 'FAIL'}")

    for warning in result["evaluation"].get("warnings", []):
        print(f"  warning: {warning}")

    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--out-dir", default="reports")
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument(
        "--case",
        default=None,
        help="Run only one golden query by id, e.g. --case gene_only_acvr1",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Reduce live calls by disabling structured evidence and setting retmax=1.",
    )

    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    jsonl_path = out_dir / "golden_query_results.jsonl"
    summary_path = out_dir / "golden_query_summary.md"

    cases = select_cases(args.case)

    if args.fast:
        apply_fast_mode(cases)

    timeout = httpx.Timeout(
        connect=15.0,
        read=args.timeout,
        write=30.0,
        pool=15.0,
    )

    results: List[Dict[str, Any]] = []

    with httpx.Client(timeout=timeout) as client:
        for case in cases:
            results.append(
                run_case(
                    client=client,
                    base_url=args.base_url.rstrip("/"),
                    case=case,
                )
            )

    with jsonl_path.open("w", encoding="utf-8") as f:
        for result in results:
            f.write(json.dumps(result, ensure_ascii=False) + "\n")

    write_summary(results, summary_path)

    print(f"\nWrote {jsonl_path}")
    print(f"Wrote {summary_path}")


if __name__ == "__main__":
    main()
