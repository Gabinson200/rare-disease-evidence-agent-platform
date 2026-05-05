"""Format broker evidence packages into conservative agent responses."""

from __future__ import annotations

from typing import Any, Dict, List


class EvidenceResponseFormatter:
    """Convert /evidence/query JSON into a readable research-oriented response."""

    def format(self, payload: Dict[str, Any]) -> str:
        normalized_bundle = payload.get("normalized_bundle", {}) or {}
        entities = normalized_bundle.get("entities", []) or []
        alternatives = normalized_bundle.get("alternatives", []) or []
        literature_results = payload.get("literature_results", []) or []
        structured_evidence = payload.get("structured_evidence", {}) or {}
        evidence_graph = payload.get("evidence_graph", {}) or {}
        trace = payload.get("trace", {}) or {}

        sections: List[str] = []

        sections.append(self._format_interpretation(entities, alternatives))
        sections.append(self._format_literature(literature_results))
        sections.append(self._format_structured_evidence(structured_evidence))
        sections.append(self._format_graph(evidence_graph))
        sections.append(self._format_trace(trace, normalized_bundle))
        sections.append(self._format_safety_note())

        return "\n\n".join(section for section in sections if section.strip())

    def _format_interpretation(
        self,
        entities: List[Dict[str, Any]],
        alternatives: List[Dict[str, Any]],
    ) -> str:
        lines = ["## Interpreted entities"]

        if not entities:
            lines.append("I did not find any high-confidence normalized entities.")
        else:
            for entity in entities:
                label = entity.get("preferred_label", "unknown")
                entity_type = entity.get("entity_type", "unknown")
                confidence = entity.get("confidence", None)
                source_ids = entity.get("source_ids", {}) or {}

                id_text = self._format_source_ids(source_ids)
                confidence_text = f", confidence {confidence:.2f}" if isinstance(confidence, (int, float)) else ""

                if id_text:
                    lines.append(f"- {label} ({entity_type}; {id_text}{confidence_text})")
                else:
                    lines.append(f"- {label} ({entity_type}{confidence_text})")

        if alternatives:
            lines.append("")
            lines.append("Potential alternatives were also returned, so this query may contain ambiguity:")
            for entity in alternatives[:5]:
                label = entity.get("preferred_label", "unknown")
                entity_type = entity.get("entity_type", "unknown")
                confidence = entity.get("confidence", None)
                confidence_text = f", confidence {confidence:.2f}" if isinstance(confidence, (int, float)) else ""
                lines.append(f"- {label} ({entity_type}{confidence_text})")

        return "\n".join(lines)

    def _format_literature(self, literature_results: List[Dict[str, Any]]) -> str:
        lines = ["## Literature evidence"]

        if not literature_results:
            lines.append("No literature results were returned by the broker.")
            return "\n".join(lines)

        for index, article in enumerate(literature_results[:5], start=1):
            title = article.get("title", "Untitled article")
            year = article.get("year")
            journal = article.get("journal")
            pmid = article.get("pmid")
            score = article.get("score")

            citation_parts = []
            if journal:
                citation_parts.append(str(journal))
            if year:
                citation_parts.append(str(year))
            if pmid:
                citation_parts.append(f"PMID:{pmid}")
            if isinstance(score, (int, float)):
                citation_parts.append(f"score:{score:.2f}")

            suffix = f" ({'; '.join(citation_parts)})" if citation_parts else ""
            lines.append(f"{index}. {title}{suffix}")

        return "\n".join(lines)

    def _format_structured_evidence(self, structured: Dict[str, Any]) -> str:
        lines = ["## Structured evidence"]

        if not structured:
            lines.append("No structured evidence block was returned.")
            return "\n".join(lines)

        relationships = structured.get("relationships", []) or []
        genes = structured.get("genes", []) or []
        diseases = structured.get("diseases", []) or []
        compounds = structured.get("compounds", []) or []
        trials = structured.get("trials", []) or []

        summary_bits = []
        if genes:
            summary_bits.append(f"{len(genes)} gene record(s)")
        if diseases:
            summary_bits.append(f"{len(diseases)} disease record(s)")
        if compounds:
            summary_bits.append(f"{len(compounds)} compound record(s)")
        if trials:
            summary_bits.append(f"{len(trials)} trial record(s)")
        if relationships:
            summary_bits.append(f"{len(relationships)} relationship(s)")

        if summary_bits:
            lines.append("Returned: " + ", ".join(summary_bits) + ".")
        else:
            lines.append("No structured records were returned.")

        for relationship in relationships[:5]:
            rtype = relationship.get("relationship_type", "relationship")
            source = relationship.get("source", "unknown source")
            confidence = relationship.get("confidence")
            confidence_text = f", confidence {confidence:.2f}" if isinstance(confidence, (int, float)) else ""
            lines.append(f"- {rtype} from {source}{confidence_text}")

        return "\n".join(lines)

    def _format_graph(self, graph: Dict[str, Any]) -> str:
        lines = ["## Evidence graph"]

        if not graph:
            lines.append("No evidence graph was returned.")
            return "\n".join(lines)

        nodes = graph.get("nodes", []) or []
        edges = graph.get("edges", []) or []
        summaries = graph.get("ranked_summaries", []) or []

        lines.append(f"The graph contains {len(nodes)} node(s) and {len(edges)} edge(s).")

        for summary in summaries[:3]:
            lines.append(f"- {summary}")

        return "\n".join(lines)

    def _format_trace(self, trace: Dict[str, Any], normalized_bundle: Dict[str, Any]) -> str:
        lines = ["## Trace and limitations"]

        warnings = trace.get("warnings", []) or []
        steps = trace.get("steps", []) or []

        if steps:
            step_names = [step.get("step", "unknown") for step in steps]
            lines.append("Pipeline steps: " + " → ".join(step_names) + ".")

        normalization_trace = normalized_bundle.get("normalization_trace", {}) or {}
        connector_calls = normalization_trace.get("connector_calls", []) or []

        if connector_calls:
            connector_text = []
            for call in connector_calls:
                surface = call.get("surface_text", "?")
                connector = call.get("connector", "?")
                status = call.get("status", "?")
                returned = call.get("records_returned", "?")
                connector_text.append(f"{surface}->{connector}:{status}/{returned}")
            lines.append("Normalization calls: " + "; ".join(connector_text) + ".")

        if warnings:
            lines.append("Warnings:")
            for warning in warnings:
                lines.append(f"- {warning}")
        else:
            lines.append("No broker-level warnings were returned.")

        return "\n".join(lines)

    def _format_safety_note(self) -> str:
        return (
            "## Safety note\n"
            "This is an evidence retrieval summary, not medical advice, diagnosis, or treatment guidance. "
            "Biomedical claims should be reviewed against the cited source records before being used in research or clinical contexts."
        )

    def _format_source_ids(self, source_ids: Dict[str, Any]) -> str:
        if not source_ids:
            return ""

        preferred_order = [
            "orpha",
            "mondo",
            "medgen",
            "mesh",
            "hgnc",
            "entrez",
            "ensembl",
            "pubchem",
            "inchikey",
            "pmid",
            "pmcid",
            "doi",
            "nct",
        ]

        parts = []
        for key in preferred_order:
            value = source_ids.get(key)
            if value:
                parts.append(f"{key}:{value}")

        for key, value in source_ids.items():
            if key not in preferred_order and value:
                parts.append(f"{key}:{value}")

        return ", ".join(parts)
