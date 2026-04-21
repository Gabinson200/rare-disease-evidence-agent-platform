"""Broker service orchestrating normalization, search, joining, and ranking."""

import asyncio
from typing import Any, Dict, List, Optional, Sequence

from .connectors import CONNECTOR_REGISTRY, get_connector
from .models import (
    Dossier,
    EntityType,
    EvidenceGraph,
    LiteratureResult,
    NormalizationResponse,
    NormalizedEntity,
    PubMedSearchFilters,
    StructuredEvidenceResult,
)


class Broker:
    """Core orchestrator for the evidence retrieval platform."""

    def __init__(self) -> None:
        self.connectors = CONNECTOR_REGISTRY

    async def normalize_entities(
        self,
        raw_query: str,
        expected_entity_types: Optional[List[EntityType]] = None,
        disambiguation_preferences: Optional[Dict[str, Any]] = None,
    ) -> NormalizationResponse:
        normalized_entities: List[NormalizedEntity] = []
        alternative_candidates: List[NormalizedEntity] = []

        connectors: Sequence[str]
        if expected_entity_types:
            connectors = []
            for etype in expected_entity_types:
                if etype == EntityType.disease:
                    connectors.append("orphadata")
                elif etype == EntityType.gene:
                    connectors.append("hgnc")
                elif etype == EntityType.phenotype:
                    connectors.append("hpo")
                elif etype == EntityType.variant:
                    connectors.append("clinvar")
                elif etype == EntityType.compound:
                    connectors.append("pubchem")
                elif etype == EntityType.trial:
                    connectors.append("clinicaltrials")
        else:
            connectors = [
                "orphadata",
                "hgnc",
                "hpo",
                "clinvar",
                "pubchem",
                "clinicaltrials",
            ]

        async def call_normalizer(name: str) -> List[Dict[str, Any]]:
            connector = get_connector(name)
            try:
                return await connector.normalize(raw_query)
            except NotImplementedError:
                return []

        tasks = [call_normalizer(name) for name in connectors]
        results = await asyncio.gather(*tasks)

        for connector_results in results:
            for record in connector_results:
                try:
                    entity = NormalizedEntity.model_validate(record)
                    normalized_entities.append(entity)
                except Exception:
                    try:
                        entity_alt = NormalizedEntity.model_validate(record)
                        alternative_candidates.append(entity_alt)
                    except Exception:
                        continue

        return NormalizationResponse(
            entities=normalized_entities,
            alternatives=alternative_candidates or None,
        )

    async def normalize_gene(self, raw_gene: str) -> NormalizationResponse:
        connector = get_connector("hgnc")
        records = await connector.normalize(raw_gene)
        entities = [NormalizedEntity.model_validate(record) for record in records]

        return NormalizationResponse(
            entities=entities[:1],
            alternatives=entities[1:] or None,
        )

    async def crosswalk_gene_identifier(
        self,
        identifier: str,
        namespace: str = "hgnc_id",
    ) -> Dict[str, str]:
        connector = get_connector("hgnc")
        return await connector.crosswalk_ids(identifier, namespace=namespace)

    async def search_literature(
        self,
        disease_ids: Optional[List[str]] = None,
        gene_ids: Optional[List[str]] = None,
        phenotype_ids: Optional[List[str]] = None,
        compound_ids: Optional[List[str]] = None,
        keywords: Optional[str] = None,
        filters: Optional[PubMedSearchFilters | Dict[str, Any]] = None,
        normalized_bundle: Optional[NormalizationResponse] = None,
    ) -> List[LiteratureResult]:
        if isinstance(filters, PubMedSearchFilters):
            filter_payload = filters.model_dump(exclude_none=True)
        else:
            filter_payload = filters or {}

        derived_terms = self._extract_literature_terms_from_bundle(normalized_bundle)

        query: Dict[str, Any] = {
            "disease_ids": disease_ids,
            "gene_ids": gene_ids,
            "phenotype_ids": phenotype_ids,
            "compound_ids": compound_ids,
            "keywords": keywords,
            "filters": filter_payload,
            # Europe PMC-friendly derived terms from normalized entities
            "disease_terms": derived_terms["disease_terms"],
            "gene_terms": derived_terms["gene_terms"],
            "phenotype_terms": derived_terms["phenotype_terms"],
            "compound_terms": derived_terms["compound_terms"],
            "article_ids": derived_terms["article_ids"],
        }

        pubmed = get_connector("pubmed")
        europepmc = get_connector("europepmc")

        pubmed_results, europepmc_results = await asyncio.gather(
            pubmed.search(query),
            europepmc.search(query),
            return_exceptions=True,
        )

        if isinstance(pubmed_results, Exception):
            pubmed_results = []
        if isinstance(europepmc_results, Exception):
            europepmc_results = []

        combined: List[LiteratureResult] = [
            *pubmed_results,
            *europepmc_results,
        ]

        def dedupe_key(article: LiteratureResult) -> str:
            if article.pmid:
                return f"pmid:{article.pmid}"
            if article.pmcid:
                return f"pmcid:{article.pmcid}"
            if article.doi:
                return f"doi:{article.doi.lower()}"
            return f"title:{article.title.strip().lower()}"

        def choose_better(a: LiteratureResult, b: LiteratureResult) -> LiteratureResult:
            a_source = a.provenance.source.lower()
            b_source = b.provenance.source.lower()

            if a_source == "pubmed" and b_source != "pubmed":
                return a
            if b_source == "pubmed" and a_source != "pubmed":
                return b

            if bool(a.abstract) != bool(b.abstract):
                return a if a.abstract else b

            a_authors = len(a.authors or [])
            b_authors = len(b.authors or [])
            if a_authors != b_authors:
                return a if a_authors > b_authors else b

            if a.score != b.score:
                return a if a.score > b.score else b

            return a if a_source == "pubmed" else b

        deduped: Dict[str, LiteratureResult] = {}
        for article in combined:
            key = dedupe_key(article)
            if key not in deduped:
                deduped[key] = article
            else:
                deduped[key] = choose_better(deduped[key], article)

        merged_results = list(deduped.values())
        merged_results.sort(key=lambda x: x.score, reverse=True)
        return merged_results

    async def search_structured_evidence(
        self,
        normalized_bundle: NormalizationResponse,
        requested_evidence_types: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> StructuredEvidenceResult:
        filters = filters or {}
        requested = {x.lower() for x in (requested_evidence_types or [])}

        def entity_key(entity: NormalizedEntity) -> str:
            for key in (
                "orpha",
                "mondo",
                "medgen",
                "medgen_uid",
                "mesh",
                "hpo",
                "hgnc",
                "entrez",
                "clinvar",
                "vcv",
                "rcv",
                "scv",
                "dbsnp",
                "pubchem",
                "inchikey",
                "nct",
            ):
                value = (entity.source_ids or {}).get(key)
                if value:
                    return f"{entity.entity_type}:{key}:{value}"
            return f"{entity.entity_type}:label:{entity.preferred_label.lower()}"

        def dedupe_entities(entities: List[NormalizedEntity]) -> List[NormalizedEntity]:
            out: Dict[str, NormalizedEntity] = {}
            for entity in entities:
                out[entity_key(entity)] = entity
            return list(out.values())

        def merge_entities(base: NormalizedEntity, enriched: NormalizedEntity) -> NormalizedEntity:
            merged_source_ids = dict(base.source_ids or {})
            merged_source_ids.update(enriched.source_ids or {})

            merged_synonyms: List[str] = []
            seen = set()
            for value in list(base.synonyms or []) + list(enriched.synonyms or []):
                key = value.strip().lower()
                if key and key not in seen:
                    seen.add(key)
                    merged_synonyms.append(value)

            merged_provenance = dict(base.provenance or {})
            merged_provenance.update(enriched.provenance or {})

            return NormalizedEntity(
                entity_type=base.entity_type,
                preferred_label=enriched.preferred_label or base.preferred_label,
                source_ids=merged_source_ids,
                synonyms=merged_synonyms,
                description=enriched.description or base.description,
                confidence=max(base.confidence, enriched.confidence),
                provenance=merged_provenance,
            )

        def add_relationship(
            rels: List[Dict[str, Any]],
            *,
            relationship_type: str,
            source: str,
            confidence: float,
            subject: NormalizedEntity,
            object_: NormalizedEntity,
            directionality: str,
            provenance: Optional[Dict[str, Any]] = None,
        ) -> None:
            rels.append(
                {
                    "relationship_type": relationship_type,
                    "source": source,
                    "confidence": round(float(confidence), 4),
                    "directionality": directionality,
                    "subject": subject.model_dump(),
                    "object": object_.model_dump(),
                    "provenance": provenance or {},
                }
            )

        async def try_orpha_enrich_disease(entity: NormalizedEntity) -> NormalizedEntity:
            if entity.entity_type != EntityType.disease:
                return entity

            orphadata = get_connector("orphadata")
            orpha_code = (entity.source_ids or {}).get("orpha")

            try:
                if orpha_code:
                    enriched_raw = await orphadata.fetch_by_id(f"ORPHA:{orpha_code}")
                else:
                    enriched_candidates = await orphadata.normalize(entity.preferred_label)
                    enriched_raw = enriched_candidates[0] if enriched_candidates else None

                if enriched_raw:
                    enriched = NormalizedEntity.model_validate(enriched_raw)
                    return merge_entities(entity, enriched)
            except Exception:
                pass

            return entity

        async def try_pubchem_enrich_compound(entity: NormalizedEntity) -> NormalizedEntity:
            if entity.entity_type != EntityType.compound:
                return entity

            pubchem = get_connector("pubchem")
            identifiers_to_try: List[str] = []

            for key in ("pubchem", "inchikey", "smiles"):
                value = (entity.source_ids or {}).get(key)
                if value:
                    identifiers_to_try.append(str(value))

            identifiers_to_try.append(entity.preferred_label)
            identifiers_to_try.extend((entity.synonyms or [])[:2])

            seen = set()
            for identifier in identifiers_to_try:
                cleaned = identifier.strip()
                if not cleaned:
                    continue
                lowered = cleaned.lower()
                if lowered in seen:
                    continue
                seen.add(lowered)

                try:
                    raw_records = await pubchem.normalize(cleaned)
                    if raw_records:
                        enriched = NormalizedEntity.model_validate(raw_records[0])
                        return merge_entities(entity, enriched)
                except Exception:
                    continue

            return entity

        async def try_clinvar_condition_to_disease(condition_name: str) -> Optional[NormalizedEntity]:
            if not condition_name or not condition_name.strip():
                return None

            orphadata = get_connector("orphadata")
            try:
                candidates = await orphadata.normalize(condition_name.strip())
                if not candidates:
                    return None
                return NormalizedEntity.model_validate(candidates[0])
            except Exception:
                return None

        by_type: Dict[EntityType, List[NormalizedEntity]] = {}
        for entity in normalized_bundle.entities:
            by_type.setdefault(entity.entity_type, []).append(entity)

        diseases = list(by_type.get(EntityType.disease, []) or [])
        genes = list(by_type.get(EntityType.gene, []) or [])
        variants = list(by_type.get(EntityType.variant, []) or [])
        phenotypes = list(by_type.get(EntityType.phenotype, []) or [])
        compounds = list(by_type.get(EntityType.compound, []) or [])
        trials = list(by_type.get(EntityType.trial, []) or [])
        relationships: List[Dict[str, Any]] = []

        # ------------------------------------------------------------------
        # Disease enrichment: make sure disease entities are hydrated through ORPHA
        # ------------------------------------------------------------------
        if diseases and (not requested or "diseases" in requested or "relationships" in requested):
            enriched_diseases: List[NormalizedEntity] = []
            for disease in diseases:
                enriched_diseases.append(await try_orpha_enrich_disease(disease))
            diseases = dedupe_entities(enriched_diseases)

        # ------------------------------------------------------------------
        # Phenotype-first retrieval: HPO -> MedGen disease candidates
        # ------------------------------------------------------------------
        if phenotypes and (not requested or "diseases" in requested or "relationships" in requested):
            hpo = get_connector("hpo")
            raw_disease_candidates = await hpo.propose_disease_candidates(
                phenotypes,
                max_candidates=int(filters.get("max_disease_candidates", 10)),
            )

            enriched_disease_candidates: List[NormalizedEntity] = []

            for raw in raw_disease_candidates:
                candidate = NormalizedEntity.model_validate(raw)
                candidate = await try_orpha_enrich_disease(candidate)
                enriched_disease_candidates.append(candidate)

                matched_phenotypes = (candidate.provenance or {}).get("matched_phenotypes", [])
                for phenotype in phenotypes:
                    if phenotype.preferred_label in matched_phenotypes:
                        add_relationship(
                            relationships,
                            relationship_type="phenotype_suggests_disease",
                            source="medgen",
                            confidence=candidate.confidence,
                            subject=phenotype,
                            object_=candidate,
                            directionality="phenotype_to_disease",
                            provenance={"matched_phenotypes": matched_phenotypes},
                        )

            diseases = dedupe_entities(diseases + enriched_disease_candidates)

        # ------------------------------------------------------------------
        # Gene enrichment: HGNC-normalized genes -> NCBI Gene metadata + disease links
        # ------------------------------------------------------------------
        if genes and (not requested or "genes" in requested or "diseases" in requested or "relationships" in requested):
            ncbi_gene = get_connector("ncbi_gene")
            enriched_genes: List[NormalizedEntity] = []
            linked_diseases: List[NormalizedEntity] = []

            for gene in genes:
                entrez = (gene.source_ids or {}).get("entrez")
                if entrez:
                    query_payload = {"gene_ids": [entrez], "filters": {"retmax": 1}}
                else:
                    query_payload = {
                        "gene_terms": [gene.preferred_label] + list((gene.synonyms or [])[:2]),
                        "filters": {"retmax": 1},
                    }

                try:
                    raw_records = await ncbi_gene.search(query_payload)
                except Exception:
                    raw_records = []

                if not raw_records:
                    enriched_genes.append(gene)
                    continue

                enriched = NormalizedEntity.model_validate(raw_records[0])
                merged_gene = merge_entities(gene, enriched)
                enriched_genes.append(merged_gene)

                disease_links = (merged_gene.provenance or {}).get("disease_links", []) or []
                for raw_disease in disease_links:
                    try:
                        disease_entity = NormalizedEntity.model_validate(raw_disease)
                        disease_entity = await try_orpha_enrich_disease(disease_entity)
                    except Exception:
                        continue

                    linked_diseases.append(disease_entity)
                    add_relationship(
                        relationships,
                        relationship_type="gene_associated_with_disease",
                        source="medgen",
                        confidence=disease_entity.confidence,
                        subject=merged_gene,
                        object_=disease_entity,
                        directionality="gene_to_disease",
                        provenance={"via": "ncbi_gene_connector"},
                    )

            genes = dedupe_entities(enriched_genes)
            diseases = dedupe_entities(diseases + linked_diseases)

        # ------------------------------------------------------------------
        # Variant enrichment: ClinVar-driven retrieval from disease/gene/variant inputs
        # ------------------------------------------------------------------
        if (diseases or genes or variants) and (not requested or "variants" in requested or "relationships" in requested):
            clinvar = get_connector("clinvar")

            disease_terms = [d.preferred_label for d in diseases[:5]]
            gene_terms = [g.preferred_label for g in genes[:5]]

            variant_seed_terms: List[str] = []
            for variant in variants[:5]:
                if "clinvar" in (variant.source_ids or {}):
                    variant_seed_terms.append(str(variant.source_ids["clinvar"]))
                elif "vcv" in (variant.source_ids or {}):
                    variant_seed_terms.append(str(variant.source_ids["vcv"]))
                elif "dbsnp" in (variant.source_ids or {}):
                    variant_seed_terms.append(str(variant.source_ids["dbsnp"]))
                else:
                    variant_seed_terms.append(variant.preferred_label)

            variant_query: Dict[str, Any] = {
                "variant_ids": variant_seed_terms,
                "gene_terms": gene_terms,
                "disease_terms": disease_terms,
                "phenotype_terms": [p.preferred_label for p in phenotypes[:5]],
                "filters": {
                    "retmax": int(filters.get("max_variant_candidates", 10)),
                    **{
                        k: v for k, v in filters.items()
                        if k in {"clinvar_significance", "variant_review_status"}
                    },
                },
            }

            try:
                raw_variants = await clinvar.search(variant_query)
            except Exception:
                raw_variants = []

            fetched_variants: List[NormalizedEntity] = []
            linked_variant_diseases: List[NormalizedEntity] = []

            for raw_variant in raw_variants:
                variant_entity = NormalizedEntity.model_validate(raw_variant)
                fetched_variants.append(variant_entity)

                conditions = (variant_entity.provenance or {}).get("conditions", []) or []
                for condition in conditions[:5]:
                    disease_entity = await try_clinvar_condition_to_disease(condition)
                    if disease_entity:
                        linked_variant_diseases.append(disease_entity)
                        add_relationship(
                            relationships,
                            relationship_type="variant_associated_with_disease",
                            source="clinvar",
                            confidence=min(variant_entity.confidence, disease_entity.confidence),
                            subject=variant_entity,
                            object_=disease_entity,
                            directionality="variant_to_disease",
                            provenance={"condition_name": condition},
                        )

            variants = dedupe_entities(variants + fetched_variants)
            diseases = dedupe_entities(diseases + linked_variant_diseases)

            for gene in genes:
                for variant in fetched_variants[:8]:
                    add_relationship(
                        relationships,
                        relationship_type="gene_has_variant_candidate",
                        source="clinvar",
                        confidence=min(gene.confidence, variant.confidence),
                        subject=gene,
                        object_=variant,
                        directionality="gene_to_variant",
                        provenance={"via_search_terms": gene_terms},
                    )

            for disease in diseases:
                for variant in fetched_variants[:8]:
                    add_relationship(
                        relationships,
                        relationship_type="disease_has_variant_candidate",
                        source="clinvar",
                        confidence=min(disease.confidence, variant.confidence),
                        subject=disease,
                        object_=variant,
                        directionality="disease_to_variant",
                        provenance={"via_search_terms": disease_terms},
                    )

        # ------------------------------------------------------------------
        # Compound enrichment: hydrate compounds through PubChem
        # ------------------------------------------------------------------
        if compounds and (not requested or "compounds" in requested or "relationships" in requested or "trials" in requested):
            enriched_compounds: List[NormalizedEntity] = []
            for compound in compounds:
                enriched_compounds.append(await try_pubchem_enrich_compound(compound))
            compounds = dedupe_entities(enriched_compounds)

        # ------------------------------------------------------------------
        # Trial enrichment: ClinicalTrials.gov search from disease/compound/gene/phenotype context
        # ------------------------------------------------------------------
        if (diseases or compounds or genes or phenotypes or trials) and (
            not requested or "trials" in requested or "relationships" in requested
        ):
            trials_connector = get_connector("clinicaltrials")

            trial_query: Dict[str, Any] = {
                "disease_terms": [d.preferred_label for d in diseases[:5]],
                "compound_terms": [c.preferred_label for c in compounds[:5]],
                "gene_terms": [g.preferred_label for g in genes[:3]],
                "phenotype_terms": [p.preferred_label for p in phenotypes[:3]],
                "trial_ids": [t.source_ids.get("nct", t.preferred_label) for t in trials[:5]],
                "filters": {
                    "retmax": int(filters.get("max_trial_candidates", 10)),
                    **{
                        k: v for k, v in filters.items()
                        if k in {
                            "recruiting_status",
                            "phase",
                            "sex",
                            "age_group",
                            "country",
                            "sponsor",
                            "date_updated_from",
                        }
                    },
                },
            }

            try:
                raw_trials = await trials_connector.search(trial_query)
            except Exception:
                raw_trials = []

            fetched_trials = [NormalizedEntity.model_validate(t) for t in raw_trials]
            trials = dedupe_entities(trials + fetched_trials)

            for disease in diseases:
                for trial in fetched_trials[:8]:
                    add_relationship(
                        relationships,
                        relationship_type="disease_studied_in_trial_candidate",
                        source="clinicaltrials",
                        confidence=min(disease.confidence, trial.confidence),
                        subject=disease,
                        object_=trial,
                        directionality="disease_to_trial",
                        provenance={"via_search_terms": [disease.preferred_label]},
                    )

            for compound in compounds:
                for trial in fetched_trials[:8]:
                    add_relationship(
                        relationships,
                        relationship_type="compound_studied_in_trial_candidate",
                        source="clinicaltrials",
                        confidence=min(compound.confidence, trial.confidence),
                        subject=compound,
                        object_=trial,
                        directionality="compound_to_trial",
                        provenance={"via_search_terms": [compound.preferred_label]},
                    )

        relationships = relationships or []

        return StructuredEvidenceResult(
            diseases=diseases or None,
            genes=genes or None,
            variants=variants or None,
            phenotypes=phenotypes or None,
            compounds=compounds or None,
            trials=trials or None,
            relationships=relationships or None,
        )

    async def assemble_evidence_graph(
        self,
        normalized_bundle: NormalizationResponse,
        literature_results: Sequence[LiteratureResult],
        structured_evidence_results: StructuredEvidenceResult,
        scoring_profile: Optional[str] = None,
    ) -> EvidenceGraph:
        import re
        from collections import Counter

        profile = (scoring_profile or "default").lower()
        mention_threshold = {
            "precision": 0.80,
            "sensitive": 0.55,
            "default": 0.65,
        }.get(profile, 0.65)

        def entity_key(entity: NormalizedEntity) -> str:
            for key in (
                "orpha",
                "mondo",
                "medgen",
                "medgen_uid",
                "mesh",
                "hpo",
                "hgnc",
                "entrez",
                "clinvar",
                "vcv",
                "rcv",
                "scv",
                "dbsnp",
                "pubchem",
                "inchikey",
                "smiles",
                "nct",
                "pmid",
                "pmcid",
                "doi",
            ):
                value = (entity.source_ids or {}).get(key)
                if value:
                    return f"{entity.entity_type}:{key}:{value}"
            return f"{entity.entity_type}:label:{entity.preferred_label.strip().lower()}"

        def article_to_node(article: LiteratureResult) -> NormalizedEntity:
            source_ids: Dict[str, str] = {}
            if article.pmid:
                source_ids["pmid"] = article.pmid
            if article.pmcid:
                source_ids["pmcid"] = article.pmcid
            if article.doi:
                source_ids["doi"] = article.doi

            provenance: Dict[str, Any]
            if hasattr(article.provenance, "model_dump"):
                provenance = article.provenance.model_dump()
            else:
                provenance = {"source": getattr(article.provenance, "source", "unknown")}

            return NormalizedEntity(
                entity_type=EntityType.article,
                preferred_label=article.title,
                source_ids=source_ids,
                synonyms=[],
                description=article.abstract,
                confidence=article.score,
                provenance=provenance,
            )

        def add_node(node_map: Dict[str, NormalizedEntity], entity: NormalizedEntity) -> str:
            key = entity_key(entity)
            if key not in node_map:
                node_map[key] = entity
            else:
                existing = node_map[key]
                merged_source_ids = dict(existing.source_ids or {})
                merged_source_ids.update(entity.source_ids or {})

                merged_synonyms: List[str] = []
                seen = set()
                for value in list(existing.synonyms or []) + list(entity.synonyms or []):
                    cleaned = value.strip()
                    lowered = cleaned.lower()
                    if cleaned and lowered not in seen:
                        seen.add(lowered)
                        merged_synonyms.append(cleaned)

                merged_provenance = dict(existing.provenance or {})
                merged_provenance.update(entity.provenance or {})

                node_map[key] = NormalizedEntity(
                    entity_type=existing.entity_type,
                    preferred_label=existing.preferred_label or entity.preferred_label,
                    source_ids=merged_source_ids,
                    synonyms=merged_synonyms,
                    description=existing.description or entity.description,
                    confidence=max(existing.confidence, entity.confidence),
                    provenance=merged_provenance,
                )
            return key

        def add_edge(
            edge_list: List[Dict[str, Any]],
            *,
            edge_type: str,
            source: str,
            confidence: float,
            subject_key: str,
            object_key: str,
            directionality: str,
            provenance: Optional[Dict[str, Any]] = None,
        ) -> None:
            edge_list.append(
                {
                    "edge_type": edge_type,
                    "source": source,
                    "confidence": round(float(confidence), 4),
                    "subject_key": subject_key,
                    "object_key": object_key,
                    "directionality": directionality,
                    "provenance": provenance or {},
                }
            )

        def collect_candidate_terms(entity: NormalizedEntity, max_terms: int = 5) -> List[str]:
            raw_terms = [entity.preferred_label] + list(entity.synonyms or [])
            out: List[str] = []
            seen = set()

            for term in raw_terms:
                cleaned = term.strip()
                lowered = cleaned.lower()
                if not cleaned or lowered in seen:
                    continue

                # Avoid very short ambiguous non-gene/non-variant tokens.
                if entity.entity_type not in {EntityType.gene, EntityType.variant}:
                    if len(cleaned) < 4 and " " not in cleaned:
                        continue

                seen.add(lowered)
                out.append(cleaned)

                if len(out) >= max_terms:
                    break

            return out

        def match_entity_in_article(
            entity: NormalizedEntity,
            article: LiteratureResult,
        ) -> tuple[float, Optional[str], Optional[str]]:
            title = article.title or ""
            abstract = article.abstract or ""
            title_lower = title.lower()
            abstract_lower = abstract.lower()
            full_text = f"{title} {abstract}"
            full_text_lower = full_text.lower()

            best_score = 0.0
            best_term: Optional[str] = None
            best_location: Optional[str] = None

            candidate_terms = collect_candidate_terms(entity)

            for idx, term in enumerate(candidate_terms):
                is_preferred = idx == 0
                term_lower = term.lower()

                score_title = 0.0
                score_abstract = 0.0

                if entity.entity_type == EntityType.gene:
                    if len(term) < 3:
                        continue
                    pattern = re.compile(rf"(?<![A-Za-z0-9]){re.escape(term)}(?![A-Za-z0-9])", re.IGNORECASE)
                    if pattern.search(title):
                        score_title = 0.94 if is_preferred else 0.84
                    if pattern.search(abstract):
                        score_abstract = 0.76 if is_preferred else 0.66

                elif entity.entity_type == EntityType.variant:
                    if len(term) < 4:
                        continue
                    if term_lower in title_lower:
                        score_title = 0.92 if is_preferred else 0.82
                    if term_lower in abstract_lower:
                        score_abstract = 0.74 if is_preferred else 0.64

                else:
                    if term_lower in title_lower:
                        score_title = 0.88 if is_preferred else 0.78
                    if term_lower in abstract_lower:
                        score_abstract = 0.68 if is_preferred else 0.58

                score = max(score_title, score_abstract)
                if score > best_score:
                    best_score = score
                    best_term = term
                    best_location = "title" if score_title >= score_abstract and score_title > 0 else (
                        "abstract" if score_abstract > 0 else None
                    )

            return best_score, best_term, best_location

        # ------------------------------------------------------------------
        # Build node map from normalized input + structured evidence + articles
        # ------------------------------------------------------------------
        node_map: Dict[str, NormalizedEntity] = {}

        for entity in normalized_bundle.entities:
            add_node(node_map, entity)

        for entity_group in (
            structured_evidence_results.diseases or [],
            structured_evidence_results.genes or [],
            structured_evidence_results.variants or [],
            structured_evidence_results.phenotypes or [],
            structured_evidence_results.compounds or [],
            structured_evidence_results.trials or [],
        ):
            for entity in entity_group:
                add_node(node_map, entity)

        article_nodes: List[NormalizedEntity] = []
        for article in literature_results:
            article_node = article_to_node(article)
            article_nodes.append(article_node)
            add_node(node_map, article_node)

        # ------------------------------------------------------------------
        # Structured relationship edges
        # ------------------------------------------------------------------
        edges: List[Dict[str, Any]] = []
        structured_edge_count = 0
        literature_edge_count = 0

        for raw_rel in (structured_evidence_results.relationships or []):
            try:
                subject = NormalizedEntity.model_validate(raw_rel["subject"])
                object_ = NormalizedEntity.model_validate(raw_rel["object"])
            except Exception:
                continue

            subject_key = add_node(node_map, subject)
            object_key = add_node(node_map, object_)

            add_edge(
                edges,
                edge_type=str(raw_rel.get("relationship_type", "related_to")),
                source=str(raw_rel.get("source", "structured")),
                confidence=float(raw_rel.get("confidence", 0.5)),
                subject_key=subject_key,
                object_key=object_key,
                directionality=str(raw_rel.get("directionality", "directed")),
                provenance=raw_rel.get("provenance") or {},
            )
            structured_edge_count += 1

        # ------------------------------------------------------------------
        # Article -> entity mention edges
        # ------------------------------------------------------------------
        non_article_nodes = [
            entity
            for entity in node_map.values()
            if entity.entity_type != EntityType.article
        ]

        article_entity_matches: Dict[str, Dict[str, float]] = {}

        for article, article_node in zip(literature_results, article_nodes):
            article_key = entity_key(article_node)
            article_entity_matches[article_key] = {}

            for entity in non_article_nodes:
                entity_key_value = entity_key(entity)
                score, matched_term, matched_location = match_entity_in_article(entity, article)

                if score < mention_threshold:
                    continue

                article_entity_matches[article_key][entity_key_value] = score

                add_edge(
                    edges,
                    edge_type="article_mentions_entity",
                    source=str(getattr(article.provenance, "source", "literature")),
                    confidence=score,
                    subject_key=article_key,
                    object_key=entity_key_value,
                    directionality="article_to_entity",
                    provenance={
                        "matched_term": matched_term,
                        "matched_location": matched_location,
                        "pmid": article.pmid,
                        "doi": article.doi,
                    },
                )
                literature_edge_count += 1

        # ------------------------------------------------------------------
        # Optional article -> relationship support edges
        # If an article mentions both endpoints of a structured relationship,
        # add a light support edge between the article and the object endpoint.
        # ------------------------------------------------------------------
        for edge in list(edges):
            if edge.get("edge_type") == "article_mentions_entity":
                continue

            subject_key = edge["subject_key"]
            object_key = edge["object_key"]

            for article_key, match_map in article_entity_matches.items():
                subject_score = match_map.get(subject_key)
                object_score = match_map.get(object_key)
                if subject_score is None or object_score is None:
                    continue

                support_confidence = min(subject_score, object_score)
                add_edge(
                    edges,
                    edge_type="article_supports_relationship_candidate",
                    source="derived",
                    confidence=support_confidence,
                    subject_key=article_key,
                    object_key=object_key,
                    directionality="article_to_entity",
                    provenance={
                        "supports_edge_type": edge["edge_type"],
                        "subject_key": subject_key,
                        "object_key": object_key,
                    },
                )
                literature_edge_count += 1

        # ------------------------------------------------------------------
        # Graph summaries / explanation
        # ------------------------------------------------------------------
        nodes = list(node_map.values())

        node_counts = Counter(node.entity_type.value for node in nodes)
        edge_counts = Counter(edge["edge_type"] for edge in edges)

        top_entities = sorted(
            [node for node in nodes if node.entity_type != EntityType.article],
            key=lambda x: x.confidence,
            reverse=True,
        )[:5]

        ranked_summaries = [
            f"Graph contains {len(nodes)} unique nodes and {len(edges)} edges.",
            (
                "Node counts — "
                f"diseases: {node_counts.get('disease', 0)}, "
                f"genes: {node_counts.get('gene', 0)}, "
                f"variants: {node_counts.get('variant', 0)}, "
                f"phenotypes: {node_counts.get('phenotype', 0)}, "
                f"compounds: {node_counts.get('compound', 0)}, "
                f"trials: {node_counts.get('trial', 0)}, "
                f"articles: {node_counts.get('article', 0)}."
            ),
            (
                "Edge counts — "
                f"structured: {structured_edge_count}, "
                f"literature-derived: {literature_edge_count}."
            ),
        ]

        if top_entities:
            ranked_summaries.append(
                "Top high-confidence entities: "
                + ", ".join(f"{entity.preferred_label} ({entity.entity_type})" for entity in top_entities)
            )

        explanation = {
            "scoring_profile": profile,
            "mention_threshold": mention_threshold,
            "node_counts_by_type": dict(node_counts),
            "edge_counts_by_type": dict(edge_counts),
            "structured_edge_count": structured_edge_count,
            "literature_edge_count": literature_edge_count,
            "notes": [
                "Structured edges come directly from search_structured_evidence().",
                "Literature edges are heuristic mention links from title/abstract matching.",
                "article_supports_relationship_candidate edges are inferred when an article mentions both endpoints of a structured relationship.",
            ],
        }

        return EvidenceGraph(
            nodes=nodes,
            edges=edges,
            ranked_summaries=ranked_summaries,
            explanation=explanation,
        )

    async def generate_dossier(
        self,
        primary_entity: NormalizedEntity,
        scope: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        output_profile: Optional[str] = None,
    ) -> Dossier:
        normalized_bundle = NormalizationResponse(entities=[primary_entity])
        structured = await self.search_structured_evidence(normalized_bundle)

        literature = await self.search_literature(
            disease_ids=[primary_entity.source_ids["orpha"]]
            if primary_entity.entity_type == EntityType.disease and "orpha" in primary_entity.source_ids
            else None,
            gene_ids=[primary_entity.source_ids["hgnc"]]
            if primary_entity.entity_type == EntityType.gene and "hgnc" in primary_entity.source_ids
            else None,
            compound_ids=[primary_entity.source_ids["pubchem"]]
            if primary_entity.entity_type == EntityType.compound and "pubchem" in primary_entity.source_ids
            else None,
            filters=filters,
            normalized_bundle=normalized_bundle,
        )

        graph = await self.assemble_evidence_graph(
            normalized_bundle,
            literature,
            structured,
            scoring_profile=output_profile,
        )

        summary = [
            f"Dossier for {primary_entity.preferred_label} (type: {primary_entity.entity_type})",
            "Number of evidence nodes: " + str(len(graph.nodes)),
        ]

        return Dossier(
            primary_entity=primary_entity,
            scope=scope,
            summary_blocks=summary,
            citation_references=[f"PMID:{art.pmid}" for art in literature if art.pmid],
            evidence_graph=graph,
        )

    def _extract_literature_terms_from_bundle(
        self,
        normalized_bundle: Optional[NormalizationResponse],
    ) -> Dict[str, List[str]]:
        if not normalized_bundle:
            return {
                "disease_terms": [],
                "gene_terms": [],
                "phenotype_terms": [],
                "compound_terms": [],
                "article_ids": [],
            }

        def unique_preserve_order(items: List[str]) -> List[str]:
            seen = set()
            out: List[str] = []
            for item in items:
                cleaned = item.strip()
                if not cleaned:
                    continue
                key = cleaned.lower()
                if key not in seen:
                    seen.add(key)
                    out.append(cleaned)
            return out

        def collect_terms(
            entities: List[NormalizedEntity],
            *,
            synonym_limit: int = 3,
        ) -> List[str]:
            terms: List[str] = []
            for entity in entities:
                if entity.preferred_label:
                    terms.append(entity.preferred_label)
                for synonym in (entity.synonyms or [])[:synonym_limit]:
                    if synonym:
                        terms.append(synonym)
            return unique_preserve_order(terms)

        diseases = [e for e in normalized_bundle.entities if e.entity_type == EntityType.disease]
        genes = [e for e in normalized_bundle.entities if e.entity_type == EntityType.gene]
        phenotypes = [e for e in normalized_bundle.entities if e.entity_type == EntityType.phenotype]
        compounds = [e for e in normalized_bundle.entities if e.entity_type == EntityType.compound]
        articles = [e for e in normalized_bundle.entities if e.entity_type == EntityType.article]

        article_ids: List[str] = []
        for article in articles:
            source_ids = article.source_ids or {}
            for key in ("pmid", "pmcid", "doi"):
                value = source_ids.get(key)
                if value:
                    article_ids.append(str(value))

        return {
            "disease_terms": collect_terms(diseases, synonym_limit=4),
            "gene_terms": collect_terms(genes, synonym_limit=3),
            "phenotype_terms": collect_terms(phenotypes, synonym_limit=3),
            "compound_terms": collect_terms(compounds, synonym_limit=3),
            "article_ids": unique_preserve_order(article_ids),
        }
