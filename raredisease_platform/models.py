"""Pydantic models defining the core data structures used by the platform."""

from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class EntityType(str, Enum):
    """Enumeration of core entity types supported by the platform."""

    disease = "disease"
    gene = "gene"
    variant = "variant"
    phenotype = "phenotype"
    compound = "compound"
    article = "article"
    trial = "trial"


class NormalizeRequest(BaseModel):
    raw_query: str
    expected_entity_types: Optional[List[EntityType]] = None
    disambiguation_preferences: Optional[Dict[str, Any]] = None


class GeneLookupRequest(BaseModel):
    raw_gene: str


class GeneCrosswalkRequest(BaseModel):
    identifier: str
    namespace: Literal[
        "hgnc_id",
        "hgnc",
        "symbol",
        "entrez_id",
        "ensembl_gene_id",
        "omim_id",
    ] = "hgnc_id"


class NormalizedEntity(BaseModel):
    """Representation of a canonical biomedical entity."""

    entity_type: EntityType = Field(..., description="The class of entity.")
    preferred_label: str = Field(..., description="Normalized preferred label.")
    source_ids: Dict[str, str] = Field(
        default_factory=dict,
        description="Mapping from source names to their stable identifiers.",
    )
    synonyms: List[str] = Field(default_factory=list, description="Known synonyms.")
    description: Optional[str] = Field(
        None,
        description="Optional free-text description from ontology sources.",
    )
    confidence: float = Field(
        0.0,
        ge=0.0,
        le=1.0,
        description="Confidence of the normalization between 0 and 1.",
    )
    provenance: Optional[Dict[str, Any]] = Field(
        None,
        description="Provenance metadata describing how this entity was derived.",
    )


class NormalizationResponse(BaseModel):
    """Response from the `normalize_entities` tool."""

    entities: List[NormalizedEntity] = Field(
        default_factory=list,
        description="List of normalized entities.",
    )
    alternatives: Optional[List[NormalizedEntity]] = Field(
        None,
        description="Candidate alternative entities when normalization is ambiguous.",
    )


class LiteratureMatchFeatures(BaseModel):
    """Features used when scoring a literature hit."""

    exact_disease_id: bool = False
    exact_gene_id: bool = False
    phenotype_overlap_strength: Optional[float] = None
    mesh_topic_importance: Optional[float] = None
    title_match_strength: Optional[float] = None
    abstract_match_strength: Optional[float] = None
    publication_type: Optional[str] = None
    recency: Optional[float] = None
    full_text_available: Optional[bool] = None
    source_trust_level: Optional[float] = None


class PubMedSearchFilters(BaseModel):
    publication_types: List[str] = Field(
        default_factory=list,
        description="Publication type clauses added to the PubMed term.",
    )
    languages: List[str] = Field(
        default_factory=list,
        description="Language filters mapped into the PubMed query term.",
    )
    language: Optional[str | List[str]] = Field(
        default=None,
        description="Backward-compatible alias for languages.",
    )
    case_reports_only: bool = Field(
        default=False,
        description="Adds the Case Reports publication type filter to the query term.",
    )
    reviews_only: bool = Field(
        default=False,
        description="Adds the Review publication type filter to the query term.",
    )
    trials_only: bool = Field(
        default=False,
        description="Adds the Clinical Trial publication type filter to the query term.",
    )
    title_only: bool = Field(
        default=False,
        description="Restricts keywords to the Title field in the PubMed term.",
    )

    sort: Literal["relevance", "pub_date", "Author", "JournalName"] = Field(
        default="relevance",
        description="Documented PubMed ESearch sort value.",
    )
    field: Optional[str] = Field(
        default=None,
        description="Optional ESearch field restriction, e.g. 'title'.",
    )
    datetype: Optional[Literal["pdat", "edat", "mdat"]] = Field(
        default="pdat",
        description="Date type used by PubMed ESearch date filtering.",
    )
    reldate: Optional[int] = Field(
        default=None,
        ge=1,
        description="Last n days, used with datetype.",
    )
    mindate: Optional[str] = Field(
        default=None,
        description="ESearch minimum date in YYYY, YYYY/MM, or YYYY/MM/DD.",
    )
    maxdate: Optional[str] = Field(
        default=None,
        description="ESearch maximum date in YYYY, YYYY/MM, or YYYY/MM/DD.",
    )

    date_from: Optional[str] = Field(
        default=None,
        description="Backward-compatible alias for mindate; YYYY-MM-DD or YYYY/MM/DD accepted.",
    )
    date_to: Optional[str] = Field(
        default=None,
        description="Backward-compatible alias for maxdate; YYYY-MM-DD or YYYY/MM/DD accepted.",
    )

    retstart: int = Field(
        default=0,
        ge=0,
        description="ESearch result offset.",
    )
    retmax: int = Field(
        default=10,
        ge=1,
        le=10000,
        description="Maximum number of UIDs to retrieve from ESearch.",
    )


class LiteratureSearchRequest(BaseModel):
    disease_ids: Optional[List[str]] = None
    gene_ids: Optional[List[str]] = None
    phenotype_ids: Optional[List[str]] = None
    compound_ids: Optional[List[str]] = None
    keywords: Optional[str] = None
    filters: Optional[PubMedSearchFilters] = None
    normalized_bundle: Optional[NormalizationResponse] = None

class LiteratureProvenance(BaseModel):
    source: str
    retrieved_at: Optional[str] = None
    raw_record: Optional[Dict[str, Any]] = None


class LiteratureResult(BaseModel):
    """Normalized representation of a literature item."""

    pmid: Optional[str] = None
    pmcid: Optional[str] = None
    doi: Optional[str] = None
    title: str
    abstract: Optional[str] = None
    year: Optional[int] = None
    journal: Optional[str] = None
    authors: Optional[List[str]] = None
    match_features: LiteratureMatchFeatures = Field(
        default_factory=LiteratureMatchFeatures
    )
    score: float = 0.0
    provenance: LiteratureProvenance


class StructuredEvidenceResult(BaseModel):
    """Container for structured evidence from non-literature sources."""

    diseases: Optional[List[NormalizedEntity]] = None
    genes: Optional[List[NormalizedEntity]] = None
    variants: Optional[List[NormalizedEntity]] = None
    phenotypes: Optional[List[NormalizedEntity]] = None
    compounds: Optional[List[NormalizedEntity]] = None
    trials: Optional[List[NormalizedEntity]] = None
    relationships: Optional[List[Dict[str, Any]]] = None

class EvidenceGraph(BaseModel):
    """Aggregated evidence graph returned by `assemble_evidence_graph`."""

    nodes: List[NormalizedEntity]
    edges: List[Dict[str, Any]]
    ranked_summaries: Optional[List[str]] = None
    explanation: Optional[Dict[str, Any]] = None


class Dossier(BaseModel):
    """Structured dossier summarizing evidence around a primary entity."""

    primary_entity: NormalizedEntity
    scope: Optional[str] = None
    summary_blocks: Optional[List[str]] = None
    citation_references: Optional[List[str]] = None
    evidence_graph: Optional[EvidenceGraph] = None
