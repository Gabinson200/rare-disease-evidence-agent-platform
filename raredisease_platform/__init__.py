"""Top level package for the rare disease evidence retrieval platform.

This package exposes a minimal, but structured, implementation of the
architecture laid out in the provided software specification.  It defines
Pydantic models for canonical entities and evidence, basic connector
interfaces, a broker orchestrating queries across connectors, and a
FastAPI application exposing these capabilities as high‑level endpoints.

The goal of this initial code base is to provide a solid foundation for
future development.  It follows the "normalize first" principle by
wrapping all incoming questions through a normalization routine before
performing any source‑specific searches【200†L18-L25】.  All functions
annotate results with provenance so downstream consumers can trace
decisions and data back to their origin【800†L14-L20】.

End users should interact with the API by importing the FastAPI app
defined in :mod:`raredisease_platform.main` and starting it via an ASGI
server.  Developers can extend the broker, add new connectors, and plug
additional ranking or caching logic without modifying the public API.
"""

from .models import (
    EntityType,
    NormalizeRequest,
    NormalizedEntity,
    NormalizationResponse,
    PubMedSearchFilters,
    LiteratureSearchRequest,
    LiteratureResult,
    StructuredEvidenceResult,
    EvidenceGraph,
    Dossier,
)
from .broker import Broker

__all__ = [
    "EntityType",
    "NormalizeRequest",
    "NormalizedEntity",
    "NormalizationResponse",
    "PubMedSearchFilters",
    "LiteratureSearchRequest",
    "LiteratureResult",
    "StructuredEvidenceResult",
    "EvidenceGraph",
    "Dossier",
    "Broker",
]
