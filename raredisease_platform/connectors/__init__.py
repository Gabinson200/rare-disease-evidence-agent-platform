"""Source connectors for external biomedical APIs.

Each connector implements a common interface consisting of methods such as
``search()``, ``fetch_by_id()``, ``normalize()``, and ``crosswalk()`` as
outlined in the specification's connector design (see section 13)【600†L1-L16】.
Connectors translate normalized queries into API requests, parse
responses into internal records, and annotate results with provenance.

This module exposes a registry of available connectors and convenience
functions for accessing them by name.  Concrete connectors live in
submodules like :mod:`raredisease_platform.connectors.pubmed`.  When
adding a new source, create a corresponding module and register it
below.
"""

from typing import Dict

from .base import BaseConnector
from .pubmed import PubMedConnector
from .orphadata import OrphadataConnector
from .hgnc import HGNCConnector
from .hpo import HPOConnector
from .clinvar import ClinVarConnector
from .pubchem import PubChemConnector
from .trials import ClinicalTrialsConnector


#: Registry of available connectors keyed by human‑readable name.
CONNECTOR_REGISTRY: Dict[str, BaseConnector] = {
    "pubmed": PubMedConnector(),
    "orphadata": OrphadataConnector(),
    "hgnc": HGNCConnector(),
    "hpo": HPOConnector(),
    "clinvar": ClinVarConnector(),
    "pubchem": PubChemConnector(),
    "clinicaltrials": ClinicalTrialsConnector(),
}


def get_connector(name: str) -> BaseConnector:
    """Retrieve a connector instance by name.

    Parameters
    ----------
    name: str
        The canonical name of the connector (e.g. ``"pubmed"``).

    Returns
    -------
    BaseConnector
        An initialized connector instance.
    """

    try:
        return CONNECTOR_REGISTRY[name]
    except KeyError as exc:
        raise ValueError(f"Unknown connector: {name}") from exc