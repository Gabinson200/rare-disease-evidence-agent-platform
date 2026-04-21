"""Common base class for all connectors.

Connectors encapsulate the logic for interacting with a specific
biomedical data source.  They transform high‑level normalized queries
into API requests, parse raw responses, and return normalized records
with provenance.  Each method may be asynchronous or synchronous, but
asynchronous implementations are preferred to enable concurrent I/O.

Concrete connectors should subclass :class:`BaseConnector` and override
the relevant methods.  Where an operation is unsupported for a given
source, the default implementation raises :class:`NotImplementedError`.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class BaseConnector(ABC):
    """Abstract base class defining the connector interface."""

    name: str = ""

    async def search(self, query: Dict[str, Any]) -> Any:
        """Search the external source with a normalized query object.

        Parameters
        ----------
        query: dict
            A source‑specific query object prepared by the broker.

        Returns
        -------
        Any
            Parsed response from the source.  The exact type depends on
            the connector and is described in the specification (see
            section 13)【600†L1-L16】.
        """
        raise NotImplementedError

    async def fetch_by_id(self, identifier: str) -> Any:
        """Retrieve a record from the source by its stable identifier.

        Not all sources support fetching by ID.  Implementations should
        raise :class:`NotImplementedError` if unsupported.
        """
        raise NotImplementedError

    async def normalize(self, text: str) -> List[Dict[str, Any]]:
        """Normalize a free‑text string into one or more canonical
        entities using this source as an authority.

        The default implementation raises :class:`NotImplementedError`.
        """
        raise NotImplementedError

    async def crosswalk(self, source_id: str) -> Dict[str, str]:
        """Map a source‑specific identifier to other identifiers.

        Returns a dictionary of external ID namespaces to identifiers.
        """
        raise NotImplementedError

    async def health_check(self) -> bool:
        """Return True if the connector can reach its underlying service."""
        return True

    async def rate_limit_policy(self) -> Dict[str, Any]:
        """Return the rate limit policy for this connector.

        The broker can use this information to schedule requests."""
        return {}