from __future__ import annotations

import sys
# Path injection must happen AFTER the future import, but BEFORE importing local modules
sys.path.insert(0, "C:/Users/adamk/Downloads/rare-disease-evidence-agent-platform")

from typing import Any, Dict, Optional, List
from mcp.server.fastmcp import FastMCP
from raredisease_platform.agent.openclaw_tools import evidence_query

mcp = FastMCP("rare-disease-evidence")

@mcp.tool(name="evidence_query")
async def evidence_query_tool(
    raw_query: str,
    expected_entity_types: Optional[List[str]] = None,
    literature_keywords: Optional[str] = None,
    literature_filters: Optional[Dict[str, Any]] = None,
    include_structured_evidence: bool = False,
    requested_evidence_types: Optional[List[str]] = None,
    deep_search: bool = False,
) -> Dict[str, Any]:
    """
    Run the rare disease evidence broker workflow: normalize entities, search literature, optionally retrieve structured evidence, assemble an evidence graph, and return traceable results.
    """
    return await evidence_query(
        raw_query=raw_query,
        expected_entity_types=expected_entity_types,
        literature_keywords=literature_keywords,
        literature_filters=literature_filters,
        include_structured_evidence=include_structured_evidence,
        requested_evidence_types=requested_evidence_types,
        deep_search=deep_search,
        broker_base_url="http://127.0.0.1:8000",
    )

if __name__ == "__main__":
    mcp.run()