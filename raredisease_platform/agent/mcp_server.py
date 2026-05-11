from __future__ import annotations

from typing import Any, Dict, Optional

from mcp.server.fastmcp import FastMCP

from raredisease_platform.agent.openclaw_tools import evidence_query

mcp = FastMCP("rare-disease-evidence")


@mcp.tool()
async def query_rare_disease_evidence(
    question: str,
    retmax: int = 3,
    deep_search: bool = False,
) -> Dict[str, Any]:
    """
    Query the rare disease evidence backend.

    Use this for biomedical evidence questions involving rare diseases,
    genes, variants, phenotypes, compounds, case reports, literature,
    or clinical trials.
    """
    return await evidence_query(
        raw_query=question,
        literature_filters={"retmax": retmax},
        deep_search=deep_search,
        broker_base_url="http://127.0.0.1:8000",
    )


if __name__ == "__main__":
    mcp.run()