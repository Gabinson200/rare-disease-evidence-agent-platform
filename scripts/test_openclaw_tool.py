"""Smoke test for the OpenClaw-facing evidence_query tool.

Run the broker first:

    python -m uvicorn raredisease_platform.main:app --reload

Then:

    python scripts/test_openclaw_tool.py
"""

from __future__ import annotations

import asyncio
import json

from raredisease_platform.agent.openclaw_tools import evidence_query


async def main() -> None:
    result = await evidence_query(
        raw_query="case reports for fibrodysplasia ossificans progressiva involving ACVR1",
        expected_entity_types=["disease", "gene"],
        literature_keywords="case report",
        literature_filters={
            "case_reports_only": True,
            "exact_gene_required": True,
            "retmax": 3,
        },
        deep_search=False,
    )

    print(json.dumps(result, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    asyncio.run(main())
