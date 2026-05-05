"""Command-line interface for the constrained evidence agent.

Example:

    python -m raredisease_platform.agent.cli "case reports for fibrodysplasia ossificans progressiva involving ACVR1"

Make sure the broker is running first:

    python -m uvicorn raredisease_platform.main:app --reload
"""

from __future__ import annotations

import argparse
import asyncio
import json

from .evidence_agent import EvidenceAgent, EvidenceAgentConfig


async def run_async() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("query", help="Biomedical evidence question to ask.")
    parser.add_argument("--broker-url", default="http://127.0.0.1:8000")
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument("--retmax", type=int, default=10)
    parser.add_argument(
        "--no-structured",
        action="store_true",
        help="Disable structured evidence retrieval.",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Print raw planned request and evidence payload as JSON.",
    )

    args = parser.parse_args()

    agent = EvidenceAgent(
        EvidenceAgentConfig(
            broker_base_url=args.broker_url,
            timeout_seconds=args.timeout,
            default_retmax=args.retmax,
            include_structured_evidence=not args.no_structured,
            return_raw_payload=args.raw,
        )
    )

    result = await agent.answer(args.query)

    if args.raw:
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print(result["answer"])


def main() -> None:
    asyncio.run(run_async())


if __name__ == "__main__":
    main()
