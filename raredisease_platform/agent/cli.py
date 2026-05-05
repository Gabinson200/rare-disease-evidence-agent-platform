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
import sys

from .evidence_agent import EvidenceAgent, EvidenceAgentConfig


def configure_stdout() -> None:
    """Make CLI output robust on Windows terminals."""
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


async def run_async() -> None:
    configure_stdout()

    parser = argparse.ArgumentParser()
    parser.add_argument("query", help="Biomedical evidence question to ask.")
    parser.add_argument("--broker-url", default="http://127.0.0.1:8000")
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument(
        "--retmax",
        type=int,
        default=3,
        help="Maximum number of literature records to request. Default is 3 for fast interactive mode.",
    )
    parser.add_argument(
        "--deep",
        action="store_true",
        help="Deep evidence mode: enables structured evidence and raises default retmax to at least 10.",
    )
    parser.add_argument(
        "--structured",
        action="store_true",
        help="Enable structured evidence retrieval without otherwise changing retmax.",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Print raw planned request and evidence payload as JSON.",
    )

    args = parser.parse_args()

    include_structured = args.structured or args.deep
    retmax = args.retmax

    if args.deep:
        retmax = max(retmax, 10)

    agent = EvidenceAgent(
        EvidenceAgentConfig(
            broker_base_url=args.broker_url,
            timeout_seconds=args.timeout,
            default_retmax=retmax,
            include_structured_evidence=include_structured,
            return_raw_payload=args.raw,
        )
    )

    result = await agent.answer(args.query)

    if args.raw:
        print(json.dumps(result, indent=2, ensure_ascii=True))
    else:
        print(result["answer"])


def main() -> None:
    asyncio.run(run_async())


if __name__ == "__main__":
    main()
