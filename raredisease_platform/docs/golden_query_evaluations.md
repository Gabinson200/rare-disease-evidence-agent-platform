# Golden Query Evaluations

This document defines the manual/live evaluation set used before connecting the broker to an agentic layer such as OpenClaw.

The purpose is not to prove biomedical completeness. The purpose is to catch regressions in the core retrieval contract:

1. Candidate spans are detected.
2. Entities are normalized before search.
3. Weak matches are exposed as alternatives or warnings.
4. Literature search uses normalized disease/gene/phenotype/compound terms.
5. The high-level `/evidence/query` endpoint returns traceable intermediate artifacts.

## How to run

Start the API:

```bash
python -m uvicorn raredisease_platform.main:app --reload
