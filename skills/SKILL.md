---
name: rare-disease-evidence
description: Use rare-disease-evidence MCP tools for rare disease literature retrieval and evidence synthesis.
---

For saved UI literature payloads, prefer the batch workflow:

1. Use `evidence_plan_agent_payload_batches` to plan batches.
2. Use `evidence_read_agent_payload` with `offset`, `max_results`, and `max_abstract_chars` to read each batch.
3. If available, use `evidence_save_batch_note` after each batch.
4. If available, use `evidence_read_batch_notes` before final synthesis.
5. Produce a cautious final answer with local paper citations like `[P1]`, `[P22]`.

Do not call `response_profile="full"` unless the user explicitly asks for raw debugging JSON.

For broad literature/counting questions, do not try to load all abstracts at once. Process small batches and synthesize from batch notes.