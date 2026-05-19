from __future__ import annotations

import html
import json
import math
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import streamlit as st


DEFAULT_BROKER_BASE_URL = "http://127.0.0.1:8000"
OUT_DIR = Path("ui_runs")
OUT_DIR.mkdir(exist_ok=True)


# ---------------------------------------------------------------------------
# Small utility helpers
# ---------------------------------------------------------------------------

def clean_text(value: Any, *, max_chars: Optional[int] = None) -> str:
    """Normalize whitespace, remove simple HTML tags, and optionally truncate."""
    if value is None:
        return ""

    text = html.unescape(str(value))
    text = re.sub(r"<[^>]+>", " ", text)
    text = " ".join(text.split())

    if max_chars is not None and max_chars > 0 and len(text) > max_chars:
        return text[: max_chars - 3].rstrip() + "..."

    return text


def remove_empty(value: Any) -> Any:
    """Recursively remove None/empty values from dicts/lists."""
    if isinstance(value, dict):
        cleaned = {k: remove_empty(v) for k, v in value.items()}
        return {
            k: v
            for k, v in cleaned.items()
            if v not in (None, "", [], {})
        }

    if isinstance(value, list):
        cleaned_list = [remove_empty(v) for v in value]
        return [v for v in cleaned_list if v not in (None, "", [], {})]

    return value


def save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )


def json_size_chars(payload: Dict[str, Any]) -> int:
    return len(json.dumps(payload, ensure_ascii=False, default=str))


def post_evidence_query(
    *,
    broker_base_url: str,
    payload: Dict[str, Any],
    timeout_seconds: int = 300,
) -> Dict[str, Any]:
    response = requests.post(
        f"{broker_base_url.rstrip('/')}/evidence/query",
        json=payload,
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    return response.json()


# ---------------------------------------------------------------------------
# Payload builders
# ---------------------------------------------------------------------------

def compact_entity(entity: Dict[str, Any]) -> Dict[str, Any]:
    source_ids = entity.get("source_ids") or {}

    keep_ids = {}
    for key in [
        "orpha",
        "mondo",
        "hgnc",
        "entrez",
        "ensembl",
        "hpo",
        "mesh",
        "medgen",
        "clinvar",
        "pubchem",
        "nct",
    ]:
        if key in source_ids:
            keep_ids[key] = source_ids[key]

    return remove_empty(
        {
            "type": entity.get("entity_type"),
            "label": entity.get("preferred_label"),
            "ids": keep_ids,
            "confidence": entity.get("confidence"),
            "synonyms": (entity.get("synonyms") or [])[:3],
        }
    )


def compact_article_for_agent(article: Dict[str, Any], *, max_abstract_chars: int) -> Dict[str, Any]:
    """Compact article object saved in *.agent.json for MCP reading."""
    match_features = article.get("match_features") or {}

    return remove_empty(
        {
            "pmid": article.get("pmid"),
            "pmcid": article.get("pmcid"),
            "doi": article.get("doi"),
            "title": clean_text(article.get("title")),
            "year": article.get("year"),
            "journal": clean_text(article.get("journal")),
            "authors": (article.get("authors") or [])[:5],
            "score": article.get("score"),
            "publication_type": match_features.get("publication_type"),
            "abstract_excerpt": clean_text(
                article.get("abstract"),
                max_chars=max_abstract_chars,
            ),
        }
    )


def article_for_abstract_pack(article: Dict[str, Any], *, index: int, max_abstract_chars: int) -> Dict[str, Any]:
    """Minimal article object intended to be fed to OpenClaw."""
    return remove_empty(
        {
            "id": f"P{index}",
            "title": clean_text(article.get("title")),
            "year": article.get("year"),
            "journal": clean_text(article.get("journal")),
            "pmid": article.get("pmid"),
            "pmcid": article.get("pmcid"),
            "doi": article.get("doi"),
            "abstract": clean_text(
                article.get("abstract_excerpt") or article.get("abstract"),
                max_chars=max_abstract_chars,
            ),
        }
    )


def build_agent_payload(
    full_result: Dict[str, Any],
    *,
    max_results_for_agent: int,
    max_abstract_chars: int,
) -> Dict[str, Any]:
    """
    Compact saved payload for the MCP reader tool.

    It keeps only result counts, compact entities, top article metadata/abstracts,
    and short warnings. The full raw backend response is saved separately.
    """
    normalized_bundle = full_result.get("normalized_bundle") or {}
    entities = normalized_bundle.get("entities") or []
    alternatives = normalized_bundle.get("alternatives") or []
    literature_results = full_result.get("literature_results") or []
    trace = full_result.get("trace") or {}
    normalization_trace = normalized_bundle.get("normalization_trace") or {}

    return remove_empty(
        {
            "response_profile": "agent_compact",
            "query_summary": {
                "literature_result_count": len(literature_results),
                "entity_count": len(entities),
                "alternative_count": len(alternatives),
            },
            "interpreted_entities": [
                compact_entity(e) for e in entities[:10] if isinstance(e, dict)
            ],
            "alternative_entities": [
                compact_entity(e) for e in alternatives[:5] if isinstance(e, dict)
            ],
            "top_literature_results": [
                compact_article_for_agent(a, max_abstract_chars=max_abstract_chars)
                for a in literature_results[:max_results_for_agent]
                if isinstance(a, dict)
            ],
            "trace_summary": {
                "pipeline_warnings": (trace.get("warnings") or [])[:5],
                "normalization_warnings": (normalization_trace.get("warnings") or [])[:5],
                "pipeline_steps": [
                    {
                        "step": step.get("step"),
                        "status": step.get("status"),
                        "result_count": step.get("result_count"),
                        "entity_count": step.get("entity_count"),
                        "alternative_count": step.get("alternative_count"),
                        "reason": step.get("reason"),
                    }
                    for step in (trace.get("steps") or [])
                    if isinstance(step, dict)
                ],
            },
        }
    )


def build_openclaw_abstract_pack(
    agent_payload: Dict[str, Any],
    *,
    offset: int,
    max_results: int,
    max_abstract_chars: int,
) -> Dict[str, Any]:
    """
    Ultra-small payload for one OpenClaw batch.

    This is the payload the model should reason over: paper IDs, titles,
    citation metadata, and abstracts.
    """
    query_summary = agent_payload.get("query_summary") or {}
    articles = agent_payload.get("top_literature_results") or []
    trace_summary = agent_payload.get("trace_summary") or {}

    offset = max(0, int(offset))
    max_results = max(1, int(max_results))
    selected = articles[offset : offset + max_results]

    papers = [
        article_for_abstract_pack(
            article,
            index=offset + local_index,
            max_abstract_chars=max_abstract_chars,
        )
        for local_index, article in enumerate(selected, start=1)
        if isinstance(article, dict)
    ]

    return remove_empty(
        {
            "response_profile": "abstract_pack",
            "literature_result_count": query_summary.get("literature_result_count"),
            "available_paper_count": len(articles),
            "offset": offset,
            "returned_paper_count": len(papers),
            "paper_id_range": f"P{offset + 1}-P{offset + len(papers)}" if papers else None,
            "papers": papers,
            "warnings": (trace_summary.get("pipeline_warnings") or [])[:3],
            "citation_instructions": (
                "Cite papers using local IDs like [P1], [P2]. "
                "Use PMID/DOI fields for verification when available."
            ),
        }
    )


def make_batch_plan(*, available_papers: int, papers_to_process: int, batch_size: int) -> List[Dict[str, int]]:
    papers_to_process = max(1, min(int(papers_to_process), int(available_papers)))
    batch_size = max(1, int(batch_size))

    batches: List[Dict[str, int]] = []
    for offset in range(0, papers_to_process, batch_size):
        count = min(batch_size, papers_to_process - offset)
        batches.append(
            {
                "batch_number": len(batches) + 1,
                "offset": offset,
                "max_results": count,
                "first_paper_id": offset + 1,
                "last_paper_id": offset + count,
            }
        )

    return batches


# ---------------------------------------------------------------------------
# OpenClaw prompt generators
# ---------------------------------------------------------------------------

def make_single_pass_mcp_prompt(
    *,
    agent_payload_path: str,
    task: str,
    word_limit: int,
    max_results: int,
    max_abstract_chars: int,
) -> str:
    filename = Path(agent_payload_path).name

    return f"""Use the rare-disease-evidence MCP tool evidence_read_agent_payload with path_or_name="{filename}", offset=0, max_results={max_results}, max_abstract_chars={max_abstract_chars}, include_trace_warnings=true.

Using only the returned abstract_pack, answer this research question:

{task}

Write a cautious answer in under {word_limit} words. Cite sources using paper IDs like [P1], [P2]. Do not request or print the full JSON.
"""


def make_batch_prompt(
    *,
    agent_payload_path: str,
    task: str,
    batch_number: int,
    offset: int,
    max_results: int,
    max_abstract_chars: int,
    batch_word_limit: int,
) -> str:
    filename = Path(agent_payload_path).name
    start_pid = offset + 1
    end_pid = offset + max_results

    return f"""Use the rare-disease-evidence MCP tool evidence_read_agent_payload with path_or_name="{filename}", offset={offset}, max_results={max_results}, max_abstract_chars={max_abstract_chars}, include_trace_warnings=true.

This is batch {batch_number}, covering papers P{start_pid}-P{end_pid}.

Using only the returned abstract_pack, write a batch summary for this research question:

{task}

In under {batch_word_limit} words:
- State whether any paper in this batch gives a direct count, catalog size, database count, or estimate.
- Separate direct counting/catalog evidence from individual case examples and conceptual/review papers.
- Cite relevant paper IDs like [P{start_pid}], [P{start_pid + 1}].
- Do not answer the final question yet; only summarize this batch.
- Do not request or print the full JSON.
"""


def make_final_synthesis_prompt(*, task: str, word_limit: int) -> str:
    return f"""Using the batch summaries above, answer the final research question:

{task}

Write a cautious synthesis in under {word_limit} words.

Required structure:
1. Direct answer: give a precise number only if the batch summaries support one.
2. Evidence basis: distinguish database/catalog/counting papers from case reports and conceptual/review papers.
3. Uncertainty: explain whether the evidence supports a fixed count, a lower bound, or no reliable count.
4. Citations: cite the most relevant local paper IDs mentioned in the batch summaries, such as [P1], [P22].

Do not ask for the full JSON.
"""


def make_recursive_batch_prompt(
    *,
    agent_payload_path: str,
    task: str,
    papers_to_process: int,
    batch_size: int,
    max_abstract_chars: int,
    batch_word_limit: int,
    final_word_limit: int,
) -> str:
    """One prompt that asks OpenClaw to orchestrate all batch tool calls itself."""
    filename = Path(agent_payload_path).name

    return f"""You are an evidence-synthesis agent with access to the rare-disease-evidence MCP tools.

Goal: answer the research question below by processing the saved evidence payload in batches. Do not ask me to copy/paste individual batch prompts. You should call the MCP tools yourself.

Research question:
{task}

Use this workflow exactly:

1. Call evidence_plan_agent_payload_batches with:
   - path_or_name=\"{filename}\"
   - papers_to_process={papers_to_process}
   - batch_size={batch_size}
   - max_abstract_chars={max_abstract_chars}

2. For every batch returned in the plan, call evidence_read_agent_payload with:
   - path_or_name=\"{filename}\"
   - offset=<batch.offset>
   - max_results=<batch.max_results>
   - max_abstract_chars={max_abstract_chars}
   - include_trace_warnings=true

3. For each batch, make a short working batch note under {batch_word_limit} words. Each batch note should identify:
   - any paper that gives a direct count, database/catalog size, cohort count, or estimate;
   - papers that are only individual examples/case reports;
   - conceptual/review/modeling papers;
   - relevant citations using paper IDs like [P1], [P22].

4. After all batches have been read, write the final answer in under {final_word_limit} words.

Final answer requirements:
- Give a precise number only if the batch evidence supports one.
- If no precise count is supported, say that clearly and explain why.
- Distinguish catalog/counting evidence from individual disease examples and conceptual/review papers.
- Cite the most relevant paper IDs from across the batches.
- Do not print raw JSON or complete abstracts.
- Do not call response_profile=full.
"""


def make_inline_prompt(
    *,
    abstract_pack: Dict[str, Any],
    task: str,
    word_limit: int,
) -> str:
    payload_text = json.dumps(abstract_pack, indent=2, ensure_ascii=False, default=str)

    return f"""Answer this research question:

{task}

Use only the compact abstract_pack below. Do not ask to read a file. Do not request the full JSON.

```json
{payload_text}
```

Write a cautious answer in under {word_limit} words. Cite sources using paper IDs like [P1], [P2].
"""


# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Rare Disease Evidence Research UI",
    layout="wide",
)

st.title("Rare Disease Evidence Research UI")

st.caption(
    "Run large backend literature searches, save full raw results locally, "
    "and generate batchable abstract packs for OpenClaw."
)

with st.sidebar:
    st.header("Backend")

    broker_url = st.text_input(
        "Broker URL",
        value=DEFAULT_BROKER_BASE_URL,
        help="The local FastAPI broker endpoint. Usually http://127.0.0.1:8000.",
    )
    broker_url = broker_url.rstrip("/")

    st.header("Search controls")

    retmax = st.slider(
        "Raw literature retmax",
        min_value=1,
        max_value=500,
        value=150,
        step=1,
        help=(
            "Maximum number of raw literature records requested from the backend. "
            "Higher values retrieve more evidence but take longer and create larger full-result files."
        ),
    )

    max_results_for_agent = st.slider(
        "Papers saved for OpenClaw",
        min_value=1,
        max_value=200,
        value=80,
        step=1,
        help=(
            "Number of top literature results saved in the .agent.json file. "
            "Batch prompts can page through this set with offset/max_results."
        ),
    )

    max_abstract_chars = st.slider(
        "Max abstract chars per paper",
        min_value=100,
        max_value=3000,
        value=700,
        step=100,
        help="Maximum abstract characters saved per paper. Lower values reduce token cost.",
    )

    timeout_seconds = st.slider(
        "Backend timeout seconds",
        min_value=30,
        max_value=600,
        value=300,
        step=30,
        help="Maximum time to wait for the backend query to complete.",
    )

    st.header("Structured evidence")

    include_structured_evidence = st.checkbox(
        "Include structured evidence",
        value=False,
        help=(
            "If disabled, the backend focuses on normalization and literature search. "
            "If enabled, it also runs extra structured database enrichment such as genes, "
            "variants, trials, compounds, and relationships. This can be slower and produce larger outputs."
        ),
    )

    if include_structured_evidence:
        st.info(
            "Structured evidence is enabled. The selected Requested structured evidence types "
            "will control which enrichment paths are attempted."
        )
    else:
        st.caption(
            "Structured evidence is disabled. Requested structured evidence types will be ignored."
        )

    st.header("Publication filters")

    case_reports_only = st.checkbox(
        "Case reports only",
        value=False,
        help="Restrict literature search to case reports when supported by the source.",
    )

    reviews_only = st.checkbox(
        "Reviews only",
        value=False,
        help="Restrict literature search to reviews when supported by the source.",
    )

    trials_only = st.checkbox(
        "Clinical trials only",
        value=False,
        help="Restrict literature search to clinical trial publications when supported by the source.",
    )

    exact_gene_required = st.checkbox(
        "Exact gene required",
        value=False,
        help=(
            "Ask the backend to prefer/filter results that visibly mention the normalized gene term "
            "in the title or abstract. Best for entity-specific gene queries."
        ),
    )

    exact_disease_required = st.checkbox(
        "Exact disease required",
        value=False,
        help=(
            "Ask the backend to prefer/filter results that visibly mention the normalized disease term "
            "in the title or abstract. Best for entity-specific disease queries."
        ),
    )

    sort = st.selectbox(
        "Sort",
        ["relevance", "pub_date", "Author", "JournalName"],
        index=0,
        help="Sort option passed to the literature source when supported.",
    )

    date_from = st.text_input(
        "Date from",
        value="",
        placeholder="YYYY/MM/DD or YYYY",
        help="Optional lower publication date bound.",
    )

    date_to = st.text_input(
        "Date to",
        value="",
        placeholder="YYYY/MM/DD or YYYY",
        help="Optional upper publication date bound.",
    )

    st.header("OpenClaw batching")

    single_pass_results = st.slider(
        "Single-pass papers",
        min_value=1,
        max_value=50,
        value=20,
        step=1,
        help="Number of papers to use in the single-call prompt.",
    )

    papers_to_batch = st.slider(
        "Papers to process in batches",
        min_value=1,
        max_value=200,
        value=80,
        step=1,
        help="Total number of saved papers to cover with batch prompts.",
    )

    batch_size = st.slider(
        "Batch size",
        min_value=5,
        max_value=50,
        value=20,
        step=5,
        help="Number of papers per OpenClaw batch prompt.",
    )

    batch_word_limit = st.slider(
        "Batch summary word limit",
        min_value=100,
        max_value=1000,
        value=350,
        step=50,
        help="Word limit for each batch summary.",
    )

    final_word_limit = st.slider(
        "Final synthesis word limit",
        min_value=200,
        max_value=2000,
        value=900,
        step=100,
        help="Word limit for the final synthesis after batch summaries.",
    )

    single_pass_word_limit = st.slider(
        "Single-pass summary word limit",
        min_value=100,
        max_value=1500,
        value=700,
        step=50,
        help="Word limit for the single-call prompt.",
    )


query = st.text_area(
    "Research question / raw query",
    value="how many digenic rare diseases are there?",
    height=100,
    help=(
        "The main natural-language query sent to the backend. "
        "For broad literature review questions, keep this phrased like a research question."
    ),
)

literature_keywords = st.text_input(
    "Literature keywords",
    value="digenic disease OR digenic inheritance OR digenic rare disease OR DIDA",
    help=(
        "Extra literature-search keywords. Useful for broad review-style questions. "
        "For entity-specific searches, this can be something like 'case report' or 'review'."
    ),
)

st.subheader("Entity interpretation")

st.markdown(
    """
    **Expected entity types** tell the backend what kinds of biomedical entities it should try
    to recognize in the query before searching literature.

    Examples:
    - Use **disease** for terms like *fibrodysplasia ossificans progressiva*.
    - Use **gene** for symbols like *ACVR1*.
    - Use **variant** for terms like *p.Arg206His*, *c.617G>A*, or *rs...*.
    - Use **phenotype** for clinical features like seizures, short stature, or heterotopic ossification.
    - Use **compound** for drugs or chemicals.
    - Use **trial** for NCT IDs or clinical trial queries.

    For broad literature/counting questions, leave this empty and let the backend search by keywords.
    """
)

expected_entity_types = st.multiselect(
    "Expected entity types",
    ["disease", "gene", "variant", "phenotype", "compound", "trial"],
    default=[],
    help=(
        "Hints for the normalization stage. Leave empty for automatic inference or broad keyword searches."
    ),
)

st.subheader("Structured evidence enrichment")

st.markdown(
    """
    **Requested structured evidence types** control which extra database enrichment steps run
    after the query has been normalized.

    These are different from literature search. Literature search uses PubMed / Europe PMC.
    Structured evidence can additionally look up related genes, diseases, variants, phenotypes,
    compounds, trials, and relationships.

    This field only matters when **Include structured evidence** is enabled in the sidebar.
    For large literature reviews, leave structured evidence disabled to keep the search faster.
    """
)

requested_evidence_types = st.multiselect(
    "Requested structured evidence types",
    ["genes", "diseases", "variants", "phenotypes", "compounds", "trials", "relationships"],
    default=[],
    help=(
        "Controls optional enrichment after normalization. Only used if Include structured evidence is enabled."
    ),
)

with st.expander("Recommended settings by query type"):
    st.markdown(
        """
        **Broad literature review / counting question**

        Example: *How many digenic rare diseases are there?*

        Recommended:
        - Leave **Expected entity types** empty.
        - Keep **Include structured evidence** disabled.
        - Use **Raw literature retmax** around 100–300.
        - Save 50–150 papers for OpenClaw.
        - Use batch prompts of 10–25 papers.

        **Entity-specific literature question**

        Example: *Case reports for fibrodysplasia ossificans progressiva involving ACVR1*

        Recommended:
        - Set **Expected entity types** to `disease` and `gene`.
        - Use `case report` as literature keywords.
        - Enable **Case reports only** if appropriate.

        **Database enrichment question**

        Example: *Find variants and relationships for ACVR1 and FOP*

        Recommended:
        - Set **Expected entity types** to `disease` and `gene`.
        - Enable **Include structured evidence**.
        - Select requested evidence types such as `variants`, `genes`, `diseases`, and `relationships`.
        """
    )


col1, col2, col3 = st.columns(3)

with col1:
    run_button = st.button("Run backend search", type="primary")

with col2:
    clear_button = st.button("Clear UI state")

with col3:
    st.write("")

if clear_button:
    st.session_state.clear()
    st.rerun()


if run_button:
    filters: Dict[str, Any] = {
        "retmax": retmax,
        "sort": sort,
    }

    if case_reports_only:
        filters["case_reports_only"] = True
    if reviews_only:
        filters["reviews_only"] = True
    if trials_only:
        filters["trials_only"] = True
    if exact_gene_required:
        filters["exact_gene_required"] = True
    if exact_disease_required:
        filters["exact_disease_required"] = True
    if date_from.strip():
        filters["mindate"] = date_from.strip()
    if date_to.strip():
        filters["maxdate"] = date_to.strip()

    payload: Dict[str, Any] = {
        "raw_query": query,
        "literature_keywords": literature_keywords or None,
        "literature_filters": filters,
        "include_structured_evidence": include_structured_evidence,
    }

    if expected_entity_types:
        payload["expected_entity_types"] = expected_entity_types

    if include_structured_evidence and requested_evidence_types:
        payload["requested_evidence_types"] = requested_evidence_types

    run_id = time.strftime("%Y%m%d_%H%M%S")
    request_path = OUT_DIR / f"{run_id}.request.json"
    full_path = OUT_DIR / f"{run_id}.full.json"
    agent_path = OUT_DIR / f"{run_id}.agent.json"
    abstract_pack_path = OUT_DIR / f"{run_id}.abstract_pack.json"
    batch_plan_path = OUT_DIR / f"{run_id}.batch_plan.json"

    save_json(request_path, payload)

    st.info("Running backend query. This may take a while for high retmax values.")

    try:
        started = time.perf_counter()
        full_result = post_evidence_query(
            broker_base_url=broker_url,
            payload=payload,
            timeout_seconds=timeout_seconds,
        )
        elapsed = time.perf_counter() - started

        agent_payload = build_agent_payload(
            full_result,
            max_results_for_agent=max_results_for_agent,
            max_abstract_chars=max_abstract_chars,
        )

        single_pass_count = min(single_pass_results, max_results_for_agent)
        abstract_pack = build_openclaw_abstract_pack(
            agent_payload,
            offset=0,
            max_results=single_pass_count,
            max_abstract_chars=max_abstract_chars,
        )

        available_for_batch = len(agent_payload.get("top_literature_results") or [])
        batch_plan = make_batch_plan(
            available_papers=available_for_batch,
            papers_to_process=min(papers_to_batch, available_for_batch),
            batch_size=batch_size,
        )

        save_json(full_path, full_result)
        save_json(agent_path, agent_payload)
        save_json(abstract_pack_path, abstract_pack)
        save_json(
            batch_plan_path,
            {
                "agent_payload": Path(agent_path).name,
                "question": query,
                "batch_size": batch_size,
                "papers_to_batch": min(papers_to_batch, available_for_batch),
                "max_abstract_chars": max_abstract_chars,
                "batches": batch_plan,
            },
        )

        st.session_state["last_request"] = payload
        st.session_state["last_full_result"] = full_result
        st.session_state["last_agent_payload"] = agent_payload
        st.session_state["last_abstract_pack"] = abstract_pack
        st.session_state["last_batch_plan"] = batch_plan
        st.session_state["last_request_path"] = str(request_path)
        st.session_state["last_full_path"] = str(full_path)
        st.session_state["last_agent_path"] = str(agent_path)
        st.session_state["last_abstract_pack_path"] = str(abstract_pack_path)
        st.session_state["last_batch_plan_path"] = str(batch_plan_path)
        st.session_state["last_elapsed"] = elapsed
        st.session_state["last_max_abstract_chars"] = max_abstract_chars
        st.session_state["last_single_pass_results"] = single_pass_count
        st.session_state["last_papers_to_batch"] = min(papers_to_batch, available_for_batch)
        st.session_state["last_batch_size"] = batch_size
        st.session_state["last_batch_word_limit"] = batch_word_limit
        st.session_state["last_final_word_limit"] = final_word_limit
        st.session_state["last_single_pass_word_limit"] = single_pass_word_limit

        st.success(f"Backend query completed in {elapsed:.2f} seconds.")

    except Exception as exc:
        st.error(f"Query failed: {type(exc).__name__}: {exc}")


if "last_agent_payload" in st.session_state:
    st.header("OpenClaw payloads")

    full_result = st.session_state["last_full_result"]
    agent_payload = st.session_state["last_agent_payload"]
    abstract_pack = st.session_state["last_abstract_pack"]
    batch_plan = st.session_state["last_batch_plan"]

    literature_results = full_result.get("literature_results") or []
    available_papers = len(agent_payload.get("top_literature_results") or [])

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Raw literature results", len(literature_results))
    c2.metric("Saved papers", available_papers)
    c3.metric("Single-pass papers", len(abstract_pack.get("papers") or []))
    c4.metric("Batch count", len(batch_plan))
    c5.metric("Elapsed seconds", f"{st.session_state['last_elapsed']:.1f}")

    st.caption(
        f"Single-pass abstract pack chars: {json_size_chars(abstract_pack):,}. "
        "Batch prompts are recommended for thorough synthesis over many papers."
    )

    st.subheader("Preview: first single-pass abstract pack")

    for paper in abstract_pack.get("papers") or []:
        label_parts = [f"{paper.get('id', '?')}. {paper.get('title', 'Untitled')}"]
        if paper.get("year"):
            label_parts.append(f"({paper['year']})")
        if paper.get("pmid"):
            label_parts.append(f"PMID:{paper['pmid']}")

        with st.expander(" ".join(label_parts)):
            st.json(paper)

    st.subheader("Interpreted entities")
    st.json(agent_payload.get("interpreted_entities") or [])

    st.subheader("Generated OpenClaw prompts")

    single_prompt = make_single_pass_mcp_prompt(
        agent_payload_path=st.session_state["last_agent_path"],
        task=query,
        word_limit=int(st.session_state.get("last_single_pass_word_limit", 500)),
        max_results=st.session_state["last_single_pass_results"],
        max_abstract_chars=st.session_state["last_max_abstract_chars"],
    )

    st.text_area(
        "Option A: single-pass prompt",
        value=single_prompt,
        height=240,
        help=(
            "Use this for a quick synthesis. For thorough review over many abstracts, use the batch prompts below."
        ),
    )

    st.subheader("Option B: recursive OpenClaw batch workflow")

    # Be robust to old Streamlit session state from a previous UI version.
    fallback_papers_to_batch = sum(
        int(batch.get("max_results", 0)) for batch in batch_plan if isinstance(batch, dict)
    ) or available_papers
    fallback_batch_size = (batch_plan[0].get("max_results") if batch_plan else 10) or 10

    papers_to_batch_value = int(st.session_state.get("last_papers_to_batch", fallback_papers_to_batch))
    batch_size_value = int(st.session_state.get("last_batch_size", fallback_batch_size))
    batch_word_limit_value = int(st.session_state.get("last_batch_word_limit", 350))
    final_word_limit_value = int(st.session_state.get("last_final_word_limit", globals().get("final_word_limit", 900)))
    max_abstract_chars_value = int(st.session_state.get("last_max_abstract_chars", max_abstract_chars))

    recursive_prompt = make_recursive_batch_prompt(
        agent_payload_path=st.session_state["last_agent_path"],
        task=query,
        papers_to_process=papers_to_batch_value,
        batch_size=batch_size_value,
        max_abstract_chars=max_abstract_chars_value,
        batch_word_limit=batch_word_limit_value,
        final_word_limit=final_word_limit_value,
    )

    st.text_area(
        "Recommended: one prompt that lets OpenClaw run all batches",
        value=recursive_prompt,
        height=520,
        help=(
            "Paste this once into OpenClaw. It tells OpenClaw to call the MCP planning tool, "
            "then recursively read each abstract batch and synthesize the final answer."
        ),
    )

    st.info(
        "This is the recommended workflow. OpenClaw should call evidence_plan_agent_payload_batches once, "
        "then call evidence_read_agent_payload once per batch, then produce one final synthesis."
    )

    with st.expander("Manual fallback: individual batch prompts"):
        st.markdown(
            "Use these only if OpenClaw fails to orchestrate the batch loop itself."
        )

        batch_prompts: List[str] = []
        for batch in batch_plan:
            prompt = make_batch_prompt(
                agent_payload_path=st.session_state["last_agent_path"],
                task=query,
                batch_number=batch["batch_number"],
                offset=batch["offset"],
                max_results=batch["max_results"],
                max_abstract_chars=max_abstract_chars_value,
                batch_word_limit=batch_word_limit_value,
            )
            batch_prompts.append(prompt)

            st.text_area(
                f"Batch {batch['batch_number']}: P{batch['first_paper_id']}-P{batch['last_paper_id']}",
                value=prompt,
                height=220,
                key=f"batch_prompt_{batch['batch_number']}",
            )

        final_prompt = make_final_synthesis_prompt(
            task=query,
            word_limit=final_word_limit_value,
        )
        st.text_area("Final synthesis prompt", value=final_prompt, height=220)

    with st.expander("Fallback: inline prompt with single-pass abstract pack"):
        inline_prompt = make_inline_prompt(
            abstract_pack=abstract_pack,
            task=query,
            word_limit=int(st.session_state.get("last_single_pass_word_limit", 500)),
        )
        st.text_area(
            "Inline OpenClaw prompt",
            value=inline_prompt,
            height=420,
            help=(
                "Use this if OpenClaw cannot read files through MCP. "
                "It pastes the single-pass abstract pack directly into the prompt."
            ),
        )

    combined_prompts = "\n\n" + ("-" * 80) + "\n\n"
    combined_prompts = combined_prompts.join(
        [
            "RECOMMENDED RECURSIVE OPENCLAW PROMPT\n\n" + recursive_prompt,
            "MANUAL BATCH PROMPTS\n\n" + "\n\n".join(batch_prompts),
            "FINAL SYNTHESIS PROMPT\n\n" + final_prompt,
            "INLINE SINGLE-PASS FALLBACK PROMPT\n\n" + inline_prompt,
        ]
    )

    st.subheader("Saved files")

    st.code(
        f"""Full raw result:
{st.session_state["last_full_path"]}

Compact agent payload used by MCP:
{st.session_state["last_agent_path"]}

Single-pass abstract pack:
{st.session_state["last_abstract_pack_path"]}

Batch plan:
{st.session_state["last_batch_plan_path"]}

Request:
{st.session_state["last_request_path"]}
""",
        language="text",
    )

    col_a, col_b, col_c = st.columns(3)

    with col_a:
        st.download_button(
            "Download abstract pack JSON",
            data=json.dumps(abstract_pack, indent=2, ensure_ascii=False, default=str),
            file_name=Path(st.session_state["last_abstract_pack_path"]).name,
            mime="application/json",
        )

    with col_b:
        st.download_button(
            "Download batch plan JSON",
            data=json.dumps(
                {
                    "agent_payload": Path(st.session_state["last_agent_path"]).name,
                    "question": query,
                    "batches": batch_plan,
                },
                indent=2,
                ensure_ascii=False,
                default=str,
            ),
            file_name=Path(st.session_state["last_batch_plan_path"]).name,
            mime="application/json",
        )

    with col_c:
        st.download_button(
            "Download combined prompts TXT",
            data=combined_prompts,
            file_name=f"{Path(st.session_state['last_agent_path']).stem}.batch_prompts.txt",
            mime="text/plain",
        )

    with st.expander("Request JSON"):
        st.json(st.session_state.get("last_request"))

    with st.expander("Single-pass abstract pack JSON"):
        st.json(abstract_pack)

    with st.expander("Compact agent JSON"):
        st.json(agent_payload)
