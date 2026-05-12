# Rare Disease Evidence Agent Platform

This project provides a local rare disease evidence retrieval backend and an MCP server that lets OpenClaw query the backend through tools.

The intended workflow is:

```text
OpenClaw / MCP Inspector
        ↓
rare-disease-evidence MCP server
        ↓
FastAPI evidence broker
        ↓
Biomedical source connectors / PubMed search
        ↓
Structured JSON evidence result
```

The backend is useful for queries such as:

```text
case reports for fibrodysplasia ossificans progressiva involving ACVR1
```

The MCP server supports both direct long-running queries and a safer background-job workflow for OpenClaw. The job workflow is recommended because some biomedical evidence queries can take more than 60 seconds, and some MCP clients or UI runtimes may not tolerate one open tool call for that long.

---

## 1. Requirements

This README assumes Windows PowerShell.

Required software:

- Python 3.11+
- Node.js / npm
- OpenClaw
- A local clone of this repository

Example project location used throughout this README:

```powershell
C:\Users\adamk\Downloads\rare-disease-evidence-agent-platform
```

If your repository is in a different location, replace that path in all commands.

---

## 2. Initial Python setup

Open PowerShell and go to the project folder:

```powershell
cd C:\Users\adamk\Downloads\rare-disease-evidence-agent-platform
```

Create a virtual environment if one does not already exist:

```powershell
py -3.11 -m venv venv
```

Activate it:

```powershell
.\venv\Scripts\Activate.ps1
```

Install dependencies:

```powershell
python -m pip install --upgrade pip
pip install fastapi uvicorn httpx pydantic mcp
```

Optional development dependencies:

```powershell
pip install pytest
```

Verify the expected Python executable exists:

```powershell
Test-Path ".\venv\Scripts\python.exe"
```

Expected output:

```text
True
```

Verify key imports:

```powershell
.\venv\Scripts\python.exe -c "import fastapi, uvicorn, httpx, mcp; print('imports ok')"
```

---

## 3. Start the backend broker API

Open a first PowerShell terminal.

```powershell
cd C:\Users\adamk\Downloads\rare-disease-evidence-agent-platform
.\venv\Scripts\Activate.ps1
```

Start the FastAPI broker:

```powershell
.\venv\Scripts\python.exe -m uvicorn raredisease_platform.main:app --host 127.0.0.1 --port 8000 --reload
```

Leave this terminal running.

The broker should now be available at:

```text
http://127.0.0.1:8000
```

Health check from another terminal:

```powershell
curl http://127.0.0.1:8000/
```

Expected output should include something like:

```json
{
  "message": "Rare Disease Evidence Retrieval Platform API",
  "docs": "/docs"
}
```

The API docs are available at:

```text
http://127.0.0.1:8000/docs
```

---

## 4. Test the broker directly

Open a second PowerShell terminal.

```powershell
cd C:\Users\adamk\Downloads\rare-disease-evidence-agent-platform
.\venv\Scripts\Activate.ps1
```

Run a direct broker query:

```powershell
$body = @{
  raw_query = "case reports for fibrodysplasia ossificans progressiva involving ACVR1"
  expected_entity_types = @("disease", "gene")
  literature_keywords = "case report"
  literature_filters = @{
    retmax = 1
  }
  include_structured_evidence = $false
  requested_evidence_types = @("genes", "diseases", "variants")
} | ConvertTo-Json -Depth 10

$sw = [System.Diagnostics.Stopwatch]::StartNew()

$result = Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/evidence/query" `
  -Method Post `
  -ContentType "application/json" `
  -Body $body `
  -TimeoutSec 300

$sw.Stop()
"Broker completed in $($sw.Elapsed.TotalSeconds) seconds"

$result | ConvertTo-Json -Depth 8 | Set-Content .\direct_broker_result.json
```

Some evidence queries may take over a minute depending on PubMed / network latency. A runtime around 60–90 seconds can be normal for certain queries.

---

## 5. MCP server overview

The MCP server is located at:

```text
raredisease_platform/agent/mcp_server.py
```

It exposes tools such as:

```text
evidence_ping
evidence_broker_health
evidence_query_fast
evidence_query
evidence_query_start
evidence_query_status
evidence_query_result
evidence_query_jobs
```

The most important tools are:

| Tool | Purpose |
|---|---|
| `evidence_ping` | Fast smoke test to prove OpenClaw can call the MCP server |
| `evidence_broker_health` | Fast health check to prove the MCP server can reach the FastAPI broker |
| `evidence_query_fast` | Direct query tool with long timeout and progress logging |
| `evidence_query` | More configurable direct query tool |
| `evidence_query_start` | Starts a long evidence query as a background job and returns immediately |
| `evidence_query_status` | Polls a background job |
| `evidence_query_result` | Fetches the completed result from a background job |
| `evidence_query_jobs` | Lists recent background jobs |

For OpenClaw, the recommended workflow is:

```text
evidence_query_start → evidence_query_status → evidence_query_result
```

This avoids keeping one MCP request open for more than a minute.

---

## 6. Test the MCP server directly

Do not expect this command to print a normal response:

```powershell
.\venv\Scripts\python.exe -m raredisease_platform.agent.mcp_server
```

A stdio MCP server waits for an MCP client over stdin/stdout. It may look like it is hanging. That is expected.

Stop it with:

```text
Ctrl+C
```

Use MCP Inspector or OpenClaw to actually call tools.

---

## 7. Test the MCP server with MCP Inspector

MCP Inspector is the easiest way to verify that the MCP server is working before using OpenClaw.

Open a terminal:

```powershell
cd C:\Users\adamk\Downloads\rare-disease-evidence-agent-platform
.\venv\Scripts\Activate.ps1
```

For short tests:

```powershell
npx @modelcontextprotocol/inspector `
  "C:\Users\adamk\Downloads\rare-disease-evidence-agent-platform\venv\Scripts\python.exe" `
  -m `
  raredisease_platform.agent.mcp_server
```

For long broker queries, increase Inspector timeouts first:

```powershell
$env:MCP_SERVER_REQUEST_TIMEOUT = "300000"
$env:MCP_REQUEST_MAX_TOTAL_TIMEOUT = "300000"
$env:MCP_REQUEST_TIMEOUT_RESET_ON_PROGRESS = "true"

npx @modelcontextprotocol/inspector `
  "C:\Users\adamk\Downloads\rare-disease-evidence-agent-platform\venv\Scripts\python.exe" `
  -m `
  raredisease_platform.agent.mcp_server
```

Inspector should open a browser window. In the Inspector UI:

1. Connect to the server.
2. Go to the **Tools** tab.
3. Click **List Tools**.
4. Test `evidence_ping`.
5. Test `evidence_broker_health`.
6. Test `evidence_query_fast` or the job-based tools.

Example `evidence_ping` input:

```json
{}
```

Example `evidence_broker_health` input:

```json
{}
```

Example `evidence_query_fast` input:

```json
{
  "raw_query": "case reports for fibrodysplasia ossificans progressiva involving ACVR1",
  "expected_entity_types": ["disease", "gene"],
  "literature_keywords": "case report",
  "timeout_seconds": 300
}
```

For OpenClaw-style usage, prefer the background job workflow.

Start a job with `evidence_query_start`:

```json
{
  "raw_query": "case reports for fibrodysplasia ossificans progressiva involving ACVR1",
  "expected_entity_types": ["disease", "gene"],
  "literature_keywords": "case report",
  "literature_filters": {
    "retmax": 1
  },
  "timeout_seconds": 300
}
```

The tool returns a `job_id`.

Check status with `evidence_query_status`:

```json
{
  "job_id": "PASTE_JOB_ID_HERE"
}
```

Fetch completed result with `evidence_query_result`:

```json
{
  "job_id": "PASTE_JOB_ID_HERE"
}
```

---

## 8. Register the MCP server with OpenClaw

OpenClaw can launch the MCP server as a local stdio child process.

Because Windows command-line JSON escaping can be annoying, use a JSON5 patch file instead of trying to pass a large JSON object directly to `openclaw mcp set`.

From the repo root:

```powershell
cd C:\Users\adamk\Downloads\rare-disease-evidence-agent-platform
```

Create the patch file:

```powershell
@'
{
  mcp: {
    servers: {
      "rare-disease-evidence": {
        command: "C:\\Users\\adamk\\Downloads\\rare-disease-evidence-agent-platform\\venv\\Scripts\\python.exe",
        args: [
          "-m",
          "raredisease_platform.agent.mcp_server"
        ],
        cwd: "C:\\Users\\adamk\\Downloads\\rare-disease-evidence-agent-platform",
        env: {
          PYTHONUTF8: "1"
        }
      }
    }
  }
}
'@ | Set-Content -Encoding UTF8 .\openclaw-mcp.patch.json5
```

Apply it:

```powershell
openclaw config patch --file .\openclaw-mcp.patch.json5 --dry-run
openclaw config patch --file .\openclaw-mcp.patch.json5
openclaw config validate
```

Inspect the registered MCP server:

```powershell
openclaw mcp list
openclaw mcp show rare-disease-evidence --json
```

Expected shape:

```json
{
  "command": "C:\\Users\\adamk\\Downloads\\rare-disease-evidence-agent-platform\\venv\\Scripts\\python.exe",
  "args": [
    "-m",
    "raredisease_platform.agent.mcp_server"
  ],
  "cwd": "C:\\Users\\adamk\\Downloads\\rare-disease-evidence-agent-platform",
  "env": {
    "PYTHONUTF8": "1"
  }
}
```

Important: do not put `PYTHONPATH` inside the OpenClaw MCP server `env` block. The MCP server already handles its own repo path, and OpenClaw rejects some interpreter-startup environment variables for stdio MCP servers.

---

## 9. Start OpenClaw

Make sure the backend broker is still running in Terminal 1.

Then open a new terminal:

```powershell
cd C:\Users\adamk\Downloads\rare-disease-evidence-agent-platform
.\venv\Scripts\Activate.ps1
```

Start OpenClaw local chat:

```powershell
openclaw chat
```

or:

```powershell
openclaw tui --local
```

---

## 10. Query OpenClaw safely

The safest OpenClaw workflow uses background jobs because biomedical evidence queries may take more than 60 seconds.

### Step 1: Smoke test the MCP server

In OpenClaw chat:

```text
Use the rare-disease-evidence MCP tool evidence_ping. Return only the tool result.
```

If this works, OpenClaw can reach the MCP server.

### Step 2: Health check the backend broker

In OpenClaw chat:

```text
Use the rare-disease-evidence MCP tool evidence_broker_health. Return only the tool result.
```

If this works, the MCP server can reach the FastAPI backend at http://127.0.0.1:8000.

### Step 3: Start a background evidence query

In OpenClaw chat:

```text
Use the rare-disease-evidence MCP tool evidence_query_start.

Query: case reports for fibrodysplasia ossificans progressiva involving ACVR1.

Use:
- expected_entity_types: disease and gene
- literature_keywords: case report
- literature_filters: retmax 1
- timeout_seconds: 300

Return only the job_id.
```

Copy the returned `job_id`.

### Step 4: Poll the job

After about 30–90 seconds, ask:

```text
Use the rare-disease-evidence MCP tool evidence_query_status with job_id <PASTE_JOB_ID>. Return only the status.
```

If the status is still `running`, wait and poll again.

### Step 5: Fetch and summarize the result

When the status is `completed`, ask:

```text
Use the rare-disease-evidence MCP tool evidence_query_result with job_id <PASTE_JOB_ID>. Summarize the interpreted entities, top literature result, PMID/DOI/year/journal if available, score if available, and trace warnings.
```

---

## 11. One-shot direct OpenClaw query

This may work for shorter queries, but the job workflow above is more reliable.

```text
Use the rare-disease-evidence MCP tool evidence_query_fast with timeout_seconds 300.

Query: case reports for fibrodysplasia ossificans progressiva involving ACVR1.

Use:
- expected_entity_types: disease and gene
- literature_keywords: case report

Wait for the result and summarize the top literature result and trace warnings.
```

If OpenClaw reports a streaming watchdog timeout, use the job workflow instead.

---

## 12. Useful logs and outputs

The MCP server writes logs here:

```text
mcp_runtime.log
```

Watch the log live:

```powershell
Get-Content .\mcp_runtime.log -Wait
```

Background job files are stored here:

```text
mcp_jobs/
```

List jobs:

```powershell
Get-ChildItem .\mcp_jobs
```

View recent log lines:

```powershell
Get-Content .\mcp_runtime.log -Tail 120
```

List recent jobs through MCP:

```text
Use the rare-disease-evidence MCP tool evidence_query_jobs with limit 10.
```

---

## 13. Recommended normal development workflow

Use three terminals.

### Terminal 1: Backend broker

```powershell
cd C:\Users\adamk\Downloads\rare-disease-evidence-agent-platform
.\venv\Scripts\Activate.ps1
.\venv\Scripts\python.exe -m uvicorn raredisease_platform.main:app --host 127.0.0.1 --port 8000 --reload
```

### Terminal 2: Logs

```powershell
cd C:\Users\adamk\Downloads\rare-disease-evidence-agent-platform
Get-Content .\mcp_runtime.log -Wait
```

### Terminal 3: OpenClaw

```powershell
cd C:\Users\adamk\Downloads\rare-disease-evidence-agent-platform
.\venv\Scripts\Activate.ps1
openclaw chat
```

Then use:

```text
Use the rare-disease-evidence MCP tool evidence_ping. Return only the tool result.
```

Then:

```text
Use the rare-disease-evidence MCP tool evidence_broker_health. Return only the tool result.
```

Then:

```text
Use the rare-disease-evidence MCP tool evidence_query_start.

Query: case reports for fibrodysplasia ossificans progressiva involving ACVR1.

Use:
- expected_entity_types: disease and gene
- literature_keywords: case report
- literature_filters: retmax 1
- timeout_seconds: 300

Return only the job_id.
```

---

## 14. Troubleshooting

### `spawn ... python.exe ENOENT`

This means the configured Python executable path does not exist.

Check:

```powershell
Test-Path "C:\Users\adamk\Downloads\rare-disease-evidence-agent-platform\venv\Scripts\python.exe"
```

If your virtual environment is named `.venv` instead of `venv`, update the OpenClaw config accordingly.

---

### `Invalid JSON: Expected property name or '}'`

This usually happens when trying to pass complex JSON directly through PowerShell.

Use the patch-file method:

```powershell
openclaw config patch --file .\openclaw-mcp.patch.json5
```

instead of inline JSON.

---

### `Config validation failed: mcp: Unrecognized key: "toolCallTimeout"`

Your OpenClaw build does not support `mcp.toolCallTimeout`.

Use the background job workflow instead of trying to force one MCP call to stay open:

```text
evidence_query_start → evidence_query_status → evidence_query_result
```

---

### MCP Inspector error: `Maximum total timeout exceeded`

The Inspector has a client-side total timeout.

Restart Inspector with:

```powershell
$env:MCP_SERVER_REQUEST_TIMEOUT = "300000"
$env:MCP_REQUEST_MAX_TOTAL_TIMEOUT = "300000"
$env:MCP_REQUEST_TIMEOUT_RESET_ON_PROGRESS = "true"
```

Then launch Inspector again:

```powershell
npx @modelcontextprotocol/inspector `
  "C:\Users\adamk\Downloads\rare-disease-evidence-agent-platform\venv\Scripts\python.exe" `
  -m `
  raredisease_platform.agent.mcp_server
```

---

### OpenClaw says the query timed out, but the backend is still working

Use the job workflow:

```text
evidence_query_start
evidence_query_status
evidence_query_result
```

This avoids keeping one MCP request open for more than a minute.

---

### `evidence_broker_health` fails

The FastAPI backend is probably not running.

Start it:

```powershell
.\venv\Scripts\python.exe -m uvicorn raredisease_platform.main:app --host 127.0.0.1 --port 8000 --reload
```

Then verify:

```powershell
curl http://127.0.0.1:8000/
```

---

### OpenClaw does not seem to call the MCP server

Check whether the log changes:

```powershell
Get-Content .\mcp_runtime.log -Tail 80
```

Then restart OpenClaw:

```powershell
openclaw chat
```

Also verify registration:

```powershell
openclaw mcp list
openclaw mcp show rare-disease-evidence --json
openclaw config validate
```

---

### Running `python -m raredisease_platform.agent.mcp_server` looks frozen

That is expected. A stdio MCP server waits for an MCP client over stdin/stdout.

Use MCP Inspector or OpenClaw to actually call tools.

---

## 15. Example OpenClaw prompts

### Smoke test

```text
Use the rare-disease-evidence MCP tool evidence_ping. Return only the tool result.
```

### Backend health

```text
Use the rare-disease-evidence MCP tool evidence_broker_health. Return only the tool result.
```

### Start evidence query

```text
Use the rare-disease-evidence MCP tool evidence_query_start.

Query: case reports for fibrodysplasia ossificans progressiva involving ACVR1.

Use expected_entity_types disease and gene.
Use literature_keywords case report.
Use literature_filters retmax 1.
Use timeout_seconds 300.

Return only the job_id.
```

### Check status

```text
Use the rare-disease-evidence MCP tool evidence_query_status with job_id <PASTE_JOB_ID>. Return only the status.
```

### Fetch and summarize

```text
Use the rare-disease-evidence MCP tool evidence_query_result with job_id <PASTE_JOB_ID>.

Summarize:
1. interpreted entities
2. top literature result
3. PMID, DOI, year, journal, and score if available
4. trace warnings and limitations
```

### Deeper query

```text
Use the rare-disease-evidence MCP tool evidence_query_start.

Query: ACVR1 rare disease evidence and case reports.

Use expected_entity_types gene and disease.
Use literature_keywords rare disease case report.
Use literature_filters retmax 5.
Use deep_search true.
Use timeout_seconds 300.

Return only the job_id.
```

---

## 16. Notes for future maintainers

For OpenClaw, prefer:

```text
evidence_query_start → evidence_query_status → evidence_query_result
```

over:

```text
evidence_query
```

because some evidence queries take more than a minute. The job workflow makes each MCP tool call short, while the broker continues working in the background.

For MCP Inspector, direct long-running tools are fine if the Inspector timeout settings are raised.

For debugging, always check:

```powershell
Get-Content .\mcp_runtime.log -Tail 120
```

and:

```powershell
Get-ChildItem .\mcp_jobs
```

---

## 17. Quick command summary

### Start backend

```powershell
cd C:\Users\adamk\Downloads\rare-disease-evidence-agent-platform
.\venv\Scripts\Activate.ps1
.\venv\Scripts\python.exe -m uvicorn raredisease_platform.main:app --host 127.0.0.1 --port 8000 --reload
```

### Start OpenClaw

```powershell
cd C:\Users\adamk\Downloads\rare-disease-evidence-agent-platform
.\venv\Scripts\Activate.ps1
openclaw chat
```

### Watch MCP logs

```powershell
cd C:\Users\adamk\Downloads\rare-disease-evidence-agent-platform
Get-Content .\mcp_runtime.log -Wait
```

### Inspect MCP registration

```powershell
openclaw mcp list
openclaw mcp show rare-disease-evidence --json
openclaw config validate
```

### Start MCP Inspector with long timeouts

```powershell
cd C:\Users\adamk\Downloads\rare-disease-evidence-agent-platform

$env:MCP_SERVER_REQUEST_TIMEOUT = "300000"
$env:MCP_REQUEST_MAX_TOTAL_TIMEOUT = "300000"
$env:MCP_REQUEST_TIMEOUT_RESET_ON_PROGRESS = "true"

npx @modelcontextprotocol/inspector `
  "C:\Users\adamk\Downloads\rare-disease-evidence-agent-platform\venv\Scripts\python.exe" `
  -m `
  raredisease_platform.agent.mcp_server
```