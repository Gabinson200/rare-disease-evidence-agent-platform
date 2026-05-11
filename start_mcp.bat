@echo off
cd /d C:\Users\adamk\Downloads\rare-disease-evidence-agent-platform
set PYTHONPATH=C:\Users\adamk\Downloads\rare-disease-evidence-agent-platform
venv\Scripts\python.exe -m raredisease_platform.agent.mcp_server 2> mcp_error.log