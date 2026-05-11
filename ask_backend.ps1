param(
    [Parameter(Mandatory=$true)]
    [string]$Question
)

$Repo = "C:\Users\adamk\Downloads\rare-disease-evidence-agent-platform"
Set-Location $Repo
$env:PYTHONPATH = $Repo

& "$Repo\venv\Scripts\python.exe" -m raredisease_platform.agent.cli `
    "$Question" `
    --broker-url http://127.0.0.1:8000 `
    --retmax 3