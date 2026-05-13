param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$PytestArgs
)

$ErrorActionPreference = 'Stop'

$RepoRoot = Split-Path -Parent $PSScriptRoot
$AppDir = Join-Path $RepoRoot 'addons/thread-observability/app'
$Python = Join-Path $RepoRoot '.venv/Scripts/python.exe'

if (-not (Test-Path $Python)) {
    throw "Expected virtualenv Python at $Python"
}

$Targets = @(
    'tests/test_direct_chat.py'
    'tests/test_dashboard_http.py'
    'tests/test_chat_http.py'
    'tests/test_assessment_http.py'
    'tests/contract/test_chat_contract.py'
)

Push-Location $AppDir
try {
    & $Python -m pytest -q --tb=short @Targets @PytestArgs
}
finally {
    Pop-Location
}