<#
.SYNOPSIS
    Create the virtual environment and install every dependency.

.DESCRIPTION
    Idempotent: safe to re-run. Produces .venv with Python plus the packages
    listed in requirements.txt. Nothing here needs a cloud account, a Docker
    daemon, or a system package manager.
#>
[CmdletBinding()]
param([switch]$Dev)

$ErrorActionPreference = 'Stop'
Set-Location -Path $PSScriptRoot

$venvDir = Join-Path $PSScriptRoot '.venv'
$venvPython = Join-Path $venvDir 'Scripts\python.exe'

if (-not (Test-Path $venvPython)) {
    Write-Host "Creating virtual environment..." -ForegroundColor Cyan
    if (Get-Command python -ErrorAction SilentlyContinue) {
        python -m venv $venvDir
    } elseif (Get-Command py -ErrorAction SilentlyContinue) {
        py -3 -m venv $venvDir
    } else {
        Write-Error 'No Python interpreter found on PATH (need Python 3.10+).'
        exit 2
    }
} else {
    Write-Host "Reusing existing .venv" -ForegroundColor DarkGray
}

Write-Host "Installing dependencies..." -ForegroundColor Cyan
& $venvPython -m pip install --upgrade pip --quiet
& $venvPython -m pip install -r requirements.txt --quiet
# Editable install registers the `retailde` console command and makes
# `python -m retailde` work from any directory -- no PYTHONPATH juggling.
& $venvPython -m pip install -e $PSScriptRoot --quiet
if ($Dev) {
    & $venvPython -m pip install pytest --quiet
}

Write-Host "`nInstalled:" -ForegroundColor Cyan
& $venvPython -m pip list --format=freeze |
    Select-String -Pattern 'duckdb|pandas|pyarrow|networkx|click|matplotlib|pytest' |
    ForEach-Object { "  $_" }

Write-Host "`nNext steps:" -ForegroundColor Green
Write-Host "  .\run_pipeline.ps1          # run the whole pipeline"
Write-Host "  .\run_pipeline.ps1 -DryRun  # inspect the task DAG first"
Write-Host "  python -m pytest tests       # run the test suite"
