<#
.SYNOPSIS
    Run the full data pipeline: extract -> stage -> quality -> load -> marts -> report.

.DESCRIPTION
    Powershell entry point for Windows. Prefers the project virtualenv so it
    works immediately after cloning, without activating anything by hand.

    Examples:
        .\run_pipeline.ps1                       # everything, from scratch
        .\run_pipeline.ps1 -DryRun               # print the DAG, execute nothing
        .\run_pipeline.ps1 -Start load.warehouse # resume from a task
        .\run_pipeline.ps1 -Stop quality.staging # run only the first half

    Exit codes: 0 = success, 1 = a quality gate failed, 2 = the run errored.

.PARAMETER DryRun
    Print the execution plan and exit without running anything.
#>
[CmdletBinding()]
param(
    [switch]$DryRun,
    [string]$Start,
    [string]$Stop
)

$ErrorActionPreference = 'Stop'
Set-Location -Path $PSScriptRoot

$venvPython = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
if (Test-Path $venvPython) {
    $python = $venvPython
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
    $python = 'python'
} else {
    Write-Error 'No Python interpreter found. Run .\setup.ps1 first.'
    exit 2
}

# src-layout: make the package importable without installing it.
$env:PYTHONPATH = if ($env:PYTHONPATH) { "$env:PYTHONPATH;$PSScriptRoot\src" }
                  else { "$PSScriptRoot\src" }

$argv = @('run')
if ($DryRun)   { $argv += '--dry-run' }
if ($Start)    { $argv += @('--start', $Start) }
if ($Stop)     { $argv += @('--stop', $Stop) }

Write-Host "python : $python" -ForegroundColor DarkGray
Write-Host "command: retailde $($argv -join ' ')" -ForegroundColor DarkGray
Write-Host ''

& $python -m retailde @argv
$code = $LASTEXITCODE

switch ($code) {
    0 { Write-Host "`nPipeline succeeded." -ForegroundColor Green }
    1 { Write-Host "`nA quality gate failed -- see the FAILED rows above." -ForegroundColor Red }
    2 { Write-Host "`nThe pipeline errored." -ForegroundColor Red }
}
exit $code
