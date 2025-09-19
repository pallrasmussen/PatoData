param(
    [int]$ThresholdSeconds = 30,
    [string]$ProjectOrSolution = "PatoData.sln",
    [string]$BinlogPrefix = "slowbuild"
)
<#!
.SYNOPSIS
    Runs a single dotnet build. If elapsed time >= threshold, emits an msbuild binary log.
.DESCRIPTION
    Designed to keep workspace clean: normal (fast) builds do not create .binlog files.
    When a slow build occurs (likely pathological CoreCompile stall), the command is re-run with /bl and
    the resulting binlog is timestamped for later analysis.
.EXAMPLE
    ./scripts/capture-slow-build-once.ps1 -ThresholdSeconds 45 -ProjectOrSolution PatoData.sln
#>

$ErrorActionPreference = 'Stop'

function Write-Info($msg) { Write-Host "[info] $msg" -ForegroundColor Cyan }
function Write-Warn($msg) { Write-Host "[warn] $msg" -ForegroundColor Yellow }
function Write-Err($msg)  { Write-Host "[err ] $msg" -ForegroundColor Red }

if (-not (Get-Command dotnet -ErrorAction SilentlyContinue)) {
    Write-Err "dotnet CLI not found in PATH."; exit 1
}

$solutionPath = Join-Path $PSScriptRoot ".." | Resolve-Path
Push-Location $solutionPath
try {
    $proj = $ProjectOrSolution
    if (-not (Test-Path $proj)) {
        Write-Err "Project/Solution '$proj' not found relative to repo root."; exit 1
    }

    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    Write-Info "Running initial build without binlog..."
    dotnet build $proj /p:EnableStyleCopAnalyzers=false | Tee-Object -Variable buildOutput | Out-Null
    $sw.Stop()
    $elapsed = [int][math]::Round($sw.Elapsed.TotalSeconds)
    Write-Info "Elapsed (no binlog): ${elapsed}s (threshold=$ThresholdSeconds s)"

    if ($elapsed -ge $ThresholdSeconds) {
        Write-Warn "Threshold exceeded. Re-running with binary log capture..."
        $timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
        $binlog = "${BinlogPrefix}_${timestamp}_${elapsed}s.binlog"
        Write-Info "Capturing binlog -> $binlog"
        dotnet build $proj /p:EnableStyleCopAnalyzers=false /bl:$binlog | Tee-Object -Variable slowOutput | Out-Null
        Write-Info "Binlog saved: $binlog"
        Write-Info "(Add to issue or inspect with MSBuild Structured Log Viewer)"
    } else {
        Write-Info "Build below threshold; no binlog generated."
    }
}
finally {
    Pop-Location
}
