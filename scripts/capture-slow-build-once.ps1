param(
    [int]$ThresholdSeconds = 30,
    [string]$ProjectOrSolution = "PatoData.sln",
    [string]$BinlogPrefix = "slowbuild",
    [switch]$CpuSample,
    [int]$CpuSampleMs = 1000
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

    $timingLog = Join-Path $solutionPath 'build_timing.log'
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    Write-Info "Running initial build without binlog..."

    # Optional lightweight CPU sampler (process total + working set) for correlation during suspected stalls
    $cpuSampler = $null
    if ($CpuSample) {
        Write-Info "Starting CPU sampler every $CpuSampleMs ms"
        $processId = $PID
        $cpuSampler = [System.Timers.Timer]::new($CpuSampleMs)
        $proc = Get-Process -Id $processId
        $lastCpu = $proc.TotalProcessorTime
        $lastTimestamp = Get-Date
        $cpuSampler.AutoReset = $true
        $cpuSampler.add_Elapsed({
            try {
                $p = Get-Process -Id $processId -ErrorAction Stop
                $now = Get-Date
                $deltaCpu = ($p.TotalProcessorTime - $lastCpu).TotalMilliseconds
                $deltaWall = ($now - $lastTimestamp).TotalMilliseconds
                $cpuPct = if ($deltaWall -gt 0) { [math]::Round(100 * $deltaCpu / ($deltaWall * [Environment]::ProcessorCount),2) } else { 0 }
                $memMB = [math]::Round($p.WorkingSet64 / 1MB,1)
                $line = "$(Get-Date -Format o);CPU%=$cpuPct;WSMB=$memMB"
                Add-Content -Path $timingLog -Value $line
                $lastCpu = $p.TotalProcessorTime
                $lastTimestamp = $now
            } catch {}
        })
        $cpuSampler.Start()
    }

    dotnet build $proj /p:EnableStyleCopAnalyzers=false | Tee-Object -Variable buildOutput | Out-Null

    if ($cpuSampler) { $cpuSampler.Stop(); $cpuSampler.Dispose() }

    $sw.Stop()
    $elapsed = [int][math]::Round($sw.Elapsed.TotalSeconds)
    Write-Info "Elapsed (no binlog): ${elapsed}s (threshold=$ThresholdSeconds s)"
    Add-Content -Path $timingLog -Value ("{0};ELAPSED={1}s;THRESHOLD={2}s;PHASE=InitialBuild" -f (Get-Date -Format o), $elapsed, $ThresholdSeconds)

    if ($elapsed -ge $ThresholdSeconds) {
        Write-Warn "Threshold exceeded. Re-running with binary log capture..."
        $timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
        $binlog = "${BinlogPrefix}_${timestamp}_${elapsed}s.binlog"
        Write-Info "Capturing binlog -> $binlog"
        dotnet build $proj /p:EnableStyleCopAnalyzers=false /bl:$binlog | Tee-Object -Variable slowOutput | Out-Null
        Write-Info "Binlog saved: $binlog"
        Write-Info "(Add to issue or inspect with MSBuild Structured Log Viewer)"
        Add-Content -Path $timingLog -Value ("{0};ELAPSED={1}s;PHASE=SlowBuildCaptured;BINLOG={2}" -f (Get-Date -Format o), $elapsed, $binlog)
    } else {
        Write-Info "Build below threshold; no binlog generated."
        Add-Content -Path $timingLog -Value ("{0};ELAPSED={1}s;PHASE=BelowThreshold" -f (Get-Date -Format o), $elapsed)
    }
}
finally {
    Pop-Location
}
