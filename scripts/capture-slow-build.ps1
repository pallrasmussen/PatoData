param(
    [int]$Attempts = 25,
    [int]$SlowThresholdSeconds = 120,
    [string]$Solution = 'PatoData.sln'
)

Write-Host "Starting build loop: Attempts=$Attempts SlowThresholdSeconds=$SlowThresholdSeconds" -ForegroundColor Cyan

for ($i = 1; $i -le $Attempts; $i++) {
    Write-Host ("Attempt {0}" -f $i) -ForegroundColor Cyan
    dotnet clean $Solution | Out-Null
    $stamp = Get-Date -Format 'HHmmss'
    $binlog = "loop{0:00}_$stamp.binlog" -f $i
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    dotnet build $Solution /bl:$binlog /p:EnableStyleCopAnalyzers=false | Out-Null
    $sw.Stop()
    $sec = [int]$sw.Elapsed.TotalSeconds
    Write-Host ("Duration: {0}s -> {1}" -f $sec, $binlog) -ForegroundColor Green
    if ($sec -ge $SlowThresholdSeconds) {
        Write-Host "Slow build captured (>= $SlowThresholdSeconds s): $binlog" -ForegroundColor Yellow
        break
    }
}

Write-Host "Build loop complete." -ForegroundColor Cyan
