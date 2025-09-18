<#
.SYNOPSIS
  Reinstalls/updates the PatoData XML Importer Windows Service and verifies binPath flags.

.DESCRIPTION
  Uses scripts/service.env.ps1 for configuration, runs install-from-env.ps1 with -Force and -Start by default,
  then inspects the service's BINARY_PATH_NAME to ensure that --verbose-import and --audit are NOT present.

.PARAMETER SkipStart
  Do not start the service after reinstall.

.NOTES
  Run PowerShell as Administrator.
#>

[CmdletBinding()]
param(
  [switch]$SkipStart,
  [switch]$Republish
)

set-strictmode -version latest
$ErrorActionPreference = 'Stop'

$envScript = Join-Path $PSScriptRoot 'service.env.ps1'
if (-not (Test-Path $envScript)) { throw "Missing: $envScript" }
. $envScript

$installer = Join-Path $PSScriptRoot 'install-from-env.ps1'
if (-not (Test-Path $installer)) { throw "Missing: $installer" }

Write-Host "Reinstalling service '$ServiceName' (Force=$true, Start=$(-not $SkipStart))..." -ForegroundColor Cyan

# Build installer args via named splatting
$invokeParams = @{}
if (-not $SkipStart) { $invokeParams['Start'] = $true }
$invokeParams['Force'] = $true
# Default is to speed up by skipping publish unless -Republish is specified
if ($Republish) {
  $invokeParams['SkipPublish'] = $false
} else {
  $invokeParams['SkipPublish'] = $true
}

& $installer @invokeParams

Write-Host "Verifying BINARY_PATH_NAME..." -ForegroundColor Cyan
$qcOut = sc.exe qc $ServiceName | Out-String
$lines = $qcOut -split "`r`n"
$binPathLine = $lines | Where-Object { $_ -match 'BINARY_PATH_NAME' } | Select-Object -First 1
$binPath = $null
if ($binPathLine) { $binPath = ($binPathLine -split ':',2)[1].Trim() }

if (-not $binPath) {
  Write-Error "Could not read BINARY_PATH_NAME for service '$ServiceName'."
  exit 1
}

Write-Host "BINARY_PATH_NAME:" -ForegroundColor DarkGray
Write-Host "  $binPath" -ForegroundColor DarkGray

$violations = @()
if ($binPath -match '(?i)--verbose-import') { $violations += '--verbose-import' }
if ($binPath -match '(?i)--audit') { $violations += '--audit' }

if ($violations.Count -gt 0) {
  Write-Error ("Unexpected flags present: {0}" -f ($violations -join ', '))
  exit 1
}

Write-Host "Verified: no --verbose-import and no --audit in binPath." -ForegroundColor Green

try {
  $svc = Get-Service -Name $ServiceName -ErrorAction Stop
  if (-not $SkipStart) {
    if ($svc.Status -ne 'Running') {
      Write-Host "Service not running; attempting to start..." -ForegroundColor Yellow
      Start-Service -Name $ServiceName
      $svc.WaitForStatus('Running','00:00:10')
      $svc = Get-Service -Name $ServiceName -ErrorAction Stop
    }
  }
  $svc | Format-Table -AutoSize Status, Name, DisplayName | Out-String | Write-Host
} catch {
  Write-Warning ("Service status check failed: {0}" -f $_.Exception.Message)
}

Write-Host "Done." -ForegroundColor Green
