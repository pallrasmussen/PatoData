<#
.SYNOPSIS
  Stops and removes the Windows Service for the PatoData XML Importer.

.EXAMPLE
  .\scripts\uninstall-importer-service.ps1 -ServiceName PatoDataXmlImporter -RemovePublish
#>

[CmdletBinding(SupportsShouldProcess=$true)]
param(
  [string]$ServiceName = 'PatoDataXmlImporter',
  [switch]$RemovePublish,
  [string]$PublishDir = (Join-Path $PSScriptRoot '..\publish\XsdAnalyzer')
)

set-strictmode -version latest
$ErrorActionPreference = 'Stop'

function Test-ServiceExists([string]$name) {
  sc.exe query $name | Out-Null
  if ($LASTEXITCODE -eq 0) { return $true } else { return $false }
}

Write-Host "Uninstalling Windows Service '$ServiceName'" -ForegroundColor Cyan

if (Test-ServiceExists $ServiceName) {
  try {
    Write-Host "Stopping service..." -ForegroundColor DarkCyan
    sc.exe stop $ServiceName | Out-Null
    Start-Sleep -Seconds 2
  } catch {}
  Write-Host "Deleting service..." -ForegroundColor DarkCyan
  sc.exe delete $ServiceName | Out-Null
} else {
  Write-Host "Service not found." -ForegroundColor Yellow
}

if ($RemovePublish) {
  $pubDir = $PublishDir
  if (-not [System.IO.Path]::IsPathRooted($pubDir)) {
    $pubDir = Join-Path (Get-Location) $pubDir
  }
  if (Test-Path $pubDir) {
    Write-Host "Removing publish directory: $pubDir" -ForegroundColor DarkCyan
    Remove-Item -Recurse -Force -LiteralPath $pubDir
  }
}

Write-Host "Done." -ForegroundColor Green
