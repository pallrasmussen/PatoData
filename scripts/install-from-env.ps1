[CmdletBinding()]
param(
  [switch]$Start,
  [switch]$Force,
  [switch]$SkipPublish
)

set-strictmode -version latest
$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot 'service.env.ps1')

$install = Join-Path $PSScriptRoot 'install-importer-service.ps1'

# Define optional vars if missing to satisfy StrictMode
if (-not (Get-Variable -Name Account -ErrorAction SilentlyContinue)) { $Account = $null }
if (-not (Get-Variable -Name Password -ErrorAction SilentlyContinue)) { $Password = $null }

# If a plain-text password came from env, coerce it to SecureString to avoid passing strings
if ($Password -and ($Password -is [string])) {
  try { $Password = ConvertTo-SecureString -String $Password -AsPlainText -Force } catch { $Password = $null }
}

# Prompt for password here (wrapper) if account is set and password not provided
$passToUse = $Password
if ($Account -and -not $passToUse) {
  Write-Host "Enter password for service account '$Account' (input hidden):" -ForegroundColor Yellow
  $passToUse = Read-Host -AsSecureString
}

# Build named parameter hashtable for safe splatting in PS 5.1
$params = @{
  ServiceName   = $ServiceName
  DisplayName   = $DisplayName
  Description   = $Description
  Connection    = $Connection
  XsdPath       = $XsdPath
  OutDir        = $OutDir
  Schema        = $Schema
  ImportDir     = $ImportDir
  VerboseImport = [bool]$VerboseImport
  Audit         = [bool]$Audit
  Publish       = (-not $SkipPublish)
  Project       = $Project
  PublishDir    = $PublishDir
  Runtime       = $Runtime
  Configuration = $Configuration
  SingleFile    = [bool]$SingleFile
  SelfContained = [bool]$SelfContained
  Force         = [bool]$Force
  Start         = [bool]$Start
}
if ($Account)   { $params.Account  = $Account }
if ($passToUse) { $params.Password = $passToUse }

# Invoke installer with named splatting
& $install @params
