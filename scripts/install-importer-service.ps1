<#
.SYNOPSIS
  Publishes XsdAnalyzer and installs it as a Windows Service that runs the XML importer in --service mode.

.DESCRIPTION
  Creates/updates a Windows Service that continuously imports XML files according to the configured XSD and paths.
  It can also publish the app to a target folder before installing the service.

.EXAMPLE
  .\scripts\install-importer-service.ps1 -Connection "Server=.\\SQLEXPRESS;Database=Pato;Trusted_Connection=True;TrustServerCertificate=True" -Start

.EXAMPLE
  .\scripts\install-importer-service.ps1 -ServiceName PatoDataXmlImporter -DisplayName "PatoData XML Importer" -Connection $env:PATO_CONN -VerboseImport -Audit -Start

.NOTES
  Requires PowerShell 5.1+ and .NET SDK for publishing. Run from any location; defaults assume repo layout.
#>

[CmdletBinding(SupportsShouldProcess=$true)]
param(
  [string]$ServiceName = 'PatoDataXmlImporter',
  [string]$DisplayName = 'PatoData XML Importer',
  [string]$Description = 'Imports Pato XML files to SQL Server based on the XSD mapping.',

  # Importer CLI options
  [Parameter(Mandatory=$true)][string]$Connection,
  [string]$XsdPath = (Join-Path $PSScriptRoot '..\161219-161219.XSD'),
  [string]$OutDir = (Join-Path $PSScriptRoot '..\out'),
  [string]$Schema = 'xsd',
  [string]$ImportDir = (Join-Path $PSScriptRoot '..\xml\in'),
  [switch]$VerboseImport,
  [switch]$Audit,

  # Remote polling (UNC) options (optional)
  [string]$RemoteSourceDir,
  [int]$RemotePollSeconds = 300,
  [string]$RemoteHistoryFile,

  # Publish settings
  [bool]$Publish = $true,
  [string]$Project = (Join-Path $PSScriptRoot '..\XsdAnalyzer\XsdAnalyzer.csproj'),
  [string]$PublishDir = (Join-Path $PSScriptRoot '..\publish\XsdAnalyzer'),
  [string]$Runtime = 'win-x64',
  [string]$Configuration = 'Release',
  [bool]$SingleFile = $true,
  [bool]$SelfContained = $false,

  # Service account and control
  [string]$Account = 'LocalSystem',
  [SecureString]$Password,
  [pscredential]$Credential,
  [switch]$Force,
  [switch]$Start
)

set-strictmode -version latest
$ErrorActionPreference = 'Stop'

# If a PSCredential is provided, prefer it for account/password
if ($Credential) {
  if (-not [string]::IsNullOrWhiteSpace($Credential.UserName)) {
    $Account = $Credential.UserName
  }
  if ($Credential.Password) {
    $Password = $Credential.Password
  }
}

# Require admin for service creation
function Test-IsAdmin {
  $currentIdentity = [Security.Principal.WindowsIdentity]::GetCurrent()
  $principal = New-Object Security.Principal.WindowsPrincipal($currentIdentity)
  return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}
if (-not (Test-IsAdmin)) {
  throw 'Administrator privileges are required to create or configure a Windows Service. Please run PowerShell as Administrator.'
}

function Resolve-FullPath([string]$path) {
  if ([string]::IsNullOrWhiteSpace($path)) { return $null }
  $p = $path
  if (-not [System.IO.Path]::IsPathRooted($p)) {
    $p = Join-Path (Get-Location) $p
  }
  try {
    $resolved = Resolve-Path -LiteralPath $p -ErrorAction Stop
    return $resolved.Path
  } catch {
    return $p
  }
}

function Convert-SecureStringToPlain([SecureString]$Secure)
{
  if (-not $Secure) { return $null }
  $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
  try { return [Runtime.InteropServices.Marshal]::PtrToStringUni($bstr) } finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
}

function Test-ServiceExists([string]$name) {
  sc.exe query $name | Out-Null
  if ($LASTEXITCODE -eq 0) { return $true } else { return $false }
}

# Wait for the service to reach a specific status (best-effort)
function Wait-ServiceStatus([string]$name, [string]$status, [int]$timeoutSeconds = 30) {
  try {
    $svc = Get-Service -Name $name -ErrorAction SilentlyContinue
    if ($svc) { $svc.WaitForStatus($status, [TimeSpan]::FromSeconds($timeoutSeconds)) }
  } catch { }
}

# Wait until the service is fully removed from SCM. Polls sc.exe query until not found.
function Wait-ServiceDeletion([string]$name, [int]$timeoutSeconds = 30) {
  $deadline = (Get-Date).AddSeconds($timeoutSeconds)
  while ((Get-Date) -lt $deadline) {
    sc.exe query $name | Out-Null
    if ($LASTEXITCODE -ne 0) { return $true } # 1060 or similar => gone
    Start-Sleep -Milliseconds 500
  }
  return $false
}
# Normalize account and prepare password
$objName = $null
$pwdPlain = $null
if ($Account -and $Account -ne 'LocalSystem') {
  switch ($Account) {
    'LocalService'   { $objName = 'NT AUTHORITY\LocalService' }
    'NetworkService' { $objName = 'NT AUTHORITY\NetworkService' }
    Default          { $objName = $Account }
  }
  if (-not $Password -and $objName -notlike 'NT AUTHORITY*') {
    Write-Host "Enter password for service account '$objName' (input hidden):" -ForegroundColor Yellow
    $secure = Read-Host -AsSecureString
    $pwdPlain = Convert-SecureStringToPlain $secure
  } elseif ($Password) {
    $pwdPlain = Convert-SecureStringToPlain $Password
  }
}


Write-Host "Installing Windows Service '$ServiceName' (DisplayName: '$DisplayName')" -ForegroundColor Cyan

# 1) Publish
if ($Publish) {
  $pubDir = Resolve-FullPath $PublishDir
  if (-not (Test-Path $pubDir)) { New-Item -ItemType Directory -Path $pubDir | Out-Null }

  $props = @()
  if ($SingleFile) {
    $props += '/p:PublishSingleFile=true'
    $props += '/p:IncludeNativeLibrariesForSelfExtract=true'
  } else {
    $props += '/p:PublishSingleFile=false'
  }
  if ($SelfContained) { $props += '/p:SelfContained=true' } else { $props += '/p:SelfContained=false' }

  Write-Host "Publishing to: $pubDir" -ForegroundColor DarkCyan
  $publishArgs = @('publish', (Resolve-FullPath $Project), '-c', $Configuration, '-r', $Runtime, '-o', $pubDir) + $props
  Write-Host ("dotnet {0}" -f ($publishArgs -join ' ')) -ForegroundColor DarkGray
  & dotnet @publishArgs
  if ($LASTEXITCODE -ne 0) { throw "dotnet publish failed with exit code $LASTEXITCODE" }
}

$exePath = Join-Path (Resolve-FullPath $PublishDir) 'XsdAnalyzer.exe'
if (-not (Test-Path $exePath)) {
  throw "Executable not found at '$exePath'. Ensure publish succeeded or set -PublishDir to a valid location."
}

# 2) Compose service binPath
foreach ($d in @('XsdPath','OutDir','ImportDir')) {
  Set-Variable -Name $d -Value (Resolve-FullPath (Get-Variable $d -ValueOnly))
}

if (-not (Test-Path $XsdPath)) { throw "XSD path not found: $XsdPath" }
if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Path $OutDir | Out-Null }
if (-not (Test-Path $ImportDir)) { New-Item -ItemType Directory -Path $ImportDir | Out-Null }

# Generate config in publish directory to unify service/CLI configuration
$configPath = Join-Path (Resolve-FullPath $PublishDir) 'appsettings.json'
$existingCfg = $null
if (Test-Path $configPath) {
  try {
    $existingRaw = Get-Content -LiteralPath $configPath -Raw -ErrorAction Stop
    if ($existingRaw.Trim().StartsWith('{')) { $existingCfg = $existingRaw | ConvertFrom-Json -ErrorAction Stop }
  } catch { $existingCfg = $null }
}
$cfg = [ordered]@{
  Xsd = $XsdPath
  OutDir = $OutDir
  Schema = $Schema
  ImportDir = $ImportDir
  Connection = $Connection
  Watch = $false
  VerboseImport = [bool]$VerboseImport
  Audit = [bool]$Audit
  DebounceMs = 200
  ReadyWaitMs = 2000
  IdempotencyEnabled = $true
  ServiceName = $ServiceName
}

# Inject remote settings only if provided to keep config clean
# If user passed new remote params, override; otherwise preserve from existing file
if ($RemoteSourceDir) {
  $cfg.RemoteSourceDir = $RemoteSourceDir
} elseif ($existingCfg -and $existingCfg.PSObject.Properties.Name -contains 'RemoteSourceDir') {
  $cfg.RemoteSourceDir = $existingCfg.RemoteSourceDir
}
if ($PSBoundParameters.ContainsKey('RemotePollSeconds')) {
  $cfg.RemotePollSeconds = $RemotePollSeconds
} elseif ($existingCfg -and $existingCfg.PSObject.Properties.Name -contains 'RemotePollSeconds') {
  $cfg.RemotePollSeconds = [int]$existingCfg.RemotePollSeconds
}
if ($RemoteHistoryFile) {
  $cfg.RemoteHistoryFile = $RemoteHistoryFile
} elseif ($existingCfg -and $existingCfg.PSObject.Properties.Name -contains 'RemoteHistoryFile') {
  $cfg.RemoteHistoryFile = $existingCfg.RemoteHistoryFile
}
$cfg | ConvertTo-Json -Depth 5 | Out-File -FilePath $configPath -Encoding UTF8 -Force

# Minimal args: point to config and ensure service name alignment
$argsList = @('--service','--config', $configPath, '--service-name', $ServiceName)

function Format-ArgumentQuoted([string]$a) {
  if ($a -match '"') { $a = ($a -replace '"','""') }
  if ($a -match '\s') { return '"' + $a + '"' } else { return $a }
}

$binPath = ('"{0}" {1}' -f $exePath, (($argsList | ForEach-Object { Format-ArgumentQuoted $_ }) -join ' '))
Write-Host "binPath: $binPath" -ForegroundColor DarkGray

# 3) Recreate if exists and -Force
$exists = Test-ServiceExists $ServiceName
if ($exists -and $Force) {
  Write-Host "Service exists; stopping and deleting (Force)..." -ForegroundColor Yellow
  try {
    sc.exe stop $ServiceName | Out-Null
    # Wait for STOPPED
    Wait-ServiceStatus -name $ServiceName -status 'Stopped' -timeoutSeconds 30
  } catch {}
  try {
    sc.exe delete $ServiceName | Out-Null
  } catch {}
  # Wait for deletion to complete to avoid 1072 when recreating
  if (-not (Wait-ServiceDeletion -name $ServiceName -timeoutSeconds 30)) {
    Write-Warning "Service '$ServiceName' still present after delete request; it may be marked for deletion. Will continue with guarded create and retry."
  }
  $exists = $false
}

# 4) Create
if (-not $exists) {
  $createArgs = @('create', $ServiceName, 'binPath=', $binPath, 'start=', 'auto', 'DisplayName=', $DisplayName)
  if ($objName) {
    $createArgs += @('obj=', $objName)
    if ($pwdPlain) { $createArgs += @('password=', $pwdPlain) }
  }
  Write-Host "Creating service..." -ForegroundColor DarkCyan
  # Retry create if service is marked for deletion (1072)
  $created = $false
  for ($i = 0; $i -lt 60 -and -not $created; $i++) {
    sc.exe @createArgs | Write-Output
    if ($LASTEXITCODE -eq 0) { $created = $true; break }
    if ($LASTEXITCODE -eq 1072) {
      if ($i -eq 0) { Write-Host "Service marked for deletion; waiting for SCM to release it..." -ForegroundColor Yellow }
      Start-Sleep -Milliseconds 500
      continue
    }
    break
  }
  if (-not $created) {
    throw "Service creation failed (sc.exe exited with code $LASTEXITCODE). Check the output above for details."
  }
  if (-not (Test-ServiceExists $ServiceName)) {
    throw "Service '$ServiceName' was not found after creation. Ensure you are running as Administrator and that the binPath is valid."
  }
  Write-Host "Created service '$ServiceName'." -ForegroundColor Green
  Write-Host "Service configuration:" -ForegroundColor DarkGray
  sc.exe qc $ServiceName | Write-Output
} else {
  Write-Host "Service already exists; updating config..." -ForegroundColor Yellow
  $configArgs = @('config', $ServiceName, 'binPath=', $binPath, 'start=', 'auto', 'DisplayName=', $DisplayName)
  if ($objName) {
    $configArgs += @('obj=', $objName)
    if ($pwdPlain) { $configArgs += @('password=', $pwdPlain) }
  }
  sc.exe @configArgs | Out-Null
}

# 5) Set description and failure recovery
if (Test-ServiceExists $ServiceName) {
  sc.exe description $ServiceName "$Description" | Out-Null
  sc.exe failure $ServiceName reset= 86400 actions= restart/60000/restart/60000/restart/60000 | Out-Null
  sc.exe failureflag $ServiceName 1 | Out-Null
}

# 6) Start if requested
if ($Start) {
  Write-Host "Starting service..." -ForegroundColor DarkCyan
  sc.exe start $ServiceName | Write-Output
  if ($LASTEXITCODE -ne 0) {
    Write-Warning "Service start reported an error (code $LASTEXITCODE). Checking status..."
  }
  $svc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
  if ($svc) {
    try { $svc.WaitForStatus('Running','00:00:10') } catch {}
    $svc | Format-Table -AutoSize Status, Name, DisplayName | Out-String | Write-Host
  } else {
    Write-Warning "Service '$ServiceName' not found when checking status after start."
  }
}

Write-Host "Done. Current status:" -ForegroundColor Green
Get-Service -Name $ServiceName -ErrorAction SilentlyContinue | Format-Table -AutoSize Status, Name, DisplayName
