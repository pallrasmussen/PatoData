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
  [string]$PublishDir = (Join-Path $env:ProgramFiles 'PatoData\XsdAnalyzer'),
  [string]$ConfigDir = (Join-Path $env:ProgramData 'PatoData\XsdAnalyzer'),
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

function Protect-ConfigFile([string]$path, [string]$serviceIdentity) {
  try {
    $acl = Get-Acl -LiteralPath $path
    $acl.SetAccessRuleProtection($true, $false)
    $fullControl = [System.Security.AccessControl.FileSystemRights]::FullControl
    $readAccess = [System.Security.AccessControl.FileSystemRights]::ReadAndExecute
    $allow = [System.Security.AccessControl.AccessControlType]::Allow
    foreach ($identity in @('BUILTIN\Administrators', 'NT AUTHORITY\SYSTEM')) {
      $acl.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($identity, $fullControl, $allow)))
    }
    if ($serviceIdentity -and $serviceIdentity -notin @('LocalSystem', 'NT AUTHORITY\SYSTEM')) {
      $acl.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($serviceIdentity, $readAccess, $allow)))
    }
    Set-Acl -LiteralPath $path -AclObject $acl
  } catch {
    throw "Failed to secure service configuration '$path': $($_.Exception.Message)"
  }
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
$pubDir = Resolve-FullPath $PublishDir
$stagingDir = $null
$backupDir = $null
$publishSwapped = $false
$serviceExistedInitially = Test-ServiceExists $ServiceName
$serviceWasRunning = $false
$originalServicePath = $null
if ($serviceExistedInitially) {
  $initialService = Get-Service -Name $ServiceName -ErrorAction Stop
  $serviceWasRunning = $initialService.Status -ne 'Stopped'
  $originalServicePath = (Get-CimInstance Win32_Service -Filter "Name='$ServiceName'" -ErrorAction Stop).PathName
}
if ($Publish) {
  $publishParent = Split-Path -Parent $pubDir
  if (-not (Test-Path $publishParent)) { New-Item -ItemType Directory -Force -Path $publishParent | Out-Null }
  $stagingDir = "$pubDir.staging.$PID"
  if (Test-Path $stagingDir) { Remove-Item -Recurse -Force -LiteralPath $stagingDir }
  New-Item -ItemType Directory -Force -Path $stagingDir | Out-Null

  $props = @()
  if ($SingleFile) {
    $props += '/p:PublishSingleFile=true'
    $props += '/p:IncludeNativeLibrariesForSelfExtract=true'
  } else {
    $props += '/p:PublishSingleFile=false'
  }
  if ($SelfContained) { $props += '/p:SelfContained=true' } else { $props += '/p:SelfContained=false' }

  Write-Host "Publishing to staging directory: $stagingDir" -ForegroundColor DarkCyan
  $publishArgs = @('publish', (Resolve-FullPath $Project), '-c', $Configuration, '-r', $Runtime, '-o', $stagingDir) + $props
  Write-Host ("dotnet {0}" -f ($publishArgs -join ' ')) -ForegroundColor DarkGray
  & dotnet @publishArgs
  if ($LASTEXITCODE -ne 0) { throw "dotnet publish failed with exit code $LASTEXITCODE" }

  $stagedExePath = Join-Path $stagingDir 'XsdAnalyzer.exe'
  if (-not (Test-Path $stagedExePath)) { throw "Staged executable not found at '$stagedExePath'." }
  if (-not $SelfContained -and -not $SingleFile) {
    $stagedRuntimeConfigPath = Join-Path $stagingDir 'XsdAnalyzer.runtimeconfig.json'
    if (-not (Test-Path $stagedRuntimeConfigPath)) {
      throw "Framework-dependent publish is incomplete: '$stagedRuntimeConfigPath' was not found."
    }
  }

  & $stagedExePath --help | Out-Null
  if ($LASTEXITCODE -ne 0) { throw "Staged executable smoke test failed with exit code $LASTEXITCODE." }

  if ($serviceExistedInitially) {
    $service = Get-Service -Name $ServiceName -ErrorAction Stop
    if ($serviceWasRunning) {
      Write-Host "Stopping service before deployment swap..." -ForegroundColor DarkCyan
      Stop-Service -Name $ServiceName -ErrorAction Stop
      $service.WaitForStatus('Stopped', [TimeSpan]::FromSeconds(30))
    }
  }

  $backupDir = "$pubDir.backup"
  if (Test-Path $backupDir) { Remove-Item -Recurse -Force -LiteralPath $backupDir }
  if (Test-Path $pubDir) { Move-Item -LiteralPath $pubDir -Destination $backupDir }
  try {
    Move-Item -LiteralPath $stagingDir -Destination $pubDir
    $publishSwapped = $true
  } catch {
    if (Test-Path $backupDir) { Move-Item -LiteralPath $backupDir -Destination $pubDir }
    throw
  }
}

$exePath = Join-Path $pubDir 'XsdAnalyzer.exe'
if (-not (Test-Path $exePath)) {
  throw "Executable not found at '$exePath'. Ensure publish succeeded or set -PublishDir to a valid location."
}
if (-not $SelfContained -and -not $SingleFile) {
  $runtimeConfigPath = Join-Path $pubDir 'XsdAnalyzer.runtimeconfig.json'
  if (-not (Test-Path $runtimeConfigPath)) {
    throw "Framework-dependent publish is incomplete: '$runtimeConfigPath' was not found. Republish the application before installing the service."
  }
}

# 2) Compose service binPath
foreach ($d in @('XsdPath','OutDir','ImportDir')) {
  Set-Variable -Name $d -Value (Resolve-FullPath (Get-Variable $d -ValueOnly))
}

if (-not (Test-Path $XsdPath)) { throw "XSD path not found: $XsdPath" }
if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Path $OutDir | Out-Null }
if (-not (Test-Path $ImportDir)) { New-Item -ItemType Directory -Path $ImportDir | Out-Null }

# Keep machine-specific configuration outside both the source tree and binary directory.
$resolvedConfigDir = Resolve-FullPath $ConfigDir
if (-not (Test-Path $resolvedConfigDir)) { New-Item -ItemType Directory -Force -Path $resolvedConfigDir | Out-Null }
$configPath = Join-Path $resolvedConfigDir 'appsettings.json'
$configBackupPath = "$configPath.backup.$PID"
$hadExistingConfig = Test-Path $configPath
if ($hadExistingConfig) { Copy-Item -LiteralPath $configPath -Destination $configBackupPath -Force }
$existingCfg = $null
$existingConfigPath = $configPath
if (-not (Test-Path $existingConfigPath)) {
  $legacyConfigCandidates = @(
    (Join-Path $PSScriptRoot '..\publish\XsdAnalyzer.appsettings.json'),
    (Join-Path $PSScriptRoot '..\appsettings.json'),
    (Join-Path $PSScriptRoot '..\publish\XsdAnalyzer\appsettings.json')
  )
  $existingConfigPath = $legacyConfigCandidates | Where-Object { Test-Path $_ } | Select-Object -First 1
}
if ($existingConfigPath -and (Test-Path $existingConfigPath)) {
  try {
    $existingRaw = Get-Content -LiteralPath $existingConfigPath -Raw -ErrorAction Stop
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
if (-not (Test-Path $configPath)) {
  throw "Failed to create service configuration at '$configPath'."
}
$configIdentity = if ($objName) { $objName } else { 'NT AUTHORITY\SYSTEM' }
Protect-ConfigFile -path $configPath -serviceIdentity $configIdentity
$writtenCfg = Get-Content -LiteralPath $configPath -Raw | ConvertFrom-Json -ErrorAction Stop
foreach ($requiredSetting in @('Xsd','OutDir','ImportDir','Connection','ServiceName')) {
  $requiredValue = $writtenCfg.$requiredSetting
  if ($null -eq $requiredValue -or [string]::IsNullOrWhiteSpace([string]$requiredValue)) {
    throw "Service configuration '$configPath' is missing required setting '$requiredSetting'."
  }
}

function Restore-PreviousDeployment {
  Write-Warning "Deployment failed; restoring the previous service state."
  if (Test-ServiceExists $ServiceName) {
    try {
      Stop-Service -Name $ServiceName -ErrorAction SilentlyContinue
      Wait-ServiceStatus -name $ServiceName -status 'Stopped' -timeoutSeconds 30
    } catch { }
  }
  if ($publishSwapped) {
    if (Test-Path $pubDir) { Remove-Item -Recurse -Force -LiteralPath $pubDir }
    if (Test-Path $backupDir) { Move-Item -LiteralPath $backupDir -Destination $pubDir }
  }
  if ($hadExistingConfig -and (Test-Path $configBackupPath)) {
    Move-Item -LiteralPath $configBackupPath -Destination $configPath -Force
  } elseif (-not $hadExistingConfig -and (Test-Path $configPath)) {
    Remove-Item -Force -LiteralPath $configPath
  }
  if ($serviceExistedInitially) {
    if (-not [string]::IsNullOrWhiteSpace($originalServicePath)) {
      sc.exe config $ServiceName binPath= $originalServicePath | Out-Null
    }
    if ($serviceWasRunning) { sc.exe start $ServiceName | Out-Null }
  } elseif (Test-ServiceExists $ServiceName) {
    sc.exe delete $ServiceName | Out-Null
  }
}

if ($Publish) {
  & $exePath --validate-config --config $configPath
  if ($LASTEXITCODE -ne 0) {
    Restore-PreviousDeployment
    throw "Published application rejected service configuration '$configPath'."
  }
}

# Minimal args: point to config and ensure service name alignment
$argsList = @('--service','--config', $configPath, '--service-name', $ServiceName)

function Format-ArgumentQuoted([string]$a) {
  if ($a -match '"') { $a = ($a -replace '"','""') }
  if ($a -match '\s') { return '"' + $a + '"' } else { return $a }
}

$binPath = ('"{0}" {1}' -f $exePath, (($argsList | ForEach-Object { Format-ArgumentQuoted $_ }) -join ' '))
Write-Host "binPath: $binPath" -ForegroundColor DarkGray

# 3) Stop an existing service when a forced configuration update was requested.
$exists = Test-ServiceExists $ServiceName
if ($exists -and $Force) {
  $existingService = Get-Service -Name $ServiceName -ErrorAction Stop
  if ($existingService.Status -ne 'Stopped') {
    Write-Host "Stopping service for forced configuration update..." -ForegroundColor Yellow
    Stop-Service -Name $ServiceName -ErrorAction Stop
    $existingService.WaitForStatus('Stopped', [TimeSpan]::FromSeconds(30))
  }
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

# 6) Start when requested or when deployment temporarily stopped a running service.
$shouldStart = [bool]$Start -or $serviceWasRunning
if ($shouldStart) {
  Write-Host "Starting service..." -ForegroundColor DarkCyan
  sc.exe start $ServiceName | Write-Output
  $startExitCode = $LASTEXITCODE
  $svc = Get-Service -Name $ServiceName -ErrorAction Stop
  try { $svc.WaitForStatus('Running', [TimeSpan]::FromSeconds(30)) } catch { }
  $svc.Refresh()
  if ($startExitCode -ne 0 -or $svc.Status -ne 'Running') {
    Restore-PreviousDeployment
    throw "Service '$ServiceName' failed to reach Running state within 30 seconds (sc.exe exit code $startExitCode, status $($svc.Status))."
  }
  $svc | Format-Table -AutoSize Status, Name, DisplayName | Out-String | Write-Host
  if ($publishSwapped -and (Test-Path $backupDir)) { Remove-Item -Recurse -Force -LiteralPath $backupDir }
}
if (Test-Path $configBackupPath) { Remove-Item -Force -LiteralPath $configBackupPath }

Write-Host "Done. Current status:" -ForegroundColor Green
Get-Service -Name $ServiceName -ErrorAction SilentlyContinue | Format-Table -AutoSize Status, Name, DisplayName
