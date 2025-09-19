<#!
.SYNOPSIS
    End-to-end deployment script for XsdAnalyzer service + database schema.
.DESCRIPTION
    Automates: publish (optional), artifact packaging, transfer (local), schema generation/application,
    Windows Service install/update, and post-deploy validation queries.

    Can be run on build machine (with -Publish) or directly on target (skips publish).

    Assumes SQL Server is reachable and account has required rights.

.PARAMETER Publish
    Perform a 'dotnet publish' before deployment.
.PARAMETER Configuration
    Build configuration for publish (Default: Release).
.PARAMETER OutputRoot
    Root deployment directory on target host (Default: C:\PatoData).
.PARAMETER ServiceName
    Windows Service name (Default: PatoXsdImporter).
.PARAMETER Connection
    SQL connection string (Integrated Security or SQL Auth).
.PARAMETER Database
    Database name (optional shortcut for validation queries if not in connection string).
.PARAMETER SqlServer
    SQL Server name/instance (optional if included in -Connection).
.PARAMETER XsdPath
    Path to XSD file (will be copied into deployment xsd/ folder if not already there).
.PARAMETER SchemaName
    Schema name to generate under (Default: xsd).
.PARAMETER GenerateSchema
    If set, runs analyzer to (re)generate schema scripts (schema.sql, schema.views.sql).
.PARAMETER ApplySchema
    If set, executes generated schema scripts against target database.
.PARAMETER ReinstallService
    If set, stops existing service (if any), deletes it, and creates a fresh one.
.PARAMETER ServiceArgs
    Additional arguments appended to service execution line.
.PARAMETER NoStart
    If set, will not start the service after install/update.
.PARAMETER SkipViews
    Skip applying views script (useful for quick table-only changes).
.PARAMETER DryRun
    Show actions without changing anything (best effort; some validations still run).
.PARAMETER Validate
    Execute basic post-deploy validation queries.
.PARAMETER Force
    Continue even if some non-critical steps warn.

.EXAMPLE
    ./scripts/deploy-end-to-end.ps1 -Publish -Connection "Server=.;Database=Pato;Trusted_Connection=True;TrustServerCertificate=True" -XsdPath ./161219-161219.XSD -GenerateSchema -ApplySchema -ReinstallService -Validate
#>

[CmdletBinding()]
param(
    [switch]$Publish,
    [string]$Configuration = 'Release',
    [string]$OutputRoot = 'C:\PatoData',
    [string]$ServiceName = 'PatoXsdImporter',
    [string]$Connection,
    [string]$Database,
    [string]$SqlServer,
    [string]$XsdPath = './161219-161219.XSD',
    [string]$SchemaName = 'xsd',
    [switch]$GenerateSchema,
    [switch]$ApplySchema,
    [switch]$ReinstallService,
    [string]$ServiceArgs,
    [switch]$NoStart,
    [switch]$SkipViews,
    [switch]$DryRun,
    [switch]$Validate,
    [switch]$Force
)

$ErrorActionPreference = 'Stop'

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }
function Err ($m){ Write-Host "[ERR ] $m" -ForegroundColor Red }
function Exec($cmd, [switch]$AllowFail){
    Info $cmd
    if($DryRun){ return }
    & powershell -NoProfile -Command $cmd
    if($LASTEXITCODE -ne 0 -and -not $AllowFail){ throw "Command failed: $cmd" }
}

if(-not $Connection){ Err "-Connection is required"; exit 1 }

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot '..')
$publishDir = Join-Path $repoRoot 'publish/XsdAnalyzer'
$deployDir = $OutputRoot
$deployAppDir = Join-Path $deployDir 'XsdAnalyzer'
$deployXsdDir = Join-Path $deployDir 'xsd'
$deployXmlDir = Join-Path $deployDir 'xml'
$deployInDir  = Join-Path $deployXmlDir 'in'
$deployImportedDir = Join-Path $deployXmlDir 'imported'
$deployErrorDir = Join-Path $deployXmlDir 'error'
$deployOutDir = Join-Path $deployDir 'out'

if($Publish){
    if(-not (Get-Command dotnet -ErrorAction SilentlyContinue)){ Err ".NET SDK not found"; exit 1 }
    Info "Publishing application..."
    Exec "dotnet publish $repoRoot/XsdAnalyzer/XsdAnalyzer.csproj -c $Configuration -o $publishDir /p:EnableStyleCopAnalyzers=false"
}

if(-not (Test-Path $publishDir)) { Err "Publish directory not found: $publishDir"; exit 1 }

# Create directory structure
foreach($d in @($deployDir,$deployAppDir,$deployXsdDir,$deployXmlDir,$deployInDir,$deployImportedDir,$deployErrorDir,$deployOutDir)){
    if(-not (Test-Path $d)){
        if(-not $DryRun){ New-Item -ItemType Directory -Force -Path $d | Out-Null }
        Info "Created $d"
    }
}

# Copy publish output
Info "Copying published artifacts..."
if(-not $DryRun){ robocopy $publishDir $deployAppDir *.* /E /PURGE | Out-Null }

# Copy XSD
if(Test-Path $XsdPath){
    $xsdDest = Join-Path $deployXsdDir (Split-Path $XsdPath -Leaf)
    Info "Copying XSD -> $xsdDest"
    if(-not $DryRun){ Copy-Item $XsdPath $xsdDest -Force }
} else { Warn "XSD path not found: $XsdPath"; if(-not $Force){ exit 1 } }

# Generate schema if requested
if($GenerateSchema){
    Info "Generating schema SQL (tables + views)..."
    $xsdArg = (Resolve-Path $XsdPath).Path
    $genCmd = "dotnet $deployAppDir/XsdAnalyzer.dll --xsd `"$xsdArg`" --out `"$deployOutDir`" --schema $SchemaName"
    Exec $genCmd
}

# Apply schema
if($ApplySchema){
    if(-not $SqlServer -and ($Connection -notmatch 'Server=' -and $Connection -notmatch 'Data Source=')){
        Warn "SqlServer not provided and not inferable from connection; skipping schema apply"; if(-not $Force){ exit 1 }
    } else {
        $server = $SqlServer
        if(-not $server){
            if($Connection -match 'Server=([^;]+)'){ $server = $Matches[1] }
            elseif($Connection -match 'Data Source=([^;]+)'){ $server = $Matches[1] }
        }
        if(-not $Database){
            if($Connection -match 'Initial Catalog=([^;]+)'){ $Database = $Matches[1] }
            elseif($Connection -match 'Database=([^;]+)'){ $Database = $Matches[1] }
        }
        if(-not $Database){ Warn "Could not infer database name"; if(-not $Force){ exit 1 } }

        $authSwitch = ''
        if($Connection -match 'Trusted_Connection=True' -or $Connection -match 'Integrated Security=True'){ $authSwitch = '-E' }
        else { Warn "Non-integrated auth: sqlcmd will require -U/-P if you need it (not parsing full cred now)" }

        $schemaSql = Join-Path $deployOutDir 'schema.sql'
        $viewsSql  = Join-Path $deployOutDir 'schema.views.sql'
        if(-not (Test-Path $schemaSql)){ Warn "Missing $schemaSql"; if(-not $Force){ exit 1 } }
        if(-not $SkipViews -and -not (Test-Path $viewsSql)){ Warn "Missing $viewsSql"; if(-not $Force){ exit 1 } }

        if(-not $DryRun -and (Test-Path $schemaSql)){
            Info "Applying schema.sql"
            sqlcmd -S $server $authSwitch -d $Database -b -i $schemaSql
        }
        if(-not $DryRun -and -not $SkipViews -and (Test-Path $viewsSql)){
            Info "Applying schema.views.sql"
            sqlcmd -S $server $authSwitch -d $Database -b -i $viewsSql
        }
    }
}

# Install or update service
$serviceExe = "C:\\Program Files\\dotnet\\dotnet.exe"
if(-not (Test-Path $serviceExe)){ Warn "dotnet runtime not found at $serviceExe; ensure runtime installed"; if(-not $Force){ exit 1 } }

$serviceBin = "$serviceExe `"$deployAppDir/XsdAnalyzer.dll`" --service --xsd `"$deployXsdDir/$(Split-Path $XsdPath -Leaf)`" --out `"$deployOutDir`" --schema $SchemaName --import-dir `"$deployInDir`" --connection `"$Connection`""
if($ServiceArgs){ $serviceBin += " $ServiceArgs" }

$existing = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if($existing){
    if($ReinstallService){
        Info "Removing existing service $ServiceName"
        if(-not $DryRun){ sc.exe stop $ServiceName | Out-Null; sc.exe delete $ServiceName | Out-Null }
        $existing = $null
    } else {
        Info "Service exists; updating binary path (stop/start)"
        if(-not $DryRun){ sc.exe stop $ServiceName | Out-Null }
        if(-not $DryRun){ sc.exe config $ServiceName binPath= $serviceBin | Out-Null }
    }
}
if(-not $existing){
    Info "Creating service $ServiceName"
    if(-not $DryRun){ sc.exe create $ServiceName binPath= $serviceBin start= auto | Out-Null }
}
if(-not $NoStart){
    Info "Starting service"
    if(-not $DryRun){ sc.exe start $ServiceName | Out-Null }
}

# Validation
if($Validate){
    if(-not $SqlServer -and ($Connection -match 'Server=([^;]+)' -or $Connection -match 'Data Source=([^;]+)')){ $SqlServer = $Matches[1] }
    if(-not $Database -and ($Connection -match 'Initial Catalog=([^;]+)' -or $Connection -match 'Database=([^;]+)')){ $Database = $Matches[1] }
    if($SqlServer -and $Database){
        Info "Running validation queries"
        $countsQuery = "SELECT 'DSET' AS T, COUNT(*) C FROM ${SchemaName}.DSET UNION ALL SELECT 'DSETREKV', COUNT(*) FROM ${SchemaName}.DSETREKV"
        $issuesQuery = "SELECT COUNT(*) AS DateNormalizationIssues FROM ${SchemaName}.vw_DSETREKV_DateNormalizationIssues"
        if(-not $DryRun){
            sqlcmd -S $SqlServer -d $Database -E -Q $countsQuery
            sqlcmd -S $SqlServer -d $Database -E -Q $issuesQuery
        }
    } else { Warn "Skipping validation: can't infer server/database" }
}

Info "Deployment complete."