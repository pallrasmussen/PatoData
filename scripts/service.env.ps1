[Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSUseDeclaredVarsMoreThanAssignments','', Justification='Variables in this env file are dot-sourced and consumed by installer scripts')]
param()

# Default environment for PatoData XML Importer service installation
# Customize these values to your environment. You can also set $env:PATO_CONN externally and keep Connection below referencing it.

$ServiceName  = 'PatoDataXmlImporter'
$DisplayName  = 'PatoData XML Importer'
$Description  = 'Imports Pato XML files to SQL Server based on the XSD mapping.'

# Server/Database (env overrides allowed)
$Server = if ($env:PATO_SERVER) { $env:PATO_SERVER } else { '.' }
$Database = if ($env:PATO_DATABASE) { $env:PATO_DATABASE } else { 'PatoData' }

# Connection string derived from above (env override PATO_CONNECTION)
if ($env:PATO_CONNECTION) {
	$Connection = $env:PATO_CONNECTION
} else {
	$Connection = "Server=$Server;Database=$Database;Trusted_Connection=True;TrustServerCertificate=True"
}

# Paths (relative to repo root)
$RepoRoot   = Split-Path -Parent $PSScriptRoot
$XsdPath    = if ($env:PATO_XSD) { $env:PATO_XSD } else { Join-Path $RepoRoot '161219-161219.XSD' }
$OutDir     = if ($env:PATO_OUT) { $env:PATO_OUT } else { Join-Path $RepoRoot 'out' }
$Schema     = if ($env:PATO_SCHEMA) { $env:PATO_SCHEMA } else { 'xsd' }
$ImportDir  = if ($env:PATO_IMPORT_DIR) { $env:PATO_IMPORT_DIR } else { Join-Path $RepoRoot 'xml\in' }

# Publish options
$Project    = Join-Path $RepoRoot 'XsdAnalyzer\XsdAnalyzer.csproj'
$PublishDir = Join-Path $RepoRoot 'publish\XsdAnalyzer'
$Runtime    = 'win-x64'
$Configuration = 'Release'
$SingleFile = $false
$SelfContained = $false

# Flags
$VerboseImport = if ($env:PATO_VERBOSE_IMPORT) { $env:PATO_VERBOSE_IMPORT -in @('1','true','True') } else { $false }
$Audit = if ($env:PATO_AUDIT) { $env:PATO_AUDIT -in @('1','true','True') } else { $false }  # set to $true to enable import_audit.csv output

# Service account: prefer environment variables; default to current user if not provided
if ($env:PATO_SERVICE_ACCOUNT) {
	$Account = $env:PATO_SERVICE_ACCOUNT
} else {
	$Account = "$env:UserDomain\$env:UserName"
}
# $Password can be provided via $env:PATO_SERVICE_PASSWORD (wrapper will secure it) or will be prompted securely by installer
if ($env:PATO_SERVICE_PASSWORD) { $Password = $env:PATO_SERVICE_PASSWORD }
