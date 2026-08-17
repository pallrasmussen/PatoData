[CmdletBinding()]
param(
  [switch]$RemovePublish,
  [switch]$RemoveConfig
)

set-strictmode -version latest
$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot 'service.env.ps1')

$uninstall = Join-Path $PSScriptRoot 'uninstall-importer-service.ps1'

& $uninstall `
  -ServiceName $ServiceName `
  -RemovePublish:([bool]$RemovePublish) `
  -PublishDir $PublishDir `
  -RemoveConfig:([bool]$RemoveConfig) `
  -ConfigDir $ConfigDir
