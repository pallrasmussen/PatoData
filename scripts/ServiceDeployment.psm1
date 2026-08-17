function Assert-PublishLayout {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory=$true)][string]$PublishDir,
    [bool]$SelfContained,
    [bool]$SingleFile
  )

  $exePath = Join-Path $PublishDir 'XsdAnalyzer.exe'
  if (-not (Test-Path -LiteralPath $exePath)) {
    throw "Executable not found at '$exePath'."
  }
  if (-not $SelfContained -and -not $SingleFile) {
    $runtimeConfigPath = Join-Path $PublishDir 'XsdAnalyzer.runtimeconfig.json'
    if (-not (Test-Path -LiteralPath $runtimeConfigPath)) {
      throw "Framework-dependent publish is incomplete: '$runtimeConfigPath' was not found."
    }
  }

  return $exePath
}

function Format-ServiceArgument {
  [CmdletBinding()]
  param([Parameter(Mandatory=$true)][string]$Argument)

  if ($Argument -match '"') { $Argument = $Argument -replace '"','""' }
  if ($Argument -match '\s') { return '"' + $Argument + '"' }
  return $Argument
}

function Test-ServiceReadinessState {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory=$true)][string]$ReadinessPath,
    [Parameter(Mandatory=$true)][int]$ProcessId
  )

  if (-not (Test-Path -LiteralPath $ReadinessPath)) { return $false }
  try {
    $readiness = Get-Content -LiteralPath $ReadinessPath -Raw | ConvertFrom-Json -ErrorAction Stop
    return [int]$readiness.ProcessId -eq $ProcessId -and [int]$readiness.TableCount -gt 0
  } catch {
    return $false
  }
}

Export-ModuleMember -Function Assert-PublishLayout,Format-ServiceArgument,Test-ServiceReadinessState