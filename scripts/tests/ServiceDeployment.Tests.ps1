$modulePath = Join-Path $PSScriptRoot '..\ServiceDeployment.psm1'
Import-Module $modulePath -Force

Describe 'ServiceDeployment' {
  It 'accepts a framework-dependent single-file publish without external runtime config' {
    New-Item -ItemType File -Force -Path (Join-Path $TestDrive 'XsdAnalyzer.exe') | Out-Null

    Assert-PublishLayout -PublishDir $TestDrive -SelfContained:$false -SingleFile:$true | Should Be (Join-Path $TestDrive 'XsdAnalyzer.exe')
  }

  It 'rejects a framework-dependent multi-file publish without runtime config' {
    New-Item -ItemType File -Force -Path (Join-Path $TestDrive 'XsdAnalyzer.exe') | Out-Null

    { Assert-PublishLayout -PublishDir $TestDrive -SelfContained:$false -SingleFile:$false } | Should Throw
  }

  It 'quotes service arguments containing spaces' {
    Format-ServiceArgument -Argument 'C:\Program Files\PatoData' | Should Be '"C:\Program Files\PatoData"'
  }

  It 'accepts readiness only for the expected process with tables' {
    $path = Join-Path $TestDrive 'service.ready.json'
    '{"ProcessId":42,"TableCount":9}' | Set-Content -LiteralPath $path

    Test-ServiceReadinessState -ReadinessPath $path -ProcessId 42 | Should Be $true
    Test-ServiceReadinessState -ReadinessPath $path -ProcessId 41 | Should Be $false
  }
}