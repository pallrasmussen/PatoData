# PatoData XSD Analyzer

A lightweight Python workspace to analyze XML Schema (XSD) files: summarize structure, list elements/attributes, and generate example XML instances.

## Features
- Parse and inspect XSDs
- Output human-readable schema summary
- Generate a minimal example XML instance

## Quick start (Python)
1. Create a virtual environment and install dependencies.
2. Run the analyzer with an XSD path.

```powershell
# From repo root
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# Analyze an XSD and write outputs to out/
python -m src.analyze_xsd --xsd .\xsd\sample.xsd --out .\out
```

Outputs:
- `out/schema_summary.txt` – formatted overview
- `out/example.xml` – generated sample instance
 - `out/schema.sql` – SQL Server DDL generated from the XSD

## Quick start (C#)
Requires .NET 8 SDK.

```powershell
# Build
dotnet build .\XsdAnalyzer\XsdAnalyzer.csproj

# Run against your XSD
dotnet run --project .\XsdAnalyzer\XsdAnalyzer.csproj -- --xsd .\161219-161219.XSD --out .\out
dotnet run --project .\XsdAnalyzer\XsdAnalyzer.csproj -- --xsd .\161219-161219.XSD --out .\out --schema xsd
dotnet run --project .\XsdAnalyzer\XsdAnalyzer.csproj -- --xsd .\161219-161219.XSD --out .\out --schema xsd --xml .\PatoUdtraek-xml_Example.XML
```

Outputs:
- `out/schema_summary.txt` – overview of types and elements
- `out/example.xml` – generated sample instance (if possible)
- `out/schema.sql` – SQL Server DDL generated from the XSD (schema-qualified; pass `--schema xsd` to target the xsd schema)
- `out/schema.views.sql` – CREATE OR ALTER VIEW scripts (per-table, parent-with-child-counts, and a curated `vw_Snomed` with CPRNR, timestamps, and REKV identifiers)
 - `out/schema.views.sql` – CREATE OR ALTER VIEW scripts (per-table, parent-with-child-counts, and a curated `vw_Snomed` with CPRNR, REKV identifiers, and all REKV *DATO fields formatted as dd-MM-yyyy; timestamp removed)
- `out/schema.samples.sql` – sample INSERT templates
- `out/schema.drop.sql` – best-effort drop script for constraints and tables
- `out/seed.sql` – seed INSERTs (if --xml provided, otherwise a placeholder with instructions)

Importer:
- Import XML files into SQL with `--import-dir <folder> --connection <conn>`; processed files move to `xml/imported`.
- Prints per-table insert counts and logs to `out/import.log`.
- Optional: `--verbose-import` logs detailed decisions (defaults applied to required cols, skips due to missing required FK, table resolution, child recursion). Use tasks "Import XMLs to SQL (watch, verbose)" or "(once, verbose)".
 - Optional: `--audit` writes a CSV audit at `out/import_audit.csv` with columns:
	 `Timestamp,File,Event,Element,Table,NewId,ParentTable,ParentId,FkColumn,Reason,Params`.
  
	Enhancements:
	- Inserts now include per-event duration (ms) in the `Reason` field for `insert` and `skip` events.
	- Each file ends with a `file-summary` row where:
		- `FkColumn` holds total file duration in ms,
		- `Reason` lists per-table totals (e.g., `DSET=2; DSETREKV=91; ...`),
		- `Params` contains the overall inserted row count.
	 Use tasks "Import XMLs to SQL (watch, verbose+audit)" or "(once, verbose+audit)".

VS Code tasks:
- Build: .NET Build
- Generate: Run XsdAnalyzer (C#)
- Apply DDL: Apply schema.sql / Apply schema.views.sql / Apply schema.samples.sql
- Drop and rebuild: Apply schema.drop.sql, or "Rebuild schema (drop → schema → views)" to reset then re-create everything.
- Clear data: Apply schema.clear.sql to delete rows from all xsd tables in dependency-safe order.

## Windows Service mode

You can run the XML importer as a Windows Service (or console-hosted background service for debugging):

- Console-hosted (debug): Run and Debug → "XsdAnalyzer: Service mode (console)" or Task: "Run Importer (service mode console)"
- Real Windows Service: install your built exe with arguments:

Args:
- `--service --xsd <path> --out <dir> --schema <name> --import-dir <folder> --connection <conn> [--verbose-import] [--audit]`

Logs:
- `out/import.log` is auto-rotated at ~2 MB. On rollover, the current file is saved as a timestamped backup (e.g., `import.log.yyyyMMdd-HHmmssfff.PID.RAND`) and a fresh `import.log` starts. A small marker is written at the top of the new file.
- `out/import_audit.csv` is written only when the audit flag is enabled (see Flags and defaults below).
- Operations helpers: Clear xsd tables (child→parent), Show xsd table counts

SQL auth note:
- The service runs under a Windows account (default: LocalSystem). When using Trusted_Connection=True, SQL Server authenticates as that account (e.g., NT AUTHORITY\SYSTEM). Ensure that login exists and has rights to the target database (PatoData), or configure the service to run under a domain/user account mapped in SQL, or use SQL authentication in the connection string.

Helper scripts:
- Install and start the service (publishes to `publish/XsdAnalyzer` by default):

	```powershell
	.\scripts\install-importer-service.ps1 -Connection "Server=.\SQLEXPRESS;Database=Pato;Trusted_Connection=True;TrustServerCertificate=True" -Start
	```

- Install from env (recommended)

	```powershell
	# Edit scripts/service.env.ps1 (Server/Database, paths, account, flags)
	.\scripts\install-from-env.ps1 -Start -Force
	```

- Uninstall the service:

	```powershell
	.\scripts\uninstall-importer-service.ps1
	```

Parameters include `-ServiceName`, `-DisplayName`, `-XsdPath`, `-OutDir`, `-Schema`, `-ImportDir`, `-VerboseImport`, `-Audit`, and publishing controls (`-PublishDir`, `-Runtime`, `-SelfContained`, `-SingleFile`).

- Reinstall and verify flags (no `--verbose-import`, no `--audit`):

	```powershell
	.\scripts\reinstall-and-verify.ps1
	# Skip starting the service:
	.\scripts\reinstall-and-verify.ps1 -SkipStart
	```

Start/stop and verify:

```powershell
# Start/stop/restart
Start-Service -Name PatoDataXmlImporter
Stop-Service -Name PatoDataXmlImporter
Restart-Service -Name PatoDataXmlImporter

# Check current status
Get-Service -Name PatoDataXmlImporter | Format-Table -Auto

# Inspect binPath flags (must NOT include --verbose-import or --audit if disabled)
sc.exe qc PatoDataXmlImporter

# Tail the importer log
Get-Content .\out\import.log -Wait
```

Flags and defaults:

- Verbose import (`--verbose-import`) is noisy and intended for diagnostics; it logs per-element decisions. Default is off.
- Audit CSV (`--audit`) writes detailed events to `out/import_audit.csv`; default is off.
- Set defaults in `scripts/service.env.ps1`:
  - `$VerboseImport = $false`
  - `$Audit = $false`
  Reinstall via `install-from-env.ps1` to apply.

Troubleshooting:

- On startup, the service writes a line like `Startup: XSD=…; ImportDir=…; OutDir=…; Schema=…; Tables=N; …` to `out/import.log`. Ensure `Tables > 0` and paths are correct.
- If `import_audit.csv` appears unexpectedly, check the service binPath (via `sc.exe qc`) and VS Code tasks to ensure `--audit` isn’t being passed.
- If files are moved but rows are not inserted, verify service account SQL/NTFS permissions and that the XSD path is accessible to the service.

## Operations quick reference

Everyday commands for managing the Windows service.

Install or update from env (recommended):

```powershell
# Edit scripts/service.env.ps1, then:
.\scripts\install-from-env.ps1 -Start -Force
```

Reinstall and verify flags (no --verbose-import, no --audit):

```powershell
.\scripts\reinstall-and-verify.ps1
# Skip starting the service after reinstall
.\scripts\reinstall-and-verify.ps1 -SkipStart
```

Start/stop/restart and status:

```powershell
Start-Service -Name PatoDataXmlImporter
Stop-Service -Name PatoDataXmlImporter
Restart-Service -Name PatoDataXmlImporter
Get-Service -Name PatoDataXmlImporter | Format-Table -Auto
```

Inspect binPath and tail logs:

```powershell
sc.exe qc PatoDataXmlImporter
Get-Content .\out\import.log -Wait
```

Flags (defaults) in `scripts/service.env.ps1`:

- `$VerboseImport = $false` (enable only for diagnostics)
- `$Audit = $false` (enable to write `out/import_audit.csv`)

After changing flags, reinstall with `install-from-env.ps1` to apply.

## Code quality: Roslyn analyzers

Analyzers are enabled solution-wide to catch bugs and code smells early:

- .NET SDK analyzers with `AnalysisLevel=latest`
- Roslynator.Analyzers for additional code-quality suggestions
 - StyleCop.Analyzers (opt-in; disabled by default)

Run a strict build (treat warnings as errors) from VS Code Tasks:

```powershell
# Tasks: .NET Build (warnings as errors)
```

Adjust severities in `.editorconfig`. To permanently enforce zero warnings, set the following in `Directory.Build.props`:

```xml
<TreatWarningsAsErrors>true</TreatWarningsAsErrors>
<CodeAnalysisTreatWarningsAsErrors>true</CodeAnalysisTreatWarningsAsErrors>
```

StyleCop usage (opt-in):

- By default, StyleCop.Analyzers is NOT included in the build. You can enable it per-invocation by setting an MSBuild property:

	- Command line:
		- `dotnet build /p:EnableStyleCopAnalyzers=true`
		- `dotnet run --project .\XsdAnalyzer\XsdAnalyzer.csproj -- /p:EnableStyleCopAnalyzers=true` (builds before run)
	- MSBuild property (solution-wide default): set `<EnableStyleCopAnalyzers>true</EnableStyleCopAnalyzers>` in `Directory.Build.props`.

- Our VS Code build tasks are configured to keep StyleCop disabled by default by passing `/p:EnableStyleCopAnalyzers=false` so strict builds stay green while we iterate.

- If you enable StyleCop with the strict build, expect many SA diagnostics initially (formatting/bracing/comments). You can:
	- Fix violations incrementally,
	- Or tune severities in `.editorconfig` / `.globalconfig` (we’ve pre-suppressed the noisiest rules to keep the signal high when opting in).

Notes:
- `.editorconfig` centralizes analyzer severities for SDK analyzers, Roslynator, and selected StyleCop rules.
- `.globalconfig` includes broad StyleCop category suppressions. It’s harmless when StyleCop is disabled; feel free to refine if you opt in long-term.

### Fast local builds vs full analyzers

To keep local (inner-loop) builds fast, full analyzers are now disabled by default unless you explicitly opt in or you're in CI.

Mechanism:
| Property | Default local | CI (ContinuousIntegrationBuild=true) |
|----------|---------------|--------------------------------------|
| EnableFullAnalyzers | false | true |
| EnableNETAnalyzers  | mirrors EnableFullAnalyzers | mirrors |
| AnalysisLevel       | latest-minimum | latest |

Opt in locally for a single build:
```powershell
dotnet build -p:EnableFullAnalyzers=true
```

Strict + full analyzers:
```powershell
dotnet build -p:EnableFullAnalyzers=true -p:TreatWarningsAsErrors=true -p:CodeAnalysisTreatWarningsAsErrors=true
```

Add StyleCop simultaneously:
```powershell
dotnet build -p:EnableFullAnalyzers=true -p:EnableStyleCopAnalyzers=true
```

Permanent opt-in (expect slower builds): set `<EnableFullAnalyzers>true</EnableFullAnalyzers>` in `Directory.Build.props`.

If builds remain slow even with analyzers disabled, check for antivirus scanning of `obj/` + `bin/` or building on a synced / network path.

## Notes
- Replace `xsd/sample.xsd` with your actual schema.
- Uses `xmlschema` for validation and `lxml` for XML generation.
 - SQL generation targets SQL Server (schema-qualified with [dbo] by default). Complex models (choice/all, references) are simplified.
 - You can choose schema name with `--schema <name>`.
 - Insert templates are appended at the end of `schema.sql` as examples.
 - Insert templates are written to `schema.samples.sql` as examples.
