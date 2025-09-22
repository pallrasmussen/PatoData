using System.Xml;
using System.Xml.Linq;
using System.Xml.Schema;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Runtime.Versioning;
using System.Runtime.CompilerServices;
using XsdAnalyzer;

[assembly: InternalsVisibleTo("XsdAnalyzer.Tests")]

// Bootstrap logging: capture very early startup issues (especially for Windows Service 1053 diagnostics)
string _bootstrapOutDir = Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "out");
try { Directory.CreateDirectory(_bootstrapOutDir); } catch { }
string _bootstrapLog = Path.Combine(_bootstrapOutDir, "startup.log");
void BootstrapLog(string msg)
{
    try { File.AppendAllText(_bootstrapLog, DateTime.Now.ToString("s") + " " + msg + Environment.NewLine); } catch { }
}
BootstrapLog("Process starting (pid=" + Environment.ProcessId + ") args=[" + string.Join(" ", args.Select(a => a.Contains(' ') ? '"' + a + '"' : a)) + "]");
try
{
    // placeholder to detect any immediate exceptions; actual logic continues below
}
catch (Exception ex)
{
    BootstrapLog("Early exception: " + ex.Message);
    throw;
}

static int PrintUsage()
{
    Console.WriteLine("Usage: XsdAnalyzer --xsd <path> [--out <dir>]");
    Console.WriteLine("       XsdAnalyzer --service --xsd <path> --out <dir> --schema <name> --import-dir <folder> --connection <conn> [--remote-source-dir <UNC>] [--remote-poll-seconds <int>] [--verbose-import] [--audit] [--debounce-ms <int>] [--ready-wait-ms <int>] [--no-idempotency]");
    Console.WriteLine("       XsdAnalyzer --xsd <path> --out <dir> --schema <name> --import-dir <folder> --connection <conn> [--remote-source-dir <UNC>] [--remote-poll-seconds <int>] [--watch] [--verbose-import] [--audit] [--debounce-ms <int>] [--ready-wait-ms <int>] [--no-idempotency]");
    Console.WriteLine();
    Console.WriteLine("Environment variables (fallbacks when args not provided):");
    Console.WriteLine("  PATO_XSD, PATO_OUT, PATO_SCHEMA, PATO_IMPORT_DIR, PATO_CONNECTION");
    Console.WriteLine("  PATO_WATCH=1|0, PATO_VERBOSE_IMPORT=1|0, PATO_AUDIT=1|0");
    Console.WriteLine("  PATO_DEBOUNCE_MS, PATO_READY_WAIT_MS, PATO_IDEMPOTENCY_ENABLED=1|0");
    Console.WriteLine("  PATO_REMOTE_SOURCE_DIR, PATO_REMOTE_POLL_SECONDS, PATO_REMOTE_HISTORY_FILE");
    return 1;
}

string? xsdPath = null;
string outDir = Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "out");
string sqlSchema = "dbo";
string? exampleXmlPath = null;
string? importFolder = null;
string? connectionString = null;
bool watch = false;
bool verboseImport = false;
bool audit = false;
bool runAsService = false;
int debounceMs = 200;
int readyWaitMs = 2000;
bool idempotencyEnabled = true;
string? serviceNameOverride = null;
string? configPath = null;
string? remoteSourceDir = null;
int remotePollSeconds = 300; // default 5 minutes
string? remoteHistoryFile = null;

for (int i = 0; i < args.Length; i++)
{
    if (args[i] == "--xsd" && i + 1 < args.Length) xsdPath = args[++i];
    else if (args[i] == "--out" && i + 1 < args.Length) outDir = args[++i];
    else if (args[i] == "--schema" && i + 1 < args.Length) sqlSchema = args[++i];
    else if (args[i] == "--xml" && i + 1 < args.Length) exampleXmlPath = args[++i];
    else if (args[i] == "--import-dir" && i + 1 < args.Length) importFolder = args[++i];
    else if (args[i] == "--connection" && i + 1 < args.Length) connectionString = args[++i];
    else if (args[i] == "--watch") watch = true;
    else if (args[i] == "--verbose-import") verboseImport = true;
    else if (args[i] == "--audit") audit = true;
    else if (args[i] == "--service") runAsService = true;
    else if (args[i] == "--service-name" && i + 1 < args.Length) serviceNameOverride = args[++i];
    else if (args[i] == "--config" && i + 1 < args.Length) configPath = args[++i];
    else if (args[i] == "--debounce-ms" && i + 1 < args.Length)
    {
        if (int.TryParse(args[++i], out var v) && v >= 0) debounceMs = v;
    }
    else if (args[i] == "--ready-wait-ms" && i + 1 < args.Length)
    {
        if (int.TryParse(args[++i], out var v) && v >= 0) readyWaitMs = v;
    }
    else if (args[i] == "--no-idempotency")
    {
        idempotencyEnabled = false;
    }
    else if (args[i] == "--remote-source-dir" && i + 1 < args.Length)
    {
        remoteSourceDir = args[++i];
    }
    else if (args[i] == "--remote-poll-seconds" && i + 1 < args.Length)
    {
        if (int.TryParse(args[++i], out var v) && v > 0) remotePollSeconds = v;
    }
}

// Environment variable fallbacks if values not provided via arguments
string? Env(string name) => Environment.GetEnvironmentVariable(name);
bool EnvBool(string name)
{
    var v = Env(name);
    if (string.IsNullOrWhiteSpace(v)) return false;
    return v.Equals("1") || v.Equals("true", StringComparison.OrdinalIgnoreCase) || v.Equals("yes", StringComparison.OrdinalIgnoreCase);
}
int? EnvInt(string name)
{
    var v = Env(name);
    if (int.TryParse(v, out var n)) return n; return null;
}

if (string.IsNullOrWhiteSpace(xsdPath)) xsdPath = Env("PATO_XSD") ?? xsdPath;
if (string.IsNullOrWhiteSpace(outDir) || outDir == Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "out"))
{
    var e = Env("PATO_OUT"); if (!string.IsNullOrWhiteSpace(e)) outDir = e!;
}
if (sqlSchema == "dbo") { var e = Env("PATO_SCHEMA"); if (!string.IsNullOrWhiteSpace(e)) sqlSchema = e!; }
if (string.IsNullOrWhiteSpace(importFolder)) importFolder = Env("PATO_IMPORT_DIR") ?? importFolder;
if (string.IsNullOrWhiteSpace(connectionString)) connectionString = Env("PATO_CONNECTION") ?? Env("PATO_CONN") ?? connectionString;
if (!watch) watch = EnvBool("PATO_WATCH");
if (!verboseImport) verboseImport = EnvBool("PATO_VERBOSE_IMPORT");
if (!audit) audit = EnvBool("PATO_AUDIT");
var dms = EnvInt("PATO_DEBOUNCE_MS"); if (dms.HasValue && debounceMs == 200) debounceMs = Math.Max(0, dms.Value);
var rw = EnvInt("PATO_READY_WAIT_MS"); if (rw.HasValue && readyWaitMs == 2000) readyWaitMs = Math.Max(0, rw.Value);
var idemEnv = Env("PATO_IDEMPOTENCY_ENABLED");
if (!string.IsNullOrWhiteSpace(idemEnv))
{
    idempotencyEnabled = idemEnv.Equals("1") || idemEnv.Equals("true", StringComparison.OrdinalIgnoreCase) || idemEnv.Equals("yes", StringComparison.OrdinalIgnoreCase);
}
var svcEnv = Env("PATO_SERVICE_NAME"); if (string.IsNullOrWhiteSpace(serviceNameOverride) && !string.IsNullOrWhiteSpace(svcEnv)) serviceNameOverride = svcEnv;
if (string.IsNullOrWhiteSpace(remoteSourceDir)) remoteSourceDir = Env("PATO_REMOTE_SOURCE_DIR") ?? remoteSourceDir;
var rpsEnv = EnvInt("PATO_REMOTE_POLL_SECONDS"); if (rpsEnv.HasValue && remotePollSeconds == 300) remotePollSeconds = Math.Max(1, rpsEnv.Value);
if (string.IsNullOrWhiteSpace(remoteHistoryFile)) remoteHistoryFile = Env("PATO_REMOTE_HISTORY_FILE") ?? remoteHistoryFile;

// Config file (lowest precedence, but before hard-coded defaults)
AppConfig? cfg = null;
if (string.IsNullOrWhiteSpace(configPath))
{
    var candidate = Path.Combine(Directory.GetCurrentDirectory(), "appsettings.json");
    if (File.Exists(candidate)) configPath = candidate;
}
if (!string.IsNullOrWhiteSpace(configPath) && File.Exists(configPath))
{
    cfg = XsdAnalyzer.AppConfig.Load(configPath);
}
// Merge from config where still missing
if (cfg is not null)
{
    xsdPath ??= cfg.Xsd;
    if (string.IsNullOrWhiteSpace(outDir) || outDir == Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "out"))
        outDir = cfg.OutDir ?? outDir;
    if (sqlSchema == "dbo" && !string.IsNullOrWhiteSpace(cfg.Schema)) sqlSchema = cfg.Schema!;
    importFolder ??= cfg.ImportDir;
    connectionString ??= cfg.Connection;
    if (!watch && cfg.Watch.HasValue) watch = cfg.Watch.Value;
    if (!verboseImport && cfg.VerboseImport.HasValue) verboseImport = cfg.VerboseImport.Value;
    if (!audit && cfg.Audit.HasValue) audit = cfg.Audit.Value;
    if (debounceMs == 200 && cfg.DebounceMs.HasValue) debounceMs = Math.Max(0, cfg.DebounceMs.Value);
    if (readyWaitMs == 2000 && cfg.ReadyWaitMs.HasValue) readyWaitMs = Math.Max(0, cfg.ReadyWaitMs.Value);
    if (cfg.IdempotencyEnabled.HasValue) idempotencyEnabled = cfg.IdempotencyEnabled.Value;
    serviceNameOverride ??= cfg.ServiceName;
    remoteSourceDir ??= cfg.RemoteSourceDir;
    if (remotePollSeconds == 300 && cfg.RemotePollSeconds.HasValue) remotePollSeconds = Math.Max(1, cfg.RemotePollSeconds.Value);
    remoteHistoryFile ??= cfg.RemoteHistoryFile;
}

var output = Path.GetFullPath(outDir);
var logPath = Path.Combine(output, "import.log");

// Windows Service mode
if (runAsService)
{
    if (string.IsNullOrWhiteSpace(importFolder) || string.IsNullOrWhiteSpace(connectionString))
    {
        Console.WriteLine("--service requires --import-dir and --connection");
        return 1;
    }
    var host = Host.CreateDefaultBuilder(args)
        .UseWindowsService(opts => opts.ServiceName = string.IsNullOrWhiteSpace(serviceNameOverride) ? "PatoData XML Importer" : serviceNameOverride)
        .ConfigureLogging((ctx, logging) =>
        {
            if (OperatingSystem.IsWindows())
            {
                try
                {
                    XsdAnalyzer.HostConfig.ConfigureWindowsEventLog(logging);
                }
                catch
                {
                    // Swallow event log registration errors to avoid service startup failure (e.g., insufficient rights)
                }
            }
        })
        .ConfigureServices(svc =>
        {
            // Resolve XSD path safely to avoid build-time exceptions
            string xsdResolved;
            if (!string.IsNullOrWhiteSpace(xsdPath))
            {
                try { xsdResolved = Path.GetFullPath(xsdPath); }
                catch { xsdResolved = xsdPath!; }
            }
            else if (!string.IsNullOrWhiteSpace(cfg?.Xsd))
            {
                try { xsdResolved = Path.GetFullPath(cfg!.Xsd!); }
                catch { xsdResolved = cfg!.Xsd!; }
            }
            else
            {
                xsdResolved = string.Empty; // Will be validated in the background service
            }
            var opts = new XsdAnalyzer.ImportServiceOptions
            {
                Xsd = xsdResolved,
                OutDir = output,
                Schema = sqlSchema,
                ImportDir = Path.GetFullPath(importFolder!),
                Connection = connectionString!,
                VerboseImport = verboseImport,
                Audit = audit,
                DebounceMs = debounceMs,
                ReadyWaitMs = readyWaitMs,
                IdempotencyEnabled = idempotencyEnabled,
                RemoteSourceDir = remoteSourceDir,
                RemotePollSeconds = remotePollSeconds,
                RemoteHistoryFile = remoteHistoryFile
            };
            svc.AddSingleton(opts);
            svc.AddHostedService<XsdAnalyzer.XmlImportBackgroundService>();
        })
        .Build();

    await host.RunAsync();
    return 0;
}

// Non-service CLI flows require an XSD path
if (string.IsNullOrWhiteSpace(xsdPath)) return PrintUsage();

try
{
    // Ensure output directory exists for CLI workflows
    try { Directory.CreateDirectory(output); } catch { }
    var summaryPath = Path.Combine(output, "schema_summary.txt");
    var examplePath = Path.Combine(output, "example.xml");
    var sqlPath = Path.Combine(output, "schema.sql");
    var sqlSamplesPath = Path.Combine(output, "schema.samples.sql");
    var dropPath = Path.Combine(output, "schema.drop.sql");
    var clearPath = Path.Combine(output, "schema.clear.sql");
    var viewsPath = Path.Combine(output, "schema.views.sql");
    var seedPath = Path.Combine(output, "seed.sql");
    var seedDryRunPath = Path.Combine(output, "seed.dryrun.sql");
    var auditPath = Path.Combine(output, "import_audit.csv");
    // Observability
    XsdAnalyzer.Observability.Configure(output);

    var schemaSet = new XmlSchemaSet();
    using (var reader = XmlReader.Create(xsdPath))
    {
        schemaSet.Add(null, reader);
    }
    schemaSet.CompilationSettings = new XmlSchemaCompilationSettings { EnableUpaCheck = true };
    schemaSet.ValidationEventHandler += (s, e) => Console.WriteLine($"[{e.Severity}] {e.Message}");
    schemaSet.Compile();

    var summary = XsdTools.Summarize(schemaSet);
    await File.WriteAllTextAsync(summaryPath, summary);
    Console.WriteLine($"Wrote {summaryPath}");

    var example = XsdTools.GenerateExample(schemaSet);
    if (example is not null)
    {
        example.Save(examplePath);
        Console.WriteLine($"Wrote {examplePath}");
    }
    else
    {
        Console.WriteLine("Skipped example XML generation: no global elements found.");
    }
    // SQL generation
    var sqlGen = new XsdAnalyzer.XsdToSqlServer(sqlSchema);
    var sql = sqlGen.Generate(schemaSet);
    await File.WriteAllTextAsync(sqlPath, sql);
    Console.WriteLine($"Wrote {sqlPath}");
    // optional sample insert templates
    var samples = sqlGen.GenerateInsertTemplates(schemaSet);
    await File.WriteAllTextAsync(sqlSamplesPath, samples);
    Console.WriteLine($"Wrote {sqlSamplesPath}");

    // Views
    var views = sqlGen.GenerateViews();
    await File.WriteAllTextAsync(viewsPath, views);
    Console.WriteLine($"Wrote {viewsPath}");

    // Create a simple DROP script (reverse dependency order)
    try
    {
        var drop = new System.Text.StringBuilder();
    drop.AppendLine("-- Drop constraints (only within target schema)");
        drop.AppendLine("-- NOTE: Adjust order if necessary for your environment");
        drop.AppendLine("-- The following assumes default naming and may require manual tweaks for complex schemas.");
    drop.AppendLine($"DECLARE @sql NVARCHAR(MAX) = N'';\n" +
            $"SELECT @sql = @sql + 'ALTER TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + ' DROP CONSTRAINT ' + QUOTENAME(fk.name) + ';\n'\n" +
            $"FROM sys.foreign_keys fk\n" +
            $"JOIN sys.tables t ON fk.parent_object_id = t.object_id\n" +
            $"JOIN sys.schemas s ON t.schema_id = s.schema_id\n" +
            $"WHERE s.name = N'{sqlSchema}';\n" +
            $"EXEC sp_executesql @sql;\n");
    drop.AppendLine("-- Drop tables (target schema only)");
    drop.AppendLine($"DECLARE @sql2 NVARCHAR(MAX) = N'';\n" +
            $"SELECT @sql2 = @sql2 + 'DROP TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + ';\n'\n" +
            $"FROM sys.tables t\n" +
            $"JOIN sys.schemas s ON t.schema_id = s.schema_id\n" +
            $"WHERE s.name = N'{sqlSchema}';\n" +
            $"EXEC sp_executesql @sql2;\n");
        await File.WriteAllTextAsync(dropPath, drop.ToString());
        Console.WriteLine($"Wrote {dropPath}");
    }
    catch { }

    // Create a CLEAR script: delete from all tables in schema in child→parent order
    try
    {
        var clear = new System.Text.StringBuilder();
        clear.AppendLine("-- Delete all rows from all tables in the target schema in child→parent order");
        clear.AppendLine("-- Generated by XsdAnalyzer; review before running in production.");
        clear.AppendLine($"DECLARE @schema sysname = N'{sqlSchema}';");
        clear.AppendLine("IF OBJECT_ID('tempdb..#Tables') IS NOT NULL DROP TABLE #Tables;");
        clear.AppendLine("CREATE TABLE #Tables (TableId INT PRIMARY KEY, SchemaName SYSNAME, TableName SYSNAME, Level INT NOT NULL DEFAULT(0));");
        clear.AppendLine("INSERT INTO #Tables(TableId, SchemaName, TableName, Level)\nSELECT t.object_id, s.name, t.name, 0\nFROM sys.tables t\nJOIN sys.schemas s ON t.schema_id = s.schema_id\nWHERE s.name = @schema;");
        clear.AppendLine("DECLARE @changed INT = 1, @i INT = 0;\nWHILE @changed = 1 AND @i < 100\nBEGIN\n    SET @changed = 0; SET @i = @i + 1;\n    UPDATE c\n        SET Level = p.Level + 1, @changed = 1\n    FROM #Tables c\n    JOIN sys.foreign_keys fk ON fk.parent_object_id = c.TableId\n    JOIN #Tables p ON p.TableId = fk.referenced_object_id\n    WHERE c.Level < p.Level + 1;\nEND;");
        clear.AppendLine("DECLARE @sql NVARCHAR(MAX) = N'';\nSELECT @sql = @sql + 'DELETE FROM ' + QUOTENAME(SchemaName) + '.' + QUOTENAME(TableName) + ';' + CHAR(10)\nFROM #Tables\nORDER BY Level DESC, TableName;\nPRINT @sql;\nEXEC sp_executesql @sql;");
        await File.WriteAllTextAsync(clearPath, clear.ToString());
        Console.WriteLine($"Wrote {clearPath}");
    }
    catch { }

    // Seed generation from example XML if provided
    try
    {
        if (!string.IsNullOrWhiteSpace(exampleXmlPath) && File.Exists(exampleXmlPath))
        {
            sqlGen.EnsureModel(schemaSet);
            var xdoc = XDocument.Load(exampleXmlPath);
            var seedSql = sqlGen.GenerateSeedFromXml(xdoc);
            await File.WriteAllTextAsync(seedPath, seedSql);
            Console.WriteLine($"Wrote {seedPath}");

            // Dry-run wrapper
            var dry = new System.Text.StringBuilder();
            dry.AppendLine("BEGIN TRANSACTION;");
            dry.AppendLine("BEGIN TRY");
            dry.AppendLine(seedSql);
            dry.AppendLine("    THROW 51000, 'Dry run - intentional rollback', 1;");
            dry.AppendLine("END TRY");
            dry.AppendLine("BEGIN CATCH");
            dry.AppendLine("    ROLLBACK TRANSACTION;");
            dry.AppendLine("END CATCH;");
            await File.WriteAllTextAsync(seedDryRunPath, dry.ToString());
            Console.WriteLine($"Wrote {seedDryRunPath}");
        }
        else
        {
            var seed = new System.Text.StringBuilder();
            seed.AppendLine("-- Seed script placeholder");
            seed.AppendLine("-- Provide --xml <example.xml> to generate seed INSERTs.");
            await File.WriteAllTextAsync(seedPath, seed.ToString());
            Console.WriteLine($"Wrote {seedPath}");
        }
    }
    catch { }

    // Import XML files from a folder if requested
    try
    {
        if (!string.IsNullOrWhiteSpace(importFolder) && !string.IsNullOrWhiteSpace(connectionString))
        {
            var inDir = Path.GetFullPath(importFolder);
            var importedDir = Path.Combine(Path.GetDirectoryName(inDir) ?? inDir, "imported");
            Directory.CreateDirectory(inDir);
            Directory.CreateDirectory(importedDir);

            sqlGen.EnsureModel(schemaSet);
            // logging helper for verbose mode
            void VerboseLog(string m)
            {
                if (!verboseImport) return;
                Console.WriteLine("[import] " + m);
                LogFile.AppendLine(logPath, DateTime.Now.ToString("s") + " [import] " + m);
            }

            // audit helper
            // Audit: write CSV only when explicitly enabled
            Action<string>? auditWriter = null;
            if (audit)
            {
                // ensure header once
                if (!File.Exists(auditPath))
                {
                    File.WriteAllText(auditPath, "Timestamp,File,Event,Element,Table,NewId,ParentTable,ParentId,FkColumn,Reason,Params" + Environment.NewLine);
                }
                auditWriter = (line) =>
                {
                    try { File.AppendAllText(auditPath, line + Environment.NewLine); }
                    catch { }
                };
            }

            var importer = new XsdAnalyzer.XmlToSqlImporter(sqlGen, connectionString, verboseImport, VerboseLog, audit, auditWriter, idempotencyEnabled);
            bool isImporting = false;
            object importGate = new object();
            Action importBatch = () =>
            {
                // Prevent re-entrancy if watcher fires multiple events quickly
                lock (importGate)
                {
                    if (isImporting) return;
                    isImporting = true;
                }
                try
                {
                    var files = Directory.EnumerateFiles(inDir, "*.xml", SearchOption.TopDirectoryOnly).ToList();
                    var errorDir = Path.Combine(Path.GetDirectoryName(inDir) ?? inDir, "error");
                    try { Directory.CreateDirectory(errorDir); } catch { }
                    foreach (var file in files)
                    {
                        try
                        {
                            // Ensure file is ready (not being written) before processing
                            FileReady.WaitForFileReady(file, maxWaitMs: readyWaitMs);
                            var sw = System.Diagnostics.Stopwatch.StartNew();
                            var result = importer.ImportFile(file);
                            var dest = Path.Combine(importedDir, Path.GetFileName(file));
                            File.Move(file, dest, overwrite: true);
                            var byTable = string.Join(", ", result.ByTable.OrderBy(kv => kv.Key, StringComparer.OrdinalIgnoreCase).Select(kv => $"{kv.Key}={kv.Value}"));
                            var msg = $"Imported {result.Total} rows{(byTable.Length>0 ? " (" + byTable + ")" : string.Empty)} and moved: {file} -> {dest}";
                            Console.WriteLine(msg);
                            LogFile.AppendLine(logPath, DateTime.Now.ToString("s") + " " + msg);
                            try { XsdAnalyzer.Observability.RecordSuccess(Path.GetFileName(file), result.Total, result.ByTable, (int)sw.ElapsedMilliseconds); } catch { }
                        }
                        catch (Exception exFile)
                        {
                            var msg = $"Failed to import {file}: {exFile.Message}";
                            Console.WriteLine(msg);
                            LogFile.AppendLine(logPath, DateTime.Now.ToString("s") + " " + msg);
                            try { XsdAnalyzer.Observability.RecordFailure(Path.GetFileName(file), exFile.Message); } catch { }
                            try
                            {
                                var destErr = Path.Combine(errorDir, Path.GetFileName(file));
                                File.Move(file, destErr, overwrite: true);
                                LogFile.AppendLine(logPath, DateTime.Now.ToString("s") + $" Moved failed file to {destErr}");
                            }
                            catch { }
                        }
                    }
                }
                finally
                {
                    lock (importGate) { isImporting = false; }
                }
            };

            // Initial run
            importBatch();

            if (watch)
            {
                Console.WriteLine($"Watching {inDir} for new XML files... Press Ctrl+C to exit.");
                using var watcher = new FileSystemWatcher(inDir, "*.xml") { IncludeSubdirectories = false, EnableRaisingEvents = true };
                System.Threading.Timer? debounceTimer = null;
                void ScheduleImport()
                {
                    try
                    {
                        if (debounceTimer is null)
                        {
                            debounceTimer = new System.Threading.Timer(_ => { try { importBatch(); } catch { } }, null, System.Threading.Timeout.Infinite, System.Threading.Timeout.Infinite);
                        }
                        debounceTimer.Change(debounceMs, System.Threading.Timeout.Infinite);
                    }
                    catch { }
                }
                FileSystemEventHandler h = (s, e) => { ScheduleImport(); };
                watcher.Created += h;
                RenamedEventHandler hr = (s, e) => { ScheduleImport(); };
                watcher.Renamed += hr;
                // Remote polling (CLI parity)
                HashSet<string>? remoteSeen = null;
                string? historyPath = null;
                if (!string.IsNullOrWhiteSpace(remoteSourceDir))
                {
                    try
                    {
                        historyPath = remoteHistoryFile ?? Path.Combine(output, "remote_copied_files.txt");
                        remoteSeen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                        if (File.Exists(historyPath))
                        {
                            foreach (var line in File.ReadAllLines(historyPath))
                            {
                                var t = line.Trim(); if (t.Length > 0) remoteSeen.Add(t);
                            }
                        }
                        // Seed with existing local XMLs (in, imported, error)
                        try
                        {
                            int seeded = 0;
                            var importedDirLocal = importedDir;
                            var errorDirLocal = Path.Combine(Path.GetDirectoryName(inDir) ?? inDir, "error");
                            foreach (var d in new[] { inDir, importedDirLocal, errorDirLocal })
                            {
                                if (!Directory.Exists(d)) continue;
                                foreach (var f in Directory.EnumerateFiles(d, "*.xml", SearchOption.TopDirectoryOnly))
                                {
                                    if (remoteSeen.Add(Path.GetFileName(f))) seeded++;
                                }
                            }
                            if (seeded > 0)
                            {
                                var seedMsg = $"[remote] Seeded history with {seeded} existing local file(s)";
                                Console.WriteLine(seedMsg);
                                LogFile.AppendLine(logPath, DateTime.Now.ToString("s") + " " + seedMsg);
                            }
                        }
                        catch { }
                        var currentUser = System.Security.Principal.WindowsIdentity.GetCurrent()?.Name ?? Environment.UserName;
                        var poll = Math.Max(30, remotePollSeconds);
                        string status = Directory.Exists(remoteSourceDir)
                            ? $"[remote] Status: watching {remoteSourceDir}; poll {poll}s; history={historyPath}; user={currentUser}"
                            : $"[remote] Status: directory not found {remoteSourceDir}; will retry; user={currentUser}";
                        Console.WriteLine(status);
                        LogFile.AppendLine(logPath, DateTime.Now.ToString("s") + " " + status);
                        // Backlog copy
                        if (Directory.Exists(remoteSourceDir))
                        {
                            var backlog = Directory.EnumerateFiles(remoteSourceDir, "*.xml", SearchOption.TopDirectoryOnly).OrderBy(f => f, StringComparer.OrdinalIgnoreCase).ToList();
                            int copied = 0;
                            foreach (var rf in backlog)
                            {
                                var name = Path.GetFileName(rf);
                                if (remoteSeen.Contains(name)) continue;
                                try
                                {
                                    var dest = Path.Combine(inDir, name);
                                    if (File.Exists(dest))
                                    {
                                        dest = Path.Combine(inDir, Path.GetFileNameWithoutExtension(dest) + "_" + DateTime.Now.ToString("yyyyMMdd_HHmmss") + Path.GetExtension(dest));
                                    }
                                    File.Copy(rf, dest, overwrite: false);
                                    remoteSeen.Add(name);
                                    if (!string.IsNullOrWhiteSpace(historyPath))
                                    {
                                        try { File.AppendAllText(historyPath, name + Environment.NewLine); } catch { }
                                    }
                                    var msg = $"[remote] Backlog copied {rf} -> {dest}";
                                    Console.WriteLine(msg);
                                    LogFile.AppendLine(logPath, DateTime.Now.ToString("s") + " " + msg);
                                    copied++;
                                }
                                catch (Exception bEx)
                                {
                                    var w = $"[remote] Failed backlog copy {rf}: {bEx.Message}";
                                    Console.WriteLine(w);
                                    LogFile.AppendLine(logPath, DateTime.Now.ToString("s") + " " + w);
                                }
                            }
                            var backlogSummary = copied > 0 ? $"[remote] Backlog copied {copied} file(s)" : "[remote] No backlog files to copy (0 new)";
                            Console.WriteLine(backlogSummary);
                            LogFile.AppendLine(logPath, DateTime.Now.ToString("s") + " " + backlogSummary);
                            if (copied > 0) importBatch();
                        }
                    }
                    catch (Exception rex)
                    {
                        var initErr = $"[remote] Init error: {rex.Message}";
                        Console.WriteLine(initErr);
                        LogFile.AppendLine(logPath, DateTime.Now.ToString("s") + " " + initErr);
                    }
                }
                object remoteGate = new object();
                bool remoteBusy = false;
                void PollRemote()
                {
                    if (remoteSeen is null || string.IsNullOrWhiteSpace(remoteSourceDir)) return;
                    lock (remoteGate)
                    {
                        if (remoteBusy) return; remoteBusy = true;
                    }
                    try
                    {
                        if (!Directory.Exists(remoteSourceDir)) return;
                        var rfiles = Directory.EnumerateFiles(remoteSourceDir, "*.xml", SearchOption.TopDirectoryOnly).OrderBy(f => f, StringComparer.OrdinalIgnoreCase).ToList();
                        foreach (var rf in rfiles)
                        {
                            var name = Path.GetFileName(rf);
                            if (remoteSeen.Contains(name)) continue;
                            try
                            {
                                var dest = Path.Combine(inDir, name);
                                if (File.Exists(dest))
                                {
                                    dest = Path.Combine(inDir, Path.GetFileNameWithoutExtension(dest) + "_" + DateTime.Now.ToString("yyyyMMdd_HHmmss") + Path.GetExtension(dest));
                                }
                                File.Copy(rf, dest, overwrite: false);
                                remoteSeen.Add(name);
                                if (!string.IsNullOrWhiteSpace(historyPath))
                                {
                                    try { File.AppendAllText(historyPath, name + Environment.NewLine); } catch { }
                                }
                                var msg = $"Copied remote XML {rf} -> {dest}";
                                Console.WriteLine(msg);
                                LogFile.AppendLine(logPath, DateTime.Now.ToString("s") + " " + msg);
                                importBatch();
                            }
                            catch (Exception pex)
                            {
                                var emsg = $"Failed copying remote file {rf}: {pex.Message}";
                                Console.WriteLine(emsg);
                                LogFile.AppendLine(logPath, DateTime.Now.ToString("s") + " " + emsg);
                            }
                        }
                    }
                    finally
                    {
                        lock (remoteGate) { remoteBusy = false; }
                    }
                }
                var remoteInterval = Math.Max(30, remotePollSeconds);
                DateTime lastRemote = DateTime.MinValue;
                while (true)
                {
                    if (remoteSeen is not null && (DateTime.UtcNow - lastRemote).TotalSeconds >= remoteInterval)
                    {
                        lastRemote = DateTime.UtcNow;
                        try { PollRemote(); } catch { }
                    }
                    await Task.Delay(1000);
                }
            }
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Import error: {ex.Message}");
    }
}
catch (Exception ex)
{
    Console.WriteLine($"Error: {ex.Message}");
    return 1;
}

return 0;

namespace XsdAnalyzer
{
    internal static class HostConfig
    {
        [SupportedOSPlatform("windows")]
        internal static void ConfigureWindowsEventLog(ILoggingBuilder logging)
        {
            #pragma warning disable CA1416 // Validate platform compatibility
            logging.AddEventLog(settings =>
            {
                settings.SourceName = "PatoData XML Importer";
                settings.LogName = "Application";
            });
            #pragma warning restore CA1416 // Validate platform compatibility
        }
    }

    static class XsdTools
    {
    public static string Summarize(XmlSchemaSet set)
    {
        var lines = new List<string>();
        lines.Add($"Schemas: {set.Schemas().Cast<XmlSchema>().Count()}");
        foreach (XmlSchema schema in set.Schemas())
        {
            lines.Add($"TargetNamespace: {schema.TargetNamespace}");
            foreach (XmlSchemaObject item in schema.Items)
            {
                switch (item)
                {
                    case XmlSchemaElement el:
                        lines.Add($"- element {QName(el.QualifiedName)} type={el.ElementSchemaType?.Name ?? el.SchemaTypeName.Name}");
                        break;
                    case XmlSchemaComplexType ct:
                        lines.Add($"- complexType {ct.Name}");
                        break;
                    case XmlSchemaSimpleType st:
                        lines.Add($"- simpleType {st.Name}");
                        break;
                }
            }
        }
        return string.Join(Environment.NewLine, lines);
    }

    public static XDocument? GenerateExample(XmlSchemaSet set)
    {
        // Pick the first global element as root
        foreach (XmlSchema schema in set.Schemas())
        {
            foreach (XmlSchemaObject item in schema.Items)
            {
                if (item is XmlSchemaElement el && el.QualifiedName.Name is { Length: > 0 })
                {
                    var root = new XElement(el.QualifiedName.Name);
                    FillElement(root, el.ElementSchemaType);
                    return new XDocument(new XDeclaration("1.0", "utf-8", "yes"), root);
                }
            }
        }
        return null;
    }

    private static void FillElement(XElement node, XmlSchemaType? type)
    {
        if (type is XmlSchemaComplexType ct)
        {
            if (ct.ContentTypeParticle is XmlSchemaSequence seq)
            {
                foreach (var item in seq.Items.Cast<XmlSchemaObject>())
                {
                    if (item is XmlSchemaElement childEl)
                    {
                        var child = new XElement(childEl.QualifiedName.Name);
                        node.Add(child);
                        FillElement(child, childEl.ElementSchemaType);
                    }
                }
            }
            // attributes
            foreach (XmlSchemaAttribute attr in ct.AttributeUses.Values)
            {
                var attrName = !string.IsNullOrEmpty(attr.QualifiedName.Name) ? attr.QualifiedName.Name : attr.Name ?? "attr";
                node.SetAttributeValue(attrName, PlaceholderFor(attr.AttributeSchemaType));
            }
        }
        else
        {
            node.Value = PlaceholderFor(type);
        }
    }

    private static string PlaceholderFor(XmlSchemaType? t) => t switch
    {
        XmlSchemaSimpleType st when st.Datatype is not null => st.Datatype.TypeCode switch
        {
            XmlTypeCode.String => "string",
            XmlTypeCode.Decimal => "0.0",
            XmlTypeCode.Int or XmlTypeCode.Integer or XmlTypeCode.Long => "0",
            XmlTypeCode.Boolean => "true",
            XmlTypeCode.Date => DateTime.UtcNow.ToString("yyyy-MM-dd"),
            XmlTypeCode.DateTime => DateTime.UtcNow.ToString("o"),
            _ => st.Datatype.ValueType?.Name?.ToLowerInvariant() ?? "value"
        },
        _ => "value"
    };

    private static string QName(XmlQualifiedName qn) => string.IsNullOrEmpty(qn.Namespace) ? qn.Name : $"{{{qn.Namespace}}}{qn.Name}";
    }

    static class FileReady
    {
        public static void WaitForFileReady(string path, int maxWaitMs = 2000)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            long? lastLen = null;
            while (sw.ElapsedMilliseconds < maxWaitMs)
            {
                try
                {
                    var fi = new FileInfo(path);
                    if (!fi.Exists) { System.Threading.Thread.Sleep(50); continue; }
                    using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read))
                    {
                        // If size is stable across checks and we can open for read, assume ready
                        if (lastLen.HasValue && lastLen.Value == fi.Length)
                        {
                            return;
                        }
                        lastLen = fi.Length;
                    }
                }
                catch
                {
                    // ignore and retry
                }
                System.Threading.Thread.Sleep(100);
            }
        }
    }
}
