using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Xml;
using System.Xml.Schema;

namespace XsdAnalyzer;

internal sealed class XmlImportBackgroundService : BackgroundService
{
    private readonly ILogger<XmlImportBackgroundService> _logger;
    private readonly ImportServiceOptions _opts;

    public XmlImportBackgroundService(ILogger<XmlImportBackgroundService> logger, ImportServiceOptions opts)
    {
        _logger = logger;
        _opts = opts;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Ensure we yield immediately so the service can report started to SCM,
        // avoiding a 1053 if initialization takes time.
        await Task.Yield();
        try
        {
            Directory.CreateDirectory(_opts.OutDir);
            var logPath = Path.Combine(_opts.OutDir, "import.log");
            var auditPath = Path.Combine(_opts.OutDir, "import_audit.csv");
            Observability.Configure(_opts.OutDir);

            // Build schema set
            var set = new XmlSchemaSet();
            using (var reader = XmlReader.Create(_opts.Xsd))
            {
                set.Add(null, reader);
            }
            set.CompilationSettings = new XmlSchemaCompilationSettings { EnableUpaCheck = true };
            set.Compile();

            var gen = new XsdToSqlServer(_opts.Schema);
            // Build the in-memory model from the XSD so importer can resolve tables
            gen.EnsureModel(set);
            // One-time startup diagnostics
            try
            {
                string Safe(string? s)
                {
                    if (string.IsNullOrWhiteSpace(s)) return string.Empty;
                    // Mask likely sensitive parts of connection string
                    var masked = s
                        .Replace("Password=", "Password=***", StringComparison.OrdinalIgnoreCase)
                        .Replace("Pwd=", "Pwd=***", StringComparison.OrdinalIgnoreCase);
                    return masked;
                }
                var diag = $"[service] Startup: XSD={_opts.Xsd}; ImportDir={_opts.ImportDir}; OutDir={_opts.OutDir}; Schema={_opts.Schema}; Tables={gen.TableCount}; Connection={Safe(_opts.Connection)}";
                _logger.LogInformation(diag);
                LogFile.AppendLine(logPath, DateTime.Now.ToString("s") + " " + diag);
            }
            catch { }
            var verbose = _opts.VerboseImport;

            void VerboseLog(string m)
            {
                if (!verbose) return;
                try { LogFile.AppendLine(logPath, DateTime.Now.ToString("s") + " [service] " + m); } catch { }
                _logger.LogInformation("{Message}", m);
            }

            // Audit: write CSV only when explicitly enabled
            Action<string>? auditWriter = null;
            if (_opts.Audit)
            {
                if (!File.Exists(auditPath))
                {
                    File.WriteAllText(auditPath, "Timestamp,File,Event,Element,Table,NewId,ParentTable,ParentId,FkColumn,Reason,Params" + Environment.NewLine);
                }
                auditWriter = (line) => { try { File.AppendAllText(auditPath, line + Environment.NewLine); } catch { } };
            }

            var importer = new XmlToSqlImporter(gen, _opts.Connection, verbose, VerboseLog, _opts.Audit, auditWriter, _opts.IdempotencyEnabled);
            Directory.CreateDirectory(_opts.ImportDir);
            var importedDir = Path.Combine(Path.GetDirectoryName(_opts.ImportDir) ?? _opts.ImportDir, "imported");
            Directory.CreateDirectory(importedDir);
            var errorDir = Path.Combine(Path.GetDirectoryName(_opts.ImportDir) ?? _opts.ImportDir, "error");
            Directory.CreateDirectory(errorDir);

            bool importing = false;
            object gate = new object();
            void ImportBatch()
            {
                lock (gate)
                {
                    if (importing) return;
                    importing = true;
                }
                try
                {
                    var files = Directory.EnumerateFiles(_opts.ImportDir, "*.xml", SearchOption.TopDirectoryOnly).ToList();
                    foreach (var file in files)
                    {
                        if (stoppingToken.IsCancellationRequested) break;
                        try
                        {
                            // Ensure file is ready before import
                            FileReady.WaitForFileReady(file, maxWaitMs: _opts.ReadyWaitMs);
                            var sw = System.Diagnostics.Stopwatch.StartNew();
                            var result = importer.ImportFile(file);
                            var dest = Path.Combine(importedDir, Path.GetFileName(file));
                            File.Move(file, dest, overwrite: true);
                            var byTable = string.Join(
                                ", ", result.ByTable.OrderBy(kv => kv.Key, StringComparer.OrdinalIgnoreCase).Select(kv => $"{kv.Key}={kv.Value}")
                            );
                            var msg = $"Imported {result.Total} rows{(byTable.Length>0 ? " (" + byTable + ")" : string.Empty)} and moved: {file} -> {dest}";
                            _logger.LogInformation("{Message}", msg);
                            LogFile.AppendLine(logPath, DateTime.Now.ToString("s") + " " + msg);
                            try { Observability.RecordSuccess(Path.GetFileName(file), result.Total, result.ByTable, (int)sw.ElapsedMilliseconds); } catch { }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Failed to import {File}", file);
                            LogFile.AppendLine(logPath, DateTime.Now.ToString("s") + " Failed to import " + file + ": " + ex.Message);
                            try { Observability.RecordFailure(Path.GetFileName(file), ex.Message); } catch { }
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
                    lock (gate) { importing = false; }
                }
            }

            // initial run
            ImportBatch();

            using var watcher = new FileSystemWatcher(_opts.ImportDir, "*.xml") { IncludeSubdirectories = false, EnableRaisingEvents = true };
            System.Threading.Timer? debounceTimer = null;
            void ScheduleImport()
            {
                try
                {
                    if (debounceTimer is null)
                    {
                        debounceTimer = new System.Threading.Timer(_ => { try { ImportBatch(); } catch { } }, null, System.Threading.Timeout.Infinite, System.Threading.Timeout.Infinite);
                    }
                    debounceTimer.Change(_opts.DebounceMs, System.Threading.Timeout.Infinite);
                }
                catch { }
            }
            FileSystemEventHandler onCreated = (s, e) => { ScheduleImport(); };
            RenamedEventHandler onRenamed = (s, e) => { ScheduleImport(); };
            watcher.Created += onCreated;
            watcher.Renamed += onRenamed;

            // Keep alive
            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                }
            }
            finally
            {
                watcher.Created -= onCreated;
                watcher.Renamed -= onRenamed;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Service initialization failed");
            throw;
        }
    }
}
