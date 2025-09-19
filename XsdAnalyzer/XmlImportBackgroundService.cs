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
            // Track remote copy in progress separately
            bool remoteCopying = false;
            object remoteGate = new object();
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

            // Remote source polling: copy new XMLs from a UNC path (if configured)
            HashSet<string>? remoteSeen = null;
            string? remoteHistoryPath = null;
            if (!string.IsNullOrWhiteSpace(_opts.RemoteSourceDir))
            {
                try
                {
                    Directory.CreateDirectory(_opts.ImportDir);
                    remoteHistoryPath = _opts.RemoteHistoryFile ?? Path.Combine(_opts.OutDir, "remote_copied_files.txt");
                    remoteSeen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    if (File.Exists(remoteHistoryPath))
                    {
                        foreach (var line in File.ReadAllLines(remoteHistoryPath))
                        {
                            var trimmed = line.Trim();
                            if (trimmed.Length > 0) remoteSeen.Add(trimmed);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed initializing remote source tracking");
                }
            }

            void PollRemote()
            {
                if (string.IsNullOrWhiteSpace(_opts.RemoteSourceDir) || remoteSeen is null) return;
                lock (remoteGate)
                {
                    if (remoteCopying) return; // prevent overlapping polls
                    remoteCopying = true;
                }
                try
                {
                    string srcDir = _opts.RemoteSourceDir!;
                    if (!Directory.Exists(srcDir)) return;
                    var remoteFiles = Directory.EnumerateFiles(srcDir, "*.xml", SearchOption.TopDirectoryOnly).OrderBy(f => f, StringComparer.OrdinalIgnoreCase).ToList();
                    foreach (var rf in remoteFiles)
                    {
                        if (remoteSeen.Contains(Path.GetFileName(rf))) continue;
                        try
                        {
                            // Basic daily guarantee: remote is expected to drop 1 file ~08:00; still copy robustly any missing.
                            var dest = Path.Combine(_opts.ImportDir, Path.GetFileName(rf));
                            // Avoid overwriting an in-progress import; if name collision, append timestamp
                            if (File.Exists(dest))
                            {
                                var alt = Path.Combine(_opts.ImportDir, Path.GetFileNameWithoutExtension(dest) + "_" + DateTime.Now.ToString("yyyyMMdd_HHmmss") + Path.GetExtension(dest));
                                dest = alt;
                            }
                            File.Copy(rf, dest, overwrite: false);
                            remoteSeen.Add(Path.GetFileName(rf));
                            try
                            {
                                if (!string.IsNullOrWhiteSpace(remoteHistoryPath))
                                    File.AppendAllText(remoteHistoryPath, Path.GetFileName(rf) + Environment.NewLine);
                            }
                            catch { }
                            _logger.LogInformation("Copied remote XML {RemoteFile} -> {Local}", rf, dest);
                            try { Observability.RecordRemoteCopy(rf, dest); } catch { }
                            // Schedule import after copy (debounced watcher will also catch create, but we call directly for immediacy)
                            ImportBatch();
                        }
                        catch (Exception copyEx)
                        {
                            _logger.LogError(copyEx, "Failed copying remote file {RemoteFile}", rf);
                        }
                    }
                }
                finally
                {
                    lock (remoteGate) { remoteCopying = false; }
                }
            }

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
                var remotePoll = Math.Max(30, _opts.RemotePollSeconds); // enforce sane minimum
                var lastRemote = DateTime.MinValue;
                while (!stoppingToken.IsCancellationRequested)
                {
                    if (!string.IsNullOrWhiteSpace(_opts.RemoteSourceDir) && (DateTime.UtcNow - lastRemote).TotalSeconds >= remotePoll)
                    {
                        lastRemote = DateTime.UtcNow;
                        try { PollRemote(); } catch { }
                    }
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
