using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;
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
            ServiceReadiness.Clear(_opts.OutDir);
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
                var diag = $"[service] Startup: XSD={_opts.Xsd}; ImportDir={_opts.ImportDir}; OutDir={_opts.OutDir}; Schema={_opts.Schema}; Tables={gen.TableCount}; Connection={RedactConnectionString(_opts.Connection)}";
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

            using var importGate = new SemaphoreSlim(1, 1);
            RemoteFileLedger? remoteLedger = null;
            string? remoteHistoryPath = null;
            if (!string.IsNullOrWhiteSpace(_opts.RemoteSourceDir))
            {
                var defaultLedgerPath = Path.Combine(_opts.OutDir, "remote_copied_files.json");
                var legacyHistoryPath = Path.Combine(_opts.OutDir, "remote_copied_files.txt");
                remoteHistoryPath = _opts.RemoteHistoryFile
                    ?? (File.Exists(defaultLedgerPath) || !File.Exists(legacyHistoryPath) ? defaultLedgerPath : legacyHistoryPath);
                remoteLedger = RemoteFileLedger.Load(remoteHistoryPath);
            }
            // Track remote copy in progress separately
            using var remoteGate = new SemaphoreSlim(1, 1);
            async Task ImportBatchAsync()
            {
                if (!await importGate.WaitAsync(0, stoppingToken)) return;
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
                            var result = await importer.ImportFileAsync(file, stoppingToken);
                            remoteLedger?.MarkLocal(file, RemoteFileStatus.Imported);
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
                            remoteLedger?.MarkLocal(file, RemoteFileStatus.Failed);
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
                    importGate.Release();
                }
            }

            async Task RunScheduledImportAsync()
            {
                try
                {
                    await ImportBatchAsync();
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Scheduled import failed");
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
                        debounceTimer = new System.Threading.Timer(_ => { _ = RunScheduledImportAsync(); }, null, System.Threading.Timeout.Infinite, System.Threading.Timeout.Infinite);
                    }
                    debounceTimer.Change(_opts.DebounceMs, System.Threading.Timeout.Infinite);
                }
                catch { }
            }
            FileSystemEventHandler onCreated = (s, e) => { ScheduleImport(); };
            RenamedEventHandler onRenamed = (s, e) => { ScheduleImport(); };
            watcher.Created += onCreated;
            watcher.Renamed += onRenamed;
            ServiceReadiness.Signal(_opts.OutDir, gen.TableCount);

            // initial run
            await ImportBatchAsync();

            // Remote source polling: copy new XMLs from a UNC path (if configured)
            if (string.IsNullOrWhiteSpace(_opts.RemoteSourceDir))
            {
                // Explicit disabled log for clarity
                try
                {
                    var disabledMsg = "[remote] Disabled (no RemoteSourceDir configured)";
                    _logger.LogInformation(disabledMsg);
                    LogFile.AppendLine(logPath, DateTime.Now.ToString("s") + " " + disabledMsg);
                }
                catch { }
            }
            else
            {
                try
                {
                    var ledger = remoteLedger ?? throw new InvalidOperationException("Remote ledger was not initialized.");
                    var currentUser = System.Security.Principal.WindowsIdentity.GetCurrent()?.Name ?? Environment.UserName;
                    Directory.CreateDirectory(_opts.ImportDir);
                    // Seed with any local XMLs (import queue, imported, error) to avoid duplicate copy/import if history missing
                    try
                    {
                        var existingFiles = new[] { _opts.ImportDir, importedDir, errorDir }
                            .Where(Directory.Exists)
                            .SelectMany(directory => Directory.EnumerateFiles(directory, "*.xml", SearchOption.TopDirectoryOnly));
                        var seeded = ledger.SeedExistingFiles(existingFiles);
                        if (seeded > 0)
                        {
                            var seedMsg = $"[remote] Seeded history with {seeded} existing local file(s)";
                            _logger.LogInformation(seedMsg);
                            LogFile.AppendLine(logPath, DateTime.Now.ToString("s") + " " + seedMsg);
                        }
                    }
                    catch { }

                    // Status banner
                    try
                    {
                        var poll = Math.Max(30, _opts.RemotePollSeconds);
                        if (Directory.Exists(_opts.RemoteSourceDir))
                        {
                            var status = $"[remote] Status: watching {_opts.RemoteSourceDir}; poll {poll}s; history={remoteHistoryPath}; user={currentUser}";
                            _logger.LogInformation(status);
                            LogFile.AppendLine(logPath, DateTime.Now.ToString("s") + " " + status);
                        }
                        else
                        {
                            var status = $"[remote] Status: directory not found {_opts.RemoteSourceDir}; will retry; user={currentUser}";
                            _logger.LogInformation(status);
                            LogFile.AppendLine(logPath, DateTime.Now.ToString("s") + " " + status);
                        }
                    }
                    catch { }

                    // Backlog copy (only if directory exists)
                    if (Directory.Exists(_opts.RemoteSourceDir))
                    {
                        var backlog = Directory.EnumerateFiles(_opts.RemoteSourceDir, "*.xml", SearchOption.TopDirectoryOnly)
                            .OrderBy(f => f, StringComparer.OrdinalIgnoreCase).ToList();
                        int copied = 0;
                        foreach (var rf in backlog)
                        {
                            var name = Path.GetFileName(rf);
                            if (ledger.ContainsCurrent(rf)) continue;
                            try
                            {
                                var dest = Path.Combine(_opts.ImportDir, name);
                                if (File.Exists(dest))
                                {
                                    dest = Path.Combine(_opts.ImportDir, Path.GetFileNameWithoutExtension(dest) + "_" + DateTime.Now.ToString("yyyyMMdd_HHmmss") + Path.GetExtension(dest));
                                }
                                File.Copy(rf, dest, overwrite: false);
                                ledger.RecordCopied(rf, dest);
                                var msg = $"[remote] Backlog copied {rf} -> {dest}";
                                _logger.LogInformation(msg);
                                LogFile.AppendLine(logPath, DateTime.Now.ToString("s") + " " + msg);
                                try { Observability.RecordRemoteCopy(rf, dest); } catch { }
                                copied++;
                            }
                            catch (Exception bEx)
                            {
                                _logger.LogWarning(bEx, "[remote] Failed backlog copy {RemoteFile}", rf);
                            }
                        }
                        var summary = copied > 0 ? $"[remote] Backlog copied {copied} file(s)" : "[remote] No backlog files to copy (0 new)";
                        _logger.LogInformation(summary);
                        LogFile.AppendLine(logPath, DateTime.Now.ToString("s") + " " + summary);
                        if (copied > 0) await ImportBatchAsync();
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed initializing remote source tracking");
                }
            }

            async Task PollRemoteAsync()
            {
                if (string.IsNullOrWhiteSpace(_opts.RemoteSourceDir) || remoteLedger is null) return;
                if (!await remoteGate.WaitAsync(0, stoppingToken)) return;
                try
                {
                    string srcDir = _opts.RemoteSourceDir!;
                    if (!Directory.Exists(srcDir)) return;
                    var remoteFiles = Directory.EnumerateFiles(srcDir, "*.xml", SearchOption.TopDirectoryOnly).OrderBy(f => f, StringComparer.OrdinalIgnoreCase).ToList();
                    foreach (var rf in remoteFiles)
                    {
                        if (remoteLedger.ContainsCurrent(rf)) continue;
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
                            remoteLedger.RecordCopied(rf, dest);
                            _logger.LogInformation("Copied remote XML {RemoteFile} -> {Local}", rf, dest);
                            try { Observability.RecordRemoteCopy(rf, dest); } catch { }
                            // Schedule import after copy (debounced watcher will also catch create, but we call directly for immediacy)
                            await ImportBatchAsync();
                        }
                        catch (Exception copyEx)
                        {
                            _logger.LogError(copyEx, "Failed copying remote file {RemoteFile}", rf);
                        }
                    }
                }
                finally
                {
                    remoteGate.Release();
                }
            }

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
                        try { await PollRemoteAsync(); } catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
                    }
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                }
            }
            finally
            {
                watcher.Created -= onCreated;
                watcher.Renamed -= onRenamed;
                debounceTimer?.Dispose();
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Service stopped by request");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Service initialization failed");
            throw;
        }
        finally
        {
            try { ServiceReadiness.Clear(_opts.OutDir); } catch { }
        }
    }

    internal static string RedactConnectionString(string? connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString)) return string.Empty;

        try
        {
            var builder = new SqlConnectionStringBuilder(connectionString);
            if (!string.IsNullOrEmpty(builder.Password)) builder.Password = "***";
            return builder.ConnectionString;
        }
        catch (ArgumentException)
        {
            return "[invalid connection string]";
        }
    }
}
