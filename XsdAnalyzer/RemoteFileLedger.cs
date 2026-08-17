using System.Text.Json;

namespace XsdAnalyzer;

internal sealed class RemoteFileLedger
{
    private readonly string _path;
    private readonly object _gate = new();
    private readonly List<RemoteFileEntry> _entries;

    private RemoteFileLedger(string path, List<RemoteFileEntry> entries)
    {
        _path = path;
        _entries = entries;
    }

    public static RemoteFileLedger Load(string path)
    {
        if (!File.Exists(path)) return new RemoteFileLedger(path, new List<RemoteFileEntry>());

        var content = File.ReadAllText(path);
        try
        {
            var entries = JsonSerializer.Deserialize<List<RemoteFileEntry>>(content);
            if (entries is not null) return new RemoteFileLedger(path, entries);
        }
        catch (JsonException)
        {
        }

        var migrated = content.Split(new[] { "\r\n", "\n" }, StringSplitOptions.RemoveEmptyEntries)
            .Select(name => new RemoteFileEntry
            {
                FileName = name.Trim(),
                Status = RemoteFileStatus.Imported,
                UpdatedAtUtc = DateTimeOffset.UtcNow
            })
            .Where(entry => entry.FileName.Length > 0)
            .ToList();
        var ledger = new RemoteFileLedger(path, migrated);
        ledger.Save();
        return ledger;
    }

    public bool ContainsCurrent(string sourcePath)
    {
        var source = new FileInfo(sourcePath);
        lock (_gate)
        {
            var exactEntry = _entries.LastOrDefault(entry =>
                string.Equals(entry.SourcePath, source.FullName, StringComparison.OrdinalIgnoreCase));
            if (exactEntry is not null)
            {
                return exactEntry.Length == source.Length
                    && exactEntry.LastWriteTimeUtc == source.LastWriteTimeUtc
                    && exactEntry.Status is RemoteFileStatus.Copied or RemoteFileStatus.Imported or RemoteFileStatus.Failed;
            }

            var legacyEntry = _entries.LastOrDefault(entry =>
                string.IsNullOrWhiteSpace(entry.SourcePath)
                && string.Equals(entry.FileName, source.Name, StringComparison.OrdinalIgnoreCase));
            if (legacyEntry is null) return false;

            legacyEntry.SourcePath = source.FullName;
            legacyEntry.Length = source.Length;
            legacyEntry.LastWriteTimeUtc = source.LastWriteTimeUtc;
            legacyEntry.UpdatedAtUtc = DateTimeOffset.UtcNow;
            SaveLocked();
            return true;
        }
    }

    public void RecordCopied(string sourcePath, string localPath)
    {
        var source = new FileInfo(sourcePath);
        lock (_gate)
        {
            _entries.RemoveAll(entry => string.Equals(entry.SourcePath, source.FullName, StringComparison.OrdinalIgnoreCase));
            _entries.Add(new RemoteFileEntry
            {
                SourcePath = source.FullName,
                FileName = source.Name,
                Length = source.Length,
                LastWriteTimeUtc = source.LastWriteTimeUtc,
                LocalPath = Path.GetFullPath(localPath),
                Status = RemoteFileStatus.Copied,
                UpdatedAtUtc = DateTimeOffset.UtcNow
            });
            SaveLocked();
        }
    }

    public void MarkLocal(string localPath, RemoteFileStatus status)
    {
        var fullPath = Path.GetFullPath(localPath);
        lock (_gate)
        {
            var entry = _entries.LastOrDefault(candidate =>
                string.Equals(candidate.LocalPath, fullPath, StringComparison.OrdinalIgnoreCase));
            if (entry is null) return;
            entry.Status = status;
            entry.UpdatedAtUtc = DateTimeOffset.UtcNow;
            SaveLocked();
        }
    }

    public int SeedExistingFiles(IEnumerable<string> paths)
    {
        var added = 0;
        lock (_gate)
        {
            foreach (var path in paths)
            {
                var fileName = Path.GetFileName(path);
                if (_entries.Any(entry => string.Equals(entry.FileName, fileName, StringComparison.OrdinalIgnoreCase))) continue;
                _entries.Add(new RemoteFileEntry
                {
                    FileName = fileName,
                    LocalPath = Path.GetFullPath(path),
                    Status = RemoteFileStatus.Imported,
                    UpdatedAtUtc = DateTimeOffset.UtcNow
                });
                added++;
            }
            if (added > 0) SaveLocked();
        }
        return added;
    }

    private void Save()
    {
        lock (_gate) SaveLocked();
    }

    private void SaveLocked()
    {
        var directory = Path.GetDirectoryName(_path);
        if (!string.IsNullOrWhiteSpace(directory)) Directory.CreateDirectory(directory);
        var temporaryPath = _path + ".tmp." + Environment.ProcessId;
        File.WriteAllText(temporaryPath, JsonSerializer.Serialize(_entries, new JsonSerializerOptions { WriteIndented = true }));
        File.Move(temporaryPath, _path, overwrite: true);
    }
}

internal sealed class RemoteFileEntry
{
    public string SourcePath { get; set; } = string.Empty;
    public string FileName { get; set; } = string.Empty;
    public long Length { get; set; }
    public DateTime LastWriteTimeUtc { get; set; }
    public string LocalPath { get; set; } = string.Empty;
    public RemoteFileStatus Status { get; set; }
    public DateTimeOffset UpdatedAtUtc { get; set; }
}

internal enum RemoteFileStatus
{
    Copied,
    Imported,
    Failed
}