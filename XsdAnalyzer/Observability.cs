using System.Text.Json;
using System.Text.Json.Serialization;

namespace XsdAnalyzer;

// Lightweight, file-based observability: emits JSONL events and a rolling stats JSON
internal static class Observability
{
    private static readonly object _gate = new();
    private static string? _eventsPath;
    private static string? _statsPath;

    private static readonly JsonSerializerOptions _jsonOpts = new()
    {
        WriteIndented = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public static void Configure(string outDir)
    {
        try
        {
            Directory.CreateDirectory(outDir);
            _eventsPath = Path.Combine(outDir, "observability.jsonl");
            _statsPath = Path.Combine(outDir, "observability.stats.json");
        }
        catch { /* ignore */ }
    }

    public static void RecordSuccess(string file, int totalRows, Dictionary<string,int> byTable, int durationMs)
    {
        var ev = new
        {
            ts = DateTime.UtcNow.ToString("o"),
            type = "file-success",
            file,
            totalRows,
            durationMs,
            byTable
        };
        AppendEvent(ev);
        UpdateStats(success: true, totalRows, byTable);
    }

    public static void RecordFailure(string file, string error)
    {
        var ev = new
        {
            ts = DateTime.UtcNow.ToString("o"),
            type = "file-failure",
            file,
            error
        };
        AppendEvent(ev);
        UpdateStats(success: false, 0, null);
    }

    private static void AppendEvent(object payload)
    {
        try
        {
            var json = JsonSerializer.Serialize(payload, _jsonOpts);
            var path = _eventsPath;
            if (!string.IsNullOrEmpty(path))
            {
                // Reuse existing rotating log writer for robust append
                LogFile.AppendLine(path!, json);
            }
        }
        catch { /* ignore */ }
    }

    private sealed class Stats
    {
        public int TotalFiles { get; set; }
        public int SuccessFiles { get; set; }
        public int FailedFiles { get; set; }
        public long TotalRows { get; set; }
        public Dictionary<string,long> PerTable { get; set; } = new(StringComparer.OrdinalIgnoreCase);
        public string? LastSuccessAt { get; set; }
        public string? LastFailureAt { get; set; }
    }

    private static void UpdateStats(bool success, int totalRows, Dictionary<string,int>? byTable)
    {
        try
        {
            lock (_gate)
            {
                var path = _statsPath;
                if (string.IsNullOrEmpty(path)) return;
                Stats s;
                try
                {
                    if (File.Exists(path!))
                    {
                        var text = File.ReadAllText(path!);
                        s = JsonSerializer.Deserialize<Stats>(text) ?? new Stats();
                    }
                    else s = new Stats();
                }
                catch { s = new Stats(); }

                s.TotalFiles++;
                if (success) { s.SuccessFiles++; s.LastSuccessAt = DateTime.UtcNow.ToString("o"); s.TotalRows += totalRows; }
                else { s.FailedFiles++; s.LastFailureAt = DateTime.UtcNow.ToString("o"); }

                if (success && byTable is not null)
                {
                    foreach (var kv in byTable)
                    {
                        if (!s.PerTable.TryGetValue(kv.Key, out var cur)) cur = 0;
                        s.PerTable[kv.Key] = cur + kv.Value;
                    }
                }

                try
                {
                    var json = JsonSerializer.Serialize(s, new JsonSerializerOptions { WriteIndented = true });
                    File.WriteAllText(path!, json);
                }
                catch { /* ignore */ }
            }
        }
        catch { /* ignore */ }
    }
}
