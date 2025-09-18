using System.Text.Json;

namespace XsdAnalyzer;

internal sealed class AppConfig
{
    public string? Xsd { get; set; }
    public string? OutDir { get; set; }
    public string? Schema { get; set; }
    public string? ImportDir { get; set; }
    public string? Connection { get; set; }
    public bool? Watch { get; set; }
    public bool? VerboseImport { get; set; }
    public bool? Audit { get; set; }
    public int? DebounceMs { get; set; }
    public int? ReadyWaitMs { get; set; }
    public bool? IdempotencyEnabled { get; set; }
    public string? ServiceName { get; set; }

    public static AppConfig? Load(string path)
    {
        try
        {
            var text = File.ReadAllText(path);
            var cfg = JsonSerializer.Deserialize<AppConfig>(text);
            return cfg;
        }
        catch { return null; }
    }
}
