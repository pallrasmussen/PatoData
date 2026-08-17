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
    // Remote polling (UNC) configuration
    public string? RemoteSourceDir { get; set; }
    public int? RemotePollSeconds { get; set; }
    public string? RemoteHistoryFile { get; set; }

    public static AppConfig Load(string path)
    {
        var text = File.ReadAllText(path);
        return JsonSerializer.Deserialize<AppConfig>(text)
            ?? throw new JsonException($"Configuration file '{path}' is empty or contains JSON null.");
    }

    public IReadOnlyList<string> ValidateForService()
    {
        var errors = new List<string>();
        AddRequiredError(errors, nameof(Xsd), Xsd);
        AddRequiredError(errors, nameof(OutDir), OutDir);
        AddRequiredError(errors, nameof(ImportDir), ImportDir);
        AddRequiredError(errors, nameof(Connection), Connection);
        AddRequiredError(errors, nameof(ServiceName), ServiceName);
        return errors;
    }

    private static void AddRequiredError(List<string> errors, string name, string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            errors.Add($"Missing required setting '{name}'.");
        }
    }
}
