namespace XsdAnalyzer;

internal sealed class ImportServiceOptions
{
    public string Xsd { get; init; } = string.Empty;
    public string OutDir { get; init; } = string.Empty;
    public string Schema { get; init; } = "dbo";
    public string ImportDir { get; init; } = string.Empty;
    public string Connection { get; init; } = string.Empty;
    public bool VerboseImport { get; init; }
    public bool Audit { get; init; }
    public int DebounceMs { get; init; } = 200;
    public int ReadyWaitMs { get; init; } = 2000;
    public bool IdempotencyEnabled { get; init; } = true;
}
