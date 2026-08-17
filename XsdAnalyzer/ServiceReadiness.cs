using System.Text.Json;

namespace XsdAnalyzer;

internal static class ServiceReadiness
{
    private const string FileName = "service.ready.json";

    public static string GetPath(string outputDirectory) => Path.Combine(outputDirectory, FileName);

    public static void Signal(string outputDirectory, int tableCount)
    {
        var path = GetPath(outputDirectory);
        var temporaryPath = path + ".tmp." + Environment.ProcessId;
        var state = new
        {
            ProcessId = Environment.ProcessId,
            ReadyAtUtc = DateTimeOffset.UtcNow,
            TableCount = tableCount
        };

        File.WriteAllText(temporaryPath, JsonSerializer.Serialize(state));
        File.Move(temporaryPath, path, overwrite: true);
    }

    public static void Clear(string outputDirectory)
    {
        var path = GetPath(outputDirectory);
        if (File.Exists(path)) File.Delete(path);
    }
}