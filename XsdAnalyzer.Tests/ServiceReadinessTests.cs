using System.IO;
using Xunit;

namespace XsdAnalyzer.Tests;

public sealed class ServiceReadinessTests
{
    [Fact]
    public void SignalAndClear_ManageAtomicReadinessMarker()
    {
        var directory = Path.Combine(Path.GetTempPath(), "PatoData-readiness-" + Path.GetRandomFileName());
        Directory.CreateDirectory(directory);
        try
        {
            ServiceReadiness.Signal(directory, 9);

            var path = ServiceReadiness.GetPath(directory);
            var content = File.ReadAllText(path);
            Assert.Contains("\"TableCount\":9", content);

            ServiceReadiness.Clear(directory);
            Assert.False(File.Exists(path));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}