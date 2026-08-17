using Xunit;

namespace XsdAnalyzer.Tests;

public sealed class StartupDiagnosticsTests
{
    [Theory]
    [InlineData("--connection", "Server=.;User ID=user;Password=secret")]
    [InlineData("--connection=Server=.;User ID=user;Password=secret", null)]
    public void FormatArguments_RedactsConnectionString(string option, string? value)
    {
        var arguments = value is null ? new[] { option } : new[] { option, value };

        var formatted = StartupDiagnostics.FormatArguments(arguments);

        Assert.DoesNotContain("secret", formatted);
        Assert.Contains("***", formatted);
    }
}