using System;
using Xunit;

namespace XsdAnalyzer.Tests;

public sealed class XmlImportBackgroundServiceTests
{
    [Fact]
    public void RedactConnectionString_PasswordAlias_DoesNotExposeSecret()
    {
        const string secret = "do-not-log-this";

        var redacted = XmlImportBackgroundService.RedactConnectionString(
            $"Server=.;Database=PatoData;User ID=importer;Pwd={secret}");

        Assert.DoesNotContain(secret, redacted, StringComparison.Ordinal);
        Assert.Contains("Password=***", redacted, StringComparison.OrdinalIgnoreCase);
    }
}