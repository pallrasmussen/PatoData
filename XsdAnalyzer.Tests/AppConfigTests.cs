using System.IO;
using System.Text.Json;
using Xunit;

namespace XsdAnalyzer.Tests;

public sealed class AppConfigTests
{
    [Fact]
    public void Load_InvalidJson_ThrowsJsonException()
    {
        var path = Path.GetTempFileName();
        try
        {
            File.WriteAllText(path, "{ invalid json");

            Assert.Throws<JsonException>(() =>
            {
                AppConfig.Load(path);
            });
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void ValidateForService_MissingRequiredSettings_ReturnsErrors()
    {
        var config = new AppConfig();

        var errors = config.ValidateForService();

        Assert.Contains("Missing required setting 'Xsd'.", errors);
        Assert.Contains("Missing required setting 'OutDir'.", errors);
        Assert.Contains("Missing required setting 'ImportDir'.", errors);
        Assert.Contains("Missing required setting 'Connection'.", errors);
        Assert.Contains("Missing required setting 'ServiceName'.", errors);
    }
}