using System.IO;
using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace XsdAnalyzer.Tests;

public sealed class XmlImporterCancellationTests
{
    [Fact]
    public async Task ImportFileAsync_PreCanceled_ThrowsBeforeDatabaseAccess()
    {
        var importer = new XmlToSqlImporter(new XsdToSqlServer("xsd"), "Server=invalid;");
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            importer.ImportFileAsync(Path.Combine(Path.GetTempPath(), "not-read.xml"), cancellation.Token));
    }
}