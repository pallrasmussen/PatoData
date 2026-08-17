using System.IO;
using Xunit;

namespace XsdAnalyzer.Tests;

public sealed class RemoteFileLedgerTests
{
    [Fact]
    public void RecordCopiedAndMarkLocal_PersistStructuredStatus()
    {
        var directory = Path.Combine(Path.GetTempPath(), "PatoData-ledger-" + Path.GetRandomFileName());
        Directory.CreateDirectory(directory);
        try
        {
            var source = Path.Combine(directory, "source.xml");
            var local = Path.Combine(directory, "local.xml");
            var ledgerPath = Path.Combine(directory, "ledger.json");
            File.WriteAllText(source, "<root />");

            var ledger = RemoteFileLedger.Load(ledgerPath);
            ledger.RecordCopied(source, local);
            ledger.MarkLocal(local, RemoteFileStatus.Imported);

            var reloaded = RemoteFileLedger.Load(ledgerPath);
            Assert.True(reloaded.ContainsCurrent(source));
            Assert.Contains("\"Status\": 1", File.ReadAllText(ledgerPath));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public void Load_LegacyLineHistory_MigratesToJson()
    {
        var directory = Path.Combine(Path.GetTempPath(), "PatoData-ledger-" + Path.GetRandomFileName());
        Directory.CreateDirectory(directory);
        try
        {
            var source = Path.Combine(directory, "existing.xml");
            var ledgerPath = Path.Combine(directory, "history.txt");
            File.WriteAllText(source, "<root />");
            File.WriteAllText(ledgerPath, "existing.xml" + System.Environment.NewLine);

            var ledger = RemoteFileLedger.Load(ledgerPath);

            Assert.True(ledger.ContainsCurrent(source));
            Assert.StartsWith("[", File.ReadAllText(ledgerPath).TrimStart());

            File.WriteAllText(source, "<root changed='true' />");
            Assert.False(ledger.ContainsCurrent(source));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}