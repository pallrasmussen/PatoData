using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Xml;
using System.Xml.Linq;
using System.Xml.Schema;
using Microsoft.Data.SqlClient;
using Xunit;

namespace XsdAnalyzer.Tests;

public class ImportTinyXmlTests
{
    private static XmlSchemaSet LoadSchema(string path)
    {
        var set = new XmlSchemaSet();
        using var reader = XmlReader.Create(path);
        set.Add(null, reader);
        set.Compile();
        return set;
    }

    [Fact]
    public void GenerateSql_ContainsExpectedTables()
    {
        var xsd = Path.Combine(AppContext.BaseDirectory, "MiniSchema.xsd");
        var set = LoadSchema(xsd);
        var gen = new XsdAnalyzer.XsdToSqlServer("xsd");
        var sql = gen.Generate(set);
        Assert.Contains("CREATE TABLE [xsd].[Root]", sql);
    // Child table naming now PascalCases parent+child (Root + Item => RootItem)
    Assert.Contains("CREATE TABLE [xsd].[RootItem]", sql);
        // Unique constraint from xs:ID
        Assert.Contains("UNIQUE ([Id])", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Import_IntoTemporaryDatabase_Succeeds()
    {
        // Try to connect to LocalDB; if not available, skip gracefully
        var hasLocalDb = TryGetLocalDbConnection(out var connStr);
        if (!hasLocalDb)
        {
            return; // skip test in environments without LocalDB
        }

        var xsd = Path.Combine(AppContext.BaseDirectory, "MiniSchema.xsd");
        var set = LoadSchema(xsd);
        var gen = new XsdAnalyzer.XsdToSqlServer("xsd");
        var ddl = gen.Generate(set);

        var dbName = "PatoData_Temp_" + Guid.NewGuid().ToString("N");
        using (var master = new SqlConnection(connStr))
        {
            master.Open();
            using (var cmd = new SqlCommand($"CREATE DATABASE [{dbName}];", master)) cmd.ExecuteNonQuery();
        }
        try
        {
            var dbConnStr = connStr + $";Initial Catalog={dbName}";
            using var conn = new SqlConnection(dbConnStr);
            conn.Open();
            using (var cmd = new SqlCommand(ddl, conn)) cmd.ExecuteNonQuery();

            // Prepare tiny XML
            var xml = new XDocument(
                new XElement("Root",
                    new XElement("Item",
                        new XAttribute("id", "i1"),
                        new XElement("Code", "A"),
                        new XElement("Amount", "1.23")
                    ),
                    new XElement("Item",
                        new XAttribute("id", "i2"),
                        new XElement("Code", "B"),
                        new XElement("Amount", "2.00")
                    )
                )
            );

            // Import
            var importer = new XsdAnalyzer.XmlToSqlImporter(gen, dbConnStr, verbose: false);
            // Ensure model built before import
            gen.EnsureModel(set);
            var tmp = Path.Combine(Path.GetTempPath(), "tiny.xml");
            xml.Save(tmp);
            var result = importer.ImportFile(tmp);

            Assert.True(result.Total >= 1);

            // Verify counts for child table
            using var countCmd = new SqlCommand("SELECT COUNT(*) FROM [xsd].[RootItem];", conn);
            var cnt = (int)(countCmd.ExecuteScalar() ?? 0);
            Assert.Equal(2, cnt);
        }
        finally
        {
            try
            {
                using var master2 = new SqlConnection(connStr);
                master2.Open();
                using var drop = new SqlCommand($"ALTER DATABASE [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{dbName}];", master2);
                drop.ExecuteNonQuery();
            }
            catch { }
        }
    }

    [Fact]
    public void Import_SameXmlTwice_IsIdempotent()
    {
        var hasLocalDb = TryGetLocalDbConnection(out var connStr);
        if (!hasLocalDb)
        {
            return; // skip if LocalDB not present
        }

        var xsd = Path.Combine(AppContext.BaseDirectory, "MiniSchema.xsd");
        var set = LoadSchema(xsd);
        var gen = new XsdAnalyzer.XsdToSqlServer("xsd");
        var ddl = gen.Generate(set);

        var dbName = "PatoData_Temp_" + Guid.NewGuid().ToString("N");
        using (var master = new SqlConnection(connStr))
        {
            master.Open();
            using var cmd = new SqlCommand($"CREATE DATABASE [{dbName}];", master);
            cmd.ExecuteNonQuery();
        }
        try
        {
            var dbConnStr = connStr + $";Initial Catalog={dbName}";
            using var conn = new SqlConnection(dbConnStr);
            conn.Open();
            using (var cmd = new SqlCommand(ddl, conn)) cmd.ExecuteNonQuery();

            var xml = new XDocument(
                new XElement("Root",
                    new XElement("Item",
                        new XAttribute("id", "i1"),
                        new XElement("Code", "A"),
                        new XElement("Amount", "1.23")
                    ),
                    new XElement("Item",
                        new XAttribute("id", "i2"),
                        new XElement("Code", "B"),
                        new XElement("Amount", "2.00")
                    )
                )
            );

            var importer = new XsdAnalyzer.XmlToSqlImporter(gen, dbConnStr, verbose: false);
            gen.EnsureModel(set);
            var tmp = Path.Combine(Path.GetTempPath(), "tiny_idem.xml");
            xml.Save(tmp);

            var first = importer.ImportFile(tmp);
            Assert.True(first.Total >= 1);

            // Second import should find duplicates and insert 0 new rows
            var second = importer.ImportFile(tmp);
            Assert.Equal(0, second.Total);

            using var countCmd = new SqlCommand("SELECT COUNT(*) FROM [xsd].[RootItem];", conn);
            var cnt = (int)(countCmd.ExecuteScalar() ?? 0);
            Assert.Equal(2, cnt); // still 2 rows only
        }
        finally
        {
            try
            {
                using var master2 = new SqlConnection(connStr);
                master2.Open();
                using var drop = new SqlCommand($"ALTER DATABASE [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{dbName}];", master2);
                drop.ExecuteNonQuery();
            }
            catch { }
        }
    }

    private static bool TryGetLocalDbConnection(out string connectionString)
    {
        // Attempt to use (localdb)\\MSSQLLocalDB
        var cs = new SqlConnectionStringBuilder
        {
            DataSource = @"(localdb)\\MSSQLLocalDB",
            IntegratedSecurity = true,
            TrustServerCertificate = true,
            InitialCatalog = "master"
        };
        try
        {
            using var c = new SqlConnection(cs.ConnectionString);
            c.Open();
            connectionString = cs.ConnectionString;
            return true;
        }
        catch
        {
            connectionString = string.Empty;
            return false;
        }
    }
}
