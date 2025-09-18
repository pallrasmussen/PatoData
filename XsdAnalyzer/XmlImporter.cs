using Microsoft.Data.SqlClient;
using System.Xml.Linq;

namespace XsdAnalyzer;

internal sealed class XmlToSqlImporter
{
    private readonly XsdToSqlServer _gen;
    private readonly string _connectionString;
    private readonly bool _verbose;
    private readonly Action<string>? _log;
    private readonly bool _auditEnabled;
    private readonly Action<string>? _audit;
    private readonly bool _idempotencyEnabled;
    private string? _currentFile;
    private DateTime _fileStart;

    public XmlToSqlImporter(XsdToSqlServer gen, string connectionString, bool verbose = false, Action<string>? log = null, bool auditEnabled = false, Action<string>? audit = null, bool idempotencyEnabled = true)
    {
        _gen = gen;
        _connectionString = connectionString;
        _verbose = verbose;
        _log = log;
        _auditEnabled = auditEnabled;
        _audit = audit;
        _idempotencyEnabled = idempotencyEnabled;
    }

    public ImportResult ImportFile(string xmlPath)
    {
    // TODO: Add unit/integration tests covering XSD <choice> discriminator round-trip when a schema with choices is available.
    // Retry read briefly to avoid reading a file that's still being written by another process
    XDocument doc;
    int attempts = 0;
    while (true)
    {
        try
        {
            doc = XDocument.Load(xmlPath);
            break;
        }
        catch (IOException) when (attempts < 5)
        {
            System.Threading.Thread.Sleep(100);
            attempts++;
            continue;
        }
    }
    _currentFile = Path.GetFileName(xmlPath);
    _fileStart = DateTime.UtcNow;
    Log($"Importing file {xmlPath}");
        using var conn = new SqlConnection(_connectionString);
        conn.Open();
        using var tx = conn.BeginTransaction();
        var total = 0;
        var byTable = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        try
        {
            total += ImportElement(doc.Root!, parentTable: null, parentId: null, conn, tx, byTable);
            tx.Commit();
        }
        catch
        {
            tx.Rollback();
            throw;
        }
        finally
        {
            if (_auditEnabled)
            {
                // file-level summary audit row
                var fileMs = (int)(DateTime.UtcNow - _fileStart).TotalMilliseconds;
                var totals = string.Join("; ", byTable.OrderBy(k => k.Key, StringComparer.OrdinalIgnoreCase).Select(kv => kv.Key + "=" + kv.Value));
                Audit(eventType: "file-summary", element: string.Empty, table: string.Empty, newId: null, parentTable: string.Empty, parentId: null, fkColumn: fileMs.ToString(), reason: totals, paramPreview: total.ToString());
            }
            _currentFile = null;
        }
        return new ImportResult(total, byTable);
    }

    private int ImportElement(XElement elem, XsdToSqlServer.TableInfo? parentTable, int? parentId, SqlConnection conn, SqlTransaction tx, Dictionary<string,int> byTable)
    {
        var inserted = 0;
        // Determine target table for this element: prefer direct match; if missing, try Parent_Child naming
        var rawName = elem.Name.LocalName;
        var ns = elem.Name.NamespaceName;
        var directName = _gen.ResolveTableName(ns, rawName, parentTable?.Name);
        XsdToSqlServer.TableInfo? table = null;
        if (directName is not null)
        {
            table = _gen.GetTableInfo(directName);
        }
        // fallback remains below via FK check
        // If both exist, prefer the one that has the expected FK to the parent
        if (parentTable is not null && table is not null)
        {
            var expectedFk = parentTable.Name + "Id";
            if (!table.Columns.Any(c => c.Name.Equals(expectedFk, StringComparison.OrdinalIgnoreCase)))
            {
                var compound = ToPascal(SanitizeIdentifier(parentTable.Name + "_" + rawName));
                var ct = _gen.GetTableInfo(compound);
                if (ct is not null && ct.Columns.Any(c => c.Name.Equals(expectedFk, StringComparison.OrdinalIgnoreCase)))
                {
                    Log($"Resolved element '{rawName}' to table '{ct.Name}' (preferred due to FK to parent {parentTable.Name}).");
                    table = ct;
                }
            }
        }
        if (table is null)
        {
            Log($"Skip element '{rawName}': no matching table; recursing into children.");
            Audit("skip-no-table", rawName, null, null, parentTable?.Name, parentId, null, "No matching table; recurse children", null);
            foreach (var child in elem.Elements()) inserted += ImportElement(child, parentTable, parentId, conn, tx, byTable);
            return inserted;
        }

        // Build column -> value map
        var values = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        if (parentTable is not null)
        {
            var fkCol = parentTable.Name + "Id";
            if (table.Columns.Any(c => c.Name.Equals(fkCol, StringComparison.OrdinalIgnoreCase)))
                values[fkCol] = parentId;
        }
        foreach (var attr in elem.Attributes())
        {
            var col = GetColName(attr.Name.LocalName);
            if (table.Columns.Any(c => c.Name.Equals(col, StringComparison.OrdinalIgnoreCase)))
                values[col] = ToDbValue(attr.Value);
        }
        foreach (var child in elem.Elements())
        {
            if (!child.HasElements && !string.IsNullOrWhiteSpace(child.Value))
            {
                var col = GetColName(child.Name.LocalName);
                if (table.Columns.Any(c => c.Name.Equals(col, StringComparison.OrdinalIgnoreCase)))
                    values[col] = ToDbValue(child.Value);
            }
        }

        // If this element was generated from a parent XSD <choice>, the child table will have ChoiceParentOption.
        // Populate it with the chosen element name to preserve which branch was taken.
        if (table.Columns.Any(c => c.Name.Equals("ChoiceParentOption", StringComparison.OrdinalIgnoreCase)))
        {
            values["ChoiceParentOption"] = rawName;
        }

    int? newId = null;
    var opStart = DateTime.UtcNow;
        var nonIdentityCols = table.Columns.Where(c => !c.Identity).Select(c => c.Name).ToList();
        if (values.Count == 0 && parentId is null)
        {
            if (table.Columns.All(c => c.Identity || c.Nullable))
            {
                // insert default row
                using var cmd = new SqlCommand($"INSERT INTO [{table.Schema}].[{table.Name}] DEFAULT VALUES; SELECT CAST(SCOPE_IDENTITY() AS INT);", conn, tx);
                newId = (int?)cmd.ExecuteScalar();
                if (newId.HasValue)
                {
                    Log($"Inserted default row into {table.Schema}.{table.Name} to anchor children; id={newId.Value}");
                    Audit("default-row-insert", rawName, table.Name, newId, parentTable?.Name, parentId, null, null, null);
                    inserted++;
                    Increment(byTable, table.Name);
                }
            }
        }
        else
        {
            var insertCols = nonIdentityCols.Where(n => values.ContainsKey(n)).ToList();
            // Ensure discriminators are included if present in table and values
            foreach (var disc in new[] { "ChoiceOption", "ChoiceParentOption" })
            {
                if (table.Columns.Any(c => c.Name.Equals(disc, StringComparison.OrdinalIgnoreCase)) && !insertCols.Contains(disc) && values.ContainsKey(disc))
                {
                    insertCols.Add(disc);
                }
            }
            string? fkColName = null;
            if (parentTable is not null)
            {
                var fkCol = parentTable.Name + "Id";
                fkColName = fkCol;
                if (!insertCols.Contains(fkCol) && table.Columns.Any(c => c.Name.Equals(fkCol, StringComparison.OrdinalIgnoreCase)))
                {
                    insertCols.Insert(0, fkCol);
                    values[fkCol] = parentId;
                }
            }
            // Ensure required (NOT NULL, non-identity) columns are present, backfilling defaults when missing
            foreach (var req in table.Columns.Where(c => !c.Identity && !c.Nullable))
            {
                if (!insertCols.Contains(req.Name))
                {
                    insertCols.Add(req.Name);
                }
                if (!values.TryGetValue(req.Name, out var v) || v is null)
                {
                    // Do not default the parent FK to a synthetic value; require a real parentId
                    if (fkColName is not null && string.Equals(req.Name, fkColName, StringComparison.OrdinalIgnoreCase))
                    {
                        // leave as null; we'll skip insert below if parentId is null
                    }
                    else
                    {
                        var defVal = DefaultForSqlType(req.Type);
                        values[req.Name] = defVal;
                        Log($"Defaulting required column {table.Name}.{req.Name} to '{defVal}' (type {req.Type}).");
                    }
                }
            }
            bool fkRequiredAndMissingParent = fkColName is not null
                && table.Columns.Any(c => c.Name.Equals(fkColName, StringComparison.OrdinalIgnoreCase) && !c.Nullable)
                && parentId is null;
            if (!fkRequiredAndMissingParent)
            {
                int? existingId = null;
                if (_idempotencyEnabled)
                {
                    // Idempotency: check existing by unique constraints when we have all columns
                    var uniques = _gen.GetUniqueConstraints(table.Name);
                    foreach (var uq in uniques)
                    {
                        if (uq.Columns.All(c => insertCols.Contains(c)))
                        {
                            // Build NULL-safe equality: ([Col] = @u_Col OR ([Col] IS NULL AND @u_Col IS NULL))
                            var preds = new List<string>();
                            foreach (var c in uq.Columns)
                            {
                                preds.Add($"([{c}] = @u_{c} OR ([{c}] IS NULL AND @u_{c} IS NULL))");
                            }
                            var where = string.Join(" AND ", preds);
                            var sel = $"SELECT TOP(1) [{table.Name}Id] FROM [{table.Schema}].[{table.Name}] WHERE {where};";
                            using var selCmd = new SqlCommand(sel, conn, tx);
                            foreach (var c in uq.Columns)
                            {
                                selCmd.Parameters.AddWithValue("@u_" + c, values.TryGetValue(c, out var v) ? v ?? (object)DBNull.Value : DBNull.Value);
                            }
                            var scalar = selCmd.ExecuteScalar();
                            if (scalar is int idv) { existingId = idv; break; }
                            if (scalar is long l) { existingId = (int)l; break; }
                        }
                    }
                }

                if (existingId.HasValue)
                {
                    newId = existingId;
                    var durMsSel = (int)(DateTime.UtcNow - opStart).TotalMilliseconds;
                    Audit("skip", rawName, table.Name, newId, parentTable?.Name, parentId, fkColName, durMsSel.ToString(), "Idempotent: matched existing row by unique key");
                }
                else
                {
                    // Generic fallback duplicate detection (tables without unique constraints)
                    if (_idempotencyEnabled)
                    {
                        // Use the columns we plan to insert (excluding identity) to look for an identical existing row
                        var dupCols = insertCols.Where(c => !table.Columns.Any(tc => tc.Name.Equals(c, StringComparison.OrdinalIgnoreCase) && tc.Identity)).ToList();
                        if (dupCols.Count > 0)
                        {
                            var preds = new List<string>();
                            foreach (var c in dupCols)
                            {
                                preds.Add($"([{c}] = @d_{c} OR ([{c}] IS NULL AND @d_{c} IS NULL))");
                            }
                            var where = string.Join(" AND ", preds);
                            var pkCol = table.Name + "Id";
                            var selGeneric = $"SELECT TOP(1) [{pkCol}] FROM [{table.Schema}].[{table.Name}] WHERE {where};";
                            using (var selDup = new SqlCommand(selGeneric, conn, tx))
                            {
                                foreach (var c in dupCols)
                                {
                                    selDup.Parameters.AddWithValue("@d_" + c, values.TryGetValue(c, out var v) ? v ?? (object)DBNull.Value : DBNull.Value);
                                }
                                var scalarDup = selDup.ExecuteScalar();
                                if (scalarDup is int idv2) existingId = idv2; else if (scalarDup is long l2) existingId = (int)l2;
                            }
                            if (existingId.HasValue)
                            {
                                newId = existingId;
                                var durMsDup = (int)(DateTime.UtcNow - opStart).TotalMilliseconds;
                                Audit("skip", rawName, table.Name, newId, parentTable?.Name, parentId, fkColName, durMsDup.ToString(), "Idempotent: matched existing row by full column set");
                            }
                        }
                    }
                }
                if (!existingId.HasValue)
                {
                    var colList = string.Join(", ", insertCols.Select(n => $"[{n}]"));
                    var paramList = string.Join(", ", insertCols.Select(n => "@p_" + n));
                    var sql = $"INSERT INTO [{table.Schema}].[{table.Name}] ({colList}) VALUES ({paramList}); SELECT CAST(SCOPE_IDENTITY() AS INT);";
                    using var cmd = new SqlCommand(sql, conn, tx);
                    foreach (var n in insertCols)
                    {
                        cmd.Parameters.AddWithValue("@p_" + n, values.TryGetValue(n, out var v) ? v ?? (object)DBNull.Value : DBNull.Value);
                    }
                    // Verbose SQL preview
                    if (_verbose)
                    {
                        Log($"SQL: INSERT INTO {table.Schema}.{table.Name} ({string.Join(", ", insertCols)}) VALUES ({string.Join(", ", insertCols.Select(n => "@p_" + n))});");
                        var pairs = new List<string>();
                        foreach (var n in insertCols)
                        {
                            values.TryGetValue(n, out var vv);
                            pairs.Add($"{n}={PreviewValue(n, vv)}");
                        }
                        Log("Params: " + string.Join(", ", pairs));
                    }
                    newId = (int?)cmd.ExecuteScalar();
                    if (newId.HasValue)
                    {
                        var durMs = (int)(DateTime.UtcNow - opStart).TotalMilliseconds;
                        // Build param preview for audit
                        var paramPairs = new List<string>();
                        foreach (var n in insertCols)
                        {
                            values.TryGetValue(n, out var vv);
                            paramPairs.Add($"{n}={PreviewValue(n, vv)}");
                        }
                        Audit("insert", rawName, table.Name, newId, parentTable?.Name, parentId, fkColName, durMs.ToString(), string.Join("; ", paramPairs));
                        inserted++;
                        Increment(byTable, table.Name);
                    }
                }
            }
            else
            {
                Log($"Skip insert into {table.Schema}.{table.Name}: required FK {fkColName} but parentId is null for element '{elem.Name.LocalName}'.");
                var durMs = (int)(DateTime.UtcNow - opStart).TotalMilliseconds;
                Audit("skip", rawName, table.Name, null, parentTable?.Name, parentId, fkColName, durMs.ToString(), "Missing required parent FK");
            }
        }

        // If current table has a ChoiceOption column, set it to this element name
        if (table.Columns.Any(c => c.Name.Equals("ChoiceOption", StringComparison.OrdinalIgnoreCase)))
        {
            // If we're inserting a new row, ChoiceOption will be included below via values
            // For default-row path or when ChoiceOption isn't in insertCols yet, ensure it gets in.
            values["ChoiceOption"] = rawName;
        }

        var nextParentTable = table;
        var nextParentId = newId;
        foreach (var child in elem.Elements())
        {
            // If the child is a simple column on the current table, skip recursion (already handled in values)
            var simpleCol = GetColName(child.Name.LocalName);
            var isSimpleColumn = !child.HasElements && table.Columns.Any(c => c.Name.Equals(simpleCol, StringComparison.OrdinalIgnoreCase));
            if (isSimpleColumn) continue;

            // Prefer inserting child into its own table when such mapping exists
            var childRaw = child.Name.LocalName;
            var directChild = GetTableName(childRaw);
            XsdToSqlServer.TableInfo? childTable = null;
            if (directChild is not null)
            {
                childTable = _gen.GetTableInfo(directChild);
            }
            if (childTable is null)
            {
                var compound = ToPascal(SanitizeIdentifier(table.Name + "_" + childRaw));
                childTable = _gen.GetTableInfo(compound) ?? childTable;
            }

            if (childTable is not null)
            {
                Log($"Recurse into child element '{childRaw}' as table '{childTable.Name}'.");
                inserted += ImportElement(child, table, nextParentId, conn, tx, byTable);
            }
            else
            {
                // Fallback: recurse as before with current table as parent
                inserted += ImportElement(child, nextParentTable, nextParentId, conn, tx, byTable);
            }
        }
        return inserted;
    }

    private static object? ToDbValue(string value)
        => (object) value; // let SQL convert NVARCHAR

    private static string GetColName(string raw)
        => ToPascal(SanitizeIdentifier(raw));

    private static string? GetTableName(string raw)
        => ToPascal(SanitizeIdentifier(raw));

    // Reuse helpers from generator via internal exposure
    private static string ToPascal(string s)
        => XsdToSqlServer.ToPascalPublic(s);
    private static string SanitizeIdentifier(string s)
        => XsdToSqlServer.SanitizeIdentifierPublic(s);

    private static void Increment(Dictionary<string,int> dict, string table)
    {
        if (!dict.TryGetValue(table, out var c)) c = 0;
        dict[table] = c + 1;
    }

    private static object DefaultForSqlType(string sqlType)
    {
        var t = sqlType.ToUpperInvariant();
        if (t.StartsWith("NVARCHAR") || t.StartsWith("NCHAR") || t.StartsWith("VARCHAR") || t.StartsWith("CHAR")) return string.Empty;
        if (t.StartsWith("DECIMAL") || t.StartsWith("NUMERIC")) return 0m;
        if (t.Contains("BIGINT")) return 0L;
        if (t.Contains("INT")) return 0; // covers INT, SMALLINT
        if (t == "TINYINT") return (byte)0;
        if (t == "BIT") return false;
        if (t == "FLOAT" || t == "REAL") return 0.0;
        if (t == "DATE") return "1900-01-01";
        if (t == "DATETIME2") return "1900-01-01T00:00:00";
        if (t == "TIME") return "00:00:00";
        return string.Empty;
    }

    private void Log(string message)
    {
        if (!_verbose) return;
        try { _log?.Invoke(message); } catch { /* ignore logging errors */ }
    }

    private void Audit(string eventType, string element, string? table, int? newId, string? parentTable, int? parentId, string? fkColumn, string? reason, string? paramPreview)
    {
        if (!_auditEnabled) return;
        try
        {
            string ts = DateTime.Now.ToString("s");
            string file = _currentFile ?? string.Empty;
            string[] cells = new[]
            {
                ts, file, eventType, element,
                table ?? string.Empty,
                newId?.ToString() ?? string.Empty,
                parentTable ?? string.Empty,
                parentId?.ToString() ?? string.Empty,
                fkColumn ?? string.Empty,
                reason ?? string.Empty,
                paramPreview ?? string.Empty
            };
            string line = string.Join(",", cells.Select(Csv));
            _audit?.Invoke(line);
        }
        catch { /* ignore audit errors */ }
    }

    private static string Csv(string? s)
    {
        s ??= string.Empty;
        var needs = s.Contains('"') || s.Contains(',') || s.Contains('\n') || s.Contains('\r');
        if (needs)
        {
            s = '"' + s.Replace("\"", "\"\"") + '"';
        }
        return s;
    }

    private static string PreviewValue(string columnName, object? value)
    {
        if (value is null || value is DBNull) return "NULL";
        try
        {
            var name = columnName ?? string.Empty;
            if (value is string s)
            {
                // Mask CPR/CPRNR-like fields
                if (name.Contains("CPR", StringComparison.OrdinalIgnoreCase))
                {
                    var masked = s.Length <= 2 ? new string('*', s.Length) : new string('*', Math.Max(0, s.Length - 2)) + s.Substring(s.Length - 2);
                    return $"'{masked}'";
                }
                // Limit very long strings in preview
                if (s.Length > 64) s = s.Substring(0, 64) + "…";
                return $"'{s.Replace("'", "''")}'";
            }
            if (value is DateTime dt) return dt.ToString("o");
            if (value is bool b) return b ? "1" : "0";
            return Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? "";
        }
        catch { return "?"; }
    }
}

internal readonly record struct ImportResult(int Total, Dictionary<string,int> ByTable);