using System.Text;
using System.Xml.Linq;
using System.Xml.Schema;

namespace XsdAnalyzer;

internal class XsdToSqlServer
{
    private readonly Dictionary<string, Table> _tables = new(StringComparer.OrdinalIgnoreCase);
    private readonly string _schema;
    private bool _modelBuilt;
    // Map (namespace, localName) -> table name for global elements
    private readonly Dictionary<string, string> _elementToTable = new(StringComparer.OrdinalIgnoreCase);

    public XsdToSqlServer(string schema = "dbo")
    {
        _schema = schema;
    }

    // Diagnostics: number of tables discovered/built from the XSD
    public int TableCount => _tables.Count;

    public string Generate(XmlSchemaSet set)
    {
        EnsureModel(set);

        // Emit SQL: CREATE TABLES, then FKs, then UNIQUEs
        var sb = new StringBuilder();
        foreach (var t in _tables.Values)
        {
            sb.AppendLine(RenderCreateTable(t));
            sb.AppendLine();
        }
        // Add computed persisted DATE columns for known NVARCHAR *DATO fields on DSETREKV
        var rekv = _tables.Values.FirstOrDefault(t => t.Name.Equals("DSETREKV", StringComparison.OrdinalIgnoreCase));
        if (rekv is not null)
        {
            foreach (var c in rekv.Columns)
            {
                if (c.Name.EndsWith("DATO", StringComparison.OrdinalIgnoreCase) && c.Type.StartsWith("NVARCHAR", StringComparison.OrdinalIgnoreCase))
                {
                    var compName = c.Name + "_DATE";
                    var expr =
                        $"(CASE WHEN {SqlName(c.Name)} IS NOT NULL AND LEN({SqlName(c.Name)}) = 8 AND {SqlName(c.Name)} NOT LIKE '%[^0-9]%' " +
                        $"THEN DATEFROMPARTS(CONVERT(INT, SUBSTRING({SqlName(c.Name)}, 1, 4)), CONVERT(INT, SUBSTRING({SqlName(c.Name)}, 5, 2)), CONVERT(INT, SUBSTRING({SqlName(c.Name)}, 7, 2)) ) ELSE NULL END)";
                    sb.AppendLine($"ALTER TABLE {QualifiedName(rekv.Schema, rekv.Name)} ADD {SqlName(compName)} AS {expr} PERSISTED;");
                }
            }
            sb.AppendLine();
        }
        foreach (var t in _tables.Values)
        {
            foreach (var fk in t.ForeignKeys)
            {
                sb.AppendLine(RenderAddForeignKey(t, fk));
            }
        }
        // Indexes for FKs
        foreach (var t in _tables.Values)
        {
            foreach (var fk in t.ForeignKeys)
            {
                sb.AppendLine(RenderCreateIndexForFk(t, fk));
            }
        }
        foreach (var t in _tables.Values)
        {
            foreach (var uq in t.UniqueConstraints)
            {
                sb.AppendLine(RenderAddUnique(t, uq));
            }
        }
        // Supporting nonclustered indexes for multi-column unique constraints (faster duplicate probes)
        foreach (var t in _tables.Values)
        {
            foreach (var uq in t.UniqueConstraints)
            {
                if (uq.Columns.Count > 1)
                {
                    var idxName = $"IX_{t.Name}_{string.Join("_", uq.Columns)}_Lookup";
                    var cols = string.Join(", ", uq.Columns.Select(c => SqlName(c)));
                    // INCLUDE primary key to cover typical lookups that only need PK after match
                    var pkCol = t.Name + "Id";
                    sb.AppendLine($"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = '{idxName}' AND object_id = OBJECT_ID('{t.Schema}.{t.Name}')) CREATE INDEX {SqlName(idxName)} ON {QualifiedName(t.Schema, t.Name)} ({cols}) INCLUDE({SqlName(pkCol)});");
                }
            }
        }
        // CHECK constraints
        foreach (var t in _tables.Values)
        {
            foreach (var ck in t.Checks)
            {
                sb.AppendLine(RenderAddCheck(t, ck));
            }
        }
        return sb.ToString();
    }

    public string GenerateViews()
    {
        var sb = new StringBuilder();
        var schema = _schema;
        // Simple per-table views
        foreach (var t in _tables.Values)
        {
            sb.AppendLine(RenderCreateOrAlterViewForTable(t, schema));
            sb.AppendLine();
        }
        // Parent with child counts views
        foreach (var parent in _tables.Values)
        {
            var children = _tables.Values.Where(ch => ch.ForeignKeys.Any(fk => fk.RefSchema.Equals(parent.Schema, StringComparison.OrdinalIgnoreCase) && fk.RefTable.Equals(parent.Name, StringComparison.OrdinalIgnoreCase))).ToList();
            if (children.Count > 0)
            {
                sb.AppendLine(RenderCreateOrAlterViewWithCounts(parent, children, schema));
                sb.AppendLine();
            }
        }
        // Curated SNOMED view if applicable
        var snomedView = TryRenderSnomedView(schema);
        if (!string.IsNullOrEmpty(snomedView))
        {
            sb.AppendLine(snomedView);
        }
        return sb.ToString();
    }

    private static string RenderCreateOrAlterViewForTable(Table t, string schema)
    {
        var cols = string.Join(", ", t.Columns.Select(c => SqlName(c.Name)));
        var sb = new StringBuilder();
        sb.AppendLine($"CREATE OR ALTER VIEW {QualifiedName(schema, "vw_" + t.Name)} AS");
        sb.AppendLine($"SELECT {cols} FROM {QualifiedName(schema, t.Name)};");
        sb.AppendLine("GO");
        return sb.ToString();
    }

    private static string RenderCreateOrAlterViewWithCounts(Table parent, List<Table> children, string schema)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"CREATE OR ALTER VIEW {QualifiedName(schema, "vw_" + parent.Name + "_WithCounts")} AS");
        var parentAlias = "p";
        var cols = string.Join(", ", parent.Columns.Select(c => parentAlias + "." + SqlName(c.Name)));
        sb.Append($"SELECT {cols}");
        foreach (var ch in children)
        {
            var fkCol = parent.Name + "Id";
            sb.Append($", (SELECT COUNT(*) FROM {QualifiedName(schema, ch.Name)} c WHERE c.{SqlName(fkCol)} = {parentAlias}.{SqlName(parent.Name + "Id")}) AS {SqlName(ch.Name + "Count")}");
        }
        sb.AppendLine();
        sb.AppendLine($"FROM {QualifiedName(schema, parent.Name)} {parentAlias};");
        sb.AppendLine("GO");
        return sb.ToString();
    }

    private string TryRenderSnomedView(string schema)
    {
        // Find tables with a column named Snomedkode (case-insensitive)
        var snomedTables = _tables.Values.Where(t => t.Columns.Any(c => c.Name.Equals("Snomedkode", StringComparison.OrdinalIgnoreCase))).ToList();
        if (snomedTables.Count == 0) return string.Empty;

        // Pre-compute DSETREKV *DATO columns (case-insensitive)
        var rekvTable = _tables.Values.FirstOrDefault(t => t.Name.Equals("DSETREKV", StringComparison.OrdinalIgnoreCase));
        var rekvDatoCols = new List<(string Name, string Type)>();
        if (rekvTable is not null)
        {
            foreach (var c in rekvTable.Columns)
            {
                if (c.Name.EndsWith("DATO", StringComparison.OrdinalIgnoreCase))
                {
                    rekvDatoCols.Add((c.Name, c.Type));
                }
            }
        }

        var parts = new List<string>();
        foreach (var t in snomedTables)
        {
            // Build join chain from child up to ancestors following FKs
            var chain = BuildAncestorChain(t);
            var aliases = new List<(Table tbl, string alias)>();
            aliases.Add((t, "c"));
            for (int i = 0; i < chain.Count; i++) aliases.Add((chain[i], "p" + (i + 1).ToString()));

            // Resolve context aliases/columns
            var dsetPair = aliases.FirstOrDefault(a => a.tbl.Name.Equals("DSET", StringComparison.OrdinalIgnoreCase));
            var dsetAlias = dsetPair.alias;
            var rekvPair = aliases.FirstOrDefault(a => a.tbl.Name.Equals("DSETREKV", StringComparison.OrdinalIgnoreCase));
            var rekvAlias = rekvPair.alias;
            // CPRNR: pick first alias (including child) that has a CPRNR column
            var cprAlias = aliases.FirstOrDefault(a => a.tbl.Columns.Any(c => c.Name.Equals("CPRNR", StringComparison.OrdinalIgnoreCase))).alias;

            // Build consistent projection: Snomedkode, CPRNR, DsetId, RekvId, REKV identifiers, SourceTable, ChoiceOption/ChoiceParentOption, and all REKV *DATO fields
            var selectCols = new List<string>();
            selectCols.Add($"c.{SqlName("Snomedkode")}");
            selectCols.Add(string.IsNullOrEmpty(cprAlias) ? "CAST(NULL AS NVARCHAR(32)) AS [CPRNR]" : $"{cprAlias}.{SqlName("CPRNR")} AS {SqlName("CPRNR")}" );
            selectCols.Add(string.IsNullOrEmpty(dsetAlias) ? "CAST(NULL AS INT) AS [DsetId]" : $"{dsetAlias}.{SqlName("DSETId")} AS {SqlName("DsetId")}" );
            selectCols.Add(string.IsNullOrEmpty(rekvAlias) ? "CAST(NULL AS INT) AS [RekvId]" : $"{rekvAlias}.{SqlName("DSETREKVId")} AS {SqlName("RekvId")}" );
            // Removed Timestamp per request
            // REKV identifiers (best-effort): STAMANSV and REKVNR from DSETREKV if available
            bool rekvHasStam = rekvPair.tbl?.Columns.Any(c => c.Name.Equals("STAMANSV", StringComparison.OrdinalIgnoreCase)) == true;
            bool rekvHasRekvnr = rekvPair.tbl?.Columns.Any(c => c.Name.Equals("REKVNR", StringComparison.OrdinalIgnoreCase)) == true;
            selectCols.Add(rekvHasStam && !string.IsNullOrEmpty(rekvAlias) ? $"CAST({rekvAlias}.{SqlName("STAMANSV")} AS NVARCHAR(255)) AS {SqlName("RekvStamansv")}" : "CAST(NULL AS NVARCHAR(255)) AS [RekvStamansv]");
            selectCols.Add(rekvHasRekvnr && !string.IsNullOrEmpty(rekvAlias) ? $"CAST({rekvAlias}.{SqlName("REKVNR")} AS NVARCHAR(64)) AS {SqlName("RekvNr")}" : "CAST(NULL AS NVARCHAR(64)) AS [RekvNr]");
            selectCols.Add($"N'{t.Name}' AS {SqlName("SourceTable")}" );
            // Choice discriminators when present (as NVARCHAR)
            bool childHasChoiceParent = t.Columns.Any(c => c.Name.Equals("ChoiceParentOption", StringComparison.OrdinalIgnoreCase));
            selectCols.Add(childHasChoiceParent ? $"CAST(c.{SqlName("ChoiceParentOption")} AS NVARCHAR(128)) AS {SqlName("ChoiceParentOption")}" : "CAST(NULL AS NVARCHAR(128)) AS [ChoiceParentOption]");
            bool childHasChoice = t.Columns.Any(c => c.Name.Equals("ChoiceOption", StringComparison.OrdinalIgnoreCase));
            selectCols.Add(childHasChoice ? $"CAST(c.{SqlName("ChoiceOption")} AS NVARCHAR(128)) AS {SqlName("ChoiceOption")}" : "CAST(NULL AS NVARCHAR(128)) AS [ChoiceOption]");

            // Append all *DATO columns from DSETREKV (use type-appropriate NULLs when REKV alias is unavailable)
            foreach (var (colName, colType) in rekvDatoCols)
            {
                if (!string.IsNullOrEmpty(rekvAlias))
                {
                    var upperType = colType.ToUpperInvariant();
                    if (upperType == "DATE" || upperType == "DATETIME2")
                    {
                        // Format native date/datetime to dd-MM-yyyy (style 105)
                        selectCols.Add($"CONVERT(NVARCHAR(10), {rekvAlias}.{SqlName(colName)}, 105) AS {SqlName(colName)}");
                    }
                    else
                    {
                        // Parse ddmmyyyy string -> dd-MM-yyyy using STUFF; validate 8 digits
                        var expr = $"CASE WHEN {rekvAlias}.{SqlName(colName)} IS NOT NULL AND LEN({rekvAlias}.{SqlName(colName)}) = 8 AND {rekvAlias}.{SqlName(colName)} NOT LIKE '%[^0-9]%' " +
                                   $"THEN STUFF(STUFF({rekvAlias}.{SqlName(colName)}, 5, 0, '-'), 3, 0, '-') ELSE NULL END";
                        selectCols.Add($"{expr} AS {SqlName(colName)}");
                    }
                }
                else
                {
                    // Keep UNION types consistent: emit NVARCHAR(10)-typed NULLs
                    selectCols.Add($"CAST(NULL AS NVARCHAR(10)) AS {SqlName(colName)}");
                }
            }

            var sb = new StringBuilder();
            sb.Append("SELECT ");
            sb.Append(string.Join(", ", selectCols));
            sb.AppendLine();
            sb.AppendLine($"FROM {QualifiedName(schema, t.Name)} c");
            // Build joins
            string prevAlias = "c";
            foreach (var (pt, alias) in aliases.Skip(1))
            {
                var fkCol = pt.Name + "Id"; // child has column ParentNameId
                sb.AppendLine($"LEFT JOIN {QualifiedName(schema, pt.Name)} {alias} ON {prevAlias}.{SqlName(pt.Name + "Id")} = {alias}.{SqlName(fkCol)}");
                prevAlias = alias;
            }
            parts.Add(sb.ToString());
        }
        var final = new StringBuilder();
        final.AppendLine($"CREATE OR ALTER VIEW {QualifiedName(schema, "vw_Snomed")} AS");
        final.AppendLine(string.Join("\nUNION ALL\n", parts));
        final.AppendLine("GO");
        return final.ToString();
    }

    private List<Table> BuildAncestorChain(Table start)
    {
        var chain = new List<Table>();
        var current = start;
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { current.Name };
        while (true)
        {
            // pick first FK parent
            var fk = current.ForeignKeys.FirstOrDefault();
            if (fk is null) break;
            if (!_tables.TryGetValue(fk.RefTable, out var parent)) break;
            if (!seen.Add(parent.Name)) break;
            chain.Add(parent);
            current = parent;
            // Stop if we reached DSET
            if (parent.Name.Equals("DSET", StringComparison.OrdinalIgnoreCase)) break;
        }
        return chain;
    }

    // Expose model access for importer
    public TableInfo? GetTableInfo(string name)
        => _tables.TryGetValue(name, out var t) ? ToInfo(t) : null;

    // Helper exposure for importer
    public static string ToPascalPublic(string s) => ToPascal(s);
    public static string SanitizeIdentifierPublic(string s) => SanitizeIdentifier(s);
    
    public string GenerateInsertTemplates(XmlSchemaSet set)
    {
        EnsureModel(set);
        var sb = new StringBuilder();
        sb.AppendLine("-- Sample INSERT templates");
        sb.AppendLine(RenderInsertTemplates());
        return sb.ToString();
    }

    // Public DTO for unique constraints exposure
    public sealed class UniqueConstraintInfo
    {
        public string Name { get; init; } = string.Empty;
        public List<string> Columns { get; init; } = new();
    }

    // Expose unique constraints (column sets) for a given table
    public List<UniqueConstraintInfo> GetUniqueConstraints(string tableName)
    {
        if (!_tables.TryGetValue(tableName, out var t)) return new List<UniqueConstraintInfo>();
        return t.UniqueConstraints
            .Select(u => new UniqueConstraintInfo { Name = u.Name, Columns = new List<string>(u.Columns) })
            .ToList();
    }

    public void EnsureModel(XmlSchemaSet set)
    {
        if (_modelBuilt) return;
        // Pre-scan for local name collisions across namespaces
        var collisions = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
        foreach (XmlSchema schema in set.Schemas())
        {
            foreach (var item in schema.Items)
            {
                if (item is XmlSchemaElement el && !string.IsNullOrEmpty(el.QualifiedName.Name))
                {
                    var ln = el.QualifiedName.Name;
                    var ns = el.QualifiedName.Namespace ?? string.Empty;
                    if (!collisions.TryGetValue(ln, out var setNs)) { setNs = new HashSet<string>(StringComparer.OrdinalIgnoreCase); collisions[ln] = setNs; }
                    setNs.Add(ns);
                }
            }
        }
        foreach (XmlSchema schema in set.Schemas())
        {
            foreach (var item in schema.Items)
            {
                if (item is XmlSchemaElement el && !string.IsNullOrEmpty(el.QualifiedName.Name))
                {
                    var needsNs = collisions.TryGetValue(el.QualifiedName.Name, out var nsSet) && nsSet.Count > 1;
                    var nsToken = needsNs ? MakeNamespaceToken(el.QualifiedName.Namespace) : null;
                    var baseName = ToPascal(SanitizeIdentifier(el.QualifiedName.Name));
                    var tableName = needsNs ? ToPascal(SanitizeIdentifier($"{nsToken}_{baseName}")) : baseName;
                    var table = GetOrCreateTable(tableName);
                    EnsurePrimaryKey(table);
                    ProcessElementIntoTable(el, table, parentTable: null);
                    ApplyIdentityConstraints(el, table);
                    // Map element to table
                    var key = BuildElementKey(el.QualifiedName.Namespace, el.QualifiedName.Name);
                    if (!_elementToTable.ContainsKey(key)) _elementToTable[key] = tableName;
                }
            }
        }
        _modelBuilt = true;
    }

    // Resolve a table name from element namespace/local name; optionally consider parent compound naming
    public string? ResolveTableName(string? xmlNamespace, string localName, string? parentTableName = null)
    {
        var key = BuildElementKey(xmlNamespace, localName);
        if (_elementToTable.TryGetValue(key, out var t)) return t;
        // Fallbacks: local only
        var baseName = ToPascal(SanitizeIdentifier(localName));
        if (_tables.ContainsKey(baseName)) return baseName;
        // Parent compound
        if (!string.IsNullOrEmpty(parentTableName))
        {
            var compound = ToPascal(SanitizeIdentifier(parentTableName + "_" + localName));
            if (_tables.ContainsKey(compound)) return compound;
        }
        return null;
    }

    private void ProcessElementIntoTable(XmlSchemaElement el, Table table, Table? parentTable)
    {
        var type = el.ElementSchemaType;
        // Simple content -> column
        if (IsSimple(type))
        {
            var colName = ToPascal(SanitizeIdentifier(el.QualifiedName.Name));
            var sqlType = MapSqlType(type);
            var col = AddOrUpdateColumn(table, colName, sqlType, nullable: el.MinOccurs == 0);
            // Unique if xs:ID
            if (IsXsId(type)) EnsureUnique(table, col.Name);
            AddChecksForColumn(table, col, type);
            return;
        }

        if (type is XmlSchemaComplexType ct)
        {
            // Attributes -> columns
            foreach (XmlSchemaAttribute attr in ct.AttributeUses.Values)
            {
                var colName = ToPascal(SanitizeIdentifier(attr.Name));
                var sqlType = MapSqlType(attr.AttributeSchemaType);
                var col = AddOrUpdateColumn(table, colName, sqlType, nullable: attr.Use != XmlSchemaUse.Required);
                if (IsXsId(attr.AttributeSchemaType)) EnsureUnique(table, col.Name);
                AddChecksForColumn(table, col, attr.AttributeSchemaType);
            }

            // Content model
            switch (ct.ContentTypeParticle)
            {
                case XmlSchemaSequence seq:
                    foreach (var obj in seq.Items)
                        HandleChildObject(obj, table);
                    break;
                case XmlSchemaChoice choice:
                    // Add discriminator
                    AddOrUpdateColumn(table, "ChoiceOption", "NVARCHAR(64)", nullable: true);
                    foreach (var obj in choice.Items)
                        HandleChildObject(obj, table, choiceContext: true);
                    break;
                case XmlSchemaAll all:
                    foreach (var obj in all.Items)
                        HandleChildObject(obj, table);
                    break;
            }
        }
    }

    private void HandleChildObject(XmlSchemaObject obj, Table table, bool choiceContext = false)
    {
        if (obj is XmlSchemaElement child)
        {
            bool multiple = child.MaxOccursString == "unbounded" || child.MaxOccurs > 1m;
            bool childIsSimple = IsSimple(child.ElementSchemaType);
            if (multiple || !childIsSimple)
            {
                // Create child table with FK to parent
                var childTableName = ToPascal(MakeChildTableName(table.Name, child.QualifiedName.Name));
                var childTable = GetOrCreateTable(childTableName);
                EnsurePrimaryKey(childTable);
                EnsureForeignKey(childTable, table);
                if (choiceContext)
                    AddOrUpdateColumn(childTable, "ChoiceParentOption", "NVARCHAR(64)", nullable: true);
                ProcessElementIntoTable(child, childTable, table);
            }
            else
            {
                var childCol = ToPascal(SanitizeIdentifier(child.QualifiedName.Name));
                var sqlType = MapSqlType(child.ElementSchemaType);
                var col = AddOrUpdateColumn(table, childCol, sqlType, nullable: child.MinOccurs == 0 || choiceContext);
                if (IsXsId(child.ElementSchemaType)) EnsureUnique(table, col.Name);
                AddChecksForColumn(table, col, child.ElementSchemaType);
            }
        }
    }

    private static bool IsSimple(XmlSchemaType? t)
    {
        if (t is null) return true;
        if (t is XmlSchemaSimpleType) return true;
        if (t is XmlSchemaComplexType ct)
        {
            return ct.ContentType == XmlSchemaContentType.TextOnly;
        }
        return false;
    }

    private static bool IsXsId(XmlSchemaType? t)
        => t is XmlSchemaSimpleType st && st.Datatype?.TypeCode == XmlTypeCode.Id;

    private static string MapSqlType(XmlSchemaType? t)
    {
        if (t is XmlSchemaSimpleType st && st.Datatype is not null)
        {
            // Apply string facets when present
            if (IsStringy(st.Datatype.TypeCode))
            {
                var len = ExtractMaxLength(st);
                if (len.HasValue)
                {
                    return $"NVARCHAR({(len.Value > 4000 ? "MAX" : len.Value.ToString())})";
                }
            }
            return st.Datatype.TypeCode switch
            {
                XmlTypeCode.String or XmlTypeCode.NormalizedString or XmlTypeCode.Token => "NVARCHAR(255)",
                XmlTypeCode.Language or XmlTypeCode.Name or XmlTypeCode.NCName or XmlTypeCode.Id or XmlTypeCode.Idref => "NVARCHAR(255)",
                XmlTypeCode.AnyUri => "NVARCHAR(512)",
                XmlTypeCode.Decimal => MapDecimalType(st),
                XmlTypeCode.Integer or XmlTypeCode.Int or XmlTypeCode.NegativeInteger or XmlTypeCode.NonNegativeInteger or XmlTypeCode.NonPositiveInteger or XmlTypeCode.PositiveInteger => "BIGINT",
                XmlTypeCode.Long => "BIGINT",
                XmlTypeCode.Short => "SMALLINT",
                XmlTypeCode.Byte => "TINYINT",
                XmlTypeCode.UnsignedInt => "INT",
                XmlTypeCode.UnsignedShort => "INT",
                XmlTypeCode.UnsignedByte => "TINYINT",
                XmlTypeCode.Boolean => "BIT",
                XmlTypeCode.Float => "REAL",
                XmlTypeCode.Double => "FLOAT",
                XmlTypeCode.Date => "DATE",
                XmlTypeCode.DateTime => "DATETIME2",
                XmlTypeCode.Time => "TIME",
                XmlTypeCode.Duration => "NVARCHAR(64)",
                _ => "NVARCHAR(255)"
            };
        }
        return "NVARCHAR(255)";
    }

    private static bool IsStringy(XmlTypeCode tc) => tc == XmlTypeCode.String || tc == XmlTypeCode.NormalizedString || tc == XmlTypeCode.Token || tc == XmlTypeCode.Language || tc == XmlTypeCode.Name || tc == XmlTypeCode.NCName || tc == XmlTypeCode.Id || tc == XmlTypeCode.Idref || tc == XmlTypeCode.AnyUri;

    private static int? ExtractMaxLength(XmlSchemaSimpleType st)
    {
        // Looks into restriction facets to find length or maxLength
        if (st.Content is XmlSchemaSimpleTypeRestriction r && r.Facets is not null)
        {
            foreach (var f in r.Facets)
            {
                switch (f)
                {
                    case XmlSchemaMaxLengthFacet max when int.TryParse(max.Value, out var ml):
                        return ml;
                    case XmlSchemaLengthFacet len when int.TryParse(len.Value, out var l):
                        return l;
                }
            }
        }
        return null;
    }

    private static string MapDecimalType(XmlSchemaSimpleType st)
    {
        // Extract totalDigits (precision) and fractionDigits (scale)
        int? precision = null; int? scale = null;
        if (st.Content is XmlSchemaSimpleTypeRestriction r && r.Facets is not null)
        {
            foreach (var f in r.Facets)
            {
                switch (f)
                {
                    case XmlSchemaTotalDigitsFacet td when int.TryParse(td.Value, out var p):
                        precision = p; break;
                    case XmlSchemaFractionDigitsFacet fd when int.TryParse(fd.Value, out var s):
                        scale = s; break;
                }
            }
        }
        // Apply sensible defaults and SQL Server bounds
        if (precision.HasValue && scale.HasValue)
        {
            var p = Math.Clamp(precision.Value, 1, 38);
            var s = Math.Clamp(scale.Value, 0, p);
            return $"DECIMAL({p},{s})";
        }
        if (precision.HasValue)
        {
            var p = Math.Clamp(precision.Value, 1, 38);
            return $"DECIMAL({p},0)";
        }
        if (scale.HasValue)
        {
            var s = Math.Clamp(scale.Value, 0, 38);
            var p = Math.Clamp(s + 10, s + 1, 38);
            return $"DECIMAL({p},{s})";
        }
        return "DECIMAL(18,6)";
    }

    private Table GetOrCreateTable(string name)
    {
        if (_tables.TryGetValue(name, out var t)) return t;
        t = new Table(name, _schema);
        _tables[name] = t;
        return t;
    }

    private static void EnsurePrimaryKey(Table t)
    {
        if (t.Columns.Any(c => c.IsPrimaryKey)) return;
        var pkName = t.Name + "Id";
        t.Columns.Insert(0, new Column(pkName, "INT", nullable: false) { Identity = true, IsPrimaryKey = true });
    }

    private static void EnsureForeignKey(Table child, Table parent)
    {
        var colName = parent.Name + "Id";
        if (!child.Columns.Any(c => string.Equals(c.Name, colName, StringComparison.OrdinalIgnoreCase)))
        {
            child.Columns.Insert(1, new Column(colName, "INT", nullable: false));
        }
        var fkName = $"FK_{child.Name}_{parent.Name}";
        if (!child.ForeignKeys.Any(f => f.Name.Equals(fkName, StringComparison.OrdinalIgnoreCase)))
        {
            child.ForeignKeys.Add(new ForeignKey(fkName, colName, parent.Schema, parent.Name, parent.Name + "Id"));
        }
    }

    private static Column AddOrUpdateColumn(Table t, string name, string type, bool nullable)
    {
        var existing = t.Columns.FirstOrDefault(c => c.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
        if (existing is null)
        {
            var c = new Column(name, type, nullable);
            t.Columns.Add(c);
            return c;
        }
        else
        {
            existing.Nullable = existing.Nullable && nullable;
            if (existing.Type.StartsWith("NVARCHAR", StringComparison.OrdinalIgnoreCase) || type.StartsWith("NVARCHAR", StringComparison.OrdinalIgnoreCase))
                existing.Type = "NVARCHAR(MAX)";
            return existing;
        }
    }

    private static void EnsureUnique(Table t, string columnName)
    {
        var uqName = $"UQ_{t.Name}_{columnName}";
        if (!t.UniqueConstraints.Any(u => u.Name.Equals(uqName, StringComparison.OrdinalIgnoreCase)))
        {
            t.UniqueConstraints.Add(new UniqueConstraint(uqName, new List<string> { columnName }));
        }
    }

    private static string RenderCreateTable(Table t)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"CREATE TABLE {QualifiedName(t.Schema, t.Name)} (");
        for (int i = 0; i < t.Columns.Count; i++)
        {
            var c = t.Columns[i];
            sb.Append("    ");
            sb.Append(SqlName(c.Name));
            sb.Append(' ').
               Append(c.Type);
            if (c.Identity) sb.Append(" IDENTITY(1,1)");
            sb.Append(c.Nullable ? " NULL" : " NOT NULL");
            if (c.IsPrimaryKey) sb.Append(" PRIMARY KEY");
            sb.Append(i == t.Columns.Count - 1 ? string.Empty : ",");
            sb.AppendLine();
        }
        sb.Append(")");
        sb.Append(";");
        return sb.ToString();
    }

    private static string RenderAddForeignKey(Table t, ForeignKey fk)
    {
        return $"ALTER TABLE {QualifiedName(t.Schema, t.Name)} ADD CONSTRAINT {SqlName(fk.Name)} FOREIGN KEY ({SqlName(fk.ColumnName)}) REFERENCES {QualifiedName(fk.RefSchema, fk.RefTable)}({SqlName(fk.RefColumn)});";
    }

    private static string RenderCreateIndexForFk(Table t, ForeignKey fk)
    {
        var idxName = $"IX_{t.Name}_{fk.ColumnName}";
        return $"CREATE INDEX {SqlName(idxName)} ON {QualifiedName(t.Schema, t.Name)} ({SqlName(fk.ColumnName)});";
    }

    private static string RenderAddUnique(Table t, UniqueConstraint uq)
    {
        var cols = string.Join(", ", uq.Columns.Select(SqlName));
        return $"ALTER TABLE {QualifiedName(t.Schema, t.Name)} ADD CONSTRAINT {SqlName(uq.Name)} UNIQUE ({cols});";
    }

    private static string RenderAddCheck(Table t, CheckConstraint ck)
    {
        return $"ALTER TABLE {QualifiedName(t.Schema, t.Name)} ADD CONSTRAINT {SqlName(ck.Name)} CHECK ({ck.Expression});";
    }

    private static string QualifiedName(string schema, string name) => $"[{schema}].[{name}]";
    private static string SqlName(string name) => $"[{name}]";

    private static string SanitizeIdentifier(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return "Unnamed";
        var chars = raw!.Select(ch => char.IsLetterOrDigit(ch) ? ch : '_').ToArray();
        var s = new string(chars).Trim('_');
        if (string.IsNullOrEmpty(s)) s = "Id";
        if (char.IsDigit(s[0])) s = "N_" + s; // cannot start with digit
        return s;
    }

    private static string ToPascal(string s)
    {
        var parts = s.Split(new[] { '_', ' ', '-' }, StringSplitOptions.RemoveEmptyEntries);
        return string.Concat(parts.Select(p => char.ToUpperInvariant(p[0]) + (p.Length > 1 ? p.Substring(1) : string.Empty)));
    }

    private static string MakeChildTableName(string parent, string child)
    {
        return SanitizeIdentifier(parent + "_" + child);
    }

    private void ApplyIdentityConstraints(XmlSchemaElement el, Table table)
    {
        // Best-effort: look for XmlSchemaKey / XmlSchemaUnique within element's constraints (if available)
        // and map their field xpaths (like "@Attr" or "Child") to column names, then add UNIQUE.
        try
        {
            // Some .NET versions expose 'Constraints' on XmlSchemaElement
            var prop = typeof(XmlSchemaElement).GetProperty("Constraints");
            if (prop != null)
            {
                if (prop.GetValue(el) is System.Collections.IEnumerable coll)
                {
                    foreach (var obj in coll)
                    {
                        switch (obj)
                        {
                            case XmlSchemaUnique uq:
                                AddUniqueFromSelectorAndFields(uq.Selector, uq.Fields, table);
                                break;
                            case XmlSchemaKey key:
                                AddUniqueFromSelectorAndFields(key.Selector, key.Fields, table);
                                break;
                        }
                    }
                }
            }
        }
        catch
        {
            // Swallow reflection differences silently; feature is best-effort.
        }
    }

    private static string BuildElementKey(string? ns, string local)
        => (ns ?? string.Empty) + "|" + (local ?? string.Empty);

    private static string MakeNamespaceToken(string? ns)
    {
        if (string.IsNullOrWhiteSpace(ns)) return "Ns";
        try
        {
            var s = ns!;
            // Take last non-empty segment after common separators
            var parts = s.Split(new[] {'/', '#', ':', '.'}, StringSplitOptions.RemoveEmptyEntries);
            var last = parts.Length > 0 ? parts[^1] : s;
            // Sanitize and Pascalize
            return ToPascal(SanitizeIdentifier(last));
        }
        catch { return "Ns"; }
    }

    private void AddUniqueFromSelectorAndFields(XmlSchemaXPath? selector, XmlSchemaObjectCollection fields, Table table)
    {
        // If selector points to current element ("." or element name), we can map the fields directly to columns
        // For nested selectors, we skip since we don't track full path here.
        var cols = new List<string>();
        foreach (var fobj in fields)
        {
            if (fobj is XmlSchemaXPath xs)
            {
                var last = xs.XPath?.Trim()?.Split('/')?.LastOrDefault();
                if (string.IsNullOrWhiteSpace(last)) continue;
                if (last!.StartsWith("@")) last = last.Substring(1);
                var colName = ToPascal(SanitizeIdentifier(last));
                if (table.Columns.Any(c => c.Name.Equals(colName, StringComparison.OrdinalIgnoreCase)))
                    cols.Add(colName);
            }
        }
        if (cols.Count > 0)
        {
            var uqName = $"UQ_{table.Name}_{string.Join("_", cols)}";
            if (!table.UniqueConstraints.Any(u => u.Name.Equals(uqName, StringComparison.OrdinalIgnoreCase)))
            {
                table.UniqueConstraints.Add(new UniqueConstraint(uqName, cols));
            }
        }
    }

    private string RenderInsertTemplates()
    {
        var sb = new StringBuilder();
        foreach (var t in _tables.Values)
        {
            var insertCols = t.Columns.Where(c => !c.Identity).ToList();
            if (insertCols.Count == 0) continue;
            var colList = string.Join(", ", insertCols.Select(c => SqlName(c.Name)));
            var values = string.Join(", ", insertCols.Select(c => SampleValueForType(c.Type, c.Nullable)));
            sb.AppendLine($"INSERT INTO {QualifiedName(t.Schema, t.Name)} ({colList}) VALUES ({values});");
            sb.AppendLine("GO");
        }
        return sb.ToString();
    }

    private static string SampleValueForType(string sqlType, bool nullable)
    {
        if (nullable) return "NULL";
        var t = sqlType.ToUpperInvariant();
        if (t.StartsWith("NVARCHAR") || t.StartsWith("NCHAR") || t.StartsWith("VARCHAR") || t.StartsWith("CHAR")) return "'sample'";
        if (t.StartsWith("DECIMAL") || t.StartsWith("NUMERIC") || t.Contains("INT") || t == "FLOAT" || t == "REAL") return "0";
        if (t == "BIT") return "0";
        if (t == "DATE") return "'2000-01-01'";
        if (t == "DATETIME2") return "'2000-01-01T00:00:00'";
        if (t == "TIME") return "'00:00:00'";
        return "NULL";
    }

    private sealed class Table
    {
        public string Schema { get; }
        public string Name { get; }
        public List<Column> Columns { get; } = new();
        public List<ForeignKey> ForeignKeys { get; } = new();
        public List<UniqueConstraint> UniqueConstraints { get; } = new();
        public List<CheckConstraint> Checks { get; } = new();
        public Table(string name, string schema) { Name = name; Schema = schema; }
    }

    private sealed class Column
    {
        public string Name { get; }
        public string Type { get; set; }
        public bool Nullable { get; set; }
        public bool Identity { get; set; }
        public bool IsPrimaryKey { get; set; }
        public Column(string name, string type, bool nullable)
        {
            Name = name; Type = type; Nullable = nullable;
        }
    }

    // Public DTOs for importer
    public sealed class ColumnInfo
    {
        public string Name { get; init; } = string.Empty;
        public string Type { get; init; } = string.Empty;
        public bool Nullable { get; init; }
        public bool Identity { get; init; }
        public bool IsPrimaryKey { get; init; }
    }

    public sealed class TableInfo
    {
        public string Schema { get; init; } = string.Empty;
        public string Name { get; init; } = string.Empty;
        public List<ColumnInfo> Columns { get; init; } = new();
    }

    private static TableInfo ToInfo(Table t)
        => new TableInfo
        {
            Schema = t.Schema,
            Name = t.Name,
            Columns = t.Columns.Select(c => new ColumnInfo
            {
                Name = c.Name,
                Type = c.Type,
                Nullable = c.Nullable,
                Identity = c.Identity,
                IsPrimaryKey = c.IsPrimaryKey
            }).ToList()
        };

    private sealed class ForeignKey
    {
        public string Name { get; }
        public string ColumnName { get; }
        public string RefSchema { get; }
        public string RefTable { get; }
        public string RefColumn { get; }
        public ForeignKey(string name, string columnName, string refSchema, string refTable, string refColumn)
            => (Name, ColumnName, RefSchema, RefTable, RefColumn) = (name, columnName, refSchema, refTable, refColumn);
    }

    private sealed class UniqueConstraint
    {
        public string Name { get; }
        public List<string> Columns { get; }
        public UniqueConstraint(string name, List<string> columns)
            => (Name, Columns) = (name, columns);
    }

    private sealed class CheckConstraint
    {
        public string Name { get; }
        public string ColumnName { get; }
        public string Expression { get; }
        public CheckConstraint(string name, string columnName, string expression)
            => (Name, ColumnName, Expression) = (name, columnName, expression);
    }

    private void AddChecksForColumn(Table table, Column col, XmlSchemaType? type)
    {
        if (type is not XmlSchemaSimpleType st) return;
        if (st.Content is not XmlSchemaSimpleTypeRestriction r || r.Facets is null) return;

        var isString = col.Type.StartsWith("NVARCHAR", StringComparison.OrdinalIgnoreCase) || col.Type.StartsWith("NCHAR", StringComparison.OrdinalIgnoreCase);
        var isDate = string.Equals(col.Type, "DATE", StringComparison.OrdinalIgnoreCase) || string.Equals(col.Type, "DATETIME2", StringComparison.OrdinalIgnoreCase) || string.Equals(col.Type, "TIME", StringComparison.OrdinalIgnoreCase);
        var isNumeric = !isString && !isDate; // crude but sufficient for our mapped types

        int? minLen = null, maxLen = null, exactLen = null;
        string? minVal = null, maxVal = null; bool minExclusive = false, maxExclusive = false;

        foreach (var f in r.Facets)
        {
            switch (f)
            {
                case XmlSchemaMinLengthFacet mln when int.TryParse(mln.Value, out var v1):
                    minLen = v1; break;
                case XmlSchemaMaxLengthFacet mxl when int.TryParse(mxl.Value, out var v2):
                    maxLen = v2; break;
                case XmlSchemaLengthFacet len when int.TryParse(len.Value, out var v3):
                    exactLen = v3; break;
                case XmlSchemaMinInclusiveFacet mi:
                    minVal = mi.Value; minExclusive = false; break;
                case XmlSchemaMaxInclusiveFacet mai:
                    maxVal = mai.Value; maxExclusive = false; break;
                case XmlSchemaMinExclusiveFacet me:
                    minVal = me.Value; minExclusive = true; break;
                case XmlSchemaMaxExclusiveFacet mae:
                    maxVal = mae.Value; maxExclusive = true; break;
            }
        }

        // String length checks
        if (isString)
        {
            // exact length has priority
            if (exactLen.HasValue)
            {
                AddCheck(table, col, $"LEN({SqlName(col.Name)}) = {exactLen.Value}");
            }
            else
            {
                if (minLen.HasValue)
                {
                    AddCheck(table, col, $"LEN({SqlName(col.Name)}) >= {minLen.Value}");
                }
                if (maxLen.HasValue)
                {
                    // Only add if NVARCHAR(MAX) so type doesn't already enforce
                    if (col.Type.Contains("MAX", StringComparison.OrdinalIgnoreCase))
                        AddCheck(table, col, $"LEN({SqlName(col.Name)}) <= {maxLen.Value}");
                }
            }
        }

        // Numeric/date bounds checks
        if ((isNumeric || isDate) && (minVal is not null || maxVal is not null))
        {
            string left = SqlName(col.Name);
            string minExpr = minVal is null ? string.Empty : BuildTypedLiteral(col.Type, minVal);
            string maxExpr = maxVal is null ? string.Empty : BuildTypedLiteral(col.Type, maxVal);

            var parts = new List<string>();
            if (minVal is not null)
                parts.Add($"{left} {(minExclusive ? ">" : ">=")} {minExpr}");
            if (maxVal is not null)
                parts.Add($"{left} {(maxExclusive ? "<" : "<=")} {maxExpr}");
            if (parts.Count > 0)
            {
                var pred = string.Join(" AND ", parts);
                // Allow NULLs if column nullable
                if (col.Nullable)
                    pred = $"({SqlName(col.Name)} IS NULL OR (" + pred + "))";
                AddCheck(table, col, pred);
            }
        }
    }

    private static void AddCheck(Table table, Column col, string predicate)
    {
        // Wrap predicate with null allowance if column is nullable and predicate doesn't already account for it
        if (col.Nullable && !predicate.Contains("IS NULL", StringComparison.OrdinalIgnoreCase))
        {
            predicate = $"({SqlName(col.Name)} IS NULL OR (" + predicate + "))";
        }
        var name = $"CK_{table.Name}_{col.Name}_{table.Checks.Count + 1}";
        if (!table.Checks.Any(c => c.Expression.Equals(predicate, StringComparison.OrdinalIgnoreCase)))
        {
            table.Checks.Add(new CheckConstraint(name, col.Name, predicate));
        }
    }

    private static string BuildTypedLiteral(string sqlType, string raw)
    {
        var t = sqlType.ToUpperInvariant();
        if (t.StartsWith("NVARCHAR") || t.StartsWith("NCHAR") || t.StartsWith("VARCHAR") || t.StartsWith("CHAR"))
        {
            return SqlLiteral(raw);
        }
        if (t == "DATE" || t == "DATETIME2" || t == "TIME")
        {
            return SqlLiteral(raw);
        }
        // numeric and others: return as-is
        return raw;
    }

    public string GenerateSeedFromXml(XDocument doc)
    {
        var root = doc.Root;
        if (root is null) return string.Empty;
        var sb = new StringBuilder();
        sb.AppendLine("-- Seed data generated from example XML");
        EmitInserts(root, parentTable: null, parentIdVar: null, sb);
        return sb.ToString();
    }

    private void EmitInserts(XElement elem, Table? parentTable, string? parentIdVar, StringBuilder sb)
    {
        var tableName = ToPascal(SanitizeIdentifier(elem.Name.LocalName));
        if (!_tables.TryGetValue(tableName, out var table))
        {
            // No direct table for this element; try children
            foreach (var child in elem.Elements()) EmitInserts(child, parentTable, parentIdVar, sb);
            return;
        }

        var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        // Parent FK if exists
        if (parentTable is not null)
        {
            var fkCol = parentTable.Name + "Id";
            if (table.Columns.Any(c => c.Name.Equals(fkCol, StringComparison.OrdinalIgnoreCase)))
            {
                values[fkCol] = parentIdVar ?? "NULL";
            }
        }

        // Attributes -> columns
        foreach (var attr in elem.Attributes())
        {
            var col = ToPascal(SanitizeIdentifier(attr.Name.LocalName));
            if (table.Columns.Any(c => c.Name.Equals(col, StringComparison.OrdinalIgnoreCase)))
            {
                values[col] = SqlLiteral(attr.Value);
            }
        }

        // Simple child elements (no nested elements)
        foreach (var child in elem.Elements())
        {
            if (!child.HasElements && string.IsNullOrWhiteSpace(child.Value) == false)
            {
                var col = ToPascal(SanitizeIdentifier(child.Name.LocalName));
                if (table.Columns.Any(c => c.Name.Equals(col, StringComparison.OrdinalIgnoreCase)))
                {
                    values[col] = SqlLiteral(child.Value);
                }
            }
        }

        // Build INSERT
        var insertCols = table.Columns.Where(c => !c.Identity).Select(c => c.Name).Where(n => values.ContainsKey(n)).ToList();
        if (insertCols.Count == 0 && parentIdVar is null)
        {
            // No values at root: if table has no required (non-null, non-identity) columns, insert default row to anchor children
            if (CanInsertDefaultRow(table))
            {
                sb.AppendLine($"INSERT INTO {QualifiedName(table.Schema, table.Name)} DEFAULT VALUES;");
                sb.AppendLine("SET NOCOUNT ON;");
                sb.AppendLine($"DECLARE @{table.Name}Id INT = CAST(SCOPE_IDENTITY() AS INT);");
                sb.AppendLine("SET NOCOUNT OFF;");

                parentTable = table;
                parentIdVar = "@" + table.Name + "Id";
            }
            // else: skip inserting this element and continue with children without parent id
        }
        else
        {
            // include FK if present but wasn't in values yet and is required
            if (parentTable is not null)
            {
                var fkCol = parentTable.Name + "Id";
                if (!insertCols.Contains(fkCol) && table.Columns.Any(c => c.Name.Equals(fkCol, StringComparison.OrdinalIgnoreCase)))
                {
                    insertCols.Insert(0, fkCol);
                    values[fkCol] = parentIdVar ?? "NULL";
                }
            }

            var colList = string.Join(", ", insertCols.Select(SqlName));
            var valList = string.Join(", ", insertCols.Select(cn => values.TryGetValue(cn, out var v) ? v : "NULL"));
            sb.AppendLine($"INSERT INTO {QualifiedName(table.Schema, table.Name)} ({colList}) VALUES ({valList});");
            sb.AppendLine("SET NOCOUNT ON;");
            sb.AppendLine($"DECLARE @{table.Name}Id INT = CAST(SCOPE_IDENTITY() AS INT);");
            sb.AppendLine("SET NOCOUNT OFF;");

            parentTable = table; // current becomes parent for children
            parentIdVar = "@" + table.Name + "Id";
        }

        // Recurse children as separate rows for complex or repeated
        foreach (var child in elem.Elements())
        {
            // If child corresponds to a column, we already inserted above; otherwise, emit as its own row
            var childCol = ToPascal(SanitizeIdentifier(child.Name.LocalName));
            if (!(table.Columns.Any(c => c.Name.Equals(childCol, StringComparison.OrdinalIgnoreCase)) && !child.HasElements))
            {
                EmitInserts(child, parentTable, parentIdVar, sb);
            }
        }
    }

    private static bool CanInsertDefaultRow(Table t)
    {
        // true if there are no non-identity NOT NULL columns
        foreach (var c in t.Columns)
        {
            if (!c.Identity && !c.Nullable)
            {
                return false;
            }
        }
        return true;
    }

    private static string SqlLiteral(string? value)
    {
        if (value is null) return "NULL";
        var escaped = value.Replace("'", "''");
        return "N'" + escaped + "'";
    }
}
