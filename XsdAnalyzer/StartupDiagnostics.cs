namespace XsdAnalyzer;

internal static class StartupDiagnostics
{
    private static readonly HashSet<string> SecretOptions = new(StringComparer.OrdinalIgnoreCase)
    {
        "--connection"
    };

    public static string FormatArguments(IEnumerable<string> arguments)
    {
        var formatted = new List<string>();
        var redactNext = false;

        foreach (var argument in arguments)
        {
            if (redactNext)
            {
                formatted.Add("***");
                redactNext = false;
                continue;
            }

            var separatorIndex = argument.IndexOf('=');
            var option = separatorIndex >= 0 ? argument[..separatorIndex] : argument;
            if (SecretOptions.Contains(option))
            {
                formatted.Add(separatorIndex >= 0 ? option + "=***" : option);
                redactNext = separatorIndex < 0;
                continue;
            }

            formatted.Add(Quote(argument));
        }

        return string.Join(" ", formatted);
    }

    private static string Quote(string value) => value.Contains(' ') ? $"\"{value}\"" : value;
}