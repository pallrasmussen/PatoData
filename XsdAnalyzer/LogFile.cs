using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace XsdAnalyzer;

internal static class LogFile
{
    // 2 MB cap
    private const long MaxBytes = 2L * 1024 * 1024;
    private static readonly object _gate = new object();

    public static void AppendLine(string path, string line)
    {
        try
        {
            var dir = Path.GetDirectoryName(path);
            if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);

            // Cross-process coordination with a named mutex
            var mutexName = GetMutexNameForPath(path);
            using var mutex = new System.Threading.Mutex(false, mutexName, out _);
            var acquired = false;
            try
            {
                // Try to acquire quickly; if contention, wait a short while
                acquired = mutex.WaitOne(TimeSpan.FromMilliseconds(500));
            }
            catch { acquired = false; }

            try
            {
                // In-process guard to minimize re-entrancy
                lock (_gate)
                {
                    // Strict cap: rotate BEFORE append if next write would exceed cap
                    try
                    {
                        var fiPre = new FileInfo(path);
                        var bytesNeeded = Encoding.UTF8.GetByteCount(line + Environment.NewLine);
                        var preLen = fiPre.Exists ? fiPre.Length : 0L;
                        if (preLen > 0 && preLen + bytesNeeded > MaxBytes)
                        {
                            Rotate(path);
                        }
                    }
                    catch { }

                    // Append line
                    try { File.AppendAllText(path, line + Environment.NewLine); } catch { }

                    // Safety: if still over cap (e.g., concurrent writers or header), rotate now
                    try
                    {
                        var fi = new FileInfo(path);
                        if (fi.Exists && fi.Length > MaxBytes)
                        {
                            Rotate(path);
                        }
                    }
                    catch { }
                }
            }
            finally
            {
                if (acquired)
                {
                    try { mutex.ReleaseMutex(); } catch { }
                }
            }
        }
        catch { }
    }

    private static void Rotate(string path)
    {
        var backup = BuildBackupName(path);
        try
        {
            // Try simple move first
            try
            {
                File.Move(path, backup);
                try { File.AppendAllText(path, $"[{DateTime.Now:s}] [log] rollover -> {backup}{Environment.NewLine}"); } catch { }
                return; // rotated
            }
            catch { }

            // Fallback: copy + truncate
            var copied = false;
            try { File.Copy(path, backup, overwrite: false); copied = true; }
            catch { copied = false; }

            if (copied)
            {
                try
                {
                    // Truncate by recreating file
                    using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read);
                    try
                    {
                        var msg = Encoding.UTF8.GetBytes($"[{DateTime.Now:s}] [log] rollover (copy+truncate) -> {backup}{Environment.NewLine}");
                        fs.Write(msg, 0, msg.Length);
                    }
                    catch { }
                }
                catch { }
            }
        }
        catch { }
    }

    private static string BuildBackupName(string path)
    {
        try
        {
            var dir = Path.GetDirectoryName(path) ?? string.Empty;
            var name = Path.GetFileName(path);
            var ts = DateTime.Now.ToString("yyyyMMdd-HHmmssfff");
            var pid = Environment.ProcessId;
            var rand = Random.Shared.Next(0, 9999).ToString("D4");
            var backup = Path.Combine(dir, $"{name}.{ts}.{pid}.{rand}");
            return backup;
        }
        catch
        {
            return path + ".bak";
        }
    }

    private static string GetMutexNameForPath(string path)
    {
        try
        {
            // Use SHA1 of full path to make a stable, compact name
            var bytes = Encoding.UTF8.GetBytes(Path.GetFullPath(path));
            using var sha1 = SHA1.Create();
            var hash = sha1.ComputeHash(bytes);
            var hex = BitConverter.ToString(hash).Replace("-", string.Empty);
            return "Global\\PatoLog_" + hex;
        }
        catch
        {
            // Fallback to a sanitized name
            var safe = new string((path ?? "log").ToCharArray()).Replace('\\', '_').Replace('/', '_').Replace(':', '_');
            return "Global\\PatoLog_" + safe;
        }
    }
}
