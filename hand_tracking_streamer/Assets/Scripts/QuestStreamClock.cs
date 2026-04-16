using System.Diagnostics;
using System.Text;

public static class QuestStreamClock
{
    private static readonly double TicksToNs = 1_000_000_000.0 / Stopwatch.Frequency;

    public static ulong GetMonotonicTimestampNs()
    {
        return (ulong)(Stopwatch.GetTimestamp() * TicksToNs);
    }

    public static uint NextFrameId(ref uint frameId)
    {
        unchecked
        {
            frameId++;
            if (frameId == 0)
            {
                frameId = 1;
            }
            return frameId;
        }
    }

    public static void AppendHeaderWithMeta(
        StringBuilder sb,
        string source,
        string section,
        uint frameId,
        ulong timestampNs
    )
    {
        sb.Append(source)
          .Append(" ")
          .Append(section)
          .Append(" | f = ")
          .Append(frameId)
          .Append(" | t = ")
          .Append(timestampNs)
          .Append(":");
    }

    public static string BuildLabelWithMeta(string source, string section, uint frameId, ulong timestampNs)
    {
        StringBuilder sb = new StringBuilder(96);
        AppendHeaderWithMeta(sb, source, section, frameId, timestampNs);
        return sb.ToString();
    }
}
