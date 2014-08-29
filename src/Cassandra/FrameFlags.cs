using System;

namespace Cassandra
{
    [Flags]
    internal enum FrameFlags : byte
    {
        Compressed = 0x01,
        Tracing = 0x02
    }
}