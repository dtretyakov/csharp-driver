namespace Cassandra
{
    internal enum FrameOperation : byte
    {
        Error = 0x00,
        Startup = 0x01,
        Ready = 0x02,
        Authenticate = 0x03,
        Options = 0x05,
        Supported = 0x06,
        Query = 0x07,
        Result = 0x08,
        Prepare = 0x09,
        Execute = 0x0A,
        Register = 0x0B,
        Event = 0x0C,
        Batch = 0x0D,
        AuthChallenge = 0x0E,
        AuthResponse = 0x0F,
        AuthSuccess = 0x10
    }
}