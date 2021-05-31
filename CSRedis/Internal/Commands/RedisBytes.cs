using Redis.NET.Internal.IO;

namespace Redis.NET.Internal.Commands
{
    class RedisBytes : RedisCommand<byte[]>
    {
        public RedisBytes(string command, params object[] args)
            : base(command, args)
        { }

        public override byte[] Parse(RedisReader reader) => reader.ReadBulkBytes(true);
    }
}