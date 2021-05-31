using Redis.NET.Internal.IO;

namespace Redis.NET.Internal.Commands
{
    class RedisBool : RedisCommand<bool>
    {
        public RedisBool(string command, params object[] args)
            : base(command, args)
        { }

        public override bool Parse(RedisReader reader) => reader.ReadInt() == 1;
    }
}