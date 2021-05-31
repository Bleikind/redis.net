using Redis.NET.Internal.IO;

namespace Redis.NET.Internal.Commands
{
    class RedisObject : RedisCommand<object>
    {
        public RedisObject(string command, params object[] args) : base(command, args)
        { }

        public override object Parse(RedisReader reader) => reader.Read();

        public class Strings : RedisCommand<object>
        {
            public Strings(string command, params object[] args) : base(command, args)
            { }

            public override object Parse(RedisReader reader) => reader.Read(true);
        }
    }
}