﻿using Redis.NET.Internal.IO;

namespace Redis.NET.Internal.Commands
{
    class RedisInt : RedisCommand<long>
    {
        public RedisInt(string command, params object[] args)
            : base(command, args)
        { }

        public override long Parse(RedisReader reader) => reader.ReadInt();

        public class Nullable : RedisCommand<long?>
        {
            public Nullable(string command, params object[] args) : base(command, args)
            { }

            public override long? Parse(RedisReader reader)
            {
                RedisMessage type = reader.ReadType();
                if (type == RedisMessage.Int)
                {
                    return reader.ReadInt(false);
                }

                reader.ReadBulkString(false);
                return null;
            }
        }
    }
}