using Redis.NET.Internal.IO;
using System.Globalization;

namespace Redis.NET.Internal.Commands
{
    class RedisFloat : RedisCommand<double>
    {
        public RedisFloat(string command, params object[] args)
            : base(command, args)
        { }

        public override double Parse(RedisReader reader) => FromString(reader.ReadBulkString());

        static double FromString(string input) => double.Parse(input, NumberStyles.Float, CultureInfo.InvariantCulture);

        public class Nullable : RedisCommand<double?>
        {
            public Nullable(string command, params object[] args)
                : base(command, args)
            { }

            public override double? Parse(RedisReader reader) => reader.ReadBulkString() == null ? null : (double?)FromString(reader.ReadBulkString());
        }
    }
}