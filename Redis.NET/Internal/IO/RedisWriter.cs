using System.Globalization;
using System.IO;
using System.Text;

namespace Redis.NET.Internal.IO
{
    class RedisWriter
    {
        public char Bulk { get; } = (char)RedisMessage.Bulk;
        public char MultiBulk { get; } = (char) RedisMessage.MultiBulk;
        public string EOL { get; } = "\r\n";

        readonly RedisIO _io;

        public RedisWriter(RedisIO io) => _io = io;

        public int Write(RedisCommand command, Stream stream)
        {
            byte[] data = _io.Encoding.GetBytes(Prepare(command));
            stream.Write(data, 0, data.Length);
            return data.Length;
        }

        public int Write(RedisCommand command, byte[] buffer, int offset)
        {
            string prepared = Prepare(command);
            return _io.Encoding.GetBytes(prepared, 0, prepared.Length, buffer, offset);
        }

        string Prepare(RedisCommand command)
        {
            var parts = command.Command.Split(' ');
            var length = parts.Length + command.Arguments.Length;
            var sb = new StringBuilder();

            sb.Append(MultiBulk).Append(length).Append(EOL);

            foreach (string part in parts)
            {
                sb.Append(Bulk).Append(_io.Encoding.GetByteCount(part)).Append(EOL).Append(part).Append(EOL);
            }

            foreach (var arg in command.Arguments)
            {
                string str = string.Format(CultureInfo.InvariantCulture, "{0}", arg);
                sb.Append(Bulk).Append(_io.Encoding.GetByteCount(str)).Append(EOL).Append(str).Append(EOL);
            }

            return sb.ToString();
        }
    }
}