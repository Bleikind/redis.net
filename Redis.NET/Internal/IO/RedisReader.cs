using System;
using System.IO;
using System.Text;

namespace Redis.NET.Internal.IO
{
    class RedisReader
    {
        readonly Stream _stream;
        readonly RedisIO _io;

        public RedisReader(RedisIO io)
        {
            _stream = io.Stream;
            _io = io;
        }

        public RedisMessage ReadType()
        {
            RedisMessage type = (RedisMessage)_stream.ReadByte();
            return type == RedisMessage.Error ? throw new RedisException(ReadStatus(false)) : type;
        }

        public string ReadStatus(bool checkType = true)
        {
            if (checkType)
            {
                ExpectType(RedisMessage.Status);
            }

            return ReadLine();
        }

        public long ReadInt(bool checkType = true)
        {
            if (checkType)
            {
                ExpectType(RedisMessage.Int);
            }

            string line = ReadLine();
            return long.Parse(line.ToString());
        }

        public object ReadBulk(bool checkType = true, bool asString = false) => asString ? ReadBulkString(checkType) : (object)ReadBulkBytes(checkType);

        public byte[] ReadBulkBytes(bool checkType = true)
        {
            if (checkType)
            {
                ExpectType(RedisMessage.Bulk);
            }

            int size = (int)ReadInt(false);
            if (size == -1)
            {
                return null;
            }

            byte[] bulk = new byte[size];
            int bytes_read = 0;
            //UNUSED int bytes_remaining = size;

            while (bytes_read < size)
            {
                bytes_read += _stream.Read(bulk, bytes_read, size - bytes_read);
            }

            ExpectBytesRead(size, bytes_read);
            ReadCRLF();
            return bulk;
        }

        public void ReadBulkBytes(Stream destination, int bufferSize, bool checkType = true)
        {
            if (checkType)
            {
                ExpectType(RedisMessage.Bulk);
            }

            int size = (int) ReadInt(false);
            if (size == -1)
            {
                return;
            }

            byte[] buffer = new byte[bufferSize];
            int position = 0;
            while (position < size)
            {
                int bytes_to_buffer = Math.Min(buffer.Length, size - position);
                int bytes_read = 0;
                while (bytes_read < bytes_to_buffer)
                {
                    int bytes_to_read = Math.Min(bytes_to_buffer - bytes_read, size - position);
                    bytes_read += _stream.Read(buffer, bytes_read, bytes_to_read);
                }
                position += bytes_read;
                destination.Write(buffer, 0, bytes_read);
            }

            ExpectBytesRead(size, position);
            ReadCRLF();
        }

        public string ReadBulkString(bool checkType = true)
        {
            byte[] bulk = ReadBulkBytes(checkType);
            return bulk == null ? null : _io.Encoding.GetString(bulk);
        }

        public void ExpectType(RedisMessage expectedType)
        {
            RedisMessage type = ReadType();
            if ((int)type == -1)
            {
                throw new EndOfStreamException($"Unexpected end of stream; expected type '{expectedType}'");
            }

            if (type != expectedType)
            {
                throw new RedisProtocolException($"Unexpected response type: {type} (expecting {expectedType})");
            }
        }

        public void ExpectMultiBulk(long expectedSize)
        {
            ExpectType(RedisMessage.MultiBulk);
            ExpectSize(expectedSize);
        }

        public void ExpectSize(long expectedSize)
        {
            long size = ReadInt(false);
            ExpectSize(expectedSize, size);
        }

        public void ExpectSize(long expectedSize, long actualSize)
        {
            if (actualSize != expectedSize)
            {
                throw new RedisProtocolException($"Expected {expectedSize} elements; got {actualSize}");
            }
        }

        public void ReadCRLF() // TODO: remove hardcoded
        {
            var r = _stream.ReadByte();
            var n = _stream.ReadByte();
            if (r != 13 && n != 10)
            {
                throw new RedisProtocolException($"Expecting CRLF; got bytes: {r}, {n}");
            }
        }

        public object[] ReadMultiBulk(bool checkType = true, bool bulkAsString = false)
        {
            if (checkType)
            {
                ExpectType(RedisMessage.MultiBulk);
            }

            long count = ReadInt(false);
            if (count == -1)
            {
                return null;
            }

            object[] lines = new object[count];
            for (int i = 0; i < count; i++)
            {
                lines[i] = Read(bulkAsString);
            }

            return lines;
        }

        public object Read(bool bulkAsString = false)
        {
            RedisMessage type = ReadType();
            switch (type)
            {
                case RedisMessage.Bulk:
                    return ReadBulk(false, bulkAsString);

                case RedisMessage.Int:
                    return ReadInt(false);

                case RedisMessage.MultiBulk:
                    return ReadMultiBulk(false, bulkAsString);

                case RedisMessage.Status:
                    return ReadStatus(false);

                case RedisMessage.Error:
                    throw new RedisException(ReadStatus(false));

                default:
                    throw new RedisProtocolException($"Unsupported response type: {type}");
            }
        }

        string ReadLine()
        {
            StringBuilder sb = new StringBuilder();
            char c;
            bool should_break = false;
            while (true)
            {
                 c = (char)_stream.ReadByte();
                if (c == '\r') // TODO: remove hardcoded
                {
                    should_break = true;
                }
                else if (c == '\n' && should_break)
                {
                    break;
                }
                else
                {
                    sb.Append(c);
                    should_break = false;
                }
            }
            return sb.ToString();
        }

        void ExpectBytesRead(long expecting, long actual)
        {
            if (actual != expecting)
            {
                throw new RedisProtocolException($"Expecting {expecting} bytes; got {actual} bytes");
            }
        }
    }
}