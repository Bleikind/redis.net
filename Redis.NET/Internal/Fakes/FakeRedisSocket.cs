﻿using Redis.NET.Internal.IO;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Redis.NET.Internal.Fakes
{
    internal class FakeRedisSocket : IRedisSocket
    {
        bool _connected;
        readonly FakeStream _stream;

        public bool Connected { get { return _connected; } }

        public int ReceiveTimeout { get; set; }

        public int SendTimeout { get; set; }

        public FakeRedisSocket(params string[] responses) : this(Encoding.UTF8, responses)
        { }

        public FakeRedisSocket(Encoding encoding, params string[] responses) : this(ToBytes(encoding, responses))
        { }

        public FakeRedisSocket(params byte[][] responses)
        {
            _stream = new FakeStream();

            foreach (var response in responses)
            {
                _stream.AddResponse(response);
            }
        }

        public void Connect(EndPoint endpoint) => _connected = true;

        public bool ConnectAsync(SocketAsyncEventArgs args) => false;

        public bool SendAsync(SocketAsyncEventArgs args)
        {
            _stream.Write(args.Buffer, args.Offset, args.Count);
            return false;
        }

        public Stream GetStream() => _stream;

        public void Dispose()
        {
            _stream.Dispose();
            _connected = false;
        }

        public string GetMessage() => GetMessage(Encoding.UTF8);
        public string GetMessage(Encoding encoding) => encoding.GetString(_stream.GetMessage());

        static byte[][] ToBytes(Encoding encoding, string[] strings)
        {
            var set = new byte[strings.Length][];
            for (var i = 0; i < strings.Length; i++)
            {
                set[i] = encoding.GetBytes(strings[i]);
            }

            return set;
        }
    }
}