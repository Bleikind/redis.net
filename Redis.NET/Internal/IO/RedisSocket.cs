﻿using System;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;

namespace Redis.NET.Internal.IO
{
    class RedisSocket : IRedisSocket
    {
        readonly bool _ssl;
        Socket _socket;
        EndPoint _remote;

        public bool Connected { get => _socket != null && _socket.Connected; }

        public int ReceiveTimeout
        {
            get => _socket.ReceiveTimeout;
            set => _socket.ReceiveTimeout = value;
        }

        public int SendTimeout
        {
            get => _socket.SendTimeout;
            set => _socket.SendTimeout = value;
        }

        public RedisSocket(bool ssl) => _ssl = ssl;

        public void Connect(EndPoint endpoint)
        {
            InitSocket(endpoint);
            _socket.Connect(endpoint);
        }

        public bool ConnectAsync(SocketAsyncEventArgs args)
        {
            InitSocket(args.RemoteEndPoint);
            return _socket.ConnectAsync(args);
        }

        public bool SendAsync(SocketAsyncEventArgs args) => _socket.SendAsync(args);

        public Stream GetStream()
        {
            var netStream = new NetworkStream(_socket);

            if (!_ssl)
            {
                return netStream;
            }

            var sslStream = new SslStream(netStream, true);
            sslStream.AuthenticateAsClient(GetHostForAuthentication());
            return sslStream;
        }

        public void Dispose() => _socket.Dispose();

        void InitSocket(EndPoint endpoint)
        {
            if (_socket != null)
            {
                _socket.Dispose();
            }

            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _remote = endpoint;
        }

        string GetHostForAuthentication()
        {
            switch (_remote)
            {
                case null:
                    throw new ArgumentNullException("Remote endpoint is not set");
                case DnsEndPoint _:
                    return (_remote as DnsEndPoint).Host;
                case IPEndPoint _:
                    return (_remote as IPEndPoint).Address.ToString();
            }

            throw new InvalidOperationException("Cannot get remote host");
        }
    }
}