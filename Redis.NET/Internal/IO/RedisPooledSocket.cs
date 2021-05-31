using System.IO;
using System.Net;
using System.Net.Sockets;

namespace Redis.NET.Internal.IO
{
    class RedisPooledSocket : IRedisSocket
    {
        Socket _socket;
        readonly SocketPool _pool;

        public bool Connected { get { return _socket != null && _socket.Connected; } }

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

        public RedisPooledSocket(SocketPool pool) => _pool = pool;

        public void Connect(EndPoint endpoint)
        {
            _socket = _pool.Connect();
            System.Diagnostics.Debug.WriteLine($"Got socket #{ _socket.Handle}");
        }

        public bool ConnectAsync(SocketAsyncEventArgs args) => _pool.ConnectAsync(args, out _socket);

        public bool SendAsync(SocketAsyncEventArgs args) => _socket.SendAsync(args);

        public Stream GetStream() => new NetworkStream(_socket);

        public void Dispose() => _pool.Release(_socket);
    }
}