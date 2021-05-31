using Redis.NET.Internal.IO;
using System;
using System.IO;
using System.Net;
using System.Net.Sockets;

namespace Redis.NET
{
    /// <summary>
    /// Represents a pooled collection of Redis connections
    /// </summary>
    public class RedisConnectionPool : IDisposable
    {
        readonly EndPoint _endPoint;
        readonly SocketPool _pool;

        /// <summary>
        /// Create a new connection pool
        /// </summary>
        /// <param name="host">Redis server host</param>
        /// <param name="port">Redis server port</param>
        /// <param name="max">Maximum simultaneous connections</param>
        public RedisConnectionPool(string host, int port, int max) : this(new DnsEndPoint(host, port), max)
        { }

        /// <summary>
        /// Create a new connection pool
        /// </summary>
        /// <param name="endPoint">Redis server</param>
        /// <param name="max">Maximum simultaneous connections</param>
        
        public RedisConnectionPool(EndPoint endPoint, int max)
        {
            _pool = new SocketPool(endPoint, max);
            _endPoint = endPoint;
        }

        /// <summary>
        /// Get a pooled Redis Client instance
        /// </summary>
        /// <param name="asyncConcurrency">Max concurrent threads (default 1000)</param>
        /// <param name="asyncBufferSize">Async thread buffer size (default 10240 bytes)</param>
        /// <returns>RedisClient instance from pool</returns>
        public RedisClient GetClient(int asyncConcurrency, int asyncBufferSize) => new RedisClient(new RedisPooledSocket(_pool), _endPoint, asyncConcurrency, asyncBufferSize);

        /// <summary>
        /// Get a pooled Redis Client instance
        /// </summary>
        /// <returns>RedisClient instance from pool</returns>
        public RedisClient GetClient() => new RedisClient(new RedisPooledSocket(_pool), _endPoint);

        /// <summary>
        /// Get a pooled Sentinel Client instance
        /// </summary>
        /// <param name="asyncConcurrency">Max concurrent threads (default 1000)</param>
        /// <param name="asyncBufferSize">Async thread buffer size (default 10240 bytes)</param>
        /// <returns>Sentinel Client from pool</returns>
        public RedisSentinelClient GetSentinelClient(int asyncConcurrency, int asyncBufferSize) => new RedisSentinelClient(new RedisPooledSocket(_pool), _endPoint, asyncConcurrency, asyncBufferSize);

        /// <summary>
        /// Get a pooled Sentinel Client instance
        /// </summary>
        /// <returns>Sentinel Client from pool</returns>
        public RedisSentinelClient GetSentinelClient() => new RedisSentinelClient(new RedisPooledSocket(_pool), _endPoint);

        /// <summary>
        /// Close all open pooled connections
        /// </summary>
        public void Dispose() => _pool.Dispose();
    }

    class RedisPooledSocket : IRedisSocket
    {
        Socket _socket;
        readonly SocketPool _pool;

        public bool Connected { get { return _socket != null && _socket.Connected; } }

        public int ReceiveTimeout
        {
            get { return _socket.ReceiveTimeout; }
            set { _socket.ReceiveTimeout = value; }
        }

        public int SendTimeout
        {
            get { return _socket.SendTimeout; }
            set { _socket.SendTimeout = value; }
        }

        public RedisPooledSocket(SocketPool pool) => _pool = pool;

        public void Connect(EndPoint endpoint)
        {
            _socket = _pool.Connect();
            System.Diagnostics.Debug.WriteLine($"Got socket #{_socket.Handle}");
        }

        public bool ConnectAsync(SocketAsyncEventArgs args) => _pool.ConnectAsync(args, out _socket);

        public bool SendAsync(SocketAsyncEventArgs args) => _socket.SendAsync(args);

        public Stream GetStream() => new NetworkStream(_socket);

        public void Dispose() => _pool.Release(_socket);
    }
}