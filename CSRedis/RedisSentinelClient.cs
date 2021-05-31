using Redis.NET.Event;
using Redis.NET.Internal;
using Redis.NET.Internal.IO;
using System;
using System.Net;
using System.Text;

namespace Redis.NET
{
    /// <summary>
    /// Represents a client connection to a Redis sentinel instance
    /// </summary>
    public partial class RedisSentinelClient : IDisposable
    {
        /// <summary>
        /// Default port setting
        /// </summary>
        public static int DefaultPort { get; } = 26379;

        /// <summary>
        /// Default ssl setting
        /// </summary>
        public static bool DefaultSSL { get; } = false;

        /// <summary>
        /// Default concurrency value
        /// </summary>
        public static int DefaultConcurrency { get; } = 1000;

        /// <summary>
        /// Default buffer size
        /// </summary>
        public static int DefaultBufferSize { get; } = 1024;

        readonly RedisConnector _connector;
        readonly SubscriptionListener _subscription;

        /// <summary>
        /// Occurs when a subscription message is received
        /// </summary>
        public event EventHandler<RedisSubscriptionReceivedEventArgs> SubscriptionReceived;

        /// <summary>
        /// Occurs when a subscription channel is added or removed
        /// </summary>
        public event EventHandler<RedisSubscriptionChangedEventArgs> SubscriptionChanged;

        /// <summary>
        /// Occurs when the connection has sucessfully reconnected
        /// </summary>
        public event EventHandler Reconnected;

        /// <summary>
        /// Get the Redis sentinel hostname
        /// </summary>
        public string Host { get { return GetHost(); } }

        /// <summary>
        /// Get the Redis sentinel port
        /// </summary>
        public int Port { get { return GetPort(); } }

        /// <summary>
        /// Get a value indicating whether the Redis sentinel client is connected to the server
        /// </summary>
        public bool Connected { get { return _connector.IsConnected; } }

        /// <summary>
        /// Get the string encoding used to communicate with the server
        /// </summary>
        public Encoding Encoding { get { return _connector.Encoding; } }

        /// <summary>
        /// Get or set the connection read timeout (milliseconds)
        /// </summary>
        public int ReceiveTimeout
        {
            get { return _connector.ReceiveTimeout; }
            set { _connector.ReceiveTimeout = value; }
        }

        /// <summary>
        /// Get or set the connection send timeout (milliseconds)
        /// </summary>
        public int SendTimeout
        {
            get { return _connector.SendTimeout; }
            set { _connector.SendTimeout = value; }
        }

        /// <summary>
        /// Get or set the number of times to attempt a reconnect after a connection fails
        /// </summary>
        public int ReconnectAttempts
        {
            get { return _connector.ReconnectAttempts; }
            set { _connector.ReconnectAttempts = value; }
        }

        /// <summary>
        /// Get or set the amount of time to wait between reconnect attempts
        /// </summary>
        public int ReconnectWait
        {
            get { return _connector.ReconnectWait; }
            set { _connector.ReconnectWait = value; }
        }

        /// <summary>
        /// Create a new RedisSentinelClient using default port and encoding
        /// </summary>
        /// <param name="host">Redis sentinel hostname</param>
        public RedisSentinelClient(string host)
            : this(host, DefaultPort)
        { }

        /// <summary>
        /// Create a new RedisSentinelClient using default encoding
        /// </summary>
        /// <param name="host">Redis sentinel hostname</param>
        /// <param name="port">Redis sentinel port</param>
        public RedisSentinelClient(string host, int port)
            : this(host, port, DefaultSSL)
        { }

        /// <summary>
        /// Create a new RedisSentinelClient using default encoding
        /// </summary>
        /// <param name="host">Redis sentinel hostname</param>
        /// <param name="port">Redis sentinel port</param>
        /// <param name="ssl">Set to true if remote Redis server expects SSL</param>
        public RedisSentinelClient(string host, int port, bool ssl)
            : this(new RedisSocket(ssl), new DnsEndPoint(host, port), DefaultConcurrency, DefaultBufferSize)
        { }

        internal RedisSentinelClient(IRedisSocket socket, EndPoint endpoint)
            : this(socket, endpoint, DefaultConcurrency, DefaultBufferSize)
        { }

        internal RedisSentinelClient(IRedisSocket socket, EndPoint endpoint, int concurrency, int bufferSize)
        {
            _connector = new RedisConnector(endpoint, socket, concurrency, bufferSize);
            _subscription = new SubscriptionListener(_connector);

            _subscription.MessageReceived += OnSubscriptionReceived;
            _subscription.Changed += OnSubscriptionChanged;
            _connector.Connected += OnConnectionReconnected;
        }

        /// <summary>
        /// Release resoures used by the current RedisSentinelClient
        /// </summary>
        public void Dispose()
        {
            if (_connector != null)
            {
                _connector.Dispose();
            }
        }

        void OnSubscriptionReceived(object sender, RedisSubscriptionReceivedEventArgs args) => SubscriptionReceived?.Invoke(this, args);

        void OnSubscriptionChanged(object sender, RedisSubscriptionChangedEventArgs args) => SubscriptionChanged?.Invoke(this, args);

        void OnConnectionReconnected(object sender, EventArgs args) => Reconnected?.Invoke(this, args);

        string GetHost()
        {
            switch (_connector.EndPoint)
            {
                case IPEndPoint _:
                    return (_connector.EndPoint as IPEndPoint).Address.ToString();
                case DnsEndPoint _:
                    return (_connector.EndPoint as DnsEndPoint).Host;
                default:
                    return null;
            }
        }

        int GetPort()
        {
            switch (_connector.EndPoint)
            {
                case IPEndPoint _:
                    return (_connector.EndPoint as IPEndPoint).Port;
                case DnsEndPoint _:
                    return (_connector.EndPoint as DnsEndPoint).Port;
                default:
                    return -1;
            }
        }
    }
}