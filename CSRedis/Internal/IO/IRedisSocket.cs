using System;
using System.IO;
using System.Net;
using System.Net.Sockets;

namespace Redis.NET.Internal.IO
{
    interface IRedisSocket : IDisposable
    {
        bool Connected { get; }
        int ReceiveTimeout { get; set; }
        int SendTimeout { get; set; }
        void Connect(EndPoint endpoint);
        bool ConnectAsync(SocketAsyncEventArgs args);
        bool SendAsync(SocketAsyncEventArgs args);
        Stream GetStream();
    }
}