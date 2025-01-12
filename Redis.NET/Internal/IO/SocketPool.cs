﻿using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace Redis.NET.Internal.IO
{
    class SocketPool : IDisposable
    {
        readonly EndPoint _endPoint;
        readonly ConcurrentStack<Socket> _pool;
        readonly int _max;

        public SocketPool(EndPoint endPoint, int max)
        {
            _max = max;
            _endPoint = endPoint;
            _pool = new ConcurrentStack<Socket>();
        }

        public Socket Connect()
        {
            Socket socket = Acquire();
            if (!socket.Connected)
            {
                socket.Connect(_endPoint);
            }

            return socket;
        }

        public bool ConnectAsync(SocketAsyncEventArgs connectArgs, out Socket socket)
        {
            socket = Acquire();
            return !socket.Connected && socket.ConnectAsync(connectArgs);
        }

        public void Release(Socket socket) => _pool.Push(socket);

        public void Dispose()
        {
            foreach (var socket in _pool)
            {
                System.Diagnostics.Debug.WriteLine($"Disposing socket #{socket.Handle}");
                socket.Dispose();
            }
        }

        Socket Acquire()
        {
            if (!_pool.TryPop(out Socket socket))
            {
                Add();
                return Acquire();
            }
            else if (socket.IsBound && !socket.Connected)
            {
                socket.Dispose();
                return Acquire();
            }
            else if (socket.Poll(1000, SelectMode.SelectRead))
            {
                socket.Dispose();
                return Acquire();
            }

            return socket;
        }

        void Add()
        {
            if (_pool.Count > _max)
            {
                throw new InvalidOperationException("Maximum sockets");
            }

            _pool.Push(SocketFactory());
        }

        Socket SocketFactory()
        {
            System.Diagnostics.Debug.WriteLine("NEW SOCKET");
            return new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        }
    }
}