using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading;

namespace Redis.NET.Internal.IO
{
    class SocketAsyncPool : IDisposable
    {
        readonly byte[] _buffer;
        readonly Stack<SocketAsyncEventArgs> _pool;
        readonly Semaphore _acquisitionGate;

        public event EventHandler<SocketAsyncEventArgs> Completed;

        public SocketAsyncPool(int concurrency, int bufferSize)
        {
            _pool = new Stack<SocketAsyncEventArgs>();
            _buffer = new byte[concurrency * bufferSize];
            _acquisitionGate = new Semaphore(concurrency, concurrency);

            for (int i = 0; i < _buffer.Length; i += bufferSize)
            {
                var args = new SocketAsyncEventArgs();
                args.Completed += OnSocketCompleted;
                args.SetBuffer(_buffer, i, bufferSize);
                _pool.Push(args);
            }
        }

        public SocketAsyncEventArgs Acquire()
        {
            if (!_acquisitionGate.WaitOne())
            {
                throw new Exception();
            }

            lock (_pool)
            {
                return _pool.Pop();
            }
        }

        public void Release(SocketAsyncEventArgs args)
        {
            lock (_pool)
            {
                if (args.Buffer.Equals(_buffer))
                {
                    _pool.Push(args);
                }
                else
                {
                    args.Dispose();
                }
            }
            _acquisitionGate.Release();
        }

        public void Dispose()
        {
            Array.Clear(_buffer, 0, _buffer.Length);
            for (int i = 0; i < _pool.Count; i++)
            {
                _pool.Pop().Dispose();
            }
        }

        void OnSocketCompleted(object sender, SocketAsyncEventArgs e) => Completed?.Invoke(sender, e);
    }
}