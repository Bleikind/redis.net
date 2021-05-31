using Redis.NET.Internal.IO;
using System;
using System.IO;

namespace Redis.NET.Internal
{
    abstract class RedisListener<TResponse>
    {
        readonly RedisConnector _connection;

        public bool Listening { get; private set; }
        protected RedisConnector Connection { get => _connection; }

        public RedisListener(RedisConnector connection) => _connection = connection;

        protected void Listen(Func<RedisReader, TResponse> func)
        {
            Listening = true;
            do
            {
                try
                {
                    TResponse value = _connection.Read(func);
                    OnParsed(value);
                }
                catch (IOException)
                {
                    if (_connection.IsConnected)
                    {
                        throw;
                    }

                    break;
                }
            } while (Continue());
            Listening = false;
        }

        protected void Write<T>(RedisCommand<T> command) => _connection.Write(command);

        protected T Call<T>(RedisCommand<T> command) => _connection.Call(command);

        protected abstract void OnParsed(TResponse value);
        protected abstract bool Continue();
    }
}