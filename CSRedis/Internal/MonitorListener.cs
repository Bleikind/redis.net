using Redis.NET.Event;
using System;

namespace Redis.NET.Internal
{
    class MonitorListener : RedisListener<object>
    {
        public event EventHandler<RedisMonitorEventArgs> MonitorReceived;

        public MonitorListener(RedisConnector connection) : base(connection)
        { }

        public string Start()
        {
            string status = Call(RedisCommands.Monitor());
            Listen(x => x.Read());
            return status;
        }

        protected override void OnParsed(object value) => OnMonitorReceived(value);

        protected override bool Continue() => Connection.IsConnected;

        void OnMonitorReceived(object message) => MonitorReceived?.Invoke(this, new RedisMonitorEventArgs(message));
    }
}