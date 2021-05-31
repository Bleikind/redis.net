using Redis.NET.Event;
using Redis.NET.Internal.Commands;
using System;

namespace Redis.NET.Internal
{
    class SubscriptionListener : RedisListener<RedisSubscriptionResponse>
    {
        long _count;

        public event EventHandler<RedisSubscriptionReceivedEventArgs> MessageReceived;
        public event EventHandler<RedisSubscriptionChangedEventArgs> Changed;

        public SubscriptionListener(RedisConnector connection) : base(connection)
        { }

        public void Send(RedisSubscription command)
        {
            Write(command);
            if (!Listening)
            {
                Listen(command.Parse);
            }
        }

        protected override void OnParsed(RedisSubscriptionResponse response)
        {
            if (response is RedisSubscriptionChannel)
            {
                OnReceivedChannel(response as RedisSubscriptionChannel);
            }
            else if (response is RedisSubscriptionMessage)
            {
                OnReceivedMessage(response as RedisSubscriptionMessage);
            }
        }

        protected override bool Continue() => _count > 0;

        void OnReceivedChannel(RedisSubscriptionChannel channel)
        {
            _count = channel.Count;
            Changed?.Invoke(this, new RedisSubscriptionChangedEventArgs(channel));
        }

        void OnReceivedMessage(RedisSubscriptionMessage message) => MessageReceived?.Invoke(this, new RedisSubscriptionReceivedEventArgs(message));
    }
}