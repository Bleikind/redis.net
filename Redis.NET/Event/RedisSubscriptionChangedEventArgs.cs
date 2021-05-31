using System;

namespace Redis.NET.Event
{
    /// <summary>
    /// Provides data for the event that is raised when a subscription channel is opened or closed
    /// </summary>
    public class RedisSubscriptionChangedEventArgs : EventArgs
    {
        /// <summary>
        /// The subscription response
        /// </summary>
        public RedisSubscriptionChannel Response { get; private set; }

        internal RedisSubscriptionChangedEventArgs(RedisSubscriptionChannel response)
        {
            Response = response;
        }
    }
}
