using System;
using System.Linq;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace StackExchange.Redis.Tests
{
    [TestFixture]
    public class PubSubCommand : TestBase
    {
        [Test]
        public void SubscriberCount()
        {
            using(var conn = Create())
            {
                RedisChannel channel = Me() + Guid.NewGuid();
                var server = conn.GetServer(conn.GetEndPoints()[0]);

                var channels = server.SubscriptionChannels(Me() + "*");
                ClassicAssert.IsFalse(channels.Contains(channel));

                long justWork = server.SubscriptionPatternCount();
                var count = server.SubscriptionSubscriberCount(channel);
                ClassicAssert.AreEqual(0, count);
                conn.GetSubscriber().Subscribe(channel, delegate { });
                count = server.SubscriptionSubscriberCount(channel);
                ClassicAssert.AreEqual(1, count);

                channels = server.SubscriptionChannels(Me() + "*");
                ClassicAssert.IsTrue(channels.Contains(channel));
            }
        }
    }
}
