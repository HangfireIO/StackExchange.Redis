using System;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace StackExchange.Redis.Tests.Issues
{
    [TestFixture]
    public class SO25113323 : TestBase
    {
        [Test]
        public void SetExpirationToPassed()
        {
            var key = Me();
            using (var conn =  Create())
            {
                // Given
                var cache = conn.GetDatabase();
                cache.KeyDelete(key);
                cache.HashSet(key, "full", "test", When.NotExists, CommandFlags.PreferMaster);

                Thread.Sleep(10 * 1000);

                // When
                var expiresOn = DateTime.UtcNow.AddSeconds(-10);

                var firstResult = cache.KeyExpire(key, expiresOn, CommandFlags.PreferMaster);
                var secondResult = cache.KeyExpire(key, expiresOn, CommandFlags.PreferMaster);
                var exists = cache.KeyExists(key);
                var ttl = cache.KeyTimeToLive(key);

                // Then
                ClassicAssert.IsTrue(firstResult, "first"); // could set the first time, but this nukes the key
                ClassicAssert.IsFalse(secondResult, "second"); // can't set, since nuked
                ClassicAssert.IsFalse(exists, "exists"); // does not exist since nuked
                ClassicAssert.IsNull(ttl, "ttl"); // no expiry since nuked
            }
        }
    }
}
