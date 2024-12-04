using System;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace StackExchange.Redis.Tests.Issues
{
    [TestFixture]
    public class SO24807536 : TestBase
    {
        public void Exec()
        {
            var key = Me();
            using(var conn = Create())
            {
                var cache = conn.GetDatabase();

                // setup some data
                cache.KeyDelete(key);
                cache.HashSet(key, "full", "some value");
                cache.KeyExpire(key, TimeSpan.FromSeconds(3));

                // test while exists
                var exists = cache.KeyExists(key);
                var ttl = cache.KeyTimeToLive(key);
                var fullWait = cache.HashGetAsync(key, "full", flags: CommandFlags.None);
                ClassicAssert.IsTrue(exists, "key exists");
                ClassicAssert.IsNotNull(ttl, "ttl");
                ClassicAssert.AreEqual("some value", (string)fullWait.Result);

                // wait for expiry
                Thread.Sleep(TimeSpan.FromSeconds(4));

                // test once expired
                exists = cache.KeyExists(key);
                ttl = cache.KeyTimeToLive(key);
                fullWait = cache.HashGetAsync(key, "full", flags: CommandFlags.None);                
                ClassicAssert.IsFalse(exists, "key exists");
                ClassicAssert.IsNull(ttl, "ttl");
                ClassicAssert.IsNull((string)fullWait.Result);
            }
        }
    }
}
