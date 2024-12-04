using System.Linq;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace StackExchange.Redis.Tests
{
    public class Migrate : TestBase
    {
        public void Basic()
        {
            var fromConfig = new ConfigurationOptions { EndPoints = { { PrimaryServer, SecurePort } }, Password = SecurePassword };
            var toConfig = new ConfigurationOptions { EndPoints = { { PrimaryServer, PrimaryPort } } };
            using (var from = ConnectionMultiplexer.Connect(fromConfig))
            using (var to = ConnectionMultiplexer.Connect(toConfig))
            {
                RedisKey key = Me();
                var fromDb = from.GetDatabase();
                var toDb = to.GetDatabase();
                fromDb.KeyDelete(key);
                toDb.KeyDelete(key);
                fromDb.StringSet(key, "foo");
                var dest = to.GetEndPoints(true).Single();
                fromDb.KeyMigrate(key, dest);
                ClassicAssert.IsFalse(fromDb.KeyExists(key));
                ClassicAssert.IsTrue(toDb.KeyExists(key));
                string s = toDb.StringGet(key);
                ClassicAssert.AreEqual("foo", s);
            }
        }
    }
}
