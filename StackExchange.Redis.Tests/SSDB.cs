using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace StackExchange.Redis.Tests
{
    [TestFixture, Ignore("No such machine available")]
    public class SSDB : TestBase
    {
        [Test]
        public void ConnectToSSDB()
        {
            var config = new ConfigurationOptions
            {
                EndPoints = { { "ubuntu", 8888 } },
                CommandMap = CommandMap.SSDB
            };
            RedisKey key = Me();
            using (var conn = ConnectionMultiplexer.Connect(config))
            {
                var db = conn.GetDatabase(0);
                db.KeyDelete(key);
                ClassicAssert.IsTrue(db.StringGet(key).IsNull);
                db.StringSet(key, "abc");
                ClassicAssert.AreEqual("abc", (string)db.StringGet(key));
            }
        }
    }
}
