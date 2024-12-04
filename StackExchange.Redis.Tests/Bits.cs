using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace StackExchange.Redis.Tests
{
    [TestFixture]
    public class Bits : TestBase
    {
        [Test]
        public void BasicOps()
        {
            using (var conn = Create())
            {
                var db = conn.GetDatabase();
                RedisKey key = Me();

                db.KeyDelete(key, CommandFlags.FireAndForget);
                db.StringSetBit(key, 10, true);
                ClassicAssert.True(db.StringGetBit(key, 10));
                ClassicAssert.False(db.StringGetBit(key, 11));
            }
        }
    }
}