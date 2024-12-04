using System.Linq;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace StackExchange.Redis.Tests
{
    [TestFixture]
    public class Lists : TestBase
    {
        [Test]
        public void Ranges()
        {
            using(var conn = Create())
            {
                var db = conn.GetDatabase();
                RedisKey key = Me();

                db.KeyDelete(key, CommandFlags.FireAndForget);
                db.ListRightPush(key, "abcdefghijklmnopqrstuvwxyz".Select(x => (RedisValue)x.ToString()).ToArray());
                
                ClassicAssert.AreEqual(26, db.ListLength(key));
                ClassicAssert.AreEqual("abcdefghijklmnopqrstuvwxyz", string.Concat(db.ListRange(key)));

                var last10 = db.ListRange(key, -10, -1);
                ClassicAssert.AreEqual("qrstuvwxyz", string.Concat(last10));
                db.ListTrim(key, 0, -11);

                ClassicAssert.AreEqual(16, db.ListLength(key));
                ClassicAssert.AreEqual("abcdefghijklmnop", string.Concat(db.ListRange(key)));



            }
        }
    }
}
