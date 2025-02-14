﻿using System.Linq;
using System.Text;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace StackExchange.Redis.Tests
{
    [TestFixture]
    public class Keys : TestBase
    {
        [Test]
        public void TestScan()
        {
            using(var muxer = Create(allowAdmin: true))
            {
                const int Database = 4;
                var db = muxer.GetDatabase(Database);
                GetServer(muxer).FlushDatabase(flags: CommandFlags.FireAndForget);

                const int Count = 1000;
                for (int i = 0; i < Count; i++)
                    db.StringSet("x" + i, "y" + i, flags: CommandFlags.FireAndForget);

                var count = GetServer(muxer).Keys(Database).Count();
                ClassicAssert.AreEqual(Count, count);
            }
        }

        [Test]
        public void RandomKey()
        {
            using(var conn = Create(allowAdmin: true, syncTimeout: 60 * 1000))
            {
                var db = conn.GetDatabase();
                conn.GetServer(PrimaryServer, PrimaryPort).FlushDatabase();
                string anyKey = db.KeyRandom();

                ClassicAssert.IsNull(anyKey);
                db.StringSet("abc", "def");
                byte[] keyBytes = db.KeyRandom();

                ClassicAssert.AreEqual("abc", Encoding.UTF8.GetString(keyBytes));
            }
        }

        [Test]
        public void Zeros()
        {
            using(var conn = Create())
            {
                var db = conn.GetDatabase();
                db.KeyDelete("abc");
                db.StringSet("abc", 123);
                int k = (int)db.StringGet("abc");
                ClassicAssert.AreEqual(123, k);

                db.KeyDelete("abc");
                int i = (int)db.StringGet("abc");
                ClassicAssert.AreEqual(0, i);

                ClassicAssert.IsTrue(db.StringGet("abc").IsNull);
                int? value = (int?)db.StringGet("abc");
                ClassicAssert.IsFalse(value.HasValue);

            }
        }

        [Test]
        public void PrependAppend()
        {
            {
                // simple
                RedisKey key = "world";
                var ret = key.Prepend("hello");
                ClassicAssert.AreEqual("helloworld", (string)ret);
            }

            {
                RedisKey key1 = "world";
                RedisKey key2 = Encoding.UTF8.GetBytes("hello");
                var key3 = key1.Prepend(key2);
                ClassicAssert.IsTrue(object.ReferenceEquals(key1.KeyValue, key3.KeyValue));
                ClassicAssert.IsTrue(object.ReferenceEquals(key2.KeyValue, key3.KeyPrefix));
                ClassicAssert.AreEqual("helloworld", (string)key3);
            }

            {
                RedisKey key = "hello";
                var ret = key.Append("world");
                ClassicAssert.AreEqual("helloworld", (string)ret);
            }

            {
                RedisKey key1 = Encoding.UTF8.GetBytes("hello");
                RedisKey key2 = "world";
                var key3 = key1.Append(key2);
                ClassicAssert.IsTrue(object.ReferenceEquals(key2.KeyValue, key3.KeyValue));
                ClassicAssert.IsTrue(object.ReferenceEquals(key1.KeyValue, key3.KeyPrefix));
                ClassicAssert.AreEqual("helloworld", (string)key3);
            }
        }
    }
}
