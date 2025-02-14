﻿using System.Linq;
using System.Text;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Tests
{
    [TestFixture]
    public class Strings // http://redis.io/commands#string
    {
        [Test]
        public void Append()
        {
            using (var muxer = Config.GetUnsecuredConnection(waitForOpen: true))
            {
                var conn = muxer.GetDatabase(2);
                var server = Config.GetServer(muxer);
                conn.KeyDelete("append");
                var l0 = server.Features.StringLength ? conn.StringLengthAsync("append") : null;

                var s0 = conn.StringGetAsync("append");

                conn.StringSetAsync("append", "abc");
                var s1 = conn.StringGetAsync("append");
                var l1 = server.Features.StringLength ? conn.StringLengthAsync("append") : null;

                var result = conn.StringAppendAsync("append", Encode("defgh"));
                var s3 = conn.StringGetAsync("append");
                var l2 = server.Features.StringLength ? conn.StringLengthAsync("append") : null;

                ClassicAssert.AreEqual(null, (string)conn.Wait(s0));
                ClassicAssert.AreEqual("abc", (string)conn.Wait(s1));
                ClassicAssert.AreEqual(8, conn.Wait(result));
                ClassicAssert.AreEqual("abcdefgh", (string)conn.Wait(s3));

                if (server.Features.StringLength)
                {
                    ClassicAssert.AreEqual(0, conn.Wait(l0));
                    ClassicAssert.AreEqual(3, conn.Wait(l1));
                    ClassicAssert.AreEqual(8, conn.Wait(l2));
                }
            }
        }
        [Test]
        public void Set()
        {
            using (var muxer = Config.GetUnsecuredConnection())
            {
                var conn = muxer.GetDatabase(2);
                conn.KeyDeleteAsync("set");

                conn.StringSetAsync("set", "abc");
                var v1 = conn.StringGetAsync("set");

                conn.StringSetAsync("set", Encode("def"));
                var v2 = conn.StringGetAsync("set");

                ClassicAssert.AreEqual("abc", (string)conn.Wait(v1));
                ClassicAssert.AreEqual("def", (string)Decode(conn.Wait(v2)));
            }
        }

        [Test]
        public void SetNotExists()
        {
            using (var muxer = Config.GetUnsecuredConnection())
            {
                var conn = muxer.GetDatabase(2);
                conn.KeyDeleteAsync("set");
                conn.KeyDeleteAsync("set2");
                conn.KeyDeleteAsync("set3");
                conn.StringSetAsync("set", "abc");

                var x0 = conn.StringSetAsync("set", "def", when: When.NotExists);
                var x1 = conn.StringSetAsync("set", Encode("def"), when: When.NotExists);
                var x2 = conn.StringSetAsync("set2", "def", when: When.NotExists);
                var x3 = conn.StringSetAsync("set3", Encode("def"), when: When.NotExists);

                var s0 = conn.StringGetAsync("set");
                var s2 = conn.StringGetAsync("set2");
                var s3 = conn.StringGetAsync("set3");

                ClassicAssert.IsFalse(conn.Wait(x0));
                ClassicAssert.IsFalse(conn.Wait(x1));
                ClassicAssert.IsTrue(conn.Wait(x2));
                ClassicAssert.IsTrue(conn.Wait(x3));
                ClassicAssert.AreEqual("abc", (string)conn.Wait(s0));
                ClassicAssert.AreEqual("def", (string)conn.Wait(s2));
                ClassicAssert.AreEqual("def", (string)conn.Wait(s3));
            }
        }

        [Test]
        public void Ranges()
        {
            using (var muxer = Config.GetUnsecuredConnection(waitForOpen: true))
            {
                if (!Config.GetFeatures(muxer).StringSetRange) ClassicAssert.Inconclusive();
                var conn = muxer.GetDatabase(2);

                conn.KeyDeleteAsync("range");

                conn.StringSetAsync("range", "abcdefghi");
                conn.StringSetRangeAsync("range", 2, "xy");
                conn.StringSetRangeAsync("range", 4, Encode("z"));

                var val = conn.StringGetAsync("range");

                ClassicAssert.AreEqual("abxyzfghi", (string)conn.Wait(val));
            }
        }

        [Test]
        public void IncrDecr()
        {
            using (var muxer = Config.GetUnsecuredConnection())
            {
                var conn = muxer.GetDatabase(2);
                conn.KeyDeleteAsync("incr");

                conn.StringSetAsync("incr", "2");
                var v1 = conn.StringIncrementAsync("incr");
                var v2 = conn.StringIncrementAsync("incr", 5);
                var v3 = conn.StringIncrementAsync("incr", -2);
                var v4 = conn.StringDecrementAsync("incr");
                var v5 = conn.StringDecrementAsync("incr", 5);
                var v6 = conn.StringDecrementAsync("incr", -2);
                var s = conn.StringGetAsync("incr");

                ClassicAssert.AreEqual(3, conn.Wait(v1));
                ClassicAssert.AreEqual(8, conn.Wait(v2));
                ClassicAssert.AreEqual(6, conn.Wait(v3));
                ClassicAssert.AreEqual(5, conn.Wait(v4));
                ClassicAssert.AreEqual(0, conn.Wait(v5));
                ClassicAssert.AreEqual(2, conn.Wait(v6));
                ClassicAssert.AreEqual("2", (string)conn.Wait(s));
            }
        }
        [Test]
        public void IncrDecrFloat()
        {
            using (var muxer = Config.GetUnsecuredConnection(waitForOpen: true))
            {
                if (!Config.GetFeatures(muxer).IncrementFloat) ClassicAssert.Inconclusive();
                var conn = muxer.GetDatabase(2);
                conn.KeyDelete("incr");

                conn.StringSetAsync("incr", "2");
                var v1 = conn.StringIncrementAsync("incr", 1.1);
                var v2 = conn.StringIncrementAsync("incr", 5.0);
                var v3 = conn.StringIncrementAsync("incr", -2.0);
                var v4 = conn.StringIncrementAsync("incr", -1.0);
                var v5 = conn.StringIncrementAsync("incr", -5.0);
                var v6 = conn.StringIncrementAsync("incr", 2.0);

                var s = conn.StringGetAsync("incr");

                Config.AssertNearlyEqual(3.1, conn.Wait(v1));
                Config.AssertNearlyEqual(8.1, conn.Wait(v2));
                Config.AssertNearlyEqual(6.1, conn.Wait(v3));
                Config.AssertNearlyEqual(5.1, conn.Wait(v4));
                Config.AssertNearlyEqual(0.1, conn.Wait(v5));
                Config.AssertNearlyEqual(2.1, conn.Wait(v6));
                ClassicAssert.AreEqual("2.1", (string)conn.Wait(s));
            }
        }

        [Test]
        public void GetRange()
        {
            using (var muxer = Config.GetUnsecuredConnection(waitForOpen: true))
            {
                var conn = muxer.GetDatabase(2);
                conn.KeyDeleteAsync("range");

                conn.StringSetAsync("range", "abcdefghi");
                var s = conn.StringGetRangeAsync("range", 2, 4);
                var b = conn.StringGetRangeAsync("range", 2, 4);

                ClassicAssert.AreEqual("cde", (string)conn.Wait(s));
                ClassicAssert.AreEqual("cde", Decode(conn.Wait(b)));
            }
        }

        [Test]
        public void BitCount()
        {
            using (var muxer = Config.GetUnsecuredConnection(waitForOpen: true))
            {
                if (!Config.GetFeatures(muxer).BitwiseOperations) ClassicAssert.Inconclusive();
                
                var conn = muxer.GetDatabase(0);
                conn.StringSetAsync("mykey", "foobar");
                var r1 = conn.StringBitCountAsync("mykey");
                var r2 = conn.StringBitCountAsync("mykey", 0, 0);
                var r3 = conn.StringBitCountAsync("mykey", 1, 1);

                ClassicAssert.AreEqual(26, conn.Wait(r1));
                ClassicAssert.AreEqual(4, conn.Wait(r2));
                ClassicAssert.AreEqual(6, conn.Wait(r3));
            }
        }

        [Test]
        public void BitOp()
        {
            using (var muxer = Config.GetUnsecuredConnection(waitForOpen: true))
            {
                if (!Config.GetFeatures(muxer).BitwiseOperations) ClassicAssert.Inconclusive();
                var conn = muxer.GetDatabase(0);
                conn.StringSetAsync("key1", new byte[] { 3 });
                conn.StringSetAsync("key2", new byte[] { 6 });
                conn.StringSetAsync("key3", new byte[] { 12 });

                var len_and = conn.StringBitOperationAsync(Bitwise.And, "and", new RedisKey[] { "key1", "key2", "key3" });
                var len_or = conn.StringBitOperationAsync(Bitwise.Or, "or", new RedisKey[] { "key1", "key2", "key3" });
                var len_xor = conn.StringBitOperationAsync(Bitwise.Xor, "xor", new RedisKey[] { "key1", "key2", "key3" });
                var len_not = conn.StringBitOperationAsync(Bitwise.Not, "not", "key1");

                ClassicAssert.AreEqual(1, conn.Wait(len_and));
                ClassicAssert.AreEqual(1, conn.Wait(len_or));
                ClassicAssert.AreEqual(1, conn.Wait(len_xor));
                ClassicAssert.AreEqual(1, conn.Wait(len_not));

                var r_and = ((byte[])conn.Wait(conn.StringGetAsync("and"))).Single();
                var r_or = ((byte[])conn.Wait(conn.StringGetAsync("or"))).Single();
                var r_xor = ((byte[])conn.Wait(conn.StringGetAsync("xor"))).Single();
                var r_not = ((byte[])conn.Wait(conn.StringGetAsync("not"))).Single();

                ClassicAssert.AreEqual((byte)(3 & 6 & 12), r_and);
                ClassicAssert.AreEqual((byte)(3 | 6 | 12), r_or);
                ClassicAssert.AreEqual((byte)(3 ^ 6 ^ 12), r_xor);
                ClassicAssert.AreEqual(unchecked((byte)(~3)), r_not);

            }

        }

        [Test]
        public void RangeString()
        {
            using (var muxer = Config.GetUnsecuredConnection())
            {
                var conn = muxer.GetDatabase(0);
                conn.StringSetAsync("my key", "hello world");
                var result = conn.StringGetRangeAsync("my key", 2, 6);
                ClassicAssert.AreEqual("llo w", (string)conn.Wait(result));
            }
        }
        static byte[] Encode(string value) { return Encoding.UTF8.GetBytes(value); }
        static string Decode(byte[] value) { return Encoding.UTF8.GetString(value); }
    }
}
