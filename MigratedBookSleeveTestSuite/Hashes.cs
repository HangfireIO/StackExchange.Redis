using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Tests
{
    [TestFixture]
    public class Hashes // http://redis.io/commands#hash
    {
        [Test]
        public void TestIncrBy()
        {
            using (var muxer = Config.GetUnsecuredConnection())
            {
                var conn = muxer.GetDatabase(5);
                conn.KeyDeleteAsync("hash-test");
                for (int i = 1; i < 1000; i++)
                {
                    ClassicAssert.AreEqual(i, conn.HashIncrementAsync("hash-test", "a", 1).Result);
                    ClassicAssert.AreEqual(-i, conn.HashIncrementAsync("hash-test", "b", -1).Result);
                    //ClassicAssert.AreEqual(i, conn.Wait(conn.Hashes.Increment(5, "hash-test", "a", 1)));
                    //ClassicAssert.AreEqual(-i, conn.Wait(conn.Hashes.Increment(5, "hash-test", "b", -1)));
                }
            }
        }

        [Test]
        public void Scan()
        {
            using (var muxer = Config.GetUnsecuredConnection(waitForOpen: true))
            {
                if (!Config.GetFeatures(muxer).Scan) Assert.Inconclusive();
                const int db = 3;
                var conn = muxer.GetDatabase(db);
                
                const string key = "hash-scan";
                conn.KeyDeleteAsync(key);
                conn.HashSetAsync(key, "abc", "def");
                conn.HashSetAsync(key, "ghi", "jkl");
                conn.HashSetAsync(key, "mno", "pqr");

                var t1 = conn.HashScan(key);
                var t2 = conn.HashScan(key, "*h*");
                var t3 = conn.HashScan(key);
                var t4 = conn.HashScan(key, "*h*");

                var v1 = t1.ToArray();
                var v2 = t2.ToArray();
                var v3 = t3.ToArray();
                var v4 = t4.ToArray();

                ClassicAssert.AreEqual(3, v1.Length);
                ClassicAssert.AreEqual(1, v2.Length);
                ClassicAssert.AreEqual(3, v3.Length);
                ClassicAssert.AreEqual(1, v4.Length);
                Array.Sort(v1, (x, y) => string.Compare(x.Name, y.Name));
                Array.Sort(v2, (x, y) => string.Compare(x.Name, y.Name));
                Array.Sort(v3, (x, y) => string.Compare(x.Name, y.Name));
                Array.Sort(v4, (x, y) => string.Compare(x.Name, y.Name));

                ClassicAssert.AreEqual("abc=def,ghi=jkl,mno=pqr", string.Join(",", v1.Select(pair => pair.Name + "=" + (string)pair.Value)));
                ClassicAssert.AreEqual("ghi=jkl", string.Join(",", v2.Select(pair => pair.Name + "=" + (string)pair.Value)));
                ClassicAssert.AreEqual("abc=def,ghi=jkl,mno=pqr", string.Join(",", v3.Select(pair => pair.Name + "=" + pair.Value)));
                ClassicAssert.AreEqual("ghi=jkl", string.Join(",", v4.Select(pair => pair.Name + "=" + pair.Value)));
            }
        }
        [Test]
        public void TestIncrementOnHashThatDoesntExist()
        {
            using (var muxer = Config.GetUnsecuredConnection())
            {
                var conn = muxer.GetDatabase(0);
                conn.KeyDeleteAsync("keynotexist");
                var result1 = conn.Wait(conn.HashIncrementAsync("keynotexist", "fieldnotexist", 1));
                var result2 = conn.Wait(conn.HashIncrementAsync("keynotexist", "anotherfieldnotexist", 1));
                ClassicAssert.AreEqual(1, result1);
                ClassicAssert.AreEqual(1, result2);
            }
        }
        [Test]
        public void TestIncrByFloat()
        {
            using (var muxer = Config.GetUnsecuredConnection(waitForOpen: true))
            {
                var conn = muxer.GetDatabase(5);
                if (!Config.GetFeatures(muxer).IncrementFloat) Assert.Inconclusive();
                {
                    conn.KeyDeleteAsync("hash-test");
                    for (int i = 1; i < 1000; i++)
                    {
                        ClassicAssert.AreEqual((double)i, conn.HashIncrementAsync("hash-test", "a", 1.0).Result);
                        ClassicAssert.AreEqual((double)(-i), conn.HashIncrementAsync("hash-test", "b", -1.0).Result);
                    }
                }
            }
        }


        [Test]
        public void TestGetAll()
        {
            using (var muxer = Config.GetUnsecuredConnection())
            {
                var conn = muxer.GetDatabase(6);
                const string key = "hash test";
                conn.KeyDeleteAsync(key);
                var shouldMatch = new Dictionary<Guid, int>();
                var random = new Random();

                for (int i = 1; i < 1000; i++)
                {
                    var guid = Guid.NewGuid();
                    var value = random.Next(Int32.MaxValue);

                    shouldMatch[guid] = value;

                    var x = conn.HashIncrementAsync(key, guid.ToString(), value).Result; // Kill Async
                }
#pragma warning disable 618
                var inRedis = conn.HashGetAllAsync(key).Result.ToDictionary(
                    x => Guid.Parse(x.Name), x => int.Parse(x.Value));
#pragma warning restore 618

                ClassicAssert.AreEqual(shouldMatch.Count, inRedis.Count);

                foreach (var k in shouldMatch.Keys)
                {
                    ClassicAssert.AreEqual(shouldMatch[k], inRedis[k]);
                }
            }
        }

        [Test]
        public void TestGet()
        {
            using (var muxer = Config.GetUnsecuredConnection())
            {
                var key = "hash test";
                var conn = muxer.GetDatabase(6);
                var shouldMatch = new Dictionary<Guid, int>();
                var random = new Random();

                for (int i = 1; i < 1000; i++)
                {
                    var guid = Guid.NewGuid();
                    var value = random.Next(Int32.MaxValue);

                    shouldMatch[guid] = value;

                    var x = conn.HashIncrementAsync(key, guid.ToString(), value).Result; // Kill Async
                }

                foreach (var k in shouldMatch.Keys)
                {
                    var inRedis = conn.HashGetAsync(key, k.ToString()).Result;
                    var num = int.Parse((string)inRedis);

                    ClassicAssert.AreEqual(shouldMatch[k], num);
                }
            }
        }

        [Test]
        public void TestSet() // http://redis.io/commands/hset
        {
            using (var muxer = Config.GetUnsecuredConnection())
            {
                var conn = muxer.GetDatabase(9);
                conn.KeyDeleteAsync("hashkey");

                var val0 = conn.HashGetAsync("hashkey", "field");
                var set0 = conn.HashSetAsync("hashkey", "field", "value1");
                var val1 = conn.HashGetAsync("hashkey", "field");
                var set1 = conn.HashSetAsync("hashkey", "field", "value2");
                var val2 = conn.HashGetAsync("hashkey", "field");

                var set2 = conn.HashSetAsync("hashkey", "field-blob", Encoding.UTF8.GetBytes("value3"));
                var val3 = conn.HashGetAsync("hashkey", "field-blob");

                var set3 = conn.HashSetAsync("hashkey", "empty_type1", "");
                var val4 = conn.HashGetAsync("hashkey", "empty_type1");
                var set4 = conn.HashSetAsync("hashkey", "empty_type2", RedisValue.EmptyString);
                var val5 = conn.HashGetAsync("hashkey", "empty_type2");

                ClassicAssert.AreEqual(null, (string)val0.Result);
                ClassicAssert.AreEqual(true, set0.Result);
                ClassicAssert.AreEqual("value1", (string)val1.Result);
                ClassicAssert.AreEqual(false, set1.Result);
                ClassicAssert.AreEqual("value2", (string)val2.Result);

                ClassicAssert.AreEqual(true, set2.Result);
                ClassicAssert.AreEqual("value3", (string)val3.Result);

                ClassicAssert.AreEqual(true, set3.Result);
                ClassicAssert.AreEqual("", (string)val4.Result);
                ClassicAssert.AreEqual(true, set4.Result);
                ClassicAssert.AreEqual("", (string)val5.Result);
            }
        }
        [Test]
        public void TestSetNotExists() // http://redis.io/commands/hsetnx
        {
            using (var muxer = Config.GetUnsecuredConnection())
            {
                var conn = muxer.GetDatabase(9);
                conn.KeyDeleteAsync("hashkey");

                var val0 = conn.HashGetAsync("hashkey", "field");
                var set0 = conn.HashSetAsync("hashkey", "field", "value1", When.NotExists);
                var val1 = conn.HashGetAsync("hashkey", "field");
                var set1 = conn.HashSetAsync("hashkey", "field", "value2", When.NotExists);
                var val2 = conn.HashGetAsync("hashkey", "field");

                var set2 = conn.HashSetAsync("hashkey", "field-blob", Encoding.UTF8.GetBytes("value3"), When.NotExists);
                var val3 = conn.HashGetAsync("hashkey", "field-blob");
                var set3 = conn.HashSetAsync("hashkey", "field-blob", Encoding.UTF8.GetBytes("value3"), When.NotExists);

                ClassicAssert.AreEqual(null, (string)val0.Result);
                ClassicAssert.AreEqual(true, set0.Result);
                ClassicAssert.AreEqual("value1", (string)val1.Result);
                ClassicAssert.AreEqual(false, set1.Result);
                ClassicAssert.AreEqual("value1", (string)val2.Result);

                ClassicAssert.AreEqual(true, set2.Result);
                ClassicAssert.AreEqual("value3", (string)val3.Result);
                ClassicAssert.AreEqual(false, set3.Result);

            }
        }
        [Test]
        public void TestDelSingle() // http://redis.io/commands/hdel
        {
            using (var muxer = Config.GetUnsecuredConnection())
            {
                var conn = muxer.GetDatabase(9);
                conn.KeyDeleteAsync("hashkey");
                var del0 = conn.HashDeleteAsync("hashkey", "field");

                conn.HashSetAsync("hashkey", "field", "value");

                var del1 = conn.HashDeleteAsync("hashkey", "field");
                var del2 = conn.HashDeleteAsync("hashkey", "field");

                ClassicAssert.AreEqual(false, del0.Result);
                ClassicAssert.AreEqual(true, del1.Result);
                ClassicAssert.AreEqual(false, del2.Result);

            }
        }
        [Test]
        public void TestDelMulti() // http://redis.io/commands/hdel
        {
            using (var muxer = Config.GetUnsecuredConnection())
            {
                var conn = muxer.GetDatabase(3);
                conn.HashSetAsync("TestDelMulti", "key1", "val1");
                conn.HashSetAsync("TestDelMulti", "key2", "val2");
                conn.HashSetAsync("TestDelMulti", "key3", "val3");

                var s1 = conn.HashExistsAsync("TestDelMulti", "key1");
                var s2 = conn.HashExistsAsync("TestDelMulti", "key2");
                var s3 = conn.HashExistsAsync("TestDelMulti", "key3");

                var removed = conn.HashDeleteAsync("TestDelMulti", new RedisValue[] { "key1", "key3" });

                var d1 = conn.HashExistsAsync("TestDelMulti", "key1");
                var d2 = conn.HashExistsAsync("TestDelMulti", "key2");
                var d3 = conn.HashExistsAsync("TestDelMulti", "key3");

                ClassicAssert.IsTrue(conn.Wait(s1));
                ClassicAssert.IsTrue(conn.Wait(s2));
                ClassicAssert.IsTrue(conn.Wait(s3));

                ClassicAssert.AreEqual(2, conn.Wait(removed));

                ClassicAssert.IsFalse(conn.Wait(d1));
                ClassicAssert.IsTrue(conn.Wait(d2));
                ClassicAssert.IsFalse(conn.Wait(d3));

                var removeFinal = conn.HashDeleteAsync("TestDelMulti", new RedisValue[] { "key2" });

                ClassicAssert.AreEqual(0, conn.Wait(conn.HashLengthAsync("TestDelMulti")));
                ClassicAssert.AreEqual(1, conn.Wait(removeFinal));
            }
        }

        [Test]
        public void TestDelMultiInsideTransaction() // http://redis.io/commands/hdel
        {
            using (var outer = Config.GetUnsecuredConnection())
            {

                var conn = outer.GetDatabase(3).CreateTransaction();
                {
                    conn.HashSetAsync("TestDelMulti", "key1", "val1");
                    conn.HashSetAsync("TestDelMulti", "key2", "val2");
                    conn.HashSetAsync("TestDelMulti", "key3", "val3");

                    var s1 = conn.HashExistsAsync("TestDelMulti", "key1");
                    var s2 = conn.HashExistsAsync("TestDelMulti", "key2");
                    var s3 = conn.HashExistsAsync("TestDelMulti", "key3");

                    var removed = conn.HashDeleteAsync("TestDelMulti", new RedisValue[] { "key1", "key3" });

                    var d1 = conn.HashExistsAsync("TestDelMulti", "key1");
                    var d2 = conn.HashExistsAsync("TestDelMulti", "key2");
                    var d3 = conn.HashExistsAsync("TestDelMulti", "key3");

                    conn.Execute();

                    ClassicAssert.IsTrue(conn.Wait(s1));
                    ClassicAssert.IsTrue(conn.Wait(s2));
                    ClassicAssert.IsTrue(conn.Wait(s3));

                    ClassicAssert.AreEqual(2, conn.Wait(removed));

                    ClassicAssert.IsFalse(conn.Wait(d1));
                    ClassicAssert.IsTrue(conn.Wait(d2));
                    ClassicAssert.IsFalse(conn.Wait(d3));
                }

            }
        }
        [Test]
        public void TestExists() // http://redis.io/commands/hexists
        {
            using (var muxer = Config.GetUnsecuredConnection())
            {
                var conn = muxer.GetDatabase(9);
                conn.KeyDeleteAsync("hashkey");
                var ex0 = conn.HashExistsAsync("hashkey", "field");
                conn.HashSetAsync("hashkey", "field", "value");
                var ex1 = conn.HashExistsAsync("hashkey", "field");
                conn.HashDeleteAsync("hashkey", "field");
                var ex2 = conn.HashExistsAsync("hashkey", "field");

                ClassicAssert.AreEqual(false, ex0.Result);
                ClassicAssert.AreEqual(true, ex1.Result);
                ClassicAssert.AreEqual(false, ex0.Result);

            }
        }

        [Test]
        public void TestHashKeys() // http://redis.io/commands/hkeys
        {
            using (var muxer = Config.GetUnsecuredConnection())
            {
                var conn = muxer.GetDatabase(9);
                conn.KeyDeleteAsync("hashkey");

                var keys0 = conn.HashKeysAsync("hashkey");

                conn.HashSetAsync("hashkey", "foo", "abc");
                conn.HashSetAsync("hashkey", "bar", "def");

                var keys1 = conn.HashKeysAsync("hashkey");

                ClassicAssert.AreEqual(0, keys0.Result.Length);

                var arr = keys1.Result;
                ClassicAssert.AreEqual(2, arr.Length);
                ClassicAssert.AreEqual("foo", (string)arr[0]);
                ClassicAssert.AreEqual("bar", (string)arr[1]);

            }
        }

        [Test]
        public void TestHashValues() // http://redis.io/commands/hvals
        {
            using (var muxer = Config.GetUnsecuredConnection())
            {
                var conn = muxer.GetDatabase(9);
                conn.KeyDeleteAsync("hashkey");

                var keys0 = conn.HashValuesAsync("hashkey");

                conn.HashSetAsync("hashkey", "foo", "abc");
                conn.HashSetAsync("hashkey", "bar", "def");

                var keys1 = conn.HashValuesAsync("hashkey");

                ClassicAssert.AreEqual(0, keys0.Result.Length);

                var arr = keys1.Result;
                ClassicAssert.AreEqual(2, arr.Length);
                ClassicAssert.AreEqual("abc", Encoding.UTF8.GetString(arr[0]));
                ClassicAssert.AreEqual("def", Encoding.UTF8.GetString(arr[1]));

            }
        }

        [Test]
        public void TestHashLength() // http://redis.io/commands/hlen
        {
            using (var muxer = Config.GetUnsecuredConnection())
            {
                var conn = muxer.GetDatabase(9);
                conn.KeyDeleteAsync("hashkey");

                var len0 = conn.HashLengthAsync("hashkey");

                conn.HashSetAsync("hashkey", "foo", "abc");
                conn.HashSetAsync("hashkey", "bar", "def");

                var len1 = conn.HashLengthAsync("hashkey");

                ClassicAssert.AreEqual(0, len0.Result);
                ClassicAssert.AreEqual(2, len1.Result);

            }
        }

        [Test]
        public void TestGetMulti() // http://redis.io/commands/hmget
        {
            using (var muxer = Config.GetUnsecuredConnection())
            {
                var conn = muxer.GetDatabase(9);
                conn.KeyDeleteAsync("hashkey");

                RedisValue[] fields = { "foo", "bar", "blop" };
                var result0 = conn.HashGetAsync("hashkey", fields);

                conn.HashSetAsync("hashkey", "foo", "abc");
                conn.HashSetAsync("hashkey", "bar", "def");

                var result1 = conn.HashGetAsync("hashkey", fields);

                var result2 = conn.HashGetAsync("hashkey", fields);

                var arr0 = result0.Result;
                var arr1 = result1.Result;
                var arr2 = result2.Result;

                ClassicAssert.AreEqual(3, arr0.Length);
                ClassicAssert.IsNull((string)arr0[0]);
                ClassicAssert.IsNull((string)arr0[1]);
                ClassicAssert.IsNull((string)arr0[2]);

                ClassicAssert.AreEqual(3, arr1.Length);
                ClassicAssert.AreEqual("abc", (string)arr1[0]);
                ClassicAssert.AreEqual("def", (string)arr1[1]);
                ClassicAssert.IsNull((string)arr1[2]);

                ClassicAssert.AreEqual(3, arr2.Length);
                ClassicAssert.AreEqual("abc", (string)arr2[0]);
                ClassicAssert.AreEqual("def", (string)arr2[1]);
                ClassicAssert.IsNull((string)arr2[2]);
            }
        }

        [Test]
        public void TestGetPairs() // http://redis.io/commands/hgetall
        {
            using (var muxer = Config.GetUnsecuredConnection())
            {
                var conn = muxer.GetDatabase(9);
                conn.KeyDeleteAsync("hashkey");

                var result0 = conn.HashGetAllAsync("hashkey");

                conn.HashSetAsync("hashkey", "foo", "abc");
                conn.HashSetAsync("hashkey", "bar", "def");

                var result1 = conn.HashGetAllAsync("hashkey");

                ClassicAssert.AreEqual(0, result0.Result.Length);
                var result = result1.Result.ToStringDictionary();
                ClassicAssert.AreEqual(2, result.Count);
                ClassicAssert.AreEqual("abc", result["foo"]);
                ClassicAssert.AreEqual("def", result["bar"]);
            }
        }

        [Test]
        public void TestSetPairs() // http://redis.io/commands/hmset
        {
            using (var muxer = Config.GetUnsecuredConnection())
            {
                var conn = muxer.GetDatabase(9);
                conn.KeyDeleteAsync("hashkey");

                var result0 = conn.HashGetAllAsync("hashkey");

                var data = new HashEntry[] {
                    new HashEntry("foo", Encoding.UTF8.GetBytes("abc")),
                    new HashEntry("bar", Encoding.UTF8.GetBytes("def"))
                };
                conn.HashSetAsync("hashkey", data);

                var result1 = conn.HashGetAllAsync("hashkey");

                ClassicAssert.AreEqual(0, result0.Result.Length);
                var result = result1.Result.ToStringDictionary();
                ClassicAssert.AreEqual(2, result.Count);
                ClassicAssert.AreEqual("abc", result["foo"]);
                ClassicAssert.AreEqual("def", result["bar"]);
            }
        }

    }
}
