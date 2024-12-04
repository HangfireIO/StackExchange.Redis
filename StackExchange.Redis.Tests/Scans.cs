using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace StackExchange.Redis.Tests
{
    [TestFixture]
    public class Scans : TestBase
    {
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void KeysScan(bool supported)
        {
            string[] disabledCommands = supported ? null : new[] { "scan" };
            using (var conn = Create(disabledCommands: disabledCommands, allowAdmin: true))
            {
                const int DB = 7;
                var db = conn.GetDatabase(DB);
                var server = GetServer(conn);
                server.FlushDatabase(DB);
                for(int i = 0 ; i < 100 ; i++)
                {
                    db.StringSet("KeysScan:" + i, Guid.NewGuid().ToString(), flags: CommandFlags.FireAndForget);
                }
                var seq = server.Keys(DB, pageSize:50);
                bool isScanning = seq is IScanningCursor;
                ClassicAssert.AreEqual(supported, isScanning, "scanning");
                ClassicAssert.AreEqual(100, seq.Distinct().Count());
                ClassicAssert.AreEqual(100, seq.Distinct().Count());
                ClassicAssert.AreEqual(100, server.Keys(DB, "KeysScan:*").Distinct().Count());
                // 7, 70, 71, ..., 79
                ClassicAssert.AreEqual(11, server.Keys(DB, "KeysScan:7*").Distinct().Count());
            }
        }


        public void ScansIScanning()
        {
            using (var conn = Create(allowAdmin: true))
            {
                const int DB = 7;
                var db = conn.GetDatabase(DB);
                var server = GetServer(conn);
                server.FlushDatabase(DB);
                for (int i = 0; i < 100; i++)
                {
                    db.StringSet("ScansRepeatable:" + i, Guid.NewGuid().ToString(), flags: CommandFlags.FireAndForget);
                }
                var seq = server.Keys(DB, pageSize: 15);
                using(var iter = seq.GetEnumerator())
                {
                    IScanningCursor s0 = (IScanningCursor)seq, s1 = (IScanningCursor)iter;

                    ClassicAssert.AreEqual(15, s0.PageSize);
                    ClassicAssert.AreEqual(15, s1.PageSize);

                    // start at zero                    
                    ClassicAssert.AreEqual(0, s0.Cursor);
                    ClassicAssert.AreEqual(s0.Cursor, s1.Cursor);
                    
                    for(int i = 0 ; i < 47 ; i++)
                    {
                        ClassicAssert.IsTrue(iter.MoveNext());
                    }

                    // non-zero in the middle
                    ClassicAssert.AreNotEqual(0, s0.Cursor);
                    ClassicAssert.AreEqual(s0.Cursor, s1.Cursor);
                    
                    for (int i = 0; i < 53; i++)
                    {
                        ClassicAssert.IsTrue(iter.MoveNext());
                    }

                    // zero "next" at the end
                    ClassicAssert.IsFalse(iter.MoveNext());
                    ClassicAssert.AreNotEqual(0, s0.Cursor);
                    ClassicAssert.AreNotEqual(0, s1.Cursor);                    
                }
            }
        }

        public void ScanResume()
        {
            using (var conn = Create(allowAdmin: true))
            {
                const int DB = 7;
                var db = conn.GetDatabase(DB);
                var server = GetServer(conn);
                server.FlushDatabase(DB);
                int i;
                for (i = 0; i < 100; i++)
                {
                    db.StringSet("ScanResume:" + i, Guid.NewGuid().ToString(), flags: CommandFlags.FireAndForget);
                }
                
                var expected = new HashSet<string>();
                long snapCursor = 0;
                int snapOffset = 0, snapPageSize = 0;

                i = 0;
                var seq = server.Keys(DB, pageSize: 15);
                foreach(var key in seq)
                {
                    i++;
                    if (i < 57) continue;
                    if (i == 57)
                    {
                        snapCursor = ((IScanningCursor)seq).Cursor;
                        snapOffset = ((IScanningCursor)seq).PageOffset;
                        snapPageSize = ((IScanningCursor)seq).PageSize;
                    }
                    expected.Add((string)key);
                }                
                ClassicAssert.AreNotEqual(43, expected.Count);
                ClassicAssert.AreNotEqual(0, snapCursor);
                ClassicAssert.AreEqual(11, snapOffset);
                ClassicAssert.AreEqual(15, snapPageSize);

                seq = server.Keys(DB, pageSize: 15, cursor: snapCursor, pageOffset: snapOffset);
                var seqCur = (IScanningCursor)seq;
                ClassicAssert.AreEqual(snapCursor, seqCur.Cursor);
                ClassicAssert.AreEqual(snapPageSize, seqCur.PageSize);
                ClassicAssert.AreEqual(snapOffset, seqCur.PageOffset);
                using(var iter = seq.GetEnumerator())
                {
                    var iterCur = (IScanningCursor)iter;
                    ClassicAssert.AreEqual(snapCursor, iterCur.Cursor);
                    ClassicAssert.AreEqual(snapOffset, iterCur.PageOffset);
                    ClassicAssert.AreEqual(snapCursor, seqCur.Cursor);
                    ClassicAssert.AreEqual(snapOffset, seqCur.PageOffset);

                    ClassicAssert.IsTrue(iter.MoveNext());
                    ClassicAssert.AreEqual(snapCursor, iterCur.Cursor);
                    ClassicAssert.AreEqual(snapOffset, iterCur.PageOffset);
                    ClassicAssert.AreEqual(snapCursor, seqCur.Cursor);
                    ClassicAssert.AreEqual(snapOffset, seqCur.PageOffset);

                    ClassicAssert.IsTrue(iter.MoveNext());
                    ClassicAssert.AreEqual(snapCursor, iterCur.Cursor);
                    ClassicAssert.AreEqual(snapOffset + 1, iterCur.PageOffset);
                    ClassicAssert.AreEqual(snapCursor, seqCur.Cursor);
                    ClassicAssert.AreEqual(snapOffset + 1, seqCur.PageOffset);
                }

                int count = 0;
                foreach(var key in seq)
                {
                    expected.Remove((string)key);
                    count++;
                }
                ClassicAssert.AreEqual(0, expected.Count);
                ClassicAssert.AreEqual(44, count); // expect the initial item to be repeated

            }
        }


       

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void SetScan(bool supported)
        {
            string[] disabledCommands = supported ? null : new[] { "sscan" };
            using(var conn = Create(disabledCommands: disabledCommands))
            {
                RedisKey key = Me();
                var db = conn.GetDatabase();
                db.KeyDelete(key);

                db.SetAdd(key, "a");
                db.SetAdd(key, "b");
                db.SetAdd(key, "c");
                var arr = db.SetScan(key).ToArray();
                ClassicAssert.AreEqual(3, arr.Length);
                ClassicAssert.IsTrue(arr.Contains("a"), "a");
                ClassicAssert.IsTrue(arr.Contains("b"), "b");
                ClassicAssert.IsTrue(arr.Contains("c"), "c");
            }
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void SortedSetScan(bool supported)
        {
            string[] disabledCommands = supported ? null : new[] { "zscan" };
            using (var conn = Create(disabledCommands: disabledCommands))
            {
                RedisKey key = Me();
                var db = conn.GetDatabase();
                db.KeyDelete(key);

                db.SortedSetAdd(key, "a", 1);
                db.SortedSetAdd(key, "b", 2);
                db.SortedSetAdd(key, "c", 3);

                var arr = db.SortedSetScan(key).ToArray();
                ClassicAssert.AreEqual(3, arr.Length);
                ClassicAssert.IsTrue(arr.Any(x => x.Element == "a" && x.Score == 1), "a");
                ClassicAssert.IsTrue(arr.Any(x => x.Element == "b" && x.Score == 2), "b");
                ClassicAssert.IsTrue(arr.Any(x => x.Element == "c" && x.Score == 3), "c");

                var dictionary = arr.ToDictionary();
                ClassicAssert.AreEqual(1, dictionary["a"]);
                ClassicAssert.AreEqual(2, dictionary["b"]);
                ClassicAssert.AreEqual(3, dictionary["c"]);

                var sDictionary = arr.ToStringDictionary();
                ClassicAssert.AreEqual(1, sDictionary["a"]);
                ClassicAssert.AreEqual(2, sDictionary["b"]);
                ClassicAssert.AreEqual(3, sDictionary["c"]);

                var basic = db.SortedSetRangeByRankWithScores(key, order: Order.Ascending).ToDictionary();
                ClassicAssert.AreEqual(3, basic.Count);
                ClassicAssert.AreEqual(1, basic["a"]);
                ClassicAssert.AreEqual(2, basic["b"]);
                ClassicAssert.AreEqual(3, basic["c"]);

                basic = db.SortedSetRangeByRankWithScores(key, order: Order.Descending).ToDictionary();
                ClassicAssert.AreEqual(3, basic.Count);
                ClassicAssert.AreEqual(1, basic["a"]);
                ClassicAssert.AreEqual(2, basic["b"]);
                ClassicAssert.AreEqual(3, basic["c"]);

                var basicArr = db.SortedSetRangeByScoreWithScores(key, order: Order.Ascending);
                ClassicAssert.AreEqual(3, basicArr.Length);
                ClassicAssert.AreEqual(1, basicArr[0].Score);
                ClassicAssert.AreEqual(2, basicArr[1].Score);
                ClassicAssert.AreEqual(3, basicArr[2].Score);
                basic = basicArr.ToDictionary();
                ClassicAssert.AreEqual(3, basic.Count, "asc");
                ClassicAssert.AreEqual(1, basic["a"]);
                ClassicAssert.AreEqual(2, basic["b"]);
                ClassicAssert.AreEqual(3, basic["c"]);

                basicArr = db.SortedSetRangeByScoreWithScores(key, order: Order.Descending);
                ClassicAssert.AreEqual(3, basicArr.Length);
                ClassicAssert.AreEqual(3, basicArr[0].Score);
                ClassicAssert.AreEqual(2, basicArr[1].Score);
                ClassicAssert.AreEqual(1, basicArr[2].Score);
                basic = basicArr.ToDictionary();
                ClassicAssert.AreEqual(3, basic.Count, "desc");
                ClassicAssert.AreEqual(1, basic["a"]);
                ClassicAssert.AreEqual(2, basic["b"]);
                ClassicAssert.AreEqual(3, basic["c"]);
            }
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void HashScan(bool supported)
        {
            string[] disabledCommands = supported ? null : new[] { "hscan" };
            using (var conn = Create(disabledCommands: disabledCommands))
            {
                RedisKey key = Me();
                var db = conn.GetDatabase();
                db.KeyDelete(key);

                db.HashSet(key, "a", "1");
                db.HashSet(key, "b", "2");
                db.HashSet(key, "c", "3");

                var arr = db.HashScan(key).ToArray();
                ClassicAssert.AreEqual(3, arr.Length);
                ClassicAssert.IsTrue(arr.Any(x => x.Name == "a" && x.Value == "1"), "a");
                ClassicAssert.IsTrue(arr.Any(x => x.Name == "b" && x.Value == "2"), "b");
                ClassicAssert.IsTrue(arr.Any(x => x.Name == "c" && x.Value == "3"), "c");

                var dictionary = arr.ToDictionary();
                ClassicAssert.AreEqual(1, (long)dictionary["a"]);
                ClassicAssert.AreEqual(2, (long)dictionary["b"]);
                ClassicAssert.AreEqual(3, (long)dictionary["c"]);

                var sDictionary = arr.ToStringDictionary();
                ClassicAssert.AreEqual("1", sDictionary["a"]);
                ClassicAssert.AreEqual("2", sDictionary["b"]);
                ClassicAssert.AreEqual("3", sDictionary["c"]);


                var basic = db.HashGetAll(key).ToDictionary();
                ClassicAssert.AreEqual(3, basic.Count);
                ClassicAssert.AreEqual(1, (long)basic["a"]);
                ClassicAssert.AreEqual(2, (long)basic["b"]);
                ClassicAssert.AreEqual(3, (long)basic["c"]);
            }
        }

        [Test]
        [TestCase(10)]
        [TestCase(100)]
        [TestCase(1000)]
        [TestCase(10000)]
        public void HashScanLarge(int pageSize)
        {
            using (var conn = Create())
            {
                RedisKey key = Me();
                var db = conn.GetDatabase();
                db.KeyDelete(key);

                for(int i = 0; i < 2000;i++)
                    db.HashSet(key, "k" + i, "v" + i, flags:  CommandFlags.FireAndForget);

                int count = db.HashScan(key, pageSize: pageSize).Count();
                ClassicAssert.AreEqual(2000, count);
            }
        }

        [Test]
        [TestCase(10)]
        [TestCase(100)]
        [TestCase(1000)]
        [TestCase(10000)]
        public void SetScanLarge(int pageSize)
        {
            using (var conn = Create())
            {
                RedisKey key = Me();
                var db = conn.GetDatabase();
                db.KeyDelete(key);

                for (int i = 0; i < 2000; i++)
                    db.SetAdd(key, "s" + i, flags: CommandFlags.FireAndForget);

                int count = db.SetScan(key, pageSize: pageSize).Count();
                ClassicAssert.AreEqual(2000, count);
            }
        }

        [Test]
        [TestCase(10)]
        [TestCase(100)]
        [TestCase(1000)]
        [TestCase(10000)]
        public void SortedSetScanLarge(int pageSize)
        {
            using (var conn = Create())
            {
                RedisKey key = Me();
                var db = conn.GetDatabase();
                db.KeyDelete(key);

                for (int i = 0; i < 2000; i++)
                    db.SortedSetAdd(key, "z" + i, i, flags: CommandFlags.FireAndForget);

                int count = db.SortedSetScan(key, pageSize: pageSize).Count();
                ClassicAssert.AreEqual(2000, count);
            }
        }
    }
}
