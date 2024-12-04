using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
#if FEATURE_BOOKSLEEVE
using BookSleeve;
#endif
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis.KeyspaceIsolation;
namespace StackExchange.Redis.Tests
{
    [TestFixture]
    public class BasicOpsTests : TestBase
    {
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void PingOnce(bool preserveOrder)
        {
            using (var muxer = Create())
            {
                muxer.PreserveAsyncOrder = preserveOrder;
                var conn = muxer.GetDatabase();

                var task = conn.PingAsync();
                var duration = muxer.Wait(task);
                Console.WriteLine("Ping took: " + duration);
                ClassicAssert.IsTrue(duration.TotalMilliseconds > 0);
            }
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void RapidDispose(bool preserverOrder)
        {
            RedisKey key = Me();
            using (var primary = Create())
            {
                var conn = primary.GetDatabase();
                conn.KeyDelete(key);

                for (int i = 0; i < 10; i++)
                {
                    using (var secondary = Create(fail: true))
                    {
                        secondary.GetDatabase().StringIncrement(key, flags: CommandFlags.FireAndForget);
                        secondary.Close(allowCommandsToComplete: true);
                    }
                }

                ClassicAssert.AreEqual(10, (int)conn.StringGet(key));
            }
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void PingMany(bool preserveOrder)
        {
            using (var muxer = Create())
            {
                muxer.PreserveAsyncOrder = preserveOrder;
                var conn = muxer.GetDatabase();
                var tasks = new Task<TimeSpan>[10000];

                for (int i = 0; i < tasks.Length; i++)
                {
                    tasks[i] = conn.PingAsync();
                }
                muxer.WaitAll(tasks);
                ClassicAssert.IsTrue(tasks[0].Result.TotalMilliseconds > 0);
                ClassicAssert.IsTrue(tasks[tasks.Length - 1].Result.TotalMilliseconds > 0);
            }
        }

        [Test]
        public void GetWithNullKey()
        {
            using (var muxer = Create())
            {
                var db = muxer.GetDatabase();
                string key = null;
                ClassicAssert.Throws<ArgumentException>(
                    () => db.StringGet(key),
                    "A null key is not valid in this context");
            }
        }

        [Test]
        public void SetWithNullKey()
        {
            using (var muxer = Create())
            {
                var db = muxer.GetDatabase();
                string key = null, value = "abc";
                ClassicAssert.Throws<ArgumentException>(
                    () => db.StringSet(key, value),
                    "A null key is not valid in this context");
            }
        }

        [Test]
        public void SetWithNullValue()
        {
            using (var muxer = Create())
            {
                var db = muxer.GetDatabase();
                string key = Me(), value = null;
                db.KeyDelete(key, CommandFlags.FireAndForget);

                db.StringSet(key, "abc", flags: CommandFlags.FireAndForget);
                ClassicAssert.IsTrue(db.KeyExists(key));
                db.StringSet(key, value);
                
                var actual = (string)db.StringGet(key);
                ClassicAssert.IsNull(actual);
                ClassicAssert.IsFalse(db.KeyExists(key));
            }
        }

        [Test]
        public void SetWithDefaultValue()
        {
            using (var muxer = Create())
            {
                var db = muxer.GetDatabase();
                string key = Me();
                var value = default(RedisValue); // this is kinda 0... ish
                db.KeyDelete(key, CommandFlags.FireAndForget);

                db.StringSet(key, "abc", flags: CommandFlags.FireAndForget);
                ClassicAssert.IsTrue(db.KeyExists(key));
                db.StringSet(key, value);

                var actual = (string)db.StringGet(key);
                ClassicAssert.IsNull(actual);
                ClassicAssert.IsFalse(db.KeyExists(key));
            }
        }

        [Test]
        public void SetWithZeroValue()
        {
            using (var muxer = Create())
            {
                var db = muxer.GetDatabase();
                string key = Me();
                long value = 0;
                db.KeyDelete(key, CommandFlags.FireAndForget);

                db.StringSet(key, "abc", flags: CommandFlags.FireAndForget);
                ClassicAssert.IsTrue(db.KeyExists(key));
                db.StringSet(key, value);

                var actual = (string)db.StringGet(key);
                ClassicAssert.AreEqual("0", actual);
                ClassicAssert.IsTrue(db.KeyExists(key));
            }
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void GetSetAsync(bool preserveOrder)
        {
            using (var muxer = Create())
            {
                muxer.PreserveAsyncOrder = preserveOrder;
                var conn = muxer.GetDatabase();

                RedisKey key = Me();
                var d0 = conn.KeyDeleteAsync(key);
                var d1 = conn.KeyDeleteAsync(key);
                var g1 = conn.StringGetAsync(key);
                var s1 = conn.StringSetAsync(key, "123");
                var g2 = conn.StringGetAsync(key);
                var d2 = conn.KeyDeleteAsync(key);

                muxer.Wait(d0);
                ClassicAssert.IsFalse(muxer.Wait(d1));
                ClassicAssert.IsNull((string)muxer.Wait(g1));
                ClassicAssert.IsTrue(muxer.Wait(g1).IsNull);
                muxer.Wait(s1);
                ClassicAssert.AreEqual("123", (string)muxer.Wait(g2));
                ClassicAssert.AreEqual(123, (int)muxer.Wait(g2));
                ClassicAssert.IsFalse(muxer.Wait(g2).IsNull);
                ClassicAssert.IsTrue(muxer.Wait(d2));
            }
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void GetSetSync(bool preserveOrder)
        {
            using (var muxer = Create())
            {
                muxer.PreserveAsyncOrder = preserveOrder;
                var conn = muxer.GetDatabase();

                RedisKey key = Me();
                conn.KeyDelete(key);
                var d1 = conn.KeyDelete(key);
                var g1 = conn.StringGet(key);
                conn.StringSet(key, "123");
                var g2 = conn.StringGet(key);
                var d2 = conn.KeyDelete(key);

                ClassicAssert.IsFalse(d1);
                ClassicAssert.IsNull((string)g1);
                ClassicAssert.IsTrue(g1.IsNull);

                ClassicAssert.AreEqual("123", (string)g2);
                ClassicAssert.AreEqual(123, (int)g2);
                ClassicAssert.IsFalse(g2.IsNull);
                ClassicAssert.IsTrue(d2);
            }
        }

        [Test]
        [TestCase(true, true)]
        [TestCase(true, false)]
        [TestCase(false, true)]
        [TestCase(false, false)]
        public void MassiveBulkOpsAsync(bool preserveOrder, bool withContinuation)
        {
#if DEBUG
            var oldAsyncCompletionCount = ConnectionMultiplexer.GetAsyncCompletionWorkerCount();
#endif
            using (var muxer = Create())
            {
                muxer.PreserveAsyncOrder = preserveOrder;
                RedisKey key = "MBOA";
                var conn = muxer.GetDatabase();
                muxer.Wait(conn.PingAsync());

#if CORE_CLR
                int number = 0;
#endif
                Action<Task> nonTrivial = delegate
                {
#if !CORE_CLR
                    Thread.SpinWait(5);
#else
                    for (int i = 0; i < 50; i++)
                    {
                        number++;
                    }
#endif
                };
                var watch = Stopwatch.StartNew();
                for (int i = 0; i <= AsyncOpsQty; i++)
                {
                    var t = conn.StringSetAsync(key, i);
                    if (withContinuation) t.ContinueWith(nonTrivial);
                }
                int val = (int)muxer.Wait(conn.StringGetAsync(key));
                ClassicAssert.AreEqual(AsyncOpsQty, val);
                watch.Stop();
                Console.WriteLine("{2}: Time for {0} ops: {1}ms ({3}, {4}); ops/s: {5}", AsyncOpsQty, watch.ElapsedMilliseconds, Me(),
                    withContinuation ? "with continuation" : "no continuation", preserveOrder ? "preserve order" : "any order",
                    AsyncOpsQty / watch.Elapsed.TotalSeconds);
#if DEBUG
                Console.WriteLine("Async completion workers: " + (ConnectionMultiplexer.GetAsyncCompletionWorkerCount() - oldAsyncCompletionCount));
#endif
            }
        }

        [Test]
        [TestCase(false, false)]
        [TestCase(true, true)]
        [TestCase(true, false)]
        public void GetWithExpiry(bool exists, bool hasExpiry)
        {
            using(var conn = Create())
            {
                var db = conn.GetDatabase();
                RedisKey key = Me();
                db.KeyDelete(key);
                if (exists)
                {
                    if (hasExpiry)
                        db.StringSet(key, "val", TimeSpan.FromMinutes(5));
                    else
                        db.StringSet(key, "val");
                }
                var async = db.StringGetWithExpiryAsync(key);
                var syncResult = db.StringGetWithExpiry(key);
                var asyncResult = db.Wait(async);

                if(exists)
                {
                    ClassicAssert.AreEqual("val", (string)asyncResult.Value);
                    ClassicAssert.AreEqual(hasExpiry, asyncResult.Expiry.HasValue);
                    if (hasExpiry) ClassicAssert.IsTrue(asyncResult.Expiry.Value.TotalMinutes >= 4.9 && asyncResult.Expiry.Value.TotalMinutes <= 5);
                    ClassicAssert.AreEqual("val", (string)syncResult.Value);
                    ClassicAssert.AreEqual(hasExpiry, syncResult.Expiry.HasValue);
                    if (hasExpiry) ClassicAssert.IsTrue(syncResult.Expiry.Value.TotalMinutes >= 4.9 && syncResult.Expiry.Value.TotalMinutes <= 5);
                }
                else
                {
                    ClassicAssert.IsTrue(asyncResult.Value.IsNull);
                    ClassicAssert.IsFalse(asyncResult.Expiry.HasValue);
                    ClassicAssert.IsTrue(syncResult.Value.IsNull);
                    ClassicAssert.IsFalse(syncResult.Expiry.HasValue);
                }
            }
        }
        [Test]
        public void GetWithExpiryWrongTypeAsync()
        {
            using (var conn = Create())
            {
                var db = conn.GetDatabase();
                RedisKey key = Me();
                db.KeyDelete(key);
                db.SetAdd(key, "abc");
                ClassicAssert.Throws<RedisServerException>(() =>
                {
                    try
                    {
                        var async = db.Wait(db.StringGetWithExpiryAsync(key));
                    }
                    catch (AggregateException ex)
                    {
                        throw ex.InnerExceptions[0];
                    }
                    ClassicAssert.Fail();
                },
                "A null key is not valid in this context");
            }
        }

        [Test]
        public void GetWithExpiryWrongTypeSync()
        {
            ClassicAssert.Throws<RedisServerException>(() =>
            {
                using (var conn = Create())
                {
                    var db = conn.GetDatabase();
                    RedisKey key = Me();
                    db.KeyDelete(key);
                    db.SetAdd(key, "abc");
                    db.StringGetWithExpiry(key);
                    ClassicAssert.Fail();
                }
            },
            "WRONGTYPE Operation against a key holding the wrong kind of value");
        }

#if FEATURE_BOOKSLEEVE
        [Test]
        [TestCase(true, true, ResultCompletionMode.ConcurrentIfContinuation)]
        [TestCase(true, false, ResultCompletionMode.ConcurrentIfContinuation)]
        [TestCase(false, true, ResultCompletionMode.ConcurrentIfContinuation)]
        [TestCase(false, false, ResultCompletionMode.ConcurrentIfContinuation)]
        [TestCase(true, true, ResultCompletionMode.Concurrent)]
        [TestCase(true, false, ResultCompletionMode.Concurrent)]
        [TestCase(false, true, ResultCompletionMode.Concurrent)]
        [TestCase(false, false, ResultCompletionMode.Concurrent)]
        [TestCase(true, true, ResultCompletionMode.PreserveOrder)]
        [TestCase(true, false, ResultCompletionMode.PreserveOrder)]
        [TestCase(false, true, ResultCompletionMode.PreserveOrder)]
        [TestCase(false, false, ResultCompletionMode.PreserveOrder)]
        public void MassiveBulkOpsAsyncOldStyle(bool withContinuation, bool suspendFlush, ResultCompletionMode completionMode)
        {
            using (var conn = GetOldStyleConnection())
            {
                const int db = 0;
                string key = "MBOQ";
                conn.CompletionMode = completionMode;
                conn.Wait(conn.Server.Ping());
                Action<Task> nonTrivial = delegate
                {
                    Thread.SpinWait(5);
                };
                var watch = Stopwatch.StartNew();

                if (suspendFlush) conn.SuspendFlush();
                try
                {

                    for (int i = 0; i <= AsyncOpsQty; i++)
                    {
                        var t = conn.Strings.Set(db, key, i);
                        if (withContinuation) t.ContinueWith(nonTrivial);
                    }
                } finally
                {
                    if (suspendFlush) conn.ResumeFlush();
                }
                int val = (int)conn.Wait(conn.Strings.GetInt64(db, key));
                ClassicAssert.AreEqual(AsyncOpsQty, val);
                watch.Stop();
                Console.WriteLine("{2}: Time for {0} ops: {1}ms ({3}, {4}, {5}); ops/s: {6}", AsyncOpsQty, watch.ElapsedMilliseconds, Me(),
                    withContinuation ? "with continuation" : "no continuation",
                    suspendFlush ? "suspend flush" : "flush at whim",
                    completionMode, AsyncOpsQty / watch.Elapsed.TotalSeconds);
            }
        }
#endif

        [Test]
        [TestCase(true, 1)]
        [TestCase(false, 1)]
        [TestCase(true, 5)]
        [TestCase(false, 5)]
        [TestCase(true, 10)]
        [TestCase(false, 10)]
        [TestCase(true, 50)]
        [TestCase(false, 50)]
        public void MassiveBulkOpsSync(bool preserveOrder, int threads)
        {
            int workPerThread = SyncOpsQty / threads;
            using (var muxer = Create())
            {
                muxer.PreserveAsyncOrder = preserveOrder;
                RedisKey key = "MBOS";
                var conn = muxer.GetDatabase();
                conn.KeyDelete(key);
#if DEBUG
                long oldWorkerCount = ConnectionMultiplexer.GetAsyncCompletionWorkerCount();
#endif
                var timeTaken = RunConcurrent(delegate
                {
                    for (int i = 0; i < workPerThread; i++)
                    {
                        conn.StringIncrement(key);
                    }
                }, threads);

                int val = (int)conn.StringGet(key);
                ClassicAssert.AreEqual(workPerThread * threads, val);
                Console.WriteLine("{2}: Time for {0} ops on {4} threads: {1}ms ({3}); ops/s: {5}",
                    threads * workPerThread, timeTaken.TotalMilliseconds, Me()
                    , preserveOrder ? "preserve order" : "any order", threads, (workPerThread * threads) / timeTaken.TotalSeconds);
#if DEBUG
                long newWorkerCount = ConnectionMultiplexer.GetAsyncCompletionWorkerCount();
                Console.WriteLine("Workers {0}", newWorkerCount - oldWorkerCount);
#endif
            }
        }

#if FEATURE_BOOKSLEEVE
        [Test]
        [TestCase(ResultCompletionMode.Concurrent, 1)]
        [TestCase(ResultCompletionMode.ConcurrentIfContinuation, 1)]
        [TestCase(ResultCompletionMode.PreserveOrder, 1)]
        [TestCase(ResultCompletionMode.Concurrent, 5)]
        [TestCase(ResultCompletionMode.ConcurrentIfContinuation, 5)]
        [TestCase(ResultCompletionMode.PreserveOrder, 5)]
        [TestCase(ResultCompletionMode.Concurrent, 10)]
        [TestCase(ResultCompletionMode.ConcurrentIfContinuation, 10)]
        [TestCase(ResultCompletionMode.PreserveOrder, 10)]
        [TestCase(ResultCompletionMode.Concurrent, 50)]
        [TestCase(ResultCompletionMode.ConcurrentIfContinuation, 50)]
        [TestCase(ResultCompletionMode.PreserveOrder, 50)]
        public void MassiveBulkOpsSyncOldStyle(ResultCompletionMode completionMode, int threads)
        {
            int workPerThread = SyncOpsQty / threads;

            using (var conn = GetOldStyleConnection())
            {
                const int db = 0;
                string key = "MBOQ";
                conn.CompletionMode = completionMode;
                conn.Wait(conn.Keys.Remove(db, key));

                var timeTaken = RunConcurrent(delegate
                {
                    for (int i = 0; i < workPerThread; i++)
                    {
                        conn.Wait(conn.Strings.Increment(db, key));
                    }
                }, threads);

                int val = (int)conn.Wait(conn.Strings.GetInt64(db, key));
                ClassicAssert.AreEqual(workPerThread * threads, val);

                Console.WriteLine("{2}: Time for {0} ops on {4} threads: {1}ms ({3}); ops/s: {5}", workPerThread * threads, timeTaken.TotalMilliseconds, Me(),
                    completionMode, threads, (workPerThread * threads) / timeTaken.TotalSeconds);
            }
        }
#endif

        [Test]
        [TestCase(true, 1)]
        [TestCase(false, 1)]
        [TestCase(true, 5)]
        [TestCase(false, 5)]
        public void MassiveBulkOpsFireAndForget(bool preserveOrder, int threads)
        {
            using (var muxer = Create())
            {
                muxer.PreserveAsyncOrder = preserveOrder;
                RedisKey key = "MBOF";
                var conn = muxer.GetDatabase();
                conn.Ping();

                conn.KeyDelete(key, CommandFlags.FireAndForget);
                int perThread = AsyncOpsQty / threads;
                var elapsed = RunConcurrent(delegate
                {
                    for (int i = 0; i < perThread; i++)
                    {
                        conn.StringIncrement(key, flags: CommandFlags.FireAndForget);
                    }
                    conn.Ping();
                }, threads);
                var val = (long)conn.StringGet(key);
                ClassicAssert.AreEqual(perThread * threads, val);
                
                Console.WriteLine("{2}: Time for {0} ops over {5} threads: {1:###,###}ms ({3}); ops/s: {4:###,###,##0}",
                    val, elapsed.TotalMilliseconds, Me(),
                    preserveOrder ? "preserve order" : "any order",
                    val / elapsed.TotalSeconds, threads);
            }
        }


#if DEBUG
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void TestQuit(bool preserveOrder)
        {
            SetExpectedAmbientFailureCount(1);
            using (var muxer = Create(allowAdmin: true))
            {
                muxer.PreserveAsyncOrder = preserveOrder;
                var db = muxer.GetDatabase();
                string key = Guid.NewGuid().ToString();
                db.KeyDelete(key, CommandFlags.FireAndForget);
                db.StringSet(key, key, flags: CommandFlags.FireAndForget);
                GetServer(muxer).Quit(CommandFlags.FireAndForget);
                var watch = Stopwatch.StartNew();
                try
                {
                    db.Ping();
                    ClassicAssert.Fail();
                }
                catch (RedisConnectionException) { }
                watch.Stop();
                Console.WriteLine("Time to notice quit: {0}ms ({1})", watch.ElapsedMilliseconds,
                    preserveOrder ? "preserve order" : "any order");
                Thread.Sleep(20);
                Debug.WriteLine("Pinging...");
                ClassicAssert.AreEqual(key, (string)db.StringGet(key));
            }
        }
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void TestSevered(bool preserveOrder)
        {
            SetExpectedAmbientFailureCount(2);
            using (var muxer = Create(allowAdmin: true))
            {
                muxer.PreserveAsyncOrder = preserveOrder;
                var db = muxer.GetDatabase();
                string key = Guid.NewGuid().ToString();
                db.KeyDelete(key, CommandFlags.FireAndForget);
                db.StringSet(key, key, flags: CommandFlags.FireAndForget);
                GetServer(muxer).SimulateConnectionFailure();
                var watch = Stopwatch.StartNew();
                db.Ping();
                watch.Stop();
                Console.WriteLine("Time to re-establish: {0}ms ({1})", watch.ElapsedMilliseconds,
                    preserveOrder ? "preserve order" : "any order");
                Thread.Sleep(20);
                Debug.WriteLine("Pinging...");
                ClassicAssert.AreEqual(key, (string)db.StringGet(key));
            }
        }
#endif



        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void IncrAsync(bool preserveOrder)
        {
            using (var muxer = Create())
            {
                muxer.PreserveAsyncOrder = preserveOrder;
                var conn = muxer.GetDatabase();
                RedisKey key = Me();
                conn.KeyDelete(key, CommandFlags.FireAndForget);
                var nix = conn.KeyExistsAsync(key);
                var a = conn.StringGetAsync(key);
                var b = conn.StringIncrementAsync(key);
                var c = conn.StringGetAsync(key);
                var d = conn.StringIncrementAsync(key, 10);
                var e = conn.StringGetAsync(key);
                var f = conn.StringDecrementAsync(key, 11);
                var g = conn.StringGetAsync(key);
                var h = conn.KeyExistsAsync(key);
                ClassicAssert.IsFalse(muxer.Wait(nix));
                ClassicAssert.IsTrue(muxer.Wait(a).IsNull);
                ClassicAssert.AreEqual(0, (long)muxer.Wait(a));
                ClassicAssert.AreEqual(1, muxer.Wait(b));
                ClassicAssert.AreEqual(1, (long)muxer.Wait(c));
                ClassicAssert.AreEqual(11, muxer.Wait(d));
                ClassicAssert.AreEqual(11, (long)muxer.Wait(e));
                ClassicAssert.AreEqual(0, muxer.Wait(f));
                ClassicAssert.AreEqual(0, (long)muxer.Wait(g));
                ClassicAssert.IsTrue(muxer.Wait(h));
            }
        }
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void IncrSync(bool preserveOrder)
        {
            using (var muxer = Create())
            {
                muxer.PreserveAsyncOrder = preserveOrder;
                var conn = muxer.GetDatabase();
                RedisKey key = Me();
                conn.KeyDelete(key, CommandFlags.FireAndForget);
                var nix = conn.KeyExists(key);
                var a = conn.StringGet(key);
                var b = conn.StringIncrement(key);
                var c = conn.StringGet(key);
                var d = conn.StringIncrement(key, 10);
                var e = conn.StringGet(key);
                var f = conn.StringDecrement(key, 11);
                var g = conn.StringGet(key);
                var h = conn.KeyExists(key);
                ClassicAssert.IsFalse(nix);
                ClassicAssert.IsTrue(a.IsNull);
                ClassicAssert.AreEqual(0, (long)a);
                ClassicAssert.AreEqual(1, b);
                ClassicAssert.AreEqual(1, (long)c);
                ClassicAssert.AreEqual(11, d);
                ClassicAssert.AreEqual(11, (long)e);
                ClassicAssert.AreEqual(0, f);
                ClassicAssert.AreEqual(0, (long)g);
                ClassicAssert.IsTrue(h);
            }
        }

        [Test]
        public void IncrDifferentSizes()
        {
            using (var muxer = Create())
            {
                var db = muxer.GetDatabase();
                RedisKey key = Me();
                db.KeyDelete(key, CommandFlags.FireAndForget);
                int expected = 0;
                Incr(db, key, -129019, ref expected);
                Incr(db, key, -10023, ref expected);
                Incr(db, key, -9933, ref expected);
                Incr(db, key, -23, ref expected);
                Incr(db, key, -7, ref expected);
                Incr(db, key, -1, ref expected);
                Incr(db, key, 0, ref expected);
                Incr(db, key, 1, ref expected);
                Incr(db, key, 9, ref expected);
                Incr(db, key, 11, ref expected);
                Incr(db, key, 345, ref expected);
                Incr(db, key, 4982, ref expected);
                Incr(db, key, 13091, ref expected);
                Incr(db, key, 324092, ref expected);
                ClassicAssert.AreNotEqual(0, expected);
                var sum = (long)db.StringGet(key);
                ClassicAssert.AreEqual(expected, sum);
            }
        }

        private void Incr(IDatabase database, RedisKey key, int delta, ref int total)
        {
            database.StringIncrement(key, delta, CommandFlags.FireAndForget);
            total += delta;
        }

        [Test]
        public void WrappedDatabasePrefixIntegration()
        {
            using (var conn = Create())
            {
                var db = conn.GetDatabase().WithKeyPrefix("abc");
                db.KeyDelete("count");
                db.StringIncrement("count");
                db.StringIncrement("count");
                db.StringIncrement("count");

                int count = (int)conn.GetDatabase().StringGet("abccount");
                ClassicAssert.AreEqual(3, count);
            }
        }
    }

}
