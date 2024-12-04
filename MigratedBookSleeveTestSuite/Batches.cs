using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Tests
{
    [TestFixture]
    public class Batches
    {
        [Test]
        public void TestBatchNotSent()
        {
            using (var muxer = Config.GetUnsecuredConnection())
            {
                var conn = muxer.GetDatabase(0);
                conn.KeyDeleteAsync("batch");
                conn.StringSetAsync("batch", "batch-not-sent");
                var tasks = new List<Task>();
                var batch = conn.CreateBatch();
                
                tasks.Add(batch.KeyDeleteAsync("batch"));
                tasks.Add(batch.SetAddAsync("batch", "a"));
                tasks.Add(batch.SetAddAsync("batch", "b"));
                tasks.Add(batch.SetAddAsync("batch", "c"));

                ClassicAssert.AreEqual("batch-not-sent", (string)conn.StringGet("batch"));
            }
        }

        [Test]
        public void TestBatchSent()
        {
            using (var muxer = Config.GetUnsecuredConnection())
            {
                var conn = muxer.GetDatabase(0);
                conn.KeyDeleteAsync("batch");
                conn.StringSetAsync("batch", "batch-sent");
                var tasks = new List<Task>();
                var batch = conn.CreateBatch();
                tasks.Add(batch.KeyDeleteAsync("batch"));
                tasks.Add(batch.SetAddAsync("batch", "a"));
                tasks.Add(batch.SetAddAsync("batch", "b"));
                tasks.Add(batch.SetAddAsync("batch", "c"));
                batch.Execute();
                
                var result = conn.SetMembersAsync("batch");
                tasks.Add(result);
                Task.WhenAll(tasks.ToArray());

                var arr = result.Result;
                Array.Sort(arr, (x, y) => string.Compare(x, y));
                ClassicAssert.AreEqual(3, arr.Length);
                ClassicAssert.AreEqual("a", (string)arr[0]);
                ClassicAssert.AreEqual("b", (string)arr[1]);
                ClassicAssert.AreEqual("c", (string)arr[2]);
            }
        }
    }
}
