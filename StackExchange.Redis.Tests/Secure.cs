using System;
using System.Diagnostics;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace StackExchange.Redis.Tests
{
    [TestFixture]
    public class Secure : TestBase
    {
        protected override string GetConfiguration()
        {
            return PrimaryServer + ":" + SecurePort + ",password=" + SecurePassword +",name=MyClient";
        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void MassiveBulkOpsFireAndForgetSecure(bool preserveOrder)
        {
            using (var muxer = Create())
            {
                muxer.PreserveAsyncOrder = preserveOrder;
                RedisKey key = "MBOF";
                var conn = muxer.GetDatabase();
                conn.Ping();

                var watch = Stopwatch.StartNew();

                for (int i = 0; i <= AsyncOpsQty; i++)
                {
                    conn.StringSet(key, i, flags: CommandFlags.FireAndForget);
                }
                int val = (int)conn.StringGet(key);
                ClassicAssert.AreEqual(AsyncOpsQty, val);
                watch.Stop();
                Console.WriteLine("{2}: Time for {0} ops: {1}ms ({3}); ops/s: {4}", AsyncOpsQty, watch.ElapsedMilliseconds, Me(),
                    preserveOrder ? "preserve order" : "any order",
                    AsyncOpsQty / watch.Elapsed.TotalSeconds);
            }
        }

        [Test]
        public void CheckConfig()
        {
            var config = ConfigurationOptions.Parse(GetConfiguration());
            foreach(var ep in config.EndPoints)
                Console.WriteLine(ep);
            ClassicAssert.AreEqual(1, config.EndPoints.Count);
            ClassicAssert.AreEqual("changeme", config.Password);
        }
        [Test]
        public void Connect()
        {
            using(var server = Create())
            {
                server.GetDatabase().Ping();
            }
        }
        [Test]
        [TestCase("wrong")]
        [TestCase("")]
        public void ConnectWithWrongPassword(string password)
        {
            ClassicAssert.Throws<RedisConnectionException>(() => {
                SetExpectedAmbientFailureCount(-1);
                using (var server = Create(password: password, checkConnect: false))
                {
                    server.GetDatabase().Ping();
                }
            },
            "No connection is available to service this operation: PING");
        }
    }
}
