using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using NUnit.Framework;

namespace StackExchange.Redis.Tests
{
    [TestFixture]
    public class MultiMaster : TestBase
    {
        protected override string GetConfiguration()
        {
            return PrimaryServer + ":" + SecurePort + "," + PrimaryServer + ":" + PrimaryPort + ",password=" + SecurePassword;
        }

        [Test]
        public void CannotFlushSlave()
        {
            Assert.Throws<RedisCommandException>(() => {
                ConfigurationOptions config = GetMasterSlaveConfig();
                using (var conn = ConnectionMultiplexer.Connect(config, log: Console.WriteLine))
                {
                    var servers = conn.GetEndPoints().Select(e => conn.GetServer(e));
                    var slave = servers.First(x => x.IsSlave);
                    slave.FlushDatabase();
                }
            },
            "Command cannot be issued to a slave: FLUSHDB");
        }

        [Test]
        public void DeslaveGoesToPrimary()
        {
            ConfigurationOptions config = GetMasterSlaveConfig();
            using (var conn = ConnectionMultiplexer.Connect(config))
            {

                var primary = conn.GetServer(new IPEndPoint(IPAddress.Parse(PrimaryServer), PrimaryPort));
                var secondary = conn.GetServer(new IPEndPoint(IPAddress.Parse(PrimaryServer), SlavePort));

                primary.Ping();
                secondary.Ping();

                primary.MakeMaster(ReplicationChangeOptions.SetTiebreaker);
                secondary.MakeMaster(ReplicationChangeOptions.None);

                primary.Ping();
                secondary.Ping();

                var sb = new StringBuilder();
                void AppendLog(string msg) => sb.AppendLine(msg);

                conn.Configure(msg => { AppendLog(msg); Console.WriteLine("Configure: " + msg); });
                string log = sb.ToString();

                Assert.IsTrue(log.Contains("tie-break is unanimous at " + PrimaryServer + ":" + PrimaryPort),
                    "unanimous");
                // k, so we know everyone loves 6379; is that what we get?

                var db = conn.GetDatabase();
                RedisKey key = Me();

                EndPoint demandMaster, preferMaster, preferSlave, demandSlave;
                preferMaster = db.IdentifyEndpoint(key, CommandFlags.PreferMaster);
                demandMaster = db.IdentifyEndpoint(key, CommandFlags.DemandMaster);
                preferSlave = db.IdentifyEndpoint(key, CommandFlags.PreferSlave);

                Assert.AreEqual(primary.EndPoint, demandMaster, "demand master");
                Assert.AreEqual(primary.EndPoint, preferMaster, "prefer master");
                Assert.AreEqual(primary.EndPoint, preferSlave, "prefer slave");

                try
                {
                    demandSlave = db.IdentifyEndpoint(key, CommandFlags.DemandSlave);
                    Assert.Fail("this should not have worked");
                }
                catch (RedisConnectionException ex)
                {
                    Assert.True(ex.Message.Contains("No connection is available to service this operation: EXISTS DeslaveGoesToPrimary"));
                }

                primary.MakeMaster(ReplicationChangeOptions.Broadcast | ReplicationChangeOptions.EnslaveSubordinates | ReplicationChangeOptions.SetTiebreaker, msg => Console.WriteLine("MakeMaster: " + msg));

                primary.Ping();
                secondary.Ping();

                preferMaster = db.IdentifyEndpoint(key, CommandFlags.PreferMaster);
                demandMaster = db.IdentifyEndpoint(key, CommandFlags.DemandMaster);
                preferSlave = db.IdentifyEndpoint(key, CommandFlags.PreferSlave);
                demandSlave = db.IdentifyEndpoint(key, CommandFlags.DemandSlave);

                Assert.AreEqual(primary.EndPoint, demandMaster, "demand master");
                Assert.AreEqual(primary.EndPoint, preferMaster, "prefer master");
                Assert.AreEqual(secondary.EndPoint, preferSlave, "prefer slave");
                Assert.AreEqual(secondary.EndPoint, preferSlave, "demand slave slave");

            }
        }

        private static ConfigurationOptions GetMasterSlaveConfig()
        {
            return new ConfigurationOptions
            {
                AllowAdmin = true,
                SyncTimeout = 100000,
                EndPoints =
                {
                    { PrimaryServer, PrimaryPort },
                    { PrimaryServer, SlavePort },
                }
            };
        }

        [Test]
        public void TestMultiNoTieBreak()
        {
            var sb = new StringBuilder();
            void AppendLog(string msg) => sb.AppendLine(msg);

            using (var conn = Create(log: AppendLog, tieBreaker: ""))
            {
                var results = sb.ToString();
                Console.WriteLine(results);
                Assert.IsTrue(results.Contains("Choosing master arbitrarily"));
            }
        }

        [Test]
        [TestCase(PrimaryServer + ":" + PrimaryPortString, PrimaryServer + ":" + PrimaryPortString, PrimaryServer + ":" + PrimaryPortString)]
        [TestCase(PrimaryServer + ":" + SecurePortString, PrimaryServer + ":" + SecurePortString, PrimaryServer + ":" + SecurePortString)]
        [TestCase(PrimaryServer + ":" + SecurePortString, PrimaryServer + ":" + PrimaryPortString, null)]
        [TestCase(PrimaryServer + ":" + PrimaryPortString, PrimaryServer + ":" + SecurePortString, null)]

        [TestCase(null, PrimaryServer + ":" + PrimaryPortString, PrimaryServer + ":" + PrimaryPortString)]
        [TestCase(PrimaryServer + ":" + PrimaryPortString, null, PrimaryServer + ":" + PrimaryPortString)]
        [TestCase(null, PrimaryServer + ":" + SecurePortString, PrimaryServer + ":" + SecurePortString)]
        [TestCase(PrimaryServer + ":" + SecurePortString, null, PrimaryServer + ":" + SecurePortString)]
        [TestCase(null, null, null)]

        public void TestMultiWithTiebreak(string a, string b, string elected)
        {
            const string TieBreak = "__tie__";
            // set the tie-breakers to the expected state
            using(var aConn = ConnectionMultiplexer.Connect(PrimaryServer + ":" + PrimaryPort))
            {
                aConn.GetDatabase().StringSet(TieBreak, a);
            }
            using (var aConn = ConnectionMultiplexer.Connect(PrimaryServer + ":" + SecurePort + ",password=" + SecurePassword))
            {
                aConn.GetDatabase().StringSet(TieBreak, b);
            }

            // see what happens
            var sb = new StringBuilder();
            void AppendLog(string msg) => sb.AppendLine(msg);
            using (var conn = Create(log: AppendLog, tieBreaker: TieBreak))
            {
                string text = sb.ToString();
                Console.WriteLine(text);
                Assert.IsFalse(text.Contains("failed to nominate"), "failed to nominate");
                if (elected != null)
                {
                    Assert.IsTrue(text.Contains("Elected: " + elected), "elected");
                }
                int nullCount = (a == null ? 1 : 0) + (b == null ? 1 : 0);
                if((a == b && nullCount == 0) || nullCount == 1)
                {
                    Assert.IsTrue(text.Contains("tie-break is unanimous"), "unanimous");
                    Assert.IsFalse(text.Contains("Choosing master arbitrarily"), "arbitrarily");
                }
                else
                {
                    Assert.IsFalse(text.Contains("tie-break is unanimous"), "unanimous");
                    Assert.IsTrue(text.Contains("Choosing master arbitrarily"), "arbitrarily");
                }
            }
        }
    }
}
