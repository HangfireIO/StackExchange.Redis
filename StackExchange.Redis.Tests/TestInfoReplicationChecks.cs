using System;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace StackExchange.Redis.Tests
{
    [TestFixture]
    public class TestInfoReplicationChecks : TestBase
    {
        [Test]
        public void Exec()
        {
            using(var conn = Create())
            {
                var parsed = ConfigurationOptions.Parse(conn.Configuration);
                ClassicAssert.AreEqual(5, parsed.ConfigCheckSeconds);
                var before = conn.GetCounters();
                Thread.Sleep(TimeSpan.FromSeconds(13));
                var after = conn.GetCounters();
                int done = (int)(after.Interactive.CompletedSynchronously - before.Interactive.CompletedSynchronously);
                ClassicAssert.IsTrue(done >= 2);
            }
        }
        protected override string GetConfiguration()
        {
            return base.GetConfiguration() + ",configCheckSeconds=5";
        }
    }
}
