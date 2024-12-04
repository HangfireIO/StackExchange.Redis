using System.Net;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace StackExchange.Redis.Tests
{
    [TestFixture]
    public class Commands
    {
        [Test]
        public void Basic()
        {
            var config = ConfigurationOptions.Parse(".,$PING=p");
            ClassicAssert.AreEqual(1, config.EndPoints.Count);
            config.SetDefaultPorts();
            ClassicAssert.Contains(new DnsEndPoint(".",6379), config.EndPoints);
            var map = config.CommandMap;
            ClassicAssert.AreEqual("$PING=p", map.ToString());
            ClassicAssert.AreEqual(".:6379,$PING=p", config.ToString());
        }
    }
}
