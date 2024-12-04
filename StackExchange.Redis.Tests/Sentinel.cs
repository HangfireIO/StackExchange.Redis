using System;
using System.Linq;
using System.Net;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace StackExchange.Redis.Tests
{
    [TestFixture]
    public class Sentinel
    {
        // TODO fill in these constants before running tests
        private const string IP = "127.0.0.1";
        private const int Port = 26379;
        private const string ServiceName = "mymaster";

        private static readonly ConnectionMultiplexer Conn = GetConn();

        private static IServer GetServer() => Conn.SentinelConnection.GetServer(IP, Port);

        public static ConnectionMultiplexer GetConn()
        {
            // create a connection
            var options = new ConfigurationOptions()
            {
                CommandMap = CommandMap.Sentinel,
                EndPoints = { { IP, Port } },
                AllowAdmin = true,
                TieBreaker = "",
                ServiceName = ServiceName,
                SyncTimeout = 5000
            };
            var connection = ConnectionMultiplexer.Connect(options, Console.WriteLine);
            Thread.Sleep(3000);
            ClassicAssert.IsTrue(connection.IsConnected);
            return connection;
        }

        [Test]
        public void PingTest()
        {
            var test = GetServer().Ping();
            Console.WriteLine("ping took {0} ms", test.TotalMilliseconds);
        }

        [Test]
        public void SentinelGetMasterAddressByNameTest()
        {
            var endpoint = GetServer().SentinelGetMasterAddressByName(ServiceName);
            ClassicAssert.IsNotNull(endpoint);
            var ipEndPoint = endpoint as IPEndPoint;
            ClassicAssert.IsNotNull(ipEndPoint);
            Console.WriteLine("{0}:{1}", ipEndPoint.Address, ipEndPoint.Port);
        }

        [Test]
        public void SentinelGetMasterAddressByNameNegativeTest() 
        {
            var endpoint = GetServer().SentinelGetMasterAddressByName("FakeServiceName");
            ClassicAssert.IsNull(endpoint);
        }

        [Test]
        public void SentinelMasterTest()
        {
            var dict = GetServer().SentinelMaster(ServiceName).ToDictionary();
            ClassicAssert.AreEqual(ServiceName, dict["name"]);
            foreach (var kvp in dict)
            {
                Console.WriteLine("{0}:{1}", kvp.Key, kvp.Value);
            }
        }

        [Test]
        public void SentinelMastersTest()
        {
            var masterConfigs = GetServer().SentinelMasters();
            ClassicAssert.IsTrue(masterConfigs.First().ToDictionary().ContainsKey("name"));
            foreach (var config in masterConfigs)
            {
                foreach (var kvp in config)
                {
                    Console.WriteLine("{0}:{1}", kvp.Key, kvp.Value);
                }
            }
        }

        [Test]
        public void SentinelSlavesTest() 
        {
            var slaveConfigs = GetServer().SentinelSlaves(ServiceName);
            if (slaveConfigs.Any()) 
            {
                ClassicAssert.IsTrue(slaveConfigs.First().ToDictionary().ContainsKey("name"));
            }
            foreach (var config in slaveConfigs) 
            {
                foreach (var kvp in config) {
                    Console.WriteLine("{0}:{1}", kvp.Key, kvp.Value);
                }
            }
        }

        [Test]
        public void SentinelFailoverTest()
        {
            GetServer().SentinelFailover(ServiceName);
        }
    }
}
