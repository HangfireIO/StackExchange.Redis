﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace StackExchange.Redis.Tests
{
    [TestFixture]
    public class Cluster : TestBase
    {
        //private const string ClusterIp = "192.168.0.15";  // marc
        //private const string ClusterIp = "10.110.11.102";   // kmontrose
        private const string ClusterIp = "127.0.0.1";
        private const int ServerCount = 6, FirstPort = 7000;

        protected override string GetConfiguration()
        {
            var server = ClusterIp;
            if (string.Equals(Environment.MachineName, "MARC-LAPTOP", StringComparison.OrdinalIgnoreCase))
            {
                server = "192.168.56.101";
            }
            return string.Join(",",
            from port in Enumerable.Range(FirstPort, ServerCount)
            select server + ":" + port) + ",connectTimeout=10000";
        }

        [Test]
        public void ExportConfiguration()
        {
            if (File.Exists("cluster.zip")) File.Delete("cluster.zip");
            ClassicAssert.IsFalse(File.Exists("cluster.zip"));
            using (var muxer = Create(allowAdmin: true))
            using(var file = File.Create("cluster.zip"))
            {
                muxer.ExportConfiguration(file);
            }
            ClassicAssert.IsTrue(File.Exists("cluster.zip"));
        }

        [Test]
        public void ConnectUsesSingleSocket()
        {
            for(int i = 0; i<10;i++)
            {
                using (var muxer = Create(failMessage: i + ": "))
                {
                    var eps = muxer.GetEndPoints();
                    foreach (var ep in eps)
                    {
                        var srv = muxer.GetServer(ep);
                        var counters = srv.GetCounters();
                        ClassicAssert.AreEqual(1, counters.Interactive.SocketCount, i + "; interactive, " + ep.ToString());
                        ClassicAssert.AreEqual(1, counters.Subscription.SocketCount, i + "; subscription, " + ep.ToString());
                    }
                }
            }
        }


        [Test]
        public void CanGetTotalStats()
        {
            using(var muxer = Create())
            {
                var counters = muxer.GetCounters();
                Console.WriteLine(counters);
            }
        }

        [Test]
        public void Connect()
        {
            using (var muxer = Create())
            {
                var endpoints = muxer.GetEndPoints();

                ClassicAssert.AreEqual(ServerCount, endpoints.Length);
                var expectedPorts = new HashSet<int>(Enumerable.Range(FirstPort, ServerCount));
                int masters = 0, slaves = 0;
                var failed = new List<EndPoint>();
                foreach (var endpoint in endpoints)
                {
                    var server = muxer.GetServer(endpoint);
                    if (!server.IsConnected)
                    {
                        failed.Add(endpoint);
                    }
                    ClassicAssert.AreEqual(endpoint, server.EndPoint, "endpoint:" + endpoint);
                    ClassicAssert.IsInstanceOf<IPEndPoint>(endpoint, "endpoint-type:" + endpoint);
                    ClassicAssert.IsTrue(expectedPorts.Remove(((IPEndPoint)endpoint).Port), "port:" + endpoint);
                    ClassicAssert.AreEqual(ServerType.Cluster, server.ServerType, "server-type:" + endpoint);
                    if (server.IsSlave) slaves++;
                    else masters++;
                }
                if (failed.Count != 0)
                {
                    Console.WriteLine("{0} failues", failed.Count);
                    foreach (var fail in failed)
                    {
                        Console.WriteLine(fail);
                    }
                    ClassicAssert.Fail("not all servers connected");
                }

                ClassicAssert.AreEqual(ServerCount / 2, slaves, "slaves");
                ClassicAssert.AreEqual(ServerCount / 2, masters, "masters");

            }
        }

        [Test]
        public void TestIdentity()
        {
            using(var conn = Create())
            {
                RedisKey key = Guid.NewGuid().ToByteArray();
                var ep = conn.GetDatabase().IdentifyEndpoint(key);
                var server = conn.GetServer(ep);
                ClassicAssert.AreEqual(ep, server.ClusterConfiguration.GetBySlot(key).EndPoint);
            }
        }

        [Test]
        public void IntentionalWrongServer()
        {
            using (var conn = Create())
            {
                var endpoints = conn.GetEndPoints();
                var servers = endpoints.Select(e => conn.GetServer(e));

                var key = Me();
                const string value = "abc";
                var db = conn.GetDatabase();
                db.KeyDelete(key);
                db.StringSet(key, value);
                servers.First().Ping();
                var config = servers.First().ClusterConfiguration;
                ClassicAssert.IsNotNull(config);
                int slot = conn.HashSlot(key);
                var rightMasterNode = config.GetBySlot(key);
                ClassicAssert.IsNotNull(rightMasterNode);

                

#if DEBUG
                string a = conn.GetServer(rightMasterNode.EndPoint).StringGet(db.Database, key);
                ClassicAssert.AreEqual(value, a, "right master");

                var node = config.Nodes.FirstOrDefault(x => !x.IsSlave && x.NodeId != rightMasterNode.NodeId);
                ClassicAssert.IsNotNull(node);
                if (node != null)
                {
                    string b = conn.GetServer(node.EndPoint).StringGet(db.Database, key);
                    ClassicAssert.AreEqual(value, b, "wrong master, allow redirect");

                    try
                    {
                        string c = conn.GetServer(node.EndPoint).StringGet(db.Database, key, CommandFlags.NoRedirect);
                        ClassicAssert.Fail("wrong master, no redirect");
                    } catch (RedisServerException ex)
                    {
                        ClassicAssert.AreEqual("MOVED " + slot + " " + rightMasterNode.EndPoint.ToString(), ex.Message, "wrong master, no redirect");
                    }
                }

                node = config.Nodes.FirstOrDefault(x => x.IsSlave && x.ParentNodeId == rightMasterNode.NodeId);
                ClassicAssert.IsNotNull(node);
                if (node != null)
                {
                    string d = conn.GetServer(node.EndPoint).StringGet(db.Database, key);
                    ClassicAssert.AreEqual(value, d, "right slave");
                }

                node = config.Nodes.FirstOrDefault(x => x.IsSlave && x.ParentNodeId != rightMasterNode.NodeId);
                ClassicAssert.IsNotNull(node);
                if (node != null)
                {
                    string e = conn.GetServer(node.EndPoint).StringGet(db.Database, key);
                    ClassicAssert.AreEqual(value, e, "wrong slave, allow redirect");

                    try
                    {
                        string f = conn.GetServer(node.EndPoint).StringGet(db.Database, key, CommandFlags.NoRedirect);
                        ClassicAssert.Fail("wrong slave, no redirect");
                    }
                    catch (RedisServerException ex)
                    {
                        ClassicAssert.AreEqual("MOVED " + slot + " " + rightMasterNode.EndPoint.ToString(), ex.Message, "wrong slave, no redirect");
                    }
                }
#endif

            }
        }

        [Test]
        public void TransactionWithMultiServerKeys()
        {
            ClassicAssert.Throws<RedisCommandException>(() =>
            {
                using (var muxer = Create())
                {
                    // connect
                    var cluster = muxer.GetDatabase();
                    var anyServer = muxer.GetServer(muxer.GetEndPoints()[0]);
                    anyServer.Ping();
                    ClassicAssert.AreEqual(ServerType.Cluster, anyServer.ServerType);
                    var config = anyServer.ClusterConfiguration;
                    ClassicAssert.IsNotNull(config);

                    // invent 2 keys that we believe are served by different nodes
                    string x = Guid.NewGuid().ToString(), y;
                    var xNode = config.GetBySlot(x);
                    int abort = 1000;
                    do
                    {
                        y = Guid.NewGuid().ToString();
                    } while (--abort > 0 && config.GetBySlot(y) == xNode);
                    if (abort == 0) ClassicAssert.Inconclusive("failed to find a different node to use");
                    var yNode = config.GetBySlot(y);
                    Console.WriteLine("x={0}, served by {1}", x, xNode.NodeId);
                    Console.WriteLine("y={0}, served by {1}", y, yNode.NodeId);
                    ClassicAssert.AreNotEqual(xNode.NodeId, yNode.NodeId, "same node");

                    // wipe those keys
                    cluster.KeyDelete(x, CommandFlags.FireAndForget);
                    cluster.KeyDelete(y, CommandFlags.FireAndForget);

                    // create a transaction that attempts to assign both keys
                    var tran = cluster.CreateTransaction();
                    tran.AddCondition(Condition.KeyNotExists(x));
                    tran.AddCondition(Condition.KeyNotExists(y));
                    var setX = tran.StringSetAsync(x, "x-val");
                    var setY = tran.StringSetAsync(y, "y-val");
                    bool success = tran.Execute();

                    ClassicAssert.Fail("Expected single-slot rules to apply");
                    // the rest no longer applies while we are following single-slot rules

                    //// check that everything was aborted
                    //ClassicAssert.IsFalse(success, "tran aborted");
                    //ClassicAssert.IsTrue(setX.IsCanceled, "set x cancelled");
                    //ClassicAssert.IsTrue(setY.IsCanceled, "set y cancelled");
                    //var existsX = cluster.KeyExistsAsync(x);
                    //var existsY = cluster.KeyExistsAsync(y);
                    //ClassicAssert.IsFalse(cluster.Wait(existsX), "x exists");
                    //ClassicAssert.IsFalse(cluster.Wait(existsY), "y exists");
                }
            },
            "Multi-key operations must involve a single slot; keys can use 'hash tags' to help this, i.e. '{/users/12345}/account' and '{/users/12345}/contacts' will always be in the same slot");
        }

        [Test]
        public void TransactionWithSameServerKeys()
        {
            ClassicAssert.Throws<RedisCommandException>(() =>
            {
                using (var muxer = Create())
                {
                    // connect
                    var cluster = muxer.GetDatabase();
                    var anyServer = muxer.GetServer(muxer.GetEndPoints()[0]);
                    anyServer.Ping();
                    var config = anyServer.ClusterConfiguration;
                    ClassicAssert.IsNotNull(config);

                    // invent 2 keys that we believe are served by different nodes
                    string x = Guid.NewGuid().ToString(), y;
                    var xNode = config.GetBySlot(x);
                    int abort = 1000;
                    do
                    {
                        y = Guid.NewGuid().ToString();
                    } while (--abort > 0 && config.GetBySlot(y) != xNode);
                    if (abort == 0) ClassicAssert.Inconclusive("failed to find a key with the same node to use");
                    var yNode = config.GetBySlot(y);
                    Console.WriteLine("x={0}, served by {1}", x, xNode.NodeId);
                    Console.WriteLine("y={0}, served by {1}", y, yNode.NodeId);
                    ClassicAssert.AreEqual(xNode.NodeId, yNode.NodeId, "same node");

                    // wipe those keys
                    cluster.KeyDelete(x, CommandFlags.FireAndForget);
                    cluster.KeyDelete(y, CommandFlags.FireAndForget);

                    // create a transaction that attempts to assign both keys
                    var tran = cluster.CreateTransaction();
                    tran.AddCondition(Condition.KeyNotExists(x));
                    tran.AddCondition(Condition.KeyNotExists(y));
                    var setX = tran.StringSetAsync(x, "x-val");
                    var setY = tran.StringSetAsync(y, "y-val");
                    bool success = tran.Execute();

                    ClassicAssert.Fail("Expected single-slot rules to apply");
                    // the rest no longer applies while we are following single-slot rules

                    //// check that everything was aborted
                    //ClassicAssert.IsTrue(success, "tran aborted");
                    //ClassicAssert.IsFalse(setX.IsCanceled, "set x cancelled");
                    //ClassicAssert.IsFalse(setY.IsCanceled, "set y cancelled");
                    //var existsX = cluster.KeyExistsAsync(x);
                    //var existsY = cluster.KeyExistsAsync(y);
                    //ClassicAssert.IsTrue(cluster.Wait(existsX), "x exists");
                    //ClassicAssert.IsTrue(cluster.Wait(existsY), "y exists");
                }
            },
            "Multi-key operations must involve a single slot; keys can use 'hash tags' to help this, i.e. '{/users/12345}/account' and '{/users/12345}/contacts' will always be in the same slot");
        }

        [Test]
        public void TransactionWithSameSlotKeys()
        {
            using (var muxer = Create())
            {
                // connect
                var cluster = muxer.GetDatabase();
                var anyServer = muxer.GetServer(muxer.GetEndPoints()[0]);
                anyServer.Ping();
                var config = anyServer.ClusterConfiguration;
                ClassicAssert.IsNotNull(config);

                // invent 2 keys that we believe are in the same slot
                var guid = Guid.NewGuid().ToString();
                string x = "/{" + guid + "}/foo", y = "/{" + guid + "}/bar";

                ClassicAssert.AreEqual(muxer.HashSlot(x), muxer.HashSlot(y));
                var xNode = config.GetBySlot(x);
                var yNode = config.GetBySlot(y);
                Console.WriteLine("x={0}, served by {1}", x, xNode.NodeId);
                Console.WriteLine("y={0}, served by {1}", y, yNode.NodeId);
                ClassicAssert.AreEqual(xNode.NodeId, yNode.NodeId, "same node");

                // wipe those keys
                cluster.KeyDelete(x, CommandFlags.FireAndForget);
                cluster.KeyDelete(y, CommandFlags.FireAndForget);

                // create a transaction that attempts to assign both keys
                var tran = cluster.CreateTransaction();
                tran.AddCondition(Condition.KeyNotExists(x));
                tran.AddCondition(Condition.KeyNotExists(y));
                var setX = tran.StringSetAsync(x, "x-val");
                var setY = tran.StringSetAsync(y, "y-val");
                bool success = tran.Execute();

                // check that everything was aborted
                ClassicAssert.IsTrue(success, "tran aborted");
                ClassicAssert.IsFalse(setX.IsCanceled, "set x cancelled");
                ClassicAssert.IsFalse(setY.IsCanceled, "set y cancelled");
                var existsX = cluster.KeyExistsAsync(x);
                var existsY = cluster.KeyExistsAsync(y);
                ClassicAssert.IsTrue(cluster.Wait(existsX), "x exists");
                ClassicAssert.IsTrue(cluster.Wait(existsY), "y exists");
            }
        }

        [Test]
        [TestCase(null, 10)]
        [TestCase(null, 100)]
        [TestCase("abc", 10)]
        [TestCase("abc", 100)]

        public void Keys(string pattern, int pageSize)
        {
            using (var conn = Create(allowAdmin: true))
            {
                var cluster = conn.GetDatabase();
                var server = conn.GetEndPoints().Select(x => conn.GetServer(x)).First(x => !x.IsSlave);
                server.FlushAllDatabases();
                try
                {
                    ClassicAssert.IsFalse(server.Keys(pattern: pattern, pageSize: pageSize).Any());
                    Console.WriteLine("Complete: '{0}' / {1}", pattern, pageSize);
                } catch
                {
                    Console.WriteLine("Failed: '{0}' / {1}", pattern, pageSize);
                    throw;
                }
            }
        }

        [Test]
        [TestCase("", 0)]
        [TestCase("abc", 7638)]
        [TestCase("{abc}", 7638)]
        [TestCase("abcdef", 15101)]
        [TestCase("abc{abc}def", 7638)]
        [TestCase("c", 7365)]
        [TestCase("g", 7233)]
        [TestCase("d", 11298)]

        [TestCase("user1000", 3443)]
        [TestCase("{user1000}", 3443)]
        [TestCase("abc{user1000}", 3443)]
        [TestCase("abc{user1000}def", 3443)]
        [TestCase("{user1000}.following", 3443)]
        [TestCase("{user1000}.followers", 3443)]

        [TestCase("foo{}{bar}", 8363)]

        [TestCase("foo{{bar}}zap", 4015)]
        [TestCase("{bar", 4015)]

        [TestCase("foo{bar}{zap}", 5061)]
        [TestCase("bar", 5061)]

        public void HashSlots(string key, int slot)
        {
            using(var muxer = Create(connectTimeout: 500, pause: false))
            {
                ClassicAssert.AreEqual(slot, muxer.HashSlot(key));
            }
        }


        [Test]
        public void SScan()
        {
            using (var conn = Create())
            {
                RedisKey key = "a";
                var db = conn.GetDatabase();
                db.KeyDelete(key);

                int totalUnfiltered = 0, totalFiltered = 0;
                for (int i = 0; i < 1000; i++)
                {
                    db.SetAdd(key, i);
                    totalUnfiltered += i;
                    if (i.ToString().Contains("3")) totalFiltered += i;
                }
                var unfilteredActual = db.SetScan(key).Select(x => (int)x).Sum();
                var filteredActual = db.SetScan(key, "*3*").Select(x => (int)x).Sum();
                ClassicAssert.AreEqual(totalUnfiltered, unfilteredActual);
                ClassicAssert.AreEqual(totalFiltered, filteredActual);
            }
        }

        [Test]
        public void GetConfig()
        {
            using(var muxer = Create(allowAdmin: true))
            {
                var endpoints = muxer.GetEndPoints();
                var server = muxer.GetServer(endpoints.First());
                var nodes = server.ClusterNodes();

                ClassicAssert.AreEqual(endpoints.Length, nodes.Nodes.Count);
                foreach(var node in nodes.Nodes.OrderBy(x => x))
                {
                    Console.WriteLine(node);
                }
            }
        }

        [Test]
        public void AccessRandomKeys()
        {
            using(var conn = Create(allowAdmin: true))
            {
                
                var cluster = conn.GetDatabase();
                int slotMovedCount = 0;
                conn.HashSlotMoved += (s, a) =>
                {
                    Console.WriteLine("{0} moved from {1} to {2}", a.HashSlot, Describe(a.OldEndPoint), Describe(a.NewEndPoint));
                    Interlocked.Increment(ref slotMovedCount);
                };
                var pairs = new Dictionary<string, string>();
                const int COUNT = 500;
                Task[] send = new Task[COUNT];
                int index = 0;

                var servers = conn.GetEndPoints().Select(x => conn.GetServer(x));
                foreach (var server in servers)
                {
                    if (!server.IsSlave)
                    {
                        server.Ping();
                        server.FlushAllDatabases();
                    }
                }

                for(int i = 0; i < COUNT; i++)
                {
                    var key = Guid.NewGuid().ToString();
                    var value = Guid.NewGuid().ToString();
                    pairs.Add(key, value);
                    send[index++] = cluster.StringSetAsync(key, value);
                }
                conn.WaitAll(send);

                var expected = new string[COUNT];
                var actual = new Task<RedisValue>[COUNT];
                index = 0;
                foreach (var pair in pairs)
                {
                    expected[index] = pair.Value;
                    actual[index] = cluster.StringGetAsync(pair.Key);
                    index++;
                }
                cluster.WaitAll(actual);
                for(int i = 0; i < COUNT; i++)
                {
                    ClassicAssert.AreEqual(expected[i], (string)actual[i].Result);
                }

                int total = 0;
                Parallel.ForEach(servers, server =>
                {
                    if (!server.IsSlave)
                    {
                        int count = server.Keys(pageSize: 100).Count();
                        Console.WriteLine("{0} has {1} keys", server.EndPoint, count);
                        Interlocked.Add(ref total, count);
                    }
                });

                foreach (var server in servers)
                {
                    var counters = server.GetCounters();
                    Console.WriteLine(counters);
                }
                int final = Interlocked.CompareExchange(ref total, 0, 0);
                ClassicAssert.AreEqual(COUNT, final);
                ClassicAssert.AreEqual(0, Interlocked.CompareExchange(ref slotMovedCount, 0, 0), "slot moved count");
            }
        }

        [Test]
        [TestCase(CommandFlags.DemandMaster, false)]
        [TestCase(CommandFlags.DemandSlave, true)]
        [TestCase(CommandFlags.PreferMaster, false)]
        [TestCase(CommandFlags.PreferSlave, true)]
        public void GetFromRightNodeBasedOnFlags(CommandFlags flags, bool isSlave)
        {
            using(var muxer = Create(allowAdmin: true))
            {
                var db = muxer.GetDatabase();

                for (int i = 0; i < 1000; i++)
                {
                    try
                    {
                        var key = i.ToString();
                        var endpoint = db.IdentifyEndpoint(key, flags);
                        var server = muxer.GetServer(endpoint);
                        ClassicAssert.AreEqual(isSlave, server.IsSlave, key);
                    }
                    catch (Exception ex)
                    {
                        ClassicAssert.Fail($"An exception occurred on iteration '{i}': {ex}");
                    }
                }
            }
        }

        private static string Describe(EndPoint endpoint)
        {
            return endpoint?.ToString() ?? "(unknown)";
        }

        class TestProfiler : IProfiler
        {
            public object MyContext = new object();

            public object GetContext()
            {
                return MyContext;
            }
        }

        [Test]
        public void SimpleProfiling()
        {
            using (var conn = Create())
            {
                var profiler = new TestProfiler();

                conn.RegisterProfiler(profiler);
                conn.BeginProfiling(profiler.MyContext);
                var db = conn.GetDatabase();
                db.StringSet("hello", "world");
                var val = db.StringGet("hello");
                ClassicAssert.AreEqual("world", (string)val);

                var msgs = conn.FinishProfiling(profiler.MyContext);
                ClassicAssert.AreEqual(2, msgs.Count());
                ClassicAssert.IsTrue(msgs.Any(m => m.Command == "GET"));
                ClassicAssert.IsTrue(msgs.Any(m => m.Command == "SET"));
            }
        }

#if DEBUG
        [Test]
        public void MovedProfiling()
        {
            const string Key = "redirected-key";
            const string Value = "redirected-value";

            var profiler = new TestProfiler();

            using (var conn = Create())
            {
                conn.RegisterProfiler(profiler);

                var endpoints = conn.GetEndPoints();
                var servers = endpoints.Select(e => conn.GetServer(e));

                conn.BeginProfiling(profiler.MyContext);
                var db = conn.GetDatabase();
                db.KeyDelete(Key);
                db.StringSet(Key, Value);
                var config = servers.First().ClusterConfiguration;
                ClassicAssert.IsNotNull(config);

                int slot = conn.HashSlot(Key);
                var rightMasterNode = config.GetBySlot(Key);
                ClassicAssert.IsNotNull(rightMasterNode);

                string a = conn.GetServer(rightMasterNode.EndPoint).StringGet(db.Database, Key);
                ClassicAssert.AreEqual(Value, a, "right master");

                var wrongMasterNode = config.Nodes.FirstOrDefault(x => !x.IsSlave && x.NodeId != rightMasterNode.NodeId);
                ClassicAssert.IsNotNull(wrongMasterNode);

                string b = conn.GetServer(wrongMasterNode.EndPoint).StringGet(db.Database, Key);
                ClassicAssert.AreEqual(Value, b, "wrong master, allow redirect");

                var msgs = conn.FinishProfiling(profiler.MyContext).ToList();
                
                // verify that things actually got recorded properly, and the retransmission profilings are connected as expected
                {
                    // expect 1 DEL, 1 SET, 1 GET (to right master), 1 GET (to wrong master) that was responded to by an ASK, and 1 GET (to right master or a slave of it)
                    ClassicAssert.AreEqual(5, msgs.Count);
                    ClassicAssert.AreEqual(1, msgs.Count(c => c.Command == "DEL"));
                    ClassicAssert.AreEqual(1, msgs.Count(c => c.Command == "SET"));
                    ClassicAssert.AreEqual(3, msgs.Count(c => c.Command == "GET"));

                    var toRightMasterNotRetransmission = msgs.Where(m => m.Command == "GET" && m.EndPoint.Equals(rightMasterNode.EndPoint) && m.RetransmissionOf == null);
                    ClassicAssert.AreEqual(1, toRightMasterNotRetransmission.Count());

                    var toWrongMasterWithoutRetransmission = msgs.Where(m => m.Command == "GET" && m.EndPoint.Equals(wrongMasterNode.EndPoint) && m.RetransmissionOf == null);
                    ClassicAssert.AreEqual(1, toWrongMasterWithoutRetransmission.Count());

                    var toRightMasterOrSlaveAsRetransmission = msgs.Where(m => m.Command == "GET" && (m.EndPoint.Equals(rightMasterNode.EndPoint) || rightMasterNode.Children.Any(c => m.EndPoint.Equals(c.EndPoint))) && m.RetransmissionOf != null);
                    ClassicAssert.AreEqual(1, toRightMasterOrSlaveAsRetransmission.Count());

                    var originalWrongMaster = toWrongMasterWithoutRetransmission.Single();
                    var retransmissionToRight = toRightMasterOrSlaveAsRetransmission.Single();

                    ClassicAssert.IsTrue(object.ReferenceEquals(originalWrongMaster, retransmissionToRight.RetransmissionOf));
                }

                foreach(var msg in msgs)
                {
                    ClassicAssert.IsTrue(msg.CommandCreated != default(DateTime));
                    ClassicAssert.IsTrue(msg.CreationToEnqueued > TimeSpan.Zero);
                    ClassicAssert.IsTrue(msg.EnqueuedToSending > TimeSpan.Zero);
                    ClassicAssert.IsTrue(msg.SentToResponse > TimeSpan.Zero);
                    ClassicAssert.IsTrue(msg.ResponseToCompletion > TimeSpan.Zero);
                    ClassicAssert.IsTrue(msg.ElapsedTime > TimeSpan.Zero);

                    if (msg.RetransmissionOf != null)
                    {
                        // imprecision of DateTime.UtcNow makes this pretty approximate
                        ClassicAssert.IsTrue(msg.RetransmissionOf.CommandCreated <= msg.CommandCreated);
                        ClassicAssert.AreEqual(RetransmissionReasonType.Moved, msg.RetransmissionReason.Value);
                    }
                    else
                    {
                        ClassicAssert.IsFalse(msg.RetransmissionReason.HasValue);
                    }
                }
            }
        }
#endif
    }
}
