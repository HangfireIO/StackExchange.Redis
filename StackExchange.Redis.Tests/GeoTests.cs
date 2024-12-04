using NUnit.Framework;
using System;
using System.IO;
using System.Linq;
using NUnit.Framework.Legacy;

namespace StackExchange.Redis.Tests
{
    public class AzureTestAttribute : TestAttribute
    {
        
    }
    [TestFixture]
    public class GeoTests : TestBase
    {
        private ConnectionMultiplexer Create()
        {
            string name, password;
            GetAzureCredentials(out name, out password);
            var options = new ConfigurationOptions();
            options.EndPoints.Add(name + ".redis.cache.windows.net");
            options.Ssl = true;
            options.ConnectTimeout = 5000;
            options.Password = password;
            options.TieBreaker = "";
            var conn = ConnectionMultiplexer.Connect(options, Console.WriteLine);
            return conn;
        }
        public const int Db = 0;

        public static GeoEntry
            palermo = new GeoEntry(13.361389, 38.115556, "Palermo"),
            catania = new GeoEntry(15.087269, 37.502669, "Catania"),
            agrigento = new GeoEntry(13.5765, 37.311, "Agrigento"),
            cefalù = new GeoEntry(14.0188, 38.0084, "Cefalù");
        public static GeoEntry[] all = { palermo, catania, agrigento , cefalù };
        [AzureTest]
        public void GeoAdd()
        {
            using (var conn = Create())
            {
                var db = conn.GetDatabase(Db);
                RedisKey key = Me();
                db.KeyDelete(key);
                
                // add while not there
                ClassicAssert.IsTrue(db.GeoAdd(key, cefalù.Longitude, cefalù.Latitude, cefalù.Member));
                ClassicAssert.AreEqual(2, db.GeoAdd(key, new GeoEntry[] { palermo, catania }));                
                ClassicAssert.IsTrue(db.GeoAdd(key, agrigento));

                // now add again
                ClassicAssert.IsFalse(db.GeoAdd(key, cefalù.Longitude, cefalù.Latitude, cefalù.Member));
                ClassicAssert.AreEqual(0, db.GeoAdd(key, new GeoEntry[] { palermo, catania }));
                ClassicAssert.IsFalse(db.GeoAdd(key, agrigento));


            }
        }

        [AzureTest]
        public void GetDistance()
        {
            using (var conn = Create())
            {
                var db = conn.GetDatabase(Db);
                RedisKey key = Me();
                db.KeyDelete(key);
                db.GeoAdd(key, all);
                var val = db.GeoDistance(key, "Palermo", "Catania", GeoUnit.Meters);
                ClassicAssert.IsTrue(val.HasValue);
                var rounded = Math.Round(val.Value, 10);
                ClassicAssert.AreEqual(166274.1516, val);


                val = db.GeoDistance(key, "Palermo", "Nowhere", GeoUnit.Meters);
                ClassicAssert.IsFalse(val.HasValue);
            }
        }

        [AzureTest]
        public void GeoHash()
        {
            using (var conn = Create())
            {
                var db = conn.GetDatabase(Db);
                RedisKey key = Me();
                db.KeyDelete(key);
                db.GeoAdd(key, all);

                var hashes = db.GeoHash(key, new RedisValue[] { palermo.Member, "Nowhere", agrigento.Member });
                ClassicAssert.AreEqual(3, hashes.Length);
                ClassicAssert.AreEqual("sqc8b49rny0", hashes[0]);
                ClassicAssert.IsNull(hashes[1]);
                ClassicAssert.AreEqual("sq9skbq0760", hashes[2]);

                var hash = db.GeoHash(key, "Palermo");
                ClassicAssert.AreEqual("sqc8b49rny0", hash);

                hash = db.GeoHash(key, "Nowhere");
                ClassicAssert.IsNull(hash);
            }
        }

        [AzureTest]
        public void GeoGetPosition()
        {
            using (var conn = Create())
            {
                var db = conn.GetDatabase(Db);
                RedisKey key = Me();
                db.KeyDelete(key);
                db.GeoAdd(key, all);

                var pos = db.GeoPosition(key, palermo.Member);
                ClassicAssert.IsTrue(pos.HasValue);
                ClassicAssert.AreEqual(Math.Round(palermo.Longitude, 6), Math.Round(pos.Value.Longitude, 6));
                ClassicAssert.AreEqual(Math.Round(palermo.Latitude, 6), Math.Round(pos.Value.Latitude, 6));

                pos = db.GeoPosition(key, "Nowhere");
                ClassicAssert.IsFalse(pos.HasValue);
            }
        }

        [AzureTest]
        public void GeoRemove()
        {
            using (var conn = Create())
            {
                var db = conn.GetDatabase(Db);
                RedisKey key = Me();
                db.KeyDelete(key);
                db.GeoAdd(key, all);

                var pos = db.GeoPosition(key, "Palermo");
                ClassicAssert.IsTrue(pos.HasValue);

                ClassicAssert.IsFalse(db.GeoRemove(key, "Nowhere"));
                ClassicAssert.IsTrue(db.GeoRemove(key, "Palermo"));
                ClassicAssert.IsFalse(db.GeoRemove(key, "Palermo"));

                pos = db.GeoPosition(key, "Palermo");
                ClassicAssert.IsFalse(pos.HasValue);
            }
        }

        [AzureTest]
        public void GeoRadius()
        {
            using (var conn = Create())
            {
                var db = conn.GetDatabase(Db);
                RedisKey key = Me();
                db.KeyDelete(key);
                db.GeoAdd(key, all);

                var results = db.GeoRadius(key, cefalù.Member, 60, GeoUnit.Miles, 2, Order.Ascending);
                ClassicAssert.AreEqual(2, results.Length);

                ClassicAssert.AreEqual(results[0].Member, cefalù.Member);
                ClassicAssert.AreEqual(0, results[0].Distance.Value);
                ClassicAssert.AreEqual(Math.Round(results[0].Position.Value.Longitude, 5), Math.Round(cefalù.Position.Longitude, 5));
                ClassicAssert.AreEqual(Math.Round(results[0].Position.Value.Latitude, 5), Math.Round(cefalù.Position.Latitude, 5));
                ClassicAssert.IsFalse(results[0].Hash.HasValue);

                ClassicAssert.AreEqual(results[1].Member, palermo.Member);
                ClassicAssert.AreEqual(Math.Round(36.5319, 6), Math.Round(results[1].Distance.Value, 6));
                ClassicAssert.AreEqual(Math.Round(results[1].Position.Value.Longitude, 5), Math.Round(palermo.Position.Longitude, 5));
                ClassicAssert.AreEqual(Math.Round(results[1].Position.Value.Latitude, 5), Math.Round(palermo.Position.Latitude, 5));
                ClassicAssert.IsFalse(results[1].Hash.HasValue);

                results = db.GeoRadius(key, cefalù.Member, 60, GeoUnit.Miles, 2, Order.Ascending, GeoRadiusOptions.None);
                ClassicAssert.AreEqual(2, results.Length);
                ClassicAssert.AreEqual(results[0].Member, cefalù.Member);
                ClassicAssert.IsFalse(results[0].Position.HasValue);
                ClassicAssert.IsFalse(results[0].Distance.HasValue);
                ClassicAssert.IsFalse(results[0].Hash.HasValue);

                ClassicAssert.AreEqual(results[1].Member, palermo.Member);
                ClassicAssert.IsFalse(results[1].Position.HasValue);
                ClassicAssert.IsFalse(results[1].Distance.HasValue);
                ClassicAssert.IsFalse(results[1].Hash.HasValue);
            }
        }
    }
}
