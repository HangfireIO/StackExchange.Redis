using System;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis.KeyspaceIsolation;

namespace StackExchange.Redis.Tests
{

    [TestFixture]
    public class WithKeyPrefixTests : TestBase
    {
        [Test]
        public void BlankPrefixYieldsSame_Bytes()
        {
            using (var conn = Create())
            {
                var raw = conn.GetDatabase(1);
                var prefixed = raw.WithKeyPrefix(new byte[0]);
                ClassicAssert.AreSame(raw, prefixed);
            }
        }
        [Test]
        public void BlankPrefixYieldsSame_String()
        {
            using (var conn = Create())
            {
                var raw = conn.GetDatabase(1);
                var prefixed = raw.WithKeyPrefix("");
                ClassicAssert.AreSame(raw, prefixed);
            }
        }
        [Test]
        public void NullPrefixIsError_Bytes()
        {
            ClassicAssert.Throws<ArgumentNullException>(() => {
                using (var conn = Create())
                {
                    var raw = conn.GetDatabase(1);
                    var prefixed = raw.WithKeyPrefix((byte[])null);
                }
            });
        }
        [Test]
        public void NullPrefixIsError_String()
        {
            ClassicAssert.Throws<ArgumentNullException>(() => {
                using (var conn = Create())
                {
                    var raw = conn.GetDatabase(1);
                    var prefixed = raw.WithKeyPrefix((string)null);
                }
            });
        }

        [Test]
        [TestCase("abc")]
        [TestCase("")]
        [TestCase(null)]
        public void NullDatabaseIsError(string prefix)
        {
            ClassicAssert.Throws<ArgumentNullException>(() => {
                IDatabase raw = null;
                var prefixed = raw.WithKeyPrefix(prefix);
            });
        }
        [Test]
        public void BasicSmokeTest()
        {
            using(var conn = Create())
            {
                var raw = conn.GetDatabase(1);

                var foo = raw.WithKeyPrefix("foo");
                var foobar = foo.WithKeyPrefix("bar");

                string key = Me();

                string s = Guid.NewGuid().ToString(), t = Guid.NewGuid().ToString();

                foo.StringSet(key, s);
                var val = (string)foo.StringGet(key);
                ClassicAssert.AreEqual(s, val); // fooBasicSmokeTest

                foobar.StringSet(key, t);
                val = (string)foobar.StringGet(key);
                ClassicAssert.AreEqual(t, val); // foobarBasicSmokeTest

                val = (string)foo.StringGet("bar" + key);
                ClassicAssert.AreEqual(t, val); // foobarBasicSmokeTest

                val = (string)raw.StringGet("foo" + key);
                ClassicAssert.AreEqual(s, val); // fooBasicSmokeTest

                val = (string)raw.StringGet("foobar" + key);
                ClassicAssert.AreEqual(t, val); // foobarBasicSmokeTest
            }
        }
        [Test]
        public void ConditionTest()
        {
            using(var conn = Create())
            {
                var raw = conn.GetDatabase(2);

                var foo = raw.WithKeyPrefix("tran:");

                raw.KeyDelete("tran:abc");
                raw.KeyDelete("tran:i");

                // execute while key exists
                raw.StringSet("tran:abc", "def");
                var tran = foo.CreateTransaction();
                tran.AddCondition(Condition.KeyExists("abc"));
                tran.StringIncrementAsync("i");
                tran.Execute();

                int i = (int)raw.StringGet("tran:i");
                ClassicAssert.AreEqual(1, i);

                // repeat without key
                raw.KeyDelete("tran:abc");
                tran = foo.CreateTransaction();
                tran.AddCondition(Condition.KeyExists("abc"));
                tran.StringIncrementAsync("i");
                tran.Execute();

                i = (int)raw.StringGet("tran:i");
                ClassicAssert.AreEqual(1, i);
            }
        }
    }
}
