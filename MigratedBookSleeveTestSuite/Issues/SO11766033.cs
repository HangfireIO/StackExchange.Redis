﻿using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Tests.Issues
{
    [TestFixture]
    public class SO11766033
    {
        [Test]
        public void TestNullString()
        {
            const int db = 3;
            using (var muxer = Config.GetUnsecuredConnection(true))
            {
                var redis = muxer.GetDatabase(db);
                string expectedTestValue = null;
                var uid = Config.CreateUniqueName();
                redis.StringSetAsync(uid, "abc");
                redis.StringSetAsync(uid, expectedTestValue);
                string testValue = redis.StringGet(uid);
                ClassicAssert.IsNull(testValue);
            }
        }

        [Test]
        public void TestEmptyString()
        {
            const int db = 3;
            using (var muxer = Config.GetUnsecuredConnection(true))
            {
                var redis = muxer.GetDatabase(db);
                string expectedTestValue = "";
                var uid = Config.CreateUniqueName();

                redis.StringSetAsync(uid, expectedTestValue);
                string testValue = redis.StringGet(uid);

                ClassicAssert.AreEqual(expectedTestValue, testValue);
            }
        }
    }
}
