using System;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace StackExchange.Redis.Tests
{
    [TestFixture]
    public class TransientErrorTests : TestBase
    {

        [TestCase]
        public void TestExponentialRetry()
        {
            IReconnectRetryPolicy exponentialRetry = new ExponentialRetry(5000);
            ClassicAssert.False(exponentialRetry.ShouldRetry(0, 0));
            ClassicAssert.True(exponentialRetry.ShouldRetry(1, 5600));
            ClassicAssert.True(exponentialRetry.ShouldRetry(2, 6050));
            ClassicAssert.False(exponentialRetry.ShouldRetry(2, 4050));
        }

        [TestCase]
        public void TestExponentialMaxRetry()
        {
            IReconnectRetryPolicy exponentialRetry = new ExponentialRetry(5000);
            ClassicAssert.True(exponentialRetry.ShouldRetry(long.MaxValue, (int)TimeSpan.FromSeconds(30).TotalMilliseconds));
        }

        [TestCase]
        public void TestLinearRetry()
        {
            IReconnectRetryPolicy linearRetry = new LinearRetry(5000);
            ClassicAssert.False(linearRetry.ShouldRetry(0, 0));
            ClassicAssert.False(linearRetry.ShouldRetry(2, 4999));
            ClassicAssert.True(linearRetry.ShouldRetry(1, 5000));
        }
    }
}
