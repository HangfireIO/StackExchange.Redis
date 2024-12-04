using System;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace StackExchange.Redis.Tests
{
    [TestFixture]
    public class ExceptionFactoryTests : TestBase
    {
        [Test]
        public void NullLastException()
        {
            using (var muxer = Create(keepAlive: 1, connectTimeout: 10000, allowAdmin: true))
            {
                var conn = muxer.GetDatabase();
                ClassicAssert.Null(muxer.GetServerSnapshot()[0].LastException);
                var ex = ExceptionFactory.NoConnectionAvailable(true, true, new RedisCommand(), null, null, muxer.GetServerSnapshot());
                ClassicAssert.Null(ex.InnerException);
            }

        }

        [Test]
        public void NullSnapshot()
        {
            var ex = ExceptionFactory.NoConnectionAvailable(true, true, new RedisCommand(), null, null, null);
            ClassicAssert.Null(ex.InnerException);
        }
#if DEBUG // needs debug connection features
        [Test]
        public void MultipleEndpointsThrowAggregateException()
        {
            try
            {
                using (var muxer = Create(keepAlive: 1, connectTimeout: 10000, allowAdmin: true))
                {
                    var conn = muxer.GetDatabase();
                    muxer.AllowConnect = false;
                    SocketManager.ConnectCompletionType = CompletionType.Async;

                    foreach (var endpoint in muxer.GetEndPoints())
                    {
                        muxer.GetServer(endpoint).SimulateConnectionFailure();
                    }

                    var ex = ExceptionFactory.NoConnectionAvailable(true, true, new RedisCommand(), null, null, muxer.GetServerSnapshot());
                    ClassicAssert.IsInstanceOf<RedisConnectionException>(ex);
                    ClassicAssert.IsInstanceOf<AggregateException>(ex.InnerException);
                    var aggException = (AggregateException)ex.InnerException;
                    ClassicAssert.That(aggException.InnerExceptions.Count, Is.EqualTo(2));
                    for (int i = 0; i < aggException.InnerExceptions.Count; i++)
                    {
                        ClassicAssert.That(((RedisConnectionException)aggException.InnerExceptions[i]).FailureType, Is.EqualTo(ConnectionFailureType.SocketFailure));
                    }
                }
            }
            finally
            {
                SocketManager.ConnectCompletionType = CompletionType.Any;
                ClearAmbientFailures();
            }
        }

        [Test]
        public void NullInnerExceptionForMultipleEndpointsWithNoLastException()
        {
            try
            {
                using (var muxer = Create(keepAlive: 1, connectTimeout: 10000, allowAdmin: true))
                {
                    var conn = muxer.GetDatabase();
                    muxer.AllowConnect = false;
                    SocketManager.ConnectCompletionType = CompletionType.Async;
                    var ex = ExceptionFactory.NoConnectionAvailable(true, true, new RedisCommand(), null, null, muxer.GetServerSnapshot());
                    ClassicAssert.IsInstanceOf<RedisConnectionException>(ex);
                    ClassicAssert.Null(ex.InnerException);
                 }
            }
            finally
            {
                SocketManager.ConnectCompletionType = CompletionType.Any;
                ClearAmbientFailures();
            }
        }

        [Test]
        public void ServerTakesPrecendenceOverSnapshot()
        {
             try
            {
                using (var muxer = Create(keepAlive: 1, connectTimeout: 10000, allowAdmin: true))
                {
                    var conn = muxer.GetDatabase();
                    muxer.AllowConnect = false;
                    SocketManager.ConnectCompletionType = CompletionType.Async;

                    muxer.GetServer(muxer.GetEndPoints()[0]).SimulateConnectionFailure();

                    var ex = ExceptionFactory.NoConnectionAvailable(true, true, new RedisCommand(), null,muxer.GetServerSnapshot()[0], muxer.GetServerSnapshot());
                    ClassicAssert.IsInstanceOf<RedisConnectionException>(ex);
                    ClassicAssert.IsInstanceOf<Exception>(ex.InnerException);
                    ClassicAssert.That(muxer.GetServerSnapshot()[0].LastException, Is.EqualTo(ex.InnerException));
                }
            }
            finally
            {
                SocketManager.ConnectCompletionType = CompletionType.Any;
                ClearAmbientFailures();
            }

        }
#endif
    }
}
