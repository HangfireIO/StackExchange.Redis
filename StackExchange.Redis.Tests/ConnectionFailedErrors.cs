using NUnit.Framework;
using System.Threading;
using NUnit.Framework.Legacy;

namespace StackExchange.Redis.Tests
{
    [TestFixture]
    public class ConnectionFailedErrors : TestBase
    {
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void SSLCertificateValidationError(bool isCertValidationSucceeded)
        {
            string name, password;
            GetAzureCredentials(out name, out password);
            var options = new ConfigurationOptions();
            options.EndPoints.Add(name + ".redis.cache.windows.net");
            options.Ssl = true;
            options.Password = password;
            options.CertificateValidation += (sender, cert, chain, errors) => { return isCertValidationSucceeded; };
            options.AbortOnConnectFail = false;
            options.ConnectRetry = 1;

            using (var connection = ConnectionMultiplexer.Connect(options, log: System.Console.WriteLine))
            {
                connection.ConnectionFailed += (object sender, ConnectionFailedEventArgs e) =>
                {
                    ClassicAssert.That(e.FailureType, Is.EqualTo(ConnectionFailureType.AuthenticationFailure));
                };
                if (!isCertValidationSucceeded)
                {
                    Thread.Sleep(1000);

                    //validate that in this case it throws an certificatevalidation exception
                    var ex = ClassicAssert.Throws<RedisConnectionException>(() => connection.GetDatabase().Ping());
                    ClassicAssert.That(ex.Message.StartsWith("No connection is available to service this operation: PING; The remote certificate is invalid according to the validation procedure."), "Actual: " + ex.Message);
                    var rde = (RedisConnectionException)ex.InnerException;
                    ClassicAssert.NotNull(rde);
                    ClassicAssert.IsInstanceOf<RedisConnectionException>(rde);
                    ClassicAssert.That(rde.FailureType, Is.EqualTo(ConnectionFailureType.AuthenticationFailure));
                    ClassicAssert.That(rde.InnerException?.Message, Is.EqualTo("The remote certificate is invalid according to the validation procedure."));
                }
                else
                {
                    ClassicAssert.DoesNotThrow(() => connection.GetDatabase().Ping());
                }

                //wait for a second for connectionfailed event to fire
                Thread.Sleep(1000);
            }


        }

        [Test]
        public void AuthenticationFailureError()
        {
            string name, password;
            GetAzureCredentials(out name, out password);
            var options = new ConfigurationOptions();
            options.EndPoints.Add(name + ".redis.cache.windows.net");
            options.Ssl = true;
            options.Password = "";
            options.AbortOnConnectFail = false;
            options.ConnectRetry = 1;
            using (var muxer = ConnectionMultiplexer.Connect(options, log: System.Console.WriteLine))
            {
                muxer.ConnectionFailed += (object sender, ConnectionFailedEventArgs e) =>
                {
                    ClassicAssert.That(e.FailureType, Is.EqualTo(ConnectionFailureType.AuthenticationFailure));
                };
                var ex = ClassicAssert.Throws<RedisConnectionException>(() => muxer.GetDatabase().Ping());
                var rde = (RedisConnectionException)ex.InnerException;
                ClassicAssert.That(ex.CommandStatus, Is.EqualTo(CommandStatus.WaitingToBeSent));
                ClassicAssert.That(rde.FailureType, Is.EqualTo(ConnectionFailureType.AuthenticationFailure));
                ClassicAssert.That(rde.InnerException.Message, Is.EqualTo("Error: NOAUTH Authentication required. Verify if the Redis password provided is correct."));
                //wait for a second  for connectionfailed event to fire
                Thread.Sleep(1000);
            }
        }

        [Test]
        public void SocketFailureError()
        {
            var options = new ConfigurationOptions();
            options.EndPoints.Add(".redis.cache.windows.net");
            options.Ssl = true;
            options.Password = "";
            options.AbortOnConnectFail = false;
            options.ConnectRetry = 1;
            using (var muxer = ConnectionMultiplexer.Connect(options, log: System.Console.WriteLine))
            {
                Thread.Sleep(1000);
                var ex = ClassicAssert.Throws<RedisConnectionException>(() => muxer.GetDatabase().Ping());
                ClassicAssert.That(ex.Message.StartsWith("No connection is available to service this operation: PING; No such host is known"), "Actual: " + ex.Message);
                var rde = (RedisConnectionException)ex.InnerException;
                ClassicAssert.That(rde.FailureType, Is.EqualTo(ConnectionFailureType.SocketFailure));
            }
        }
#if DEBUG // needs AllowConnect, which is DEBUG only
        [Test]
        public void AbortOnConnectFailFalseConnectTimeoutError()
        {
            string name, password;
            GetAzureCredentials(out name, out password);
            var options = new ConfigurationOptions();
            options.EndPoints.Add(name + ".redis.cache.windows.net");
            options.Ssl = true;
            options.ConnectTimeout = 0;
            options.Password = password;
            using (var muxer = ConnectionMultiplexer.Connect(options))
            {
                var ex = ClassicAssert.Throws<RedisConnectionException>(() => muxer.GetDatabase().Ping());
                ClassicAssert.That(ex.Message, Does.Contain("ConnectTimeout"));
            }
        }

        [Test]
        public void CheckFailureRecovered()
        {
            try
            {
                using (var muxer = Create(keepAlive: 1, connectTimeout: 10000, allowAdmin: true))
                {
                    var conn = muxer.GetDatabase();
                    var server = muxer.GetServer(muxer.GetEndPoints()[0]);

                    muxer.AllowConnect = false;
                    SocketManager.ConnectCompletionType = CompletionType.Async;

                    server.SimulateConnectionFailure();

                    ClassicAssert.AreEqual(ConnectionFailureType.SocketFailure, ((RedisConnectionException)muxer.GetServerSnapshot()[0].LastException).FailureType);

                    // should reconnect within 1 keepalive interval
                    muxer.AllowConnect = true;
                    Thread.Sleep(2000);

                    ClassicAssert.Null(muxer.GetServerSnapshot()[0].LastException);
                }
            }
            finally
            {
                SocketManager.ConnectCompletionType = CompletionType.Any;
                ClearAmbientFailures();
            }
        }

#endif
        [Test]
        public void TryGetAzureRoleInstanceIdNoThrow()
        {
            ClassicAssert.IsNull(ConnectionMultiplexer.TryGetAzureRoleInstanceIdNoThrow());
        }
    }
}
