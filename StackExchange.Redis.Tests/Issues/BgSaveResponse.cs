using System;
using System.Threading;
using NUnit.Framework;

namespace StackExchange.Redis.Tests.Issues
{
    [TestFixture]
    public class BgSaveResponse : TestBase
    {
        [Test]
#pragma warning disable 0618
        [TestCase(SaveType.ForegroundSave)]
#pragma warning restore 0618
        [TestCase(SaveType.BackgroundSave)]
        [TestCase(SaveType.BackgroundRewriteAppendOnlyFile)]
        public void ShouldntThrowException(SaveType saveType)
        {
            using (var conn = Create(null, null, true))
            {
                var Server = GetServer(conn);

                for (var i = 0; i < 5; i++)
                {
                    try
                    {
                        Server.Save(saveType);
                        return;
                    }
                    catch (RedisServerException ex)
                    {
                        if (!ex.Message.Contains("already in progress"))
                        {
                            throw;
                        }
                    }

                    Thread.Sleep(1000);
                }
            }
        }
    }
}
