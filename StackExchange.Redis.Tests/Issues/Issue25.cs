using System;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace StackExchange.Redis.Tests.Issues
{
    [TestFixture]
    public class Issue25 : TestBase
    {
        [Test]
        public void CaseInsensitive()
        {
            var options = ConfigurationOptions.Parse("ssl=true");
            ClassicAssert.IsTrue(options.Ssl);
            ClassicAssert.AreEqual("ssl=True", options.ToString());

            options = ConfigurationOptions.Parse("SSL=TRUE");
            ClassicAssert.IsTrue(options.Ssl);
            ClassicAssert.AreEqual("ssl=True", options.ToString());
        }

        [Test]
        public void UnkonwnKeywordHandling_Ignore()
        {
            var options = ConfigurationOptions.Parse("ssl2=true", true);
        }
        [Test] 
        public void UnkonwnKeywordHandling_ExplicitFail()
        {
            ClassicAssert.Throws<ArgumentException>(() => {
                var options = ConfigurationOptions.Parse("ssl2=true", false);
            },
            "Keyword 'ssl2' is not supported");
        }
        [Test]
        public void UnkonwnKeywordHandling_ImplicitFail()
        {
            ClassicAssert.Throws<ArgumentException>(() => {
                var options = ConfigurationOptions.Parse("ssl2=true");
            },
            "Keyword 'ssl2' is not supported");
        }
    }
}
