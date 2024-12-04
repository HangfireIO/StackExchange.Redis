using System;
using System.Globalization;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace StackExchange.Redis.Tests
{
    [TestFixture]
    public class KeysAndValues
    {
        [Test]
        public void TestValues()
        {
            RedisValue @default = default(RedisValue);
            CheckNull(@default);

            RedisValue nullString = (string)null;
            CheckNull(nullString);

            RedisValue nullBlob = (byte[])null;
            CheckNull(nullBlob);

            RedisValue emptyString = "";
            CheckNotNull(emptyString);

            RedisValue emptyBlob = new byte[0];
            CheckNotNull(emptyBlob);

            RedisValue a0 = new string('a', 1);
            CheckNotNull(a0);
            RedisValue a1 = new string('a', 1);
            CheckNotNull(a1);
            RedisValue b0 = new [] { (byte)'b' };
            CheckNotNull(b0);
            RedisValue b1 = new [] { (byte)'b' };
            CheckNotNull(b1);

            RedisValue i4 = 1;
            CheckNotNull(i4);
            RedisValue i8 = 1L;
            CheckNotNull(i8);

            RedisValue bool1 = true;
            CheckNotNull(bool1);
            RedisValue bool2 = false;
            CheckNotNull(bool2);
            RedisValue bool3 = true;
            CheckNotNull(bool3);

            CheckSame(a0, a0);
            CheckSame(a1, a1);
            CheckSame(a0, a1);

            CheckSame(b0, b0);
            CheckSame(b1, b1);
            CheckSame(b0, b1);

            CheckSame(i4, i4);
            CheckSame(i8, i8);
            CheckSame(i4, i8);

            CheckSame(bool1, bool3);
            CheckNotSame(bool1, bool2);
        }

        private void CheckSame(RedisValue x, RedisValue y)
        {
            ClassicAssert.IsTrue(Equals(x, y));
            ClassicAssert.IsTrue(x.Equals(y));
            ClassicAssert.IsTrue(y.Equals(x));
            ClassicAssert.IsTrue(x.GetHashCode() == y.GetHashCode());
        }
        private void CheckNotSame(RedisValue x, RedisValue y)
        {
            ClassicAssert.IsFalse(Equals(x, y));
            ClassicAssert.IsFalse(x.Equals(y));
            ClassicAssert.IsFalse(y.Equals(x));
            ClassicAssert.IsFalse(x.GetHashCode() == y.GetHashCode()); // well, very unlikely
        }

        private void CheckNotNull(RedisValue value)
        {
            ClassicAssert.IsFalse(value.IsNull);
            ClassicAssert.IsNotNull((byte[])value);
            ClassicAssert.IsNotNull((string)value);
            ClassicAssert.AreNotEqual(-1, value.GetHashCode());

            ClassicAssert.IsNotNull((string)value);
            ClassicAssert.IsNotNull((byte[])value);

            CheckSame(value, value);
            CheckNotSame(value, default(RedisValue));
            CheckNotSame(value, (string)null);
            CheckNotSame(value, (byte[])null);
        }
        private void CheckNull(RedisValue value)
        {
            ClassicAssert.IsTrue(value.IsNull);
            ClassicAssert.IsTrue(value.IsNullOrEmpty);
            ClassicAssert.IsFalse(value.IsInteger);
            ClassicAssert.AreEqual(-1, value.GetHashCode());

            ClassicAssert.IsNull((string)value);
            ClassicAssert.IsNull((byte[])value);

            ClassicAssert.AreEqual(0, (int)value);
            ClassicAssert.AreEqual(0L, (long)value);

            CheckSame(value, value);
            CheckSame(value, default(RedisValue));
            CheckSame(value, (string)null);
            CheckSame(value, (byte[])null);
        }


        [Test]
        public void ValuesAreConvertible()
        {
            RedisValue val = 123;
            object o = val;
            byte[] blob = (byte[])Convert.ChangeType(o, typeof(byte[]));

            ClassicAssert.AreEqual(3, blob.Length);
            ClassicAssert.AreEqual((byte)'1', blob[0]);
            ClassicAssert.AreEqual((byte)'2', blob[1]);
            ClassicAssert.AreEqual((byte)'3', blob[2]);

            ClassicAssert.AreEqual((double)123, Convert.ToDouble(o));

            IConvertible c = (IConvertible)o;
            ClassicAssert.AreEqual((short)123, c.ToInt16(CultureInfo.InvariantCulture));
            ClassicAssert.AreEqual((int)123, c.ToInt32(CultureInfo.InvariantCulture));
            ClassicAssert.AreEqual((long)123, c.ToInt64(CultureInfo.InvariantCulture));
            ClassicAssert.AreEqual((float)123, c.ToSingle(CultureInfo.InvariantCulture));
            ClassicAssert.AreEqual("123", c.ToString(CultureInfo.InvariantCulture));
            ClassicAssert.AreEqual((double)123, c.ToDouble(CultureInfo.InvariantCulture));
            ClassicAssert.AreEqual((decimal)123, c.ToDecimal(CultureInfo.InvariantCulture));
            ClassicAssert.AreEqual((ushort)123, c.ToUInt16(CultureInfo.InvariantCulture));
            ClassicAssert.AreEqual((uint)123, c.ToUInt32(CultureInfo.InvariantCulture));
            ClassicAssert.AreEqual((ulong)123, c.ToUInt64(CultureInfo.InvariantCulture));

            blob = (byte[])c.ToType(typeof(byte[]), CultureInfo.InvariantCulture);
            ClassicAssert.AreEqual(3, blob.Length);
            ClassicAssert.AreEqual((byte)'1', blob[0]);
            ClassicAssert.AreEqual((byte)'2', blob[1]);
            ClassicAssert.AreEqual((byte)'3', blob[2]);
        }

#if false
        [Test]
        public void CanBeDynamic()
        {
            RedisValue val = "abc";
            object o = val;
            dynamic d = o;
            byte[] blob = (byte[])d; // could be in a try/catch
            ClassicAssert.AreEqual(3, blob.Length);
            ClassicAssert.AreEqual((byte)'a', blob[0]);
            ClassicAssert.AreEqual((byte)'b', blob[1]);
            ClassicAssert.AreEqual((byte)'c', blob[2]);
        }
#endif

        [Test]
        public void TryParse()
        {
            {
                RedisValue val = "1";
                int i;
                ClassicAssert.IsTrue(val.TryParse(out i));
                ClassicAssert.AreEqual(1, i);
                long l;
                ClassicAssert.IsTrue(val.TryParse(out l));
                ClassicAssert.AreEqual(1L, l);
                double d;
                ClassicAssert.IsTrue(val.TryParse(out d));
                ClassicAssert.AreEqual(1.0, l);
            }

            {
                RedisValue val = "8675309";
                int i;
                ClassicAssert.IsTrue(val.TryParse(out i));
                ClassicAssert.AreEqual(8675309, i);
                long l;
                ClassicAssert.IsTrue(val.TryParse(out l));
                ClassicAssert.AreEqual(8675309L, l);
                double d;
                ClassicAssert.IsTrue(val.TryParse(out d));
                ClassicAssert.AreEqual(8675309.0, l);
            }

            {
                RedisValue val = "3.14159";
                double d;
                ClassicAssert.IsTrue(val.TryParse(out d));
                ClassicAssert.AreEqual(3.14159, d);
            }

            {
                RedisValue val = "not a real number";
                int i;
                ClassicAssert.IsFalse(val.TryParse(out i));
                long l;
                ClassicAssert.IsFalse(val.TryParse(out l));
                double d;
                ClassicAssert.IsFalse(val.TryParse(out d));
            }
        }
    }
}
