using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Net;
using System.Net.Sockets;

namespace StackExchange.Redis
{
    /// <summary>
    /// A list of endpoints
    /// </summary>
    public sealed class EndPointCollection : Collection<EndPoint>, IEnumerable, IEnumerable<EndPoint>
    {
        private static class DefaultPorts
        {
            public static int Standard => 6379;
            public static int Ssl => 6380;
            public static int Sentinel => 26379;
        }
        
        /// <summary>
        /// Create a new EndPointCollection
        /// </summary>
        public EndPointCollection() : base()
        {
        }

        /// <summary>
        /// Create a new EndPointCollection
        /// </summary>
        public EndPointCollection(IList<EndPoint> endpoints) : base(endpoints)
        {
        }

        /// <summary>
        /// Format an endpoint
        /// </summary>
        public static string ToString(EndPoint endpoint)
        {
            return Format.ToString(endpoint);
        }

        /// <summary>
        /// Attempt to parse a string into an EndPoint
        /// </summary>
        public static EndPoint TryParse(string endpoint)
        {
            return Format.TryParseEndPoint(endpoint);
        }
        /// <summary>
        /// Adds a new endpoint to the list
        /// </summary>
        public void Add(string hostAndPort)
        {
            var endpoint = Format.TryParseEndPoint(hostAndPort);
            if (endpoint == null) throw new ArgumentException();
            Add(endpoint);
        }

        /// <summary>
        /// Adds a new endpoint to the list
        /// </summary>
        public void Add(string host, int port)
        {
            Add(Format.ParseEndPoint(host, port));
        }

        /// <summary>
        /// Adds a new endpoint to the list
        /// </summary>
        public void Add(IPAddress host, int port)
        {
            Add(new IPEndPoint(host, port));
        }

        /// <summary>
        /// Try adding a new endpoint to the list.
        /// </summary>
        /// <param name="endpoint">The endpoint to add.</param>
        /// <returns><see langword="true"/> if the endpoint was added, <see langword="false"/> if not.</returns>
        public bool TryAdd(EndPoint endpoint)
        {
            if (endpoint == null)
            {
                throw new ArgumentNullException(nameof(endpoint));
            }

            if (!Contains(endpoint))
            {
                base.InsertItem(Count, endpoint);
                return true;
            }
            else
            {
                return false;
            }
        }
        
        /// <summary>
        /// See Collection&lt;T&gt;.InsertItem()
        /// </summary>
        protected override void InsertItem(int index, EndPoint item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            if (Contains(item)) throw new ArgumentException("EndPoints must be unique", nameof(item));
            base.InsertItem(index, item);
        }
        /// <summary>
        /// See Collection&lt;T&gt;.SetItem()
        /// </summary>
        protected override void SetItem(int index, EndPoint item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            int existingIndex;
            try
            {
                existingIndex = IndexOf(item);
            } catch(NullReferenceException)
            {
                // mono has a nasty bug in DnsEndPoint.Equals; if they do bad things here: sorry, I can't help
                existingIndex = -1;
            }
            if (existingIndex >= 0 && existingIndex != index) throw new ArgumentException("EndPoints must be unique", nameof(item));
            base.SetItem(index, item);
        }

        internal void SetDefaultPorts(ServerType? serverType, bool ssl = false)
        {
            int defaultPort = serverType == ServerType.Sentinel
                ? DefaultPorts.Sentinel
                : ssl ? DefaultPorts.Ssl : DefaultPorts.Standard;

            for (int i = 0; i < Count; i++)
            {
                switch (this[i])
                {
                    case DnsEndPoint dns when dns.Port == 0:
                        this[i] = new DnsEndPoint(dns.Host, defaultPort, dns.AddressFamily);
                        break;
                    case IPEndPoint ip when ip.Port == 0:
                        this[i] = new IPEndPoint(ip.Address, defaultPort);
                        break;
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        IEnumerator<EndPoint> IEnumerable<EndPoint>.GetEnumerator() => GetEnumerator();

        /// <inheritdoc/>
        public new IEnumerator<EndPoint> GetEnumerator()
        {
            // this does *not* need to handle all threading scenarios; but we do
            // want it to at least allow overwrites of existing endpoints without
            // breaking the enumerator; in particular, this avoids a problem where
            // ResolveEndPointsAsync swaps the addresses on us
            for (int i = 0; i < Count; i++)
            {
                yield return this[i];
            }
        }
        
        internal bool HasDnsEndPoints()
        {
            foreach (var endpoint in this)
            {
                if (endpoint is DnsEndPoint)
                {
                    return true;
                }
            }
            return false;
        }
        
        #pragma warning disable 1998 // NET40 is sync, not async, currently
        internal void ResolveEndPoints(ConnectionMultiplexer multiplexer, Action<string> log)
        {
            Dictionary<string, IPAddress> cache = new Dictionary<string, IPAddress>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < Count; i++)
            {
                var dns = this[i] as DnsEndPoint;
                if (dns != null)
                {
                    try
                    {
                        IPAddress ip;
                        if (dns.Host == ".")
                        {
                            this[i] = new IPEndPoint(IPAddress.Loopback, dns.Port);
                        }
                        else if (cache.TryGetValue(dns.Host, out ip))
                        { // use cache
                            this[i] = new IPEndPoint(ip, dns.Port);
                        }
                        else
                        {
                            multiplexer.LogLocked(log, "Using DNS to resolve '{0}'...", dns.Host);
#if !CORE_CLR
                            var ips = Dns.GetHostAddresses(dns.Host);
#else
                            var ips = Dns.GetHostAddressesAsync(dns.Host).ObserveErrors().ForAwait().GetAwaiter().GetResult();
#endif
                            if (ips.Length > 0)
                            {
                                ip = ips.FirstOrDefault(x => x.AddressFamily == AddressFamily.InterNetwork || x.AddressFamily == AddressFamily.InterNetworkV6);
                                if (ip != null)
                                {
                                    multiplexer.LogLocked(log, "'{0}' => {1}", dns.Host, ip);
                                    cache[dns.Host] = ip;
                                    this[i] = new IPEndPoint(ip, dns.Port);
                                }
                            }
                        }
                    }
                    catch (Exception ex) when (!(ex is OutOfMemoryException))
                    {
                        multiplexer.OnInternalError(ex);
                        multiplexer.LogLocked(log, ex.Message);
                    }
                }
            }
        }
#pragma warning restore 1998
        
        internal EndPointCollection Clone() => new EndPointCollection(new List<EndPoint>(Items));
    }
}
