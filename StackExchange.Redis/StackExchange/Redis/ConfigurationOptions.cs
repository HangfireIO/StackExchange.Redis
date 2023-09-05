using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace StackExchange.Redis
{

    /// <summary>
    /// Specifies the proxy that is being used to communicate to redis
    /// </summary>
    public enum Proxy
    {
        /// <summary>
        /// Direct communication to the redis server(s)
        /// </summary>
        None,
        /// <summary>
        /// Communication via <a href="https://github.com/twitter/twemproxy">twemproxy</a>
        /// </summary>
        Twemproxy
    }

    /// <summary>
    /// The options relevant to a set of redis connections
    /// </summary>
    public sealed class ConfigurationOptions
#if !CORE_CLR
        : ICloneable
#endif
    {
        internal const string DefaultTieBreaker = "__Booksleeve_TieBreak", DefaultConfigurationChannel = "__Booksleeve_MasterChanged";

        private static class OptionKeys
        {
            public static int ParseInt32(string key, string value, int minValue = int.MinValue, int maxValue = int.MaxValue)
            {
                int tmp;
                if (!Format.TryParseInt32(value, out tmp)) throw new ArgumentOutOfRangeException("Keyword '" + key + "' requires an integer value");
                if (tmp < minValue) throw new ArgumentOutOfRangeException("Keyword '" + key + "' has a minimum value of " + minValue);
                if (tmp > maxValue) throw new ArgumentOutOfRangeException("Keyword '" + key + "' has a maximum value of " + maxValue);
                return tmp;
            }

            internal static bool ParseBoolean(string key, string value)
            {
                bool tmp;
                if (!Format.TryParseBoolean(value, out tmp)) throw new ArgumentOutOfRangeException("Keyword '" + key + "' requires a boolean value");
                return tmp;
            }
            internal static Version ParseVersion(string key, string value)
            {
                Version tmp;
                if (!System.Version.TryParse(value, out tmp)) throw new ArgumentOutOfRangeException("Keyword '" + key + "' requires a version value");
                return tmp;
            }
            internal static Proxy ParseProxy(string key, string value)
            {
                Proxy tmp;
                if (!Enum.TryParse(value, true, out tmp)) throw new ArgumentOutOfRangeException("Keyword '" + key + "' requires a proxy value");
                return tmp;
            }

            internal static SslProtocols ParseSslProtocols(string key, string value)
            {
                SslProtocols tmp;
                //Flags expect commas as separators, but we need to use '|' since commas are already used in the connection string to mean something else
                value = value?.Replace("|", ","); 

                if (!Enum.TryParse(value, true, out tmp)) throw new ArgumentOutOfRangeException("Keyword '" + key + "' requires an SslProtocol value (multiple values separated by '|').");

                return tmp;                
            }

            internal static void Unknown(string key)
            {
                throw new ArgumentException("Keyword '" + key + "' is not supported");
            }

            internal const string AllowAdmin = "allowAdmin", SyncTimeout = "syncTimeout",
                                ServiceName = "serviceName", ClientName = "name", KeepAlive = "keepAlive",
                        Version = "version", ConnectTimeout = "connectTimeout", User = "user", UserName = "username", Password = "password",
                        TieBreaker = "tiebreaker", WriteBuffer = "writeBuffer", Ssl = "ssl", SslHost = "sslHost", HighPrioritySocketThreads = "highPriorityThreads",
                        ConfigChannel = "configChannel", AbortOnConnectFail = "abortConnect", ResolveDns = "resolveDns",
                        ChannelPrefix = "channelPrefix", Proxy = "proxy", ConnectRetry = "connectRetry",
                        ConfigCheckSeconds = "configCheckSeconds", ResponseTimeout = "responseTimeout", DefaultDatabase = "defaultDatabase",
                        HeartbeatInterval = "heartbeatInterval", PreferIOCP = "preferIOCP";
            internal const string SslProtocols = "sslProtocols";

            private static readonly Dictionary<string, string> normalizedOptions = new[]
            {
                AllowAdmin, SyncTimeout,
                ServiceName, ClientName, KeepAlive,
                Version, ConnectTimeout, User, UserName, Password,
                TieBreaker, WriteBuffer, Ssl, SslHost, HighPrioritySocketThreads,
                ConfigChannel, AbortOnConnectFail, ResolveDns,
                ChannelPrefix, Proxy, ConnectRetry,
                ConfigCheckSeconds, DefaultDatabase,
                SslProtocols, HeartbeatInterval, PreferIOCP
            }.ToDictionary(x => x, StringComparer.OrdinalIgnoreCase);

            public static string TryNormalize(string value)
            {
                string tmp;
                if(value != null && normalizedOptions.TryGetValue(value, out tmp))
                {
                    return tmp ?? "";
                }
                return value ?? "";
            }
        }


        private readonly EndPointCollection endpoints = new EndPointCollection();

        private bool? allowAdmin, abortOnConnectFail, highPrioritySocketThreads, resolveDns, ssl, preferIOCP, checkCertificateRevocation;

        private string clientName, serviceName, password, tieBreaker, sslHost, configChannel;

        private CommandMap commandMap;

        private Version defaultVersion;

        private int? keepAlive, syncTimeout, connectTimeout, responseTimeout, writeBuffer, connectRetry, configCheckSeconds, defaultDatabase, heartbeatInterval;

        private Proxy? proxy;

        private IReconnectRetryPolicy reconnectRetryPolicy; 

        /// <summary>
        /// A LocalCertificateSelectionCallback delegate responsible for selecting the certificate used for authentication; note
        /// that this cannot be specified in the configuration-string.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1009:DeclareEventHandlersCorrectly")]
        public event LocalCertificateSelectionCallback CertificateSelection;

        /// <summary>
        /// A RemoteCertificateValidationCallback delegate responsible for validating the certificate supplied by the remote party; note
        /// that this cannot be specified in the configuration-string.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1009:DeclareEventHandlersCorrectly")]
        public event RemoteCertificateValidationCallback CertificateValidation;

        /// <summary>
        /// Gets or sets whether certificate revocation list is checked upon an authentication process, requires <see cref="SslProtocols"/>
        /// property to be explicitly set to make this option work.
        /// </summary>
        public bool CheckCertificateRevocation { get { return checkCertificateRevocation ?? true; } set { checkCertificateRevocation = value; } }

        /// <summary>
        /// Specifies the interval between heartbeat runs in milliseconds. It's expected to have multiple
        /// heartbeat invocations per the configured keep-alive value. By default interval is 1 second.
        /// </summary>
        public int HeartbeatInterval
        {
            get { return heartbeatInterval.GetValueOrDefault(ConnectionMultiplexer.MillisecondsPerHeartbeat); }
            set { heartbeatInterval = value; }
        }

        /// <summary>
        /// Gets or sets whether IOCP or dedicated threads will be used to read responses from Redis. IOCP works only
        /// on Windows, but lets to avoid having a lot of threads just for reading responses, but may work worse when
        /// CLR Thread Pool's IOCP threads starved with other work.
        /// </summary>
        public bool PreferIOCP
        {
            get { return preferIOCP.GetValueOrDefault(true); }
            set { preferIOCP = value; }
        }

        /// <summary>
        /// Gets or sets whether connect/configuration timeouts should be explicitly notified via a TimeoutException
        /// </summary>
        public bool AbortOnConnectFail { get { return abortOnConnectFail ?? GetDefaultAbortOnConnectFailSetting(); } set { abortOnConnectFail = value; } }

        /// <summary>
        /// Indicates whether admin operations should be allowed
        /// </summary>
        public bool AllowAdmin { get { return allowAdmin.GetValueOrDefault(); } set { allowAdmin = value; } }
                
        /// <summary>
        /// Indicates whether the connection should be encrypted
        /// </summary>
        [Obsolete("Please use .Ssl instead of .UseSsl"),
#if !CORE_CLR
            Browsable(false),
#endif
            EditorBrowsable(EditorBrowsableState.Never)]
        public bool UseSsl { get { return Ssl; } set { Ssl = value; } }

        /// <summary>
        /// Indicates whether the connection should be encrypted
        /// </summary>
        public bool Ssl { get { return ssl.GetValueOrDefault(); } set { ssl = value; } }

        /// <summary>
        /// Configures which Ssl/TLS protocols should be allowed.  If not set, defaults are chosen by the .NET framework.
        /// </summary>
        public SslProtocols? SslProtocols { get; set; }

        /// <summary>
        /// Automatically encodes and decodes channels
        /// </summary>
        public RedisChannel ChannelPrefix { get;set; }

        /// <summary>
        /// Create a certificate validation check that checks against the supplied issuer even if not known by the machine.
        /// </summary>
        /// <param name="issuerCertificatePath">The file system path to find the certificate at.</param>
        public void TrustIssuer(string issuerCertificatePath) => CertificateValidationCallback = TrustIssuerCallback(issuerCertificatePath);

        /// <summary>
        /// Create a certificate validation check that checks against the supplied issuer even if not known by the machine.
        /// </summary>
        /// <param name="issuer">The issuer to trust.</param>
        public void TrustIssuer(X509Certificate2 issuer) => CertificateValidationCallback = TrustIssuerCallback(issuer);

        internal static RemoteCertificateValidationCallback TrustIssuerCallback(string issuerCertificatePath)
            => TrustIssuerCallback(new X509Certificate2(issuerCertificatePath));
        private static RemoteCertificateValidationCallback TrustIssuerCallback(X509Certificate2 issuer)
        {
            if (issuer == null) throw new ArgumentNullException(nameof(issuer));

            return (object _, X509Certificate certificate, X509Chain __, SslPolicyErrors sslPolicyError)
                => sslPolicyError == SslPolicyErrors.RemoteCertificateChainErrors
                    && certificate is X509Certificate2 v2
                    && CheckTrustedIssuer(v2, issuer);
        }

        private static bool CheckTrustedIssuer(X509Certificate2 certificateToValidate, X509Certificate2 authority)
        {
            // reference: https://stackoverflow.com/questions/6497040/how-do-i-validate-that-a-certificate-was-created-by-a-particular-certification-a
            X509Chain chain = new X509Chain();
            chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            chain.ChainPolicy.RevocationFlag = X509RevocationFlag.ExcludeRoot;
            chain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;
            chain.ChainPolicy.VerificationTime = DateTime.Now;
            chain.ChainPolicy.UrlRetrievalTimeout = new TimeSpan(0, 0, 0);

            chain.ChainPolicy.ExtraStore.Add(authority);
            return chain.Build(certificateToValidate);
        }

        /// <summary>
        /// The client name to use for all connections
        /// </summary>
        public string ClientName { get { return clientName; } set { clientName = value; } }

        /// <summary>
        /// The number of times to repeat the initial connect cycle if no servers respond promptly
        /// </summary>
        public int ConnectRetry { get { return connectRetry ?? 3; } set { connectRetry = value; } }

        /// <summary>
        /// The command-map associated with this configuration
        /// </summary>
        public CommandMap CommandMap
        {
            get
            {
                if (commandMap != null) return commandMap;
                switch (Proxy)
                {
                    case Proxy.Twemproxy:
                        return CommandMap.Twemproxy;
                    default:
                        return CommandMap.Default;
                }
            }
            set
            {
                if (value == null) throw new ArgumentNullException(nameof(value));
                commandMap = value;
            }
        }

        /// <summary>
        /// Channel to use for broadcasting and listening for configuration change notification
        /// </summary>
        public string ConfigurationChannel { get { return configChannel ?? DefaultConfigurationChannel; } set { configChannel = value; } }

        /// <summary>
        /// Specifies the time in milliseconds that should be allowed for connection (defaults to 5 seconds unless SyncTimeout is higher)
        /// </summary>
        public int ConnectTimeout {
            get {
                if (connectTimeout.HasValue) return connectTimeout.GetValueOrDefault();
                return Math.Max(5000, SyncTimeout);
            }
            set { connectTimeout = value; }
        }

        /// <summary>
        /// The retry policy to be used for connection reconnects
        /// </summary>
        public IReconnectRetryPolicy ReconnectRetryPolicy { get { return reconnectRetryPolicy ?? (reconnectRetryPolicy = new LinearRetry(ConnectTimeout)); } set { reconnectRetryPolicy = value; } }


        /// <summary>
        /// The server version to assume
        /// </summary>
        public Version DefaultVersion { get { return defaultVersion ?? (IsAzureEndpoint() ? RedisFeatures.v3_0_0 : RedisFeatures.v2_0_0); } set { defaultVersion = value; } }

        /// <summary>
        /// The endpoints defined for this configuration
        /// </summary>
        public EndPointCollection EndPoints => endpoints;

        /// <summary>
        /// Use ThreadPriority.AboveNormal for SocketManager reader and writer threads (true by default). If false, ThreadPriority.Normal will be used.
        /// </summary>
        public bool HighPrioritySocketThreads { get { return highPrioritySocketThreads ?? true; } set { highPrioritySocketThreads = value; } }

        /// <summary>
        /// Specifies the time in seconds at which connections should be pinged to ensure validity
        /// </summary>
        public int KeepAlive { get { return keepAlive.GetValueOrDefault(-1); } set { keepAlive = value; } }

        /// <summary>
        /// The user to use to authenticate with the server.
        /// </summary>
        public string UserName { get; set; }

        /// <summary>
        /// The password to use to authenticate with the server
        /// </summary>
        public string Password { get { return password; } set { password = value; } }

        /// <summary>
        /// Type of proxy to use (if any); for example Proxy.Twemproxy
        /// </summary>
        public Proxy Proxy { get { return proxy.GetValueOrDefault(); } set { proxy = value; } }

        /// <summary>
        /// Indicates whether endpoints should be resolved via DNS before connecting.
        /// If enabled the ConnectionMultiplexer will not re-resolve DNS
        /// when attempting to re-connect after a connection failure.
        /// </summary>
        public bool ResolveDns { get { return resolveDns.GetValueOrDefault(); } set { resolveDns = value; } }

        /// <summary>
        /// The service name used to resolve a service via sentinel
        /// </summary>
        public string ServiceName { get { return serviceName; } set { serviceName = value; } }

        /// <summary>
        /// Gets or sets the SocketManager instance to be used with these options; if this is null a per-multiplexer
        /// SocketManager is created automatically.
        /// </summary>
        public SocketManager SocketManager {  get;set; }
        /// <summary>
        /// The target-host to use when validating SSL certificate; setting a value here enables SSL mode
        /// </summary>
        public string SslHost { get { return sslHost ?? InferSslHostFromEndpoints(); } set { sslHost = value; } }

        /// <summary>
        /// Specifies the time in milliseconds that the system should allow for synchronous operations (defaults to 1 second)
        /// </summary>
        public int SyncTimeout { get { return syncTimeout.GetValueOrDefault(1000); } set { syncTimeout = value; } }

        /// <summary>
        /// Specifies the time in milliseconds that the system should allow for responses before concluding that the socket is unhealthy
        /// (defaults to SyncTimeout)
        /// </summary>
        public int ResponseTimeout { get { return responseTimeout ?? SyncTimeout; } set { responseTimeout = value; } }

        /// <summary>
        /// Tie-breaker used to choose between masters (must match the endpoint exactly)
        /// </summary>
        public string TieBreaker { get { return tieBreaker ?? DefaultTieBreaker; } set { tieBreaker = value; } }
        /// <summary>
        /// The size of the output buffer to use
        /// </summary>
        public int WriteBuffer { get { return writeBuffer.GetValueOrDefault(4096); } set { writeBuffer = value; } }

        /// <summary>
        /// Specifies the default database to be used when calling ConnectionMultiplexer.GetDatabase() without any parameters
        /// </summary>
        public int? DefaultDatabase { get { return defaultDatabase; } set { defaultDatabase = value; } }

        internal LocalCertificateSelectionCallback CertificateSelectionCallback { get { return CertificateSelection; } private set { CertificateSelection = value; } }

        // these just rip out the underlying handlers, bypassing the event accessors - needed when creating the SSL stream
        internal RemoteCertificateValidationCallback CertificateValidationCallback { get { return CertificateValidation; } private set { CertificateValidation = value; } }

        /// <summary>
        /// Check configuration every n seconds (every minute by default)
        /// </summary>
        public int ConfigCheckSeconds { get { return configCheckSeconds.GetValueOrDefault(60); } set { configCheckSeconds = value; } }

        /// <summary>
        /// Parse the configuration from a comma-delimited configuration string
        /// </summary>
        /// <exception cref="ArgumentNullException"><paramref name="configuration"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException"><paramref name="configuration"/> is empty.</exception>
        public static ConfigurationOptions Parse(string configuration)
        {    
            var options = new ConfigurationOptions();
            options.DoParse(configuration, false);
            return options;
        }
        /// <summary>
        /// Parse the configuration from a comma-delimited configuration string
        /// </summary>
        /// <exception cref="ArgumentNullException"><paramref name="configuration"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException"><paramref name="configuration"/> is empty.</exception>
        public static ConfigurationOptions Parse(string configuration, bool ignoreUnknown)
        {
            var options = new ConfigurationOptions();
            options.DoParse(configuration, ignoreUnknown);
            return options;
        }

        /// <summary>
        /// Create a copy of the configuration
        /// </summary>
        public ConfigurationOptions Clone()
        {
            var options = new ConfigurationOptions
            {
                clientName = clientName,
                serviceName = serviceName,
                keepAlive = keepAlive,
                syncTimeout = syncTimeout,
                allowAdmin = allowAdmin,
                defaultVersion = defaultVersion,
                connectTimeout = connectTimeout,
                password = password,
                tieBreaker = tieBreaker,
                writeBuffer = writeBuffer,
                ssl = ssl,
                sslHost = sslHost,
                highPrioritySocketThreads = highPrioritySocketThreads,
                configChannel = configChannel,
                abortOnConnectFail = abortOnConnectFail,
                resolveDns = resolveDns,
                proxy = proxy,
                commandMap = commandMap,
                CertificateValidationCallback = CertificateValidationCallback,
                CertificateSelectionCallback = CertificateSelectionCallback,
                ChannelPrefix = ChannelPrefix.Clone(),
                SocketManager = SocketManager,
                connectRetry = connectRetry,
                configCheckSeconds = configCheckSeconds,
                responseTimeout = responseTimeout,
                defaultDatabase = defaultDatabase,
                ReconnectRetryPolicy = reconnectRetryPolicy,
                heartbeatInterval = heartbeatInterval,
                preferIOCP = preferIOCP,
                SslProtocols = SslProtocols,
                UserName = UserName,
                checkCertificateRevocation = checkCertificateRevocation,
            };
            foreach (var item in endpoints)
                options.endpoints.Add(item);
            return options;

        }
        
        internal bool IsSentinel => !string.IsNullOrEmpty(ServiceName);
        
        internal bool TryGetTieBreaker(out RedisKey tieBreaker)
        {
            var key = TieBreaker;
            if (!IsSentinel && !string.IsNullOrWhiteSpace(key))
            {
                tieBreaker = key;
                return true;
            }
            tieBreaker = default;
            return false;
        }

        /// <summary>
        /// Resolve the default port for any endpoints that did not have a port explicitly specified
        /// </summary>
        public void SetDefaultPorts()
        {
            EndPoints.SetDefaultPorts(ServerType.Standalone, ssl: Ssl);
        }

        /// <summary>
        /// Returns the effective configuration string for this configuration, including Redis credentials.
        /// </summary>
        public override string ToString()
        {
            // include password to allow generation of configuration strings 
            // used for connecting multiplexer
            return ToString(includePassword: true);
        }

        /// <summary>
        /// Returns the effective configuration string for this configuration
        /// with the option to include or exclude the password from the string.
        /// </summary>
        public string ToString(bool includePassword)
        {
            var sb = new StringBuilder();
            foreach (var endpoint in endpoints)
            {
                Append(sb, Format.ToString(endpoint));
            }
            Append(sb, OptionKeys.ClientName, clientName);
            Append(sb, OptionKeys.ServiceName, serviceName);
            Append(sb, OptionKeys.KeepAlive, keepAlive);
            Append(sb, OptionKeys.SyncTimeout, syncTimeout);
            Append(sb, OptionKeys.AllowAdmin, allowAdmin);
            Append(sb, OptionKeys.Version, defaultVersion);
            Append(sb, OptionKeys.ConnectTimeout, connectTimeout);
            Append(sb, OptionKeys.UserName, UserName);
            Append(sb, OptionKeys.Password, includePassword ? password : "*****");
            Append(sb, OptionKeys.TieBreaker, tieBreaker);
            Append(sb, OptionKeys.WriteBuffer, writeBuffer);
            Append(sb, OptionKeys.Ssl, ssl);
            Append(sb, OptionKeys.SslHost, sslHost); 
            Append(sb, OptionKeys.HighPrioritySocketThreads, highPrioritySocketThreads);
            Append(sb, OptionKeys.ConfigChannel, configChannel);
            Append(sb, OptionKeys.AbortOnConnectFail, abortOnConnectFail);
            Append(sb, OptionKeys.ResolveDns, resolveDns);
            Append(sb, OptionKeys.ChannelPrefix, (string)ChannelPrefix);
            Append(sb, OptionKeys.ConnectRetry, connectRetry);
            Append(sb, OptionKeys.Proxy, proxy);
            Append(sb, OptionKeys.ConfigCheckSeconds, configCheckSeconds);
            Append(sb, OptionKeys.ResponseTimeout, responseTimeout);
            Append(sb, OptionKeys.DefaultDatabase, defaultDatabase);
            Append(sb, OptionKeys.HeartbeatInterval, heartbeatInterval);
            Append(sb, OptionKeys.PreferIOCP, preferIOCP);
            commandMap?.AppendDeltas(sb);
            return sb.ToString();
        }
        
        /// <summary>
        /// Gets the command map for a given server type, since some supersede settings when connecting.
        /// </summary>
        internal CommandMap GetCommandMap(ServerType? serverType)
        {
            if (serverType == ServerType.Sentinel) return CommandMap.Sentinel;
            return CommandMap;
        }


        static void Append(StringBuilder sb, object value)
        {
            if (value == null) return;
            string s = Format.ToString(value);
            if (!string.IsNullOrWhiteSpace(s))
            {
                if (sb.Length != 0) sb.Append(',');
                sb.Append(s);
            }
        }

        static void Append(StringBuilder sb, string prefix, object value)
        {
            string s = value?.ToString();
            if (!string.IsNullOrWhiteSpace(s))
            {
                if (sb.Length != 0) sb.Append(',');
                if(!string.IsNullOrEmpty(prefix))
                {
                    sb.Append(prefix).Append('=');
                }
                sb.Append(s);
            }
        }

#if !CORE_CLR
        static bool IsOption(string option, string prefix)
        {
            return option.StartsWith(prefix, StringComparison.InvariantCultureIgnoreCase);
        }
#endif

        void Clear()
        {
            clientName = serviceName = UserName = password = tieBreaker = sslHost = configChannel = null;
            keepAlive = syncTimeout = connectTimeout = writeBuffer = connectRetry = configCheckSeconds = defaultDatabase = null;
            allowAdmin = abortOnConnectFail = highPrioritySocketThreads = resolveDns = ssl = null;
            defaultVersion = null;
            endpoints.Clear();
            commandMap = null;

            CertificateSelection = null;
            CertificateValidation = null;            
            ChannelPrefix = default(RedisChannel);
            SocketManager = null;
            heartbeatInterval = null;
            preferIOCP = null;
            checkCertificateRevocation = null;
        }

#if !CORE_CLR
        object ICloneable.Clone() { return Clone(); }
#endif

        private void DoParse(string configuration, bool ignoreUnknown)
        {
            if (configuration == null)
            {
                throw new ArgumentNullException(nameof(configuration));
            }

            if (string.IsNullOrWhiteSpace(configuration))
            {
                throw new ArgumentException("is empty", configuration);
            }

            Clear();

            // break it down by commas
            var arr = configuration.Split(StringSplits.Comma);
            Dictionary<string, string> map = null;
            foreach (var paddedOption in arr)
            {
                var option = paddedOption.Trim();

                if (string.IsNullOrWhiteSpace(option)) continue;

                // check for special tokens
                int idx = option.IndexOf('=');
                if (idx > 0)
                {
                    var key = option.Substring(0, idx).Trim();                        
                    var value = option.Substring(idx + 1).Trim();

                    switch (OptionKeys.TryNormalize(key))
                    {
                        case OptionKeys.HeartbeatInterval:
                            HeartbeatInterval = OptionKeys.ParseInt32(key, value, minValue: 0);
                            break;
                        case OptionKeys.SyncTimeout:
                            SyncTimeout = OptionKeys.ParseInt32(key, value, minValue: 1);
                            break;
                        case OptionKeys.AllowAdmin:
                            AllowAdmin = OptionKeys.ParseBoolean(key, value);
                            break;
                        case OptionKeys.AbortOnConnectFail:
                            AbortOnConnectFail = OptionKeys.ParseBoolean(key, value);
                            break;
                        case OptionKeys.ResolveDns:
                            ResolveDns = OptionKeys.ParseBoolean(key, value);
                            break;
                        case OptionKeys.ServiceName:
                            ServiceName = value;
                            break;
                        case OptionKeys.ClientName:
                            ClientName = value;
                            break;
                        case OptionKeys.ChannelPrefix:
                            ChannelPrefix = value;
                            break;
                        case OptionKeys.ConfigChannel:
                            ConfigurationChannel = value;
                            break;
                        case OptionKeys.KeepAlive:
                            KeepAlive = OptionKeys.ParseInt32(key, value);
                            break;
                        case OptionKeys.ConnectTimeout:
                            ConnectTimeout = OptionKeys.ParseInt32(key, value);
                            break;
                        case OptionKeys.ConnectRetry:
                            ConnectRetry = OptionKeys.ParseInt32(key, value);
                            break;
                        case OptionKeys.ConfigCheckSeconds:
                            ConfigCheckSeconds = OptionKeys.ParseInt32(key, value);
                            break;
                        case OptionKeys.Version:
                            DefaultVersion = OptionKeys.ParseVersion(key, value);
                            break;
                        case OptionKeys.User:
                        case OptionKeys.UserName:
                            UserName = value;
                            break;
                        case OptionKeys.Password:
                            Password = value;
                            break;
                        case OptionKeys.TieBreaker:
                            TieBreaker = value;
                            break;
                        case OptionKeys.Ssl:
                            Ssl = OptionKeys.ParseBoolean(key, value);
                            break;
                        case OptionKeys.SslHost:
                            SslHost = value;
                            break;
                        case OptionKeys.HighPrioritySocketThreads:
                            HighPrioritySocketThreads = OptionKeys.ParseBoolean(key, value);
                            break;
                        case OptionKeys.WriteBuffer:
                            WriteBuffer = OptionKeys.ParseInt32(key, value);
                            break;
                        case OptionKeys.Proxy:
                            Proxy = OptionKeys.ParseProxy(key, value);
                            break;
                        case OptionKeys.ResponseTimeout:
                            ResponseTimeout = OptionKeys.ParseInt32(key, value, minValue: 1);
                            break;
                        case OptionKeys.DefaultDatabase:
                            defaultDatabase = OptionKeys.ParseInt32(key, value);
                            break;
#if !CORE_CLR
                        case OptionKeys.SslProtocols:
                            SslProtocols = OptionKeys.ParseSslProtocols(key, value);
                            break;
#endif
                        case OptionKeys.PreferIOCP:
                            PreferIOCP = OptionKeys.ParseBoolean(key, value);
                            break;
                        default:
                            if (!string.IsNullOrEmpty(key) && key[0] == '$')
                            {
                                RedisCommand cmd;
                                var cmdName = option.Substring(1, idx - 1);
                                if (Enum.TryParse(cmdName, true, out cmd))
                                {
                                    if (map == null) map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                                    map[cmdName] = value;
                                }
                            }
                            else
                            {
                                if(!ignoreUnknown) OptionKeys.Unknown(key);
                            }
                            break;
                    }
                }
                else
                {
                    var ep = Format.TryParseEndPoint(option);
                    if (ep != null && !endpoints.Contains(ep)) endpoints.Add(ep);
                }
            }
            if (map != null && map.Count != 0)
            {
                CommandMap = CommandMap.Create(map);
            }
        }

        private bool GetDefaultAbortOnConnectFailSetting()
        {
            // Microsoft Azure team wants abortConnect=false by default
            if (IsAzureEndpoint())
                return false;

            return true;
        }
        
        private bool IsAzureEndpoint()
        {
            var result = false; 
            var dnsEndpoints = endpoints.Select(endpoint => endpoint as DnsEndPoint).Where(ep => ep != null);
            foreach(var ep in dnsEndpoints)
            {
                int firstDot = ep.Host.IndexOf('.');
                if (firstDot >= 0)
                {
                    var domain = ep.Host.Substring(firstDot).ToLowerInvariant();
                    switch(domain)
                    {
                        case ".redis.cache.windows.net":
                        case ".redis.cache.chinacloudapi.cn":
                        case ".redis.cache.usgovcloudapi.net":
                        case ".redis.cache.cloudapi.de":
                            return true;
                    }
                }
            }

            return result; 
        }

        private string InferSslHostFromEndpoints() {
            var dnsEndpoints = endpoints.Select(endpoint => endpoint as DnsEndPoint);
            string dnsHost = dnsEndpoints.FirstOrDefault()?.Host;
            if (dnsEndpoints.All(dnsEndpoint => (dnsEndpoint != null && dnsEndpoint.Host == dnsHost))) {
                return dnsHost;
            }

            return null;
        }
    }
}
