﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace StackExchange.Redis
{
    [Flags]
    internal enum UnselectableFlags
    {
        None = 0,
        RedundantMaster = 1,
        DidNotRespond = 2,
        ServerType = 4

    }

    internal sealed partial class ServerEndPoint : IDisposable
    {
        internal volatile ServerEndPoint Master;
        internal volatile ServerEndPoint[] Slaves = NoSlaves;
        private static readonly Regex nameSanitizer = new Regex("[^!-~]", RegexOptions.Compiled);
        private static readonly ServerEndPoint[] NoSlaves = new ServerEndPoint[0];
        private readonly EndPoint endpoint;


        private readonly Hashtable knownScripts = new Hashtable(StringComparer.Ordinal);
        private readonly ConnectionMultiplexer multiplexer;

        private int databases, writeEverySeconds;

        private volatile PhysicalBridge interactive, subscription;

        bool isDisposed;

        int serverType;

        private bool slaveReadOnly, isSlave;

        private volatile UnselectableFlags unselectableReasons;

        private Version version;


        internal void ResetNonConnected(Action<string> log)
        {
            interactive?.ResetNonConnected(log);
            subscription?.ResetNonConnected(null);
        }
        public ServerEndPoint(ConnectionMultiplexer multiplexer, EndPoint endpoint, Action<string> log)
        {
            this.multiplexer = multiplexer;
            this.endpoint = endpoint;
            var config = multiplexer.RawConfig;
            version = config.DefaultVersion;
            slaveReadOnly = true;
            isSlave = false;
            databases = 0;
            writeEverySeconds = config.KeepAlive > 0 ? config.KeepAlive : 60;
            serverType = (int)ServerType.Standalone;

            // overrides for twemproxy
            if (multiplexer.RawConfig.Proxy == Proxy.Twemproxy)
            {
                databases = 1;
                serverType = (int)ServerType.Twemproxy;
            }
        }

        public ClusterConfiguration ClusterConfiguration { get; private set; }

        public int Databases { get { return Volatile.Read(ref databases); } set { Volatile.Write(ref databases, value); } }

        public EndPoint EndPoint => endpoint;

        public bool HasDatabases => ServerType == ServerType.Standalone;

        public bool IsConnected
        {
            get
            {
                var tmp = interactive;
                return tmp != null && tmp.IsConnected;
            }
        }

        internal Exception LastException
        {
            get
            {
                var tmp1 = interactive;
                var tmp2 = subscription;

                //check if subscription endpoint has a better lastexception
                if (tmp2 != null && tmp2.LastException != null)
                {
                    if (tmp2.LastException.Data.Contains("Redis-FailureType") && !tmp2.LastException.Data["Redis-FailureType"].ToString().Equals(ConnectionFailureType.UnableToConnect.ToString()))
                    {
                        return tmp2.LastException;
                    }
                }
                return tmp1?.LastException;
            }
        }

        internal PhysicalBridge.State ConnectionState
        {
            get
            {
                var tmp = interactive;
                return tmp?.ConnectionState ?? PhysicalBridge.State.Disconnected;
            }
        }

        public bool IsSlave { get { return Volatile.Read(ref isSlave); } set { Volatile.Write(ref isSlave, value); } }

        public long OperationCount
        {
            get
            {
                long total = 0;
                var tmp = interactive;
                if (tmp != null) total += tmp.OperationCount;
                tmp = subscription;
                if (tmp != null) total += tmp.OperationCount;
                return total;
            }
        }

        public bool RequiresReadMode => ServerType == ServerType.Cluster && IsSlave;

        public ServerType ServerType { get { return (ServerType)Volatile.Read(ref serverType); } set { Volatile.Write(ref serverType, (int)value); } }

        public bool SlaveReadOnly { get { return Volatile.Read(ref slaveReadOnly); } set { Volatile.Write(ref slaveReadOnly, value); } }

        public bool AllowSlaveWrites { get; set; }

        public Version Version { get { return Volatile.Read(ref version); } set { Volatile.Write(ref version, value); } }

        public int WriteEverySeconds { get { return Volatile.Read(ref writeEverySeconds); } set { Volatile.Write(ref writeEverySeconds, value); } }
        internal ConnectionMultiplexer Multiplexer => multiplexer;

        public void ClearUnselectable(UnselectableFlags flags)
        {
            var oldFlags = unselectableReasons;
            if (oldFlags != 0)
            {
                unselectableReasons &= ~flags;
                if (unselectableReasons != oldFlags)
                {
                    multiplexer.Trace(unselectableReasons == 0 ? "Now usable" : ("Now unusable: " + flags), ToString());
                }
            }
        }

        public void Dispose()
        {
            isDisposed = true;
            var tmp = interactive;
            interactive = null;
            tmp?.Dispose();

            tmp = subscription;
            subscription = null;
            tmp?.Dispose();
        }

        public PhysicalBridge GetBridge(ConnectionType type, bool create = true, Action<string> log = null)
        {
            if (isDisposed) return null;
            switch (type)
            {
                case ConnectionType.Interactive:
                    return interactive ?? (create ? interactive = CreateBridge(ConnectionType.Interactive, log) : null);
                case ConnectionType.Subscription:
                    return subscription ?? (create ? subscription = CreateBridge(ConnectionType.Subscription, log) : null);
            }
            return null;
        }
        public PhysicalBridge GetBridge(RedisCommand command, bool create = true)
        {
            if (isDisposed) return null;
            switch (command)
            {
                case RedisCommand.SUBSCRIBE:
                case RedisCommand.UNSUBSCRIBE:
                case RedisCommand.PSUBSCRIBE:
                case RedisCommand.PUNSUBSCRIBE:
                    return subscription ?? (create ? subscription = CreateBridge(ConnectionType.Subscription, null) : null);
                default:
                    return interactive ?? (create ? interactive = CreateBridge(ConnectionType.Interactive, null) : null);
            }
        }

        public RedisFeatures GetFeatures()
        {
            return new RedisFeatures(version);
        }

        public void SetClusterConfiguration(ClusterConfiguration configuration)
        {
            ClusterConfiguration = configuration;

            var thisNode = configuration.Nodes.FirstOrDefault(x => x.EndPoint.Equals(this.EndPoint));
            if (thisNode != null)
            {
                List<ServerEndPoint> slaves = null;
                ServerEndPoint master = null;
                foreach (var node in configuration.Nodes)
                {
                    if (node.NodeId == thisNode.ParentNodeId)
                    {
                        master = multiplexer.GetServerEndPoint(node.EndPoint);
                    }
                    else if (node.ParentNodeId == thisNode.NodeId)
                    {
                        if (slaves == null) slaves = new List<ServerEndPoint>();
                        slaves.Add(multiplexer.GetServerEndPoint(node.EndPoint));
                    }
                }
                Master = master;
                Slaves = slaves?.ToArray() ?? NoSlaves;
            }
        }

        public void SetUnselectable(UnselectableFlags flags)
        {
            if (flags != 0)
            {
                var oldFlags = unselectableReasons;
                unselectableReasons |= flags;
                if (unselectableReasons != oldFlags)
                {
                    multiplexer.Trace(unselectableReasons == 0 ? "Now usable" : ("Now unusable: " + flags), ToString());
                }
            }
        }
        public override string ToString()
        {
            return Format.ToString(EndPoint);
        }

        public bool TryEnqueue(Message message)
        {
            var bridge = GetBridge(message.Command);
            return bridge != null && bridge.TryEnqueue(message, isSlave);
        }

        internal void Activate(ConnectionType type, Action<string> log)
        {
            GetBridge(type, true, log);
        }

        internal void AddScript(string script, byte[] hash)
        {
            lock (knownScripts)
            {
                knownScripts[script] = hash;
            }
        }

        internal void AutoConfigure(PhysicalConnection connection, Action<string> log)
        {
            if (ServerType == ServerType.Twemproxy)
            {
                // don't try to detect configuration; all the config commands are disabled, and
                // the fallback master/slave detection won't help
                return;
            }

            multiplexer.LogLocked(log, $"{Format.ToString(this)} Endpoint: Auto-configuring...");

            var autoConfigProcessor = new ResultProcessor.AutoConfigureProcessor(log);

            var commandMap = multiplexer.CommandMap;
            const CommandFlags flags = CommandFlags.FireAndForget | CommandFlags.HighPriority | CommandFlags.NoRedirect;

            var features = GetFeatures();
            Message msg;

            if (commandMap.IsAvailable(RedisCommand.CONFIG))
            {
                if (multiplexer.RawConfig.KeepAlive <= 0)
                {
                    msg = Message.Create(-1, flags, RedisCommand.CONFIG, RedisLiterals.GET, RedisLiterals.timeout);
                    msg.SetInternalCall();
                    WriteDirectOrQueueFireAndForget(connection, msg, autoConfigProcessor);
                }
                msg = Message.Create(-1, flags, RedisCommand.CONFIG, RedisLiterals.GET, RedisLiterals.slave_read_only);
                msg.SetInternalCall();
                WriteDirectOrQueueFireAndForget(connection, msg, autoConfigProcessor);
                msg = Message.Create(-1, flags, RedisCommand.CONFIG, RedisLiterals.GET, RedisLiterals.databases);
                msg.SetInternalCall();
                WriteDirectOrQueueFireAndForget(connection, msg, autoConfigProcessor);
            }
            if (commandMap.IsAvailable(RedisCommand.INFO))
            {
                Interlocked.Exchange(ref lastInfoReplicationCheckTicks, Environment.TickCount);
                if (features.InfoSections)
                {
                    msg = Message.Create(-1, flags, RedisCommand.INFO, RedisLiterals.replication);
                    msg.SetInternalCall();
                    WriteDirectOrQueueFireAndForget(connection, msg, autoConfigProcessor);

                    msg = Message.Create(-1, flags, RedisCommand.INFO, RedisLiterals.server);
                    msg.SetInternalCall();
                    WriteDirectOrQueueFireAndForget(connection, msg, autoConfigProcessor);
                }
                else
                {
                    msg = Message.Create(-1, flags, RedisCommand.INFO);
                    msg.SetInternalCall();
                    WriteDirectOrQueueFireAndForget(connection, msg, autoConfigProcessor);
                }
            }
            else if (commandMap.IsAvailable(RedisCommand.SET))
            {
                // this is a nasty way to find if we are a slave, and it will only work on up-level servers, but...
                RedisKey key = Guid.NewGuid().ToByteArray();
                msg = Message.Create(0, flags, RedisCommand.SET, key, RedisLiterals.slave_read_only, RedisLiterals.PX, 1, RedisLiterals.NX);
                msg.SetInternalCall();
                WriteDirectOrQueueFireAndForget(connection, msg, autoConfigProcessor);
            }
        }

        int _nextReplicaOffset;
        internal uint NextReplicaOffset() // used to round-robin between multiple replicas
            => (uint) System.Threading.Interlocked.Increment(ref _nextReplicaOffset);

        internal Message Close(out PhysicalBridge bridge)
        {
            bridge = interactive;
            Message result;
            if (bridge == null || !bridge.IsConnected || !multiplexer.CommandMap.IsAvailable(RedisCommand.QUIT))
            {
                result = null;
            }
            else
            {
                result = Message.Create(-1, CommandFlags.None, RedisCommand.QUIT);
            }
            return result;
        }

        internal void FlushScriptCache()
        {
            lock (knownScripts)
            {
                knownScripts.Clear();
            }
        }

        private string runId;
        internal string RunId
        {
            get { return runId; }
            set
            {
                if (value != runId) // we only care about changes
                {
                    // if we had an old run-id, and it has changed, then the
                    // server has been restarted; which means the script cache
                    // is toast
                    if (runId != null) FlushScriptCache();
                    runId = value;
                }
            }
        }

        internal ServerCounters GetCounters()
        {
            var counters = new ServerCounters(endpoint);
            interactive?.GetCounters(counters.Interactive);
            subscription?.GetCounters(counters.Subscription);
            return counters;
        }

        internal int GetOutstandingCount(RedisCommand command, out int inst, out int qu, out int qs, out int qc, out int wr, out int wq, out int @in, out int ar)
        {
            var bridge = GetBridge(command, false);
            if (bridge == null)
            {
                return inst = qu = qs = qc = wr = wq = @in = ar = 0;
            }

            return bridge.GetOutstandingCount(out inst, out qu, out qs, out qc, out wr, out wq, out @in, out ar);
        }

        internal string GetProfile()
        {
            var sb = new StringBuilder();
            sb.Append("Circular op-count snapshot; int:");
            interactive?.AppendProfile(sb);
            sb.Append("; sub:");
            subscription?.AppendProfile(sb);
            return sb.ToString();
        }

        internal byte[] GetScriptHash(string script, RedisCommand command)
        {
            var found = (byte[])knownScripts[script];
            if(found == null && command == RedisCommand.EVALSHA)
            {
                // the script provided is a hex sha; store and re-use the ascii for that
                found = Encoding.ASCII.GetBytes(script);
                lock(knownScripts)
                {
                    knownScripts[script] = found;
                }
            }
            return found;
        }

        internal string GetStormLog(RedisCommand command)
        {
            var bridge = GetBridge(command);
            return bridge?.GetStormLog();
        }

        internal Message GetTracerMessage(bool assertIdentity)
        {
            // different configurations block certain commands, as can ad-hoc local configurations, so
            // we'll do the best with what we have available.
            // note that the muxer-ctor asserts that one of ECHO, PING, TIME of GET is available
            // see also: TracerProcessor
            var map = multiplexer.CommandMap;
            Message msg;
            const CommandFlags flags = CommandFlags.NoRedirect | CommandFlags.FireAndForget;
            if (assertIdentity && map.IsAvailable(RedisCommand.ECHO))
            {
                msg = Message.Create(-1, flags, RedisCommand.ECHO, (RedisValue)multiplexer.UniqueId);
            }
            else if (map.IsAvailable(RedisCommand.PING))
            {
                msg = Message.Create(-1, flags, RedisCommand.PING);
            }
            else if (map.IsAvailable(RedisCommand.TIME))
            {
                msg = Message.Create(-1, flags, RedisCommand.TIME);
            }
            else if (!assertIdentity && map.IsAvailable(RedisCommand.ECHO))
            {
                // we'll use echo as a PING substitute if it is all we have (in preference to EXISTS)
                msg = Message.Create(-1, flags, RedisCommand.ECHO, (RedisValue)multiplexer.UniqueId);
            }
            else
            {
                map.AssertAvailable(RedisCommand.EXISTS);
                msg = Message.Create(0, flags, RedisCommand.EXISTS, (RedisValue)multiplexer.UniqueId);
            }
            msg.SetInternalCall();
            return msg;
        }

        internal bool IsSelectable(RedisCommand command)
        {
            var bridge = unselectableReasons == 0 ? GetBridge(command, false) : null;
            return bridge != null && bridge.IsConnected;
        }

        internal bool OnEstablishing(PhysicalConnection connection, Action<string> log)
        {
            try
            {
                if (connection == null) return false;
                return Handshake(connection, log);
            }
            catch (Exception ex) when (!(ex is OutOfMemoryException))
            {
                connection.RecordConnectionFailed(ConnectionFailureType.InternalFailure, ex);
                return false;
            }
        }

        internal void OnFullyEstablished(PhysicalConnection connection)
        {
            try
            {
                if (connection == null) return;
                var bridge = connection.Bridge;
                if (bridge == subscription)
                {
                    multiplexer.ResendSubscriptions(this);
                }
                multiplexer.OnConnectionRestored(endpoint, bridge.ConnectionType);
            }
            catch (Exception ex) when (!(ex is OutOfMemoryException))
            {
                connection.RecordConnectionFailed(ConnectionFailureType.InternalFailure, ex);
            }
        }
        
        internal int LastInfoReplicationCheckSecondsAgo
        {
            get
            {
                var lastInfoTicks = VolatileWrapper.Read(ref lastInfoReplicationCheckTicks);
                if (lastInfoTicks == 0) return -1;

                return unchecked(Environment.TickCount - lastInfoTicks) / 1000;
            }
        }

        private EndPoint masterEndPoint;
        public EndPoint MasterEndPoint
        {
            get { return Volatile.Read(ref masterEndPoint); }
            set { Volatile.Write(ref masterEndPoint, value); }
        }


        internal bool CheckInfoReplication()
        {
            Interlocked.Exchange(ref lastInfoReplicationCheckTicks, Environment.TickCount);
            PhysicalBridge bridge;
            if (version >= RedisFeatures.v2_8_0 && multiplexer.CommandMap.IsAvailable(RedisCommand.INFO)
                && (bridge = GetBridge(ConnectionType.Interactive, false)) != null)
            {
                var msg = Message.Create(-1, CommandFlags.FireAndForget | CommandFlags.HighPriority | CommandFlags.NoRedirect, RedisCommand.INFO, RedisLiterals.replication);
                msg.SetInternalCall();
                QueueDirectFireAndForget(msg, ResultProcessor.AutoConfigure, bridge);
                return true;
            }
            return false;
        }
        private int lastInfoReplicationCheckTicks;

        private int _heartBeatActive;
        internal void OnHeartbeat()
        {
            // don't overlap operations on an endpoint
            if (Interlocked.CompareExchange(ref _heartBeatActive, 1, 0) == 0)
            {
                try
                {


                    interactive?.OnHeartbeat(false);
                    subscription?.OnHeartbeat(false);
                }
                catch (Exception ex) when (!(ex is OutOfMemoryException))
                {
                    multiplexer.OnInternalError(ex, EndPoint);
                }
                finally
                {
                    Interlocked.Exchange(ref _heartBeatActive, 0);
                }
            }
        }

        internal bool TryQueueDirect(Message message, PhysicalBridge bridge = null)
        {
            if (bridge == null) bridge = GetBridge(message.Command);
            if (bridge == null || !bridge.TryEnqueue(message, isSlave))
            {
                return false;
            }

            return true;
        }

        internal Task<T> QueueDirectAsync<T>(Message message, ResultProcessor<T> processor, object asyncState = null, PhysicalBridge bridge = null)
        {
            var tcs = TaskSource.CreateDenyExecSync<T>(asyncState);
            var source = ResultBox<T>.Get(tcs);
            message.SetSource(processor, source);
            if (!TryQueueDirect(message, bridge))
            {
                ConnectionMultiplexer.ThrowFailed(tcs, ExceptionFactory.NoConnectionAvailable(multiplexer.IncludeDetailInExceptions, multiplexer.IncludePerformanceCountersInExceptions, message.Command, message, this, multiplexer.GetServerSnapshot()));
            }
            return tcs.Task;
        }

        internal Func<T> QueueDirect<T>(Message message, ResultProcessor<T> processor, PhysicalBridge bridge = null)
        {
            var mre = new ManualResetEventSlim(initialState: false);
            try
            {
                var source = ResultBox<T>.Get(mre);
                message.SetSource(processor, source);
                if (!TryQueueDirect(message, bridge))
                {
                    throw ExceptionFactory.NoConnectionAvailable(multiplexer.IncludeDetailInExceptions, multiplexer.IncludePerformanceCountersInExceptions, message.Command, message, this, multiplexer.GetServerSnapshot());
                }

                return () =>
                {
                    try
                    {
                        if ((message.Flags & CommandFlags.FireAndForget) != 0) return default(T);

                        if (!mre.Wait(Multiplexer.TimeoutMilliseconds))
                        {
                            Multiplexer.ThrowTimeoutException(message, this);
                        }

                        ResultBox<T>.UnwrapAndRecycle(source, true, out var result, out var exception);

                        if (exception != null)
                        {
                            throw exception;
                        }

                        Multiplexer.Trace(message + " received " + result);
                        return result;
                    }
                    finally
                    {
                        mre.Dispose();
                    }
                };
            }
            catch (Exception)
            {
                mre.Dispose();
                throw;
            }
        }

        internal void QueueDirectFireAndForget<T>(Message message, ResultProcessor<T> processor, PhysicalBridge bridge = null)
        {
            if (message != null)
            {
                message.SetSource(processor, null);
                multiplexer.Trace("Enqueue: " + message);
                (bridge ?? GetBridge(message.Command)).TryEnqueue(message, isSlave);
            }
        }

        internal void ReportNextFailure()
        {
            interactive?.ReportNextFailure();
            subscription?.ReportNextFailure();
        }

        internal string Summary()
        {
            var serverTypeString = ConnectionState == PhysicalBridge.State.ConnectedEstablished
                ? ServerType.ToString() + " v" + version
                : "Unknown";
            var sb = new StringBuilder(Format.ToString(endpoint))
                .Append(" Endpoint: ").Append(serverTypeString).Append(", ").Append(isSlave ? "slave" : "master");
            

            if (databases > 0) sb.Append("; ").Append(databases).Append(" databases");
            if (writeEverySeconds > 0)
                sb.Append("; keep-alive: ").Append(TimeSpan.FromSeconds(writeEverySeconds));
            var tmp = interactive;
            sb.Append("; int: ").Append(tmp?.ConnectionState.ToString() ?? "n/a");
            tmp = subscription;
            if(tmp == null)
            {
                sb.Append("; sub: n/a");
            } else
            {
                var state = tmp.ConnectionState;
                sb.Append("; sub: ").Append(state);
                if(state == PhysicalBridge.State.ConnectedEstablished)
                {
                    sb.Append(", ").Append(tmp.SubscriptionCount).Append(" active");
                }
            }

            var flags = unselectableReasons;
            if (flags != 0)
            {
                sb.Append("; not in use: ").Append(flags);
            }
            return sb.ToString();
        }
        internal void WriteDirectOrQueueFireAndForget<T>(PhysicalConnection connection, Message message, ResultProcessor<T> processor)
        {
            if (message != null)
            {
                message.SetSource(processor, null);
                if (connection == null)
                {
                    multiplexer.Trace("Enqueue: " + message);
                    GetBridge(message.Command).TryEnqueue(message, isSlave);
                }
                else
                {
                    multiplexer.Trace("Writing direct: " + message);
                    connection.Bridge.WriteMessageDirect(connection, message);
                }
            }
        }

        private PhysicalBridge CreateBridge(ConnectionType type, Action<string> log)
        {
            multiplexer.Trace(type.ToString());
            var bridge = new PhysicalBridge(this, type);
            bridge.TryConnect(log);
            return bridge;
        }
        bool Handshake(PhysicalConnection connection, Action<string> log)
        {
            if (connection == null)
            {
                multiplexer.Trace("No connection!?");
                return false;
            }
            multiplexer.LogLocked(log, $"{Format.ToString(connection.Bridge.Name)}: Server handshake");
            Message msg;
            // note that we need "" (not null) for password in the case of 'nopass' logins
            string username = Multiplexer.RawConfig.UserName, password = Multiplexer.RawConfig.Password ?? "";
            if (!string.IsNullOrWhiteSpace(username))
            {
                multiplexer.LogLocked(log, $"{Format.ToString(connection.Bridge.Name)}: Authenticating (user/password)");
                msg = Message.Create(-1, CommandFlags.FireAndForget, RedisCommand.AUTH, (RedisValue)username, (RedisValue)password);
                msg.SetInternalCall();
                WriteDirectOrQueueFireAndForget(connection, msg, ResultProcessor.DemandOK);
            }
            else if (!string.IsNullOrWhiteSpace(password))
            {
                multiplexer.LogLocked(log, $"{Format.ToString(connection.Bridge.Name)}: Authenticating (password)");
                msg = Message.Create(-1, CommandFlags.FireAndForget, RedisCommand.AUTH, (RedisValue)password);
                msg.SetInternalCall();
                WriteDirectOrQueueFireAndForget(connection, msg, ResultProcessor.DemandOK);
            }
            if (multiplexer.CommandMap.IsAvailable(RedisCommand.CLIENT))
            {
                string name = multiplexer.ClientName;
                if (!string.IsNullOrWhiteSpace(name))
                {
                    name = nameSanitizer.Replace(name, "");
                    if (!string.IsNullOrWhiteSpace(name))
                    {
                        multiplexer.LogLocked(log, $"{Format.ToString(connection.Bridge.Name)}: Setting client name: {name}");
                        msg = Message.Create(-1, CommandFlags.FireAndForget, RedisCommand.CLIENT, RedisLiterals.SETNAME, (RedisValue)name);
                        msg.SetInternalCall();
                        WriteDirectOrQueueFireAndForget(connection, msg, ResultProcessor.DemandOK);
                    }
                }
            }

            var connType = connection.Bridge.ConnectionType;

            if (connType == ConnectionType.Interactive)
            {
                AutoConfigure(connection, log);
            }
            var tracer = GetTracerMessage(true);
            multiplexer.LogLocked(log, $"{Format.ToString(connection.Bridge.Name)}: Sending critical tracer (handshake): {tracer.CommandAndKey}");
            tracer = LoggingMessage.Create(log, tracer);
            WriteDirectOrQueueFireAndForget(connection, tracer, ResultProcessor.EstablishConnection);


            // note: this **must** be the last thing on the subscription handshake, because after this
            // we will be in subscriber mode: regular commands cannot be sent
            if (connType == ConnectionType.Subscription)
            {
                var configChannel = multiplexer.ConfigurationChangedChannel;
                if(configChannel != null)
                {
                    msg = Message.Create(-1, CommandFlags.FireAndForget, RedisCommand.SUBSCRIBE, (RedisChannel)configChannel);
                    WriteDirectOrQueueFireAndForget(connection, msg, ResultProcessor.TrackSubscriptions);
                }
            }
            multiplexer.LogLocked(log, $"{Format.ToString(connection.Bridge.Name)}: Flushing outbound buffer");
            connection.Flush();
            return true;
        }
    }
}
