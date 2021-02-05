using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading;
#if CORE_CLR
using System.Threading.Tasks;
#endif

namespace StackExchange.Redis
{
    internal enum SocketMode
    {
        Abort,
        Async,
        Sync
    }
    /// <summary>
    /// Allows callbacks from SocketManager as work is discovered
    /// </summary>
    internal partial interface ISocketCallback
    {
        /// <summary>
        /// Indicates that a socket has connected
        /// </summary>
        bool Connected(Stream stream, Action<string> log);
        /// <summary>
        /// Indicates that the socket has signalled an error condition
        /// </summary>
        void Error(Exception exception);

        void OnHeartbeat();

        /// <summary>
        /// Indicates that data is available on the socket, and that the consumer should read synchronously from the socket while there is data
        /// </summary>
        void Read();
        /// <summary>
        /// Indicates that we cannot know whether data is available, and that the consume should commence reading asynchronously
        /// </summary>
        void StartReading();

        // check for write-read timeout
        void CheckForStaleConnection(ref SocketManager.ManagerState state);

        bool IsDataAvailable { get; }
    }

    internal struct SocketToken
    {
        internal readonly Socket Socket;
        public SocketToken(Socket socket)
        {
            Socket = socket;
        }
        public int Available => Socket?.Available ?? 0;

        public bool HasValue => Socket != null;
    }

    /// <summary>
    /// A SocketManager monitors multiple sockets for availability of data; this is done using
    /// the Socket.Select API and a dedicated reader-thread, which allows for fast responses
    /// even when the system is under ambient load. 
    /// </summary>
    public sealed partial class SocketManager : IDisposable
    {
        internal enum ManagerState
        {
            Inactive,
            Preparing,
            Faulted,
            CheckForHeartbeat,
            ExecuteHeartbeat,
            LocateActiveSockets,
            NoSocketsPause,
            PrepareActiveSockets,
            CullDeadSockets,
            NoActiveSocketsPause,
            GrowingSocketArray,
            CopyingPointersForSelect,
            ExecuteSelect,
            ExecuteSelectComplete,
            CheckForStaleConnections,

            RecordConnectionFailed_OnInternalError,
            RecordConnectionFailed_OnDisconnected,
            RecordConnectionFailed_ReportFailure,
            RecordConnectionFailed_OnConnectionFailed,
            RecordConnectionFailed_FailOutstanding,
            RecordConnectionFailed_ShutdownSocket,

            CheckForStaleConnectionsDone,
            ProcessRead,
            ProcessError,
            ProcessReadFallback

        }
        private static readonly ParameterizedThreadStart writeAllQueues = context =>
        {
            try { ((SocketManager)context).WriteAllQueues(); } catch (Exception ex) when (!(ex is OutOfMemoryException)) { }
        };

        private static readonly ParameterizedThreadStart writeOneQueue = context =>
        {
            try { ((SocketManager)context).WriteOneQueue(); } catch (Exception ex) when (!(ex is OutOfMemoryException)) { }
        };

        private readonly string name;

        private readonly Queue<PhysicalBridge> writeQueue = new Queue<PhysicalBridge>();
        internal volatile SocketMode socketMode;

        bool isDisposed;
        private bool useHighPrioritySocketThreads = true;

        /// <summary>
        /// Creates a new (optionally named) SocketManager instance
        /// </summary>
        public SocketManager(string name = null) : this(name, true, true) { }

        /// <summary>
        /// Creates a new SocketManager instance
        /// </summary>
        public SocketManager(string name, bool useHighPrioritySocketThreads, bool preferIocp)
        {
            if (string.IsNullOrWhiteSpace(name)) name = GetType().Name;
            this.name = name;
            this.useHighPrioritySocketThreads = useHighPrioritySocketThreads;

#if NETSTANDARD1_5
            if (preferIocp && RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
#else
            if (preferIocp && Environment.OSVersion.Platform == PlatformID.Win32NT)
#endif
            {
                // Since async sockets work great on Windows (please see the
                // comment below), and regular work items can't delay the
                // response reading logic, we don't need to create dedicated
                // reader threads on this OS. Less threads is always better
                // than more threads, especially when additional threads aren't
                // required.
                this.socketMode = SocketMode.Async;
            }
            else
            {
                // As of Mono 6.8 and earlier, .NET Core 3.1 and earlier, and
                // possible some future versions as well, async mode properly
                // works only on Windows systems, and only there responses are
                // dispatched in dedicated I/O threads of CLR's thread pool.
                //
                // On Linux and macOS systems responses are dispatched in regular
                // worker threads, using the same work queue that's shared with
                // other activities, such as HTTP request processing, and so on,
                // despite the fact that some blog posts in the Internet claim
                // some epoll- or kqueue-based implementation is used for async
                // sockets.
                //
                // Those other activities that don't relate to sockets may (and
                // will) cause unexpected delays in response processing, causing
                // lots of timeouts to occur. In an ideal world nothing blocks
                // us in those worker threads, but we have legacy synchronous
                // code, old non-async DbTransaction, and sync requests to Redis
                // itself.
                //
                // So we avoid such blocks by creating another reader thread per
                // socket that works preemptively with CLR's worker threads and
                // uses synchronous API.
                this.socketMode = SocketMode.Sync;
            }

            // we need a dedicated writer, because when under heavy ambient load
            // (a busy asp.net site, for example), workers are not reliable enough
#if !CORE_CLR
            Thread dedicatedWriter = new Thread(writeAllQueues, 256 * 1024); // don't need a huge stack;
            dedicatedWriter.Priority = useHighPrioritySocketThreads ? ThreadPriority.AboveNormal : ThreadPriority.Normal;
#else
            Thread dedicatedWriter = new Thread(writeAllQueues);
#endif
            dedicatedWriter.Name = name + ":Write";
            dedicatedWriter.IsBackground = true; // should not keep process alive
            dedicatedWriter.Start(this); // will self-exit when disposed
        }

        private enum CallbackOperation
        {
            Read,
            Error
        }

        /// <summary>
        /// Gets the name of this SocketManager instance
        /// </summary>
        public string Name => name;

        /// <summary>
        /// Releases all resources associated with this instance
        /// </summary>
        public void Dispose()
        {
            lock (writeQueue)
            {
                // make sure writer threads know to exit
                isDisposed = true;
                Monitor.PulseAll(writeQueue);
            }
            OnDispose();
        }

        internal SocketToken BeginConnect(EndPoint endpoint, ISocketCallback callback, ConnectionMultiplexer multiplexer, Action<string> log)
        {
            var formattedEndpoint = Format.ToString(endpoint);

            if (!IsWindowsPlatform() && endpoint is DnsEndPoint dnsEndPoint)
            {
                // Socket.BeginConnect(IPAddress[], int) doesn't work well on Unix systems due to their internal
                // implementation (please see https://github.com/dotnet/runtime/issues/16263 for details). Also,
                // when using socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true)
                // option, even when Socket.BeginConnect(string host, int port) overload is called, it begins
                // to use the Socket.BeginConnect(IPAddress[], int) method, resulting into PlatformNotSupportedException.
                // So we are querying DNS server by ourselves, using the first available IP address and always
                // passing a single endpoint to the Socket.BeginConnect method to avoid the exception.

                IPAddress[] endpointAddresses = null;

#if CORE_CLR
                multiplexer.LogLocked(log, "BeginDNSResolve: {0}", formattedEndpoint);
                Dns.GetHostAddressesAsync(dnsEndPoint.Host).ContinueWith(t =>
                {
                    multiplexer.LogLocked(log, "EndDNSResolve: {0}", formattedEndpoint);
                    endpointAddresses = t.Result;
                    multiplexer.LogLocked(log, "DNSResolve complete: {0}", formattedEndpoint);
                });
#else
                multiplexer.LogLocked(log, "BeginDNSResolve: {0}", formattedEndpoint);
                endpointAddresses = Dns.GetHostAddresses(dnsEndPoint.Host);
                multiplexer.LogLocked(log, "EndDNSResolve: {0}", formattedEndpoint);
#endif
                var endpointAddress = endpointAddresses?.FirstOrDefault(ip =>
                    ip.AddressFamily == AddressFamily.InterNetwork ||
                    ip.AddressFamily == AddressFamily.InterNetworkV6);

                if (endpointAddress != null)
                {
                    endpoint = new IPEndPoint(endpointAddress, dnsEndPoint.Port);
                }
            }

            var addressFamily = endpoint.AddressFamily == AddressFamily.Unspecified ? AddressFamily.InterNetwork : endpoint.AddressFamily; // 41526630444f27f53258eb88448d285836f097dd
            var socket = new Socket(addressFamily, SocketType.Stream, ProtocolType.Tcp); // 300baed206f671f471bd3ffe01de1ae0e7437bda
            SetFastLoopbackOption(socket);
#if NETCOREAPP3_0
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, 1);
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, 30);
#else
            SetKeepAliveOption(socket, intervalSec: 1, timeSec: 30);
#endif
            socket.NoDelay = true;
            try
            {
                CompletionType connectCompletionType = CompletionType.Any;
                this.ShouldForceConnectCompletionType(ref connectCompletionType);

                var tuple = Tuple.Create(socket, callback);
                {
#if CORE_CLR
                    multiplexer.LogLocked(log, "BeginConnect: {0}", formattedEndpoint);
                    socket.ConnectAsync(endpoint).ContinueWith(t =>
                    {
                        multiplexer.LogLocked(log, "EndConnect: {0}", formattedEndpoint);
                        EndConnectImpl(t, multiplexer, log, tuple);
                    });
#else
                    CompletionTypeHelper.RunWithCompletionType(
                        cb => {
                            multiplexer.LogLocked(log, "BeginConnect: {0}", formattedEndpoint);
                            return socket.BeginConnect(endpoint, cb, tuple);
                        },
                        ar => {
                            multiplexer.LogLocked(log, "EndConnect: {0}", formattedEndpoint);
                            EndConnectImpl(ar, multiplexer, log, tuple);
                            multiplexer.LogLocked(log, "Connect complete: {0}", formattedEndpoint);
                        },
                        connectCompletionType,
                        multiplexer.SyncConnectTimeout(false));
#endif
                }
            } 
            catch (NotImplementedException ex)
            {
                if (!(endpoint is IPEndPoint))
                {
                    throw new InvalidOperationException("BeginConnect failed with NotImplementedException; consider using IP endpoints, or enable ResolveDns in the configuration", ex);
                }
                throw;
            }
            var token = new SocketToken(socket);
            return token;
        }

        private static bool IsWindowsPlatform()
        {
#if NETSTANDARD1_5
            return RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
#else
            return Environment.OSVersion.Platform == PlatformID.Win32NT;
#endif
        }

        private static void SetKeepAliveOption(Socket socket, int intervalSec, int timeSec)
        {
            // windows only
            try
            {
                if (IsWindowsPlatform())
                {
                    int size = Marshal.SizeOf(new uint());
                    byte[] optInValue = new byte[size * 3];

                    BitConverter.GetBytes((uint)1).CopyTo(optInValue, 0);
                    BitConverter.GetBytes((uint)timeSec * 1000).CopyTo(optInValue, size);
                    BitConverter.GetBytes((uint)intervalSec * 1000).CopyTo(optInValue, size * 2);

                    socket.IOControl(IOControlCode.KeepAliveValues, optInValue, null);
                }
            }
            catch (PlatformNotSupportedException)
            {
                // Fix for https://github.com/StackExchange/StackExchange.Redis/issues/582 
                // Checking the platform can fail on some platforms. However, we don't 
                //   care if the platform check fails because this is for a Windows 
                //   optimization, and checking the platform will not fail on Windows.
            }
        }

        internal void SetFastLoopbackOption(Socket socket)
        {
            // SIO_LOOPBACK_FAST_PATH (http://msdn.microsoft.com/en-us/library/windows/desktop/jj841212%28v=vs.85%29.aspx)
            // Speeds up localhost operations significantly. OK to apply to a socket that will not be hooked up to localhost, 
            // or will be subject to WFP filtering.
            const int SIO_LOOPBACK_FAST_PATH = -1744830448;

#if !CORE_CLR
            // windows only
            if (Environment.OSVersion.Platform == PlatformID.Win32NT)
            {
                // Win8/Server2012+ only
                var osVersion = Environment.OSVersion.Version;
                if (osVersion.Major > 6 || osVersion.Major == 6 && osVersion.Minor >= 2)
                {
                    byte[] optionInValue = BitConverter.GetBytes(1);
                    socket.IOControl(SIO_LOOPBACK_FAST_PATH, optionInValue, null);
                }
            }
#else
            try
            {
                // Ioctl is not supported on other platforms at the moment
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    byte[] optionInValue = BitConverter.GetBytes(1);
                    socket.IOControl(SIO_LOOPBACK_FAST_PATH, optionInValue, null);
                }
            }
            catch (SocketException)
            {
            }
            catch (PlatformNotSupportedException)
            {
                // Fix for https://github.com/StackExchange/StackExchange.Redis/issues/582 
                // Checking the platform can fail on some platforms. However, we don't 
                //   care if the platform check fails because this is for a Windows 
                //   optimization, and checking the platform will not fail on Windows.
            }
#endif
        }

        internal void RequestWrite(PhysicalBridge bridge, bool forced)
        {
            if (Interlocked.CompareExchange(ref bridge.inWriteQueue, 1, 0) == 0 || forced)
            {
                lock (writeQueue)
                {
                    writeQueue.Enqueue(bridge);
                    if (writeQueue.Count == 1)
                    {
                        Monitor.PulseAll(writeQueue);
                    }
                    else if (writeQueue.Count >= 2)
                    { 
                        // struggling are we? let's have some help dealing with the backlog
#if NETSTANDARD1_5
                        var thread = new Thread(writeOneQueue)
#else
                        var thread = new Thread(writeOneQueue, 256 * 1024) // don't need a huge stack
#endif
                        {
#if !NETSTANDARD1_5
                            Priority = useHighPrioritySocketThreads ? ThreadPriority.AboveNormal : ThreadPriority.Normal,
#endif
                            Name = name + ":WriteHelper",
                            IsBackground = true // should not keep process alive
                        };
                        thread.Start(this);
                    }
                }
            }
        }

        internal void Shutdown(SocketToken token)
        {
            Shutdown(token.Socket);
        }

        private void EndConnectImpl(IAsyncResult ar, ConnectionMultiplexer multiplexer, Action<string> log, Tuple<Socket, ISocketCallback> tuple)
        {
            try
            {
                bool ignoreConnect = false;
                ShouldIgnoreConnect(tuple.Item2, ref ignoreConnect);
                if (ignoreConnect) return;
                var socket = tuple.Item1;
                var callback = tuple.Item2;
#if CORE_CLR
                multiplexer.Wait((Task)ar); // make it explode if invalid (note: already complete at this point)
#else
                socket.EndConnect(ar);
#endif
                var netStream = new NetworkStream(socket, false);
                var connected = callback?.Connected(netStream, log) ?? false;
                if (!connected)
                {
                    ConnectionMultiplexer.TraceWithoutContext("Aborting socket");
                    Shutdown(socket);
                    return;
                }

                switch (socketMode)
                {
                    case SocketMode.Async:
                        multiplexer.LogLocked(log, "Starting read");
                        try
                        { callback.StartReading(); }
                        catch (Exception ex) when (!(ex is OutOfMemoryException))
                        {
                            ConnectionMultiplexer.TraceWithoutContext(ex.Message);
                            Shutdown(socket);
                        }
                        break;
                    case SocketMode.Sync:
                        multiplexer.LogLocked(log, "Starting reader thread");
                        try
                        {
#if !CORE_CLR
                            Thread dedicatedReader = new Thread(performSyncRead, 256 * 1024); // don't need a huge stack;
                            dedicatedReader.Priority = useHighPrioritySocketThreads ? ThreadPriority.AboveNormal : ThreadPriority.Normal;
#else
                            Thread dedicatedReader = new Thread(performSyncRead);
#endif
                            dedicatedReader.Name = name + ":Read";
                            dedicatedReader.IsBackground = true; // should not keep process alive
                            dedicatedReader.Start(callback); // will self-exit when disposed
                        }
                        catch (Exception ex) when (!(ex is OutOfMemoryException))
                        {
                            ConnectionMultiplexer.TraceWithoutContext(ex.Message);
                            Shutdown(socket);
                        }
                        break;
                    default:
                        ConnectionMultiplexer.TraceWithoutContext($"Socket mode '{socketMode}' is not supported.");
                        Shutdown(socket);
                        break;
                }
            }
            catch (ObjectDisposedException)
            {
                multiplexer.LogLocked(log, "(socket shutdown)");
                if (tuple != null)
                {
                    try
                    { tuple.Item2.Error(null); }
                    catch (Exception inner) when (!(inner is OutOfMemoryException))
                    {
                        ConnectionMultiplexer.TraceWithoutContext(inner.Message);
                    }
                }
            }
            catch(Exception outer) when (!(outer is OutOfMemoryException))
            {
                ConnectionMultiplexer.TraceWithoutContext(outer.Message);
                if (tuple != null)
                {
                    try
                    { tuple.Item2.Error(outer); }
                    catch (Exception inner) when (!(inner is OutOfMemoryException))
                    {
                        ConnectionMultiplexer.TraceWithoutContext(inner.Message);
                    }
                }
            }
        }

        private static readonly ParameterizedThreadStart performSyncRead = context =>
        {
            try { ((ISocketCallback)context).Read(); } catch (Exception ex) when (!(ex is OutOfMemoryException)) { }
        };

        partial void OnDispose();
        partial void OnShutdown(Socket socket);

        partial void ShouldIgnoreConnect(ISocketCallback callback, ref bool ignore);
        
        partial void ShouldForceConnectCompletionType(ref CompletionType completionType);

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2202:Do not dispose objects multiple times")]
        private void Shutdown(Socket socket)
        {
            if (socket != null)
            {
                OnShutdown(socket);
                try { socket.Shutdown(SocketShutdown.Both); } catch (Exception ex) when (!(ex is OutOfMemoryException)) { }
#if !CORE_CLR
                try { socket.Close(); } catch (Exception ex) when (!(ex is OutOfMemoryException)) { }
#endif
                try { socket.Dispose(); } catch (Exception ex) when (!(ex is OutOfMemoryException)) { }
            }
        }

        private void WriteAllQueues()
        {
            while (true)
            {
                PhysicalBridge bridge;
                lock (writeQueue)
                {
                    if (writeQueue.Count == 0)
                    {
                        if (isDisposed) break; // <========= exit point
                        Monitor.Wait(writeQueue);
                        if (isDisposed) break; // (woken by Dispose)
                        if (writeQueue.Count == 0) continue; // still nothing...
                    }
                    bridge = writeQueue.Dequeue();
                }

                switch (bridge.WriteQueue(200))
                {
                    case WriteResult.MoreWork:
                    case WriteResult.QueueEmptyAfterWrite:
                        // back of the line!
                        lock (writeQueue)
                        {
                            writeQueue.Enqueue(bridge);
                        }
                        break;
                    case WriteResult.CompetingWriter:
                        break;
                    case WriteResult.NoConnection:
                        Interlocked.Exchange(ref bridge.inWriteQueue, 0);
                        break;
                    case WriteResult.NothingToDo:
                        if (!bridge.ConfirmRemoveFromWriteQueue())
                        { // more snuck in; back of the line!
                            lock (writeQueue)
                            {
                                writeQueue.Enqueue(bridge);
                            }
                        }
                        break;
                }
            }
        }

        private void WriteOneQueue()
        {
            PhysicalBridge bridge;
            lock (writeQueue)
            {
                bridge = writeQueue.Count == 0 ? null : writeQueue.Dequeue();
            }
            if (bridge == null) return;
            bool keepGoing;
            do
            {
                switch (bridge.WriteQueue(-1))
                {
                    case WriteResult.MoreWork:
                    case WriteResult.QueueEmptyAfterWrite:
                        keepGoing = true;
                        break;
                    case WriteResult.NothingToDo:
                        keepGoing = !bridge.ConfirmRemoveFromWriteQueue();
                        break;
                    case WriteResult.CompetingWriter:
                        keepGoing = false;
                        break;
                    case WriteResult.NoConnection:
                        Interlocked.Exchange(ref bridge.inWriteQueue, 0);
                        keepGoing = false;
                        break;
                    default:
                        keepGoing = false;
                        break;
                }
            } while (keepGoing);
        }
    }
}
