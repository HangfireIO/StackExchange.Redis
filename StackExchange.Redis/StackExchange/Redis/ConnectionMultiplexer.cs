using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Reflection;
#if NET40
using Microsoft.Runtime.CompilerServices;
#else
using System.IO.Compression;
using System.Runtime.CompilerServices;
#endif

namespace StackExchange.Redis
{
    internal static partial class TaskExtensions
    {
        private static readonly Action<Task> observeErrors = ObverveErrors;
        private static void ObverveErrors(this Task task)
        {
            if (task != null) GC.KeepAlive(task.Exception);
        }

        public  static Task ObserveErrors(this Task task)
        {
            task?.ContinueWith(observeErrors, TaskContinuationOptions.OnlyOnFaulted);
            return task;
        }
        public static Task<T> ObserveErrors<T>(this Task<T> task)
        {
            task?.ContinueWith(observeErrors, TaskContinuationOptions.OnlyOnFaulted);
            return task;
        }

        public static ConfiguredTaskAwaitable ForAwait(this Task task)
        {
            return task.ConfigureAwait(false);
        }
        public static ConfiguredTaskAwaitable<T> ForAwait<T>(this Task<T> task)
        {
            return task.ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Represents an inter-related group of connections to redis servers
    /// </summary>
    public sealed partial class ConnectionMultiplexer : IConnectionMultiplexer, IDisposable
    {
        private static readonly string timeoutHelpLink = "http://stackexchange.github.io/StackExchange.Redis/Timeouts";

        private static TaskFactory _factory = null;

        /// <summary>
        /// Provides a way of overriding the default Task Factory. If not set, it will use the default Task.Factory.
        /// Useful when top level code sets it's own factory which may interfere with Redis queries.
        /// </summary>
        public static TaskFactory Factory
        {
            get
            {
                return _factory ?? Task.Factory;
            }
            set
            {
                _factory = value;
                
            }
        }

        /// <summary>
        /// Get summary statistics associates with this server
        /// </summary>
        public ServerCounters GetCounters()
        {
            var snapshot = serverSnapshot;

            var counters = new ServerCounters(null);
            for (int i = 0; i < snapshot.Length; i++)
            {
                counters.Add(snapshot[i].GetCounters());
            }
            unprocessableCompletionManager.GetCounters(counters.Other);
            return counters;
        }

        /// <summary>
        /// Gets the client-name that will be used on all new connections
        /// </summary>
        public string ClientName => RawConfig.ClientName ?? ConnectionMultiplexer.GetDefaultClientName();

        private static string defaultClientName;
        private static string GetDefaultClientName()
        {
            if (defaultClientName == null)
            {
                defaultClientName =  TryGetAzureRoleInstanceIdNoThrow() ?? Environment.GetEnvironmentVariable("ComputerName");
            }
            return defaultClientName;
        }
        
        internal EndPointCollection EndPoints { get; }

        /// Tries to get the Roleinstance Id if Microsoft.WindowsAzure.ServiceRuntime is loaded.
        /// In case of any failure, swallows the exception and returns null
        internal static string TryGetAzureRoleInstanceIdNoThrow()
        {
            string roleInstanceId = null;
            // TODO: CoreCLR port pending https://github.com/dotnet/coreclr/issues/919
#if !CORE_CLR
            try
            {
                Assembly asm = null;
                foreach (var asmb in AppDomain.CurrentDomain.GetAssemblies())
                {
                    if (asmb.GetName().Name.Equals("Microsoft.WindowsAzure.ServiceRuntime"))
                    {
                        asm = asmb;
                        break;
                    }
                }
                if (asm == null)
                    return null;

                var type = asm.GetType("Microsoft.WindowsAzure.ServiceRuntime.RoleEnvironment");

                // https://msdn.microsoft.com/en-us/library/microsoft.windowsazure.serviceruntime.roleenvironment.isavailable.aspx
                if (!(bool)type.GetProperty("IsAvailable").GetValue(null, null))
                    return null;

                var currentRoleInstanceProp = type.GetProperty("CurrentRoleInstance");
                var currentRoleInstanceId = currentRoleInstanceProp.GetValue(null, null);
                roleInstanceId = currentRoleInstanceId.GetType().GetProperty("Id").GetValue(currentRoleInstanceId, null).ToString();

                if (String.IsNullOrEmpty(roleInstanceId))
                {
                    roleInstanceId = null;
                }
            }
            catch (Exception ex) when (!(ex is OutOfMemoryException))
            {
                //silently ignores the exception
                roleInstanceId = null;
            }
#endif
            return roleInstanceId;
        }

        /// <summary>
        /// Gets the configuration of the connection
        /// </summary>
        public string Configuration => RawConfig.ToString();

        internal void OnConnectionFailed(EndPoint endpoint, ConnectionType connectionType, ConnectionFailureType failureType, Exception exception, bool reconfigure)
        {
            if (isDisposed) return;
            var handler = ConnectionFailed;
            if (handler != null)
            {
                unprocessableCompletionManager.CompleteSyncOrAsync(
                    new ConnectionFailedEventArgs(handler, this, endpoint, connectionType, failureType, exception)
                );
            }
            if (reconfigure)
            {
                ReconfigureIfNeeded(endpoint, false, "connection failed");
            }
        }
        internal void OnInternalError(Exception exception, EndPoint endpoint = null, ConnectionType connectionType = ConnectionType.None, [System.Runtime.CompilerServices.CallerMemberName] string origin = null)
        {
            try
            {
                TraceExceptionWithoutContext(exception, "Internal error: " + origin, origin);
                if (isDisposed) return;
                var handler = InternalError;
                if (handler != null)
                {
                    unprocessableCompletionManager.CompleteSyncOrAsync(
                        new InternalErrorEventArgs(handler, this, endpoint, connectionType, exception, origin)
                    );
                }
            }
            catch (Exception ex) when (!(ex is OutOfMemoryException))
            {
                // our internal error event failed; whatcha gonna do, exactly?
                ConnectionMultiplexer.TraceExceptionWithoutContext(ex);
            }
        }

        internal void OnConnectionRestored(EndPoint endpoint, ConnectionType connectionType)
        {
            if (isDisposed) return;
            var handler = ConnectionRestored;
            if (handler != null)
            {
                unprocessableCompletionManager.CompleteSyncOrAsync(
                    new ConnectionFailedEventArgs(handler, this, endpoint, connectionType, ConnectionFailureType.None, null)
                );
            }
            ReconfigureIfNeeded(endpoint, false, "connection restored");
        }


        private void OnEndpointChanged(EndPoint endpoint, EventHandler<EndPointEventArgs> handler)
        {
            if (isDisposed) return;
            if (handler != null)
            {
                unprocessableCompletionManager.CompleteSyncOrAsync(
                    new EndPointEventArgs(handler, this, endpoint)
                );
            }
        }
        internal void OnConfigurationChanged(EndPoint endpoint)
        {
            OnEndpointChanged(endpoint, ConfigurationChanged);
        }
        internal void OnConfigurationChangedBroadcast(EndPoint endpoint)
        {
            OnEndpointChanged(endpoint, ConfigurationChangedBroadcast);
        }

        /// <summary>
        /// A server replied with an error message;
        /// </summary>
        public event EventHandler<RedisErrorEventArgs> ErrorMessage;
        internal void OnErrorMessage(EndPoint endpoint, string message)
        {
            if (isDisposed) return;
            var handler = ErrorMessage;
            if (handler != null)
            {
                unprocessableCompletionManager.CompleteSyncOrAsync(
                    new RedisErrorEventArgs(handler, this, endpoint, message)
                );
            }
        }

#if !NET40
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2202:Do not dispose objects multiple times")]
        static void Write<T>(ZipArchive zip, string name, Task task, Action<T, StreamWriter> callback)
        {
            var entry = zip.CreateEntry(name,
#if __MonoCS__
                CompressionLevel.Fastest
#else
                CompressionLevel.Optimal
#endif
                );
            using (var stream = entry.Open())
            using (var writer = new StreamWriter(stream))
            {
                TaskStatus status = task.Status;
                switch (status)
                {
                    case TaskStatus.RanToCompletion:
                        T val = ((Task<T>)task).Result;
                        callback(val, writer);
                        break;
                    case TaskStatus.Faulted:
                        writer.WriteLine(string.Join(", ", task.Exception.InnerExceptions.Select(x => x.Message)));
                        break;
                    default:
                        writer.WriteLine(status.ToString());
                        break;
                }
            }
        }
        /// <summary>
        /// Write the configuration of all servers to an output stream
        /// </summary>
        public void ExportConfiguration(Stream destination, ExportOptions options = ExportOptions.All)
        {
            if (destination == null) throw new ArgumentNullException(nameof(destination));

            // what is possible, given the command map?
            ExportOptions mask = 0;
            if (CommandMap.IsAvailable(RedisCommand.INFO)) mask |= ExportOptions.Info;
            if (CommandMap.IsAvailable(RedisCommand.CONFIG)) mask |= ExportOptions.Config;
            if (CommandMap.IsAvailable(RedisCommand.CLIENT)) mask |= ExportOptions.Client;
            if (CommandMap.IsAvailable(RedisCommand.CLUSTER)) mask |= ExportOptions.Cluster;
            options &= mask;

            using (var zip = new ZipArchive(destination, ZipArchiveMode.Create, true))
            {
                var arr = serverSnapshot;
                foreach (var server in arr)
                {
                    const CommandFlags flags = CommandFlags.None;
                    if (!server.IsConnected) continue;
                    var api = GetServer(server.EndPoint);

                    List<Task> tasks = new List<Task>();
                    if ((options & ExportOptions.Info) != 0)
                    {
                        tasks.Add(api.InfoRawAsync(flags: flags));
                    }
                    if ((options & ExportOptions.Config) != 0)
                    {
                        tasks.Add(api.ConfigGetAsync(flags: flags));
                    }
                    if ((options & ExportOptions.Client) != 0)
                    {
                        tasks.Add(api.ClientListAsync(flags: flags));
                    }
                    if ((options & ExportOptions.Cluster) != 0)
                    {
                        tasks.Add(api.ClusterNodesRawAsync(flags: flags));
                    }

                    WaitAllIgnoreErrors(tasks.ToArray());

                    int index = 0;
                    var prefix = Format.ToString(server.EndPoint);
                    if ((options & ExportOptions.Info) != 0)
                    {
                        Write<string>(zip, prefix + "/info.txt", tasks[index++], WriteNormalizingLineEndings);
                    }
                    if ((options & ExportOptions.Config) != 0)
                    {
                        Write<KeyValuePair<string, string>[]>(zip, prefix + "/config.txt", tasks[index++], (settings, writer) =>
                        {
                            foreach (var setting in settings)
                            {
                                writer.WriteLine("{0}={1}", setting.Key, setting.Value);
                            }
                        });
                    }
                    if ((options & ExportOptions.Client) != 0)
                    {
                        Write<ClientInfo[]>(zip, prefix + "/clients.txt", tasks[index++], (clients, writer) =>
                        {
                            foreach (var client in clients)
                            {
                                writer.WriteLine(client.Raw);
                            }
                        });
                    }
                    if ((options & ExportOptions.Cluster) != 0)
                    {
                        Write<string>(zip, prefix + "/nodes.txt", tasks[index++], WriteNormalizingLineEndings);
                    }
                }
            }
        }
#endif

        internal void MakeMaster(ServerEndPoint server, ReplicationChangeOptions options, Action<string> log)
        {
            CommandMap.AssertAvailable(RedisCommand.SLAVEOF);
            if (!RawConfig.AllowAdmin) throw ExceptionFactory.AdminModeNotEnabled(IncludeDetailInExceptions, RedisCommand.SLAVEOF, null, server);

            if (server == null) throw new ArgumentNullException(nameof(server));
            var srv = new RedisServer(this, server, null);
            if (!srv.IsConnected) throw ExceptionFactory.NoConnectionAvailable(IncludeDetailInExceptions, IncludePerformanceCountersInExceptions, RedisCommand.SLAVEOF, null, server, GetServerSnapshot());

            CommandMap.AssertAvailable(RedisCommand.SLAVEOF);

            const CommandFlags flags = CommandFlags.NoRedirect | CommandFlags.HighPriority;
            Message msg;

            LogLocked(log, "Checking {0} is available...", Format.ToString(srv.EndPoint));
            try
            {
                srv.Ping(flags); // if it isn't happy, we're not happy
            } catch (Exception ex) when (!(ex is OutOfMemoryException))
            {
                LogLocked(log, "Operation failed on {0}, aborting: {1}", Format.ToString(srv.EndPoint), ex.Message);
                throw;
            }

            var nodes = serverSnapshot;
            RedisValue newMaster = Format.ToString(server.EndPoint);

            // try and write this everywhere; don't worry if some folks reject our advances
            if (RawConfig.TryGetTieBreaker(out var tieBreakerKey) &&
                (options & ReplicationChangeOptions.SetTiebreaker) != 0 &&
                CommandMap.IsAvailable(RedisCommand.SET))
            {
                foreach (var node in nodes)
                {
                    if (!node.IsConnected || node.IsSlave) continue;
                    LogLocked(log, "Attempting to set tie-breaker on {0}...", Format.ToString(node.EndPoint));
                    msg = Message.Create(0, flags, RedisCommand.SET, tieBreakerKey, newMaster);
                    node.QueueDirectFireAndForget(msg, ResultProcessor.DemandOK);
                }
            }

            // deslave...
            LogLocked(log, "Making {0} a master...", Format.ToString(srv.EndPoint));
            try
            {
                srv.SlaveOf(null, flags);
            } catch (Exception ex) when (!(ex is OutOfMemoryException))
            {
                LogLocked(log, "Operation failed on {0}, aborting: {1}", Format.ToString(srv.EndPoint), ex.Message);
                throw;
            }

            // also, in case it was a slave a moment ago, and hasn't got the tie-breaker yet, we re-send the tie-breaker to this one
            if (!tieBreakerKey.IsNull)
            {
                LogLocked(log, "Resending tie-breaker to {0}...", Format.ToString(server.EndPoint));
                msg = Message.Create(0, flags, RedisCommand.SET, tieBreakerKey, newMaster);
                server.QueueDirectFireAndForget(msg, ResultProcessor.DemandOK);
            }



            // try and broadcast this everywhere, to catch the maximum audience
            if ((options & ReplicationChangeOptions.Broadcast) != 0 && ConfigurationChangedChannel != null
                && CommandMap.IsAvailable(RedisCommand.PUBLISH))
            {
                RedisValue channel = ConfigurationChangedChannel;
                foreach (var node in nodes)
                {
                    if (!node.IsConnected) continue;
                    LogLocked(log, "Broadcasting via {0}...", Format.ToString(node.EndPoint));
                    msg = Message.Create(-1, flags, RedisCommand.PUBLISH, channel, newMaster);
                    node.QueueDirectFireAndForget(msg, ResultProcessor.Int64);
                }
            }


            if ((options & ReplicationChangeOptions.EnslaveSubordinates) != 0)
            {
                foreach (var node in nodes)
                {
                    if (node == server || node.ServerType != ServerType.Standalone) continue;

                    LogLocked(log, "Enslaving {0}...", Format.ToString(node.EndPoint));
                    msg = RedisServer.CreateSlaveOfMessage(server.EndPoint, flags);
                    node.QueueDirectFireAndForget(msg, ResultProcessor.DemandOK);
                }
            }

            // and reconfigure the muxer
            LogLocked(log, "Reconfiguring all endpoints...");
            if (!Reconfigure(false, true, log, srv.EndPoint, "make master"))
            {
                LogLocked(log, "Verifying the configuration was incomplete; please verify");
            }
        }

// we know this has strong identity: readonly and unique to us

        internal void LogLocked(Action<string> log, string line)
        {
            log?.Invoke(line);
        }
        internal void LogLocked(Action<string> log, string line, object arg)
        {
            log?.Invoke(String.Format(line, arg));
        }
        internal void LogLocked(Action<string> log, string line, object arg0, object arg1)
        {
            log?.Invoke(String.Format(line, arg0, arg1));
        }
        internal void LogLocked(Action<string> log, string line, object arg0, object arg1, object arg2)
        {
            log?.Invoke(String.Format(line, arg0, arg1, arg2));
        }
        internal void LogLocked(Action<string> log, string line, params object[] args)
        {
            log?.Invoke(String.Format(line, args));
        }

        internal void CheckMessage(Message message)
        {
            if (!RawConfig.AllowAdmin && message.IsAdmin)
                throw ExceptionFactory.AdminModeNotEnabled(IncludeDetailInExceptions, message.Command, message, null);
            CommandMap.AssertAvailable(message.Command);
        }

        static void WriteNormalizingLineEndings(string source, StreamWriter writer)
        {
            using (var reader = new StringReader(source))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                    writer.WriteLine(line); // normalize line endings
            }
        }

        /// <summary>
        /// Raised whenever a physical connection fails
        /// </summary>
        public event EventHandler<ConnectionFailedEventArgs> ConnectionFailed;

        /// <summary>
        /// Raised whenever an internal error occurs (this is primarily for debugging)
        /// </summary>
        public event EventHandler<InternalErrorEventArgs> InternalError;

        /// <summary>
        /// Raised whenever a physical connection is established
        /// </summary>
        public event EventHandler<ConnectionFailedEventArgs> ConnectionRestored;

        /// <summary>
        /// Raised when configuration changes are detected
        /// </summary>
        public event EventHandler<EndPointEventArgs> ConfigurationChanged;

        /// <summary>
        /// Raised when nodes are explicitly requested to reconfigure via broadcast;
        /// this usually means master/slave changes
        /// </summary>
        public event EventHandler<EndPointEventArgs> ConfigurationChangedBroadcast;

        /// <summary>
        /// Gets the timeout associated with the connections
        /// </summary>
        public int TimeoutMilliseconds => timeoutMilliseconds;

        /// <summary>
        /// Gets all endpoints defined on the server
        /// </summary>
        /// <returns></returns>
        public EndPoint[] GetEndPoints(bool configuredOnly = false)
        {
            if (configuredOnly) return EndPoints.ToArray();

            return ConvertHelper.ConvertAll(serverSnapshot, x => x.EndPoint);
        }

        private readonly int timeoutMilliseconds;


        internal bool TryResend(int hashSlot, Message message, EndPoint endpoint, bool isMoved)
        {
            return serverSelectionStrategy.TryResend(hashSlot, message, endpoint, isMoved);
        }


        /// <summary>
        /// Wait for a given asynchronous operation to complete (or timeout)
        /// </summary>
        public void Wait(Task task)
        {
            if (task == null) throw new ArgumentNullException(nameof(task));
            if (!task.Wait(timeoutMilliseconds)) throw new TimeoutException();
        }

        /// <summary>
        /// Wait for a given asynchronous operation to complete (or timeout)
        /// </summary>

        public T Wait<T>(Task<T> task)
        {
            if (task == null) throw new ArgumentNullException(nameof(task));
            if (!task.Wait(timeoutMilliseconds)) throw new TimeoutException();
            return task.Result;
        }
        /// <summary>
        /// Wait for the given asynchronous operations to complete (or timeout)
        /// </summary>
        public void WaitAll(params Task[] tasks)
        {
            if (tasks == null) throw new ArgumentNullException(nameof(tasks));
            if (tasks.Length == 0) return;
            if (!Task.WaitAll(tasks, timeoutMilliseconds)) throw new TimeoutException();
        }

        private bool WaitAllIgnoreErrors(Task[] tasks)
        {
            return WaitAllIgnoreErrors(tasks, timeoutMilliseconds);
        }
        private static bool WaitAllIgnoreErrors(Task[] tasks, int timeout)
        {
            if (tasks == null) throw new ArgumentNullException(nameof(tasks));
            if (tasks.Length == 0) return true;
            var watch = Stopwatch.StartNew();
            try
            {
                // if none error, great
                if (Task.WaitAll(tasks, timeout)) return true;
            }
            catch (Exception ex) when (!(ex is OutOfMemoryException))
            {
                TraceExceptionWithoutContext(ex);
            }
            // if we get problems, need to give the non-failing ones time to finish
            // to be fair and reasonable
            for (int i = 0; i < tasks.Length; i++)
            {
                var task = tasks[i];
                if (!task.IsCanceled && !task.IsCompleted && !task.IsFaulted)
                {
                    var remaining = timeout - checked((int)watch.ElapsedMilliseconds);
                    if (remaining <= 0) return false;
                    try
                    {
                        task.Wait(remaining);
                    }
                    catch (Exception ex) when (!(ex is OutOfMemoryException))
                    {
                        TraceExceptionWithoutContext(ex);
                    }
                }
            }
            return false;
        }

#if !CORE_CLR
        private void LogLockedWithThreadPoolStats(Action<string> log, string message, out int busyWorkerCount)
        {
            busyWorkerCount = 0;
            if(log != null)
            {
                var sb = new StringBuilder();
                sb.Append(message);
                string iocp, worker;
                busyWorkerCount = GetThreadPoolStats(out iocp, out worker);
                sb.Append(", IOCP: ").Append(iocp).Append(", WORKER: ").Append(worker);
                LogLocked(log, sb.ToString());
            }
        }
#endif

        static bool AllComplete(Task[] tasks)
        {
            for(int i = 0 ; i < tasks.Length ; i++)
            {
                var task = tasks[i];
                if (!task.IsCanceled && !task.IsCompleted && !task.IsFaulted)
                    return false;
            }
            return true;
        }

        private bool WaitAllIgnoreErrors<T>(string name, Tuple<ResultBox<T>, ManualResetEvent>[] events, int timeoutMilliseconds, Action<string> log)
        {
            if (events == null) throw new ArgumentNullException(nameof(events));
            if (events.Length == 0)
            {
                LogLocked(log, "No events to wait");
                return true;
            }

            const int limit = 64;
            var index = 0;
            var started = Stopwatch.StartNew();
            
            LogLocked(log, $"Waiting for {events.Length} {name} event completion");

            // Working around the limitation that WaitHandle.WaitAll can take maximum 64 handles
            while (index < events.Length)
            {
                var iterationLimit = Math.Min(index + limit, events.Length);
                var array = new WaitHandle[iterationLimit - index];

                int i;
                
                for (i = index; i < iterationLimit; i++)
                {
                    array[i - index] = events[i].Item2;
                }

                var remaining = timeoutMilliseconds - (int)started.ElapsedMilliseconds;
                if (remaining < 0) remaining = 0;

                if (!WaitHandle.WaitAll(array, remaining))
                {
                    return false;
                }

                index = i;
            }

            LogLocked(log, $"Wait for {events.Length} {name} events completed");
            return true;
        }

        private async Task<bool> WaitAllIgnoreErrorsAsync(Task[] tasks, int timeoutMilliseconds, Action<string> log)
        {
            if (tasks == null) throw new ArgumentNullException(nameof(tasks));
            if (tasks.Length == 0)
            {
                LogLocked(log, "No tasks to await");
                return true;
            }

            if (AllComplete(tasks))
            {
                LogLocked(log, "All tasks are already complete");
                return true;
            }

            var watch = Stopwatch.StartNew();
#if !CORE_CLR
            int busyWorkerCount;
            LogLockedWithThreadPoolStats(log, "Awaiting task completion", out busyWorkerCount);
#endif
            try
            {
                // if none error, great
                var remaining = timeoutMilliseconds - checked((int)watch.ElapsedMilliseconds);
                if (remaining <= 0)
                {
#if !CORE_CLR
                    LogLockedWithThreadPoolStats(log, "Timeout before awaiting for tasks", out busyWorkerCount);
#endif
                    return false;
                }

#if NET40
                var allTasks = TaskEx.WhenAll(tasks).ObserveErrors();
                var any = TaskEx.WhenAny(allTasks, TaskEx.Delay(remaining)).ObserveErrors();
#else
                var allTasks = Task.WhenAll(tasks).ObserveErrors();
                var any = Task.WhenAny(allTasks, Task.Delay(remaining)).ObserveErrors();
#endif
                bool all = await any.ForAwait() == allTasks;
#if !CORE_CLR
                LogLockedWithThreadPoolStats(log, all ? "All tasks completed cleanly" : "Not all tasks completed cleanly", out busyWorkerCount);
#endif
                return all;
            }
            catch (Exception ex) when (!(ex is OutOfMemoryException))
            {
                TraceExceptionWithoutContext(ex);
            }

            // if we get problems, need to give the non-failing ones time to finish
            // to be fair and reasonable
            for (int i = 0; i < tasks.Length; i++)
            {
                var task = tasks[i];
                if (!task.IsCanceled && !task.IsCompleted && !task.IsFaulted)
                {
                    var remaining = timeoutMilliseconds - checked((int)watch.ElapsedMilliseconds);
                    if (remaining <= 0)
                    {
#if !CORE_CLR
                        LogLockedWithThreadPoolStats(log, "Timeout awaiting tasks", out busyWorkerCount);
#endif
                        return false;
                    }
                    try
                    {
#if NET40
                        var any = TaskEx.WhenAny(task, TaskEx.Delay(remaining)).ObserveErrors();
#else
                        var any = Task.WhenAny(task, Task.Delay(remaining)).ObserveErrors();
#endif
                        await any.ForAwait();
                    }
                    catch (Exception ex) when (!(ex is OutOfMemoryException))
                    {
                        TraceExceptionWithoutContext(ex);
                    }
                }
            }
#if !CORE_CLR
            LogLockedWithThreadPoolStats(log, "Finished awaiting tasks", out busyWorkerCount);
#endif
            return false;
        }


        /// <summary>
        /// Raised when a hash-slot has been relocated
        /// </summary>
        public event EventHandler<HashSlotMovedEventArgs> HashSlotMoved;

        internal void OnHashSlotMoved(int hashSlot, EndPoint old, EndPoint @new)
        {
            var handler = HashSlotMoved;
            if (handler != null)
            {
                unprocessableCompletionManager.CompleteSyncOrAsync(
                    new HashSlotMovedEventArgs(handler, this, hashSlot, old, @new)
                );
            }
        }

        /// <summary>
        /// Compute the hash-slot of a specified key
        /// </summary>
        public int HashSlot(RedisKey key)
        {
            return serverSelectionStrategy.HashSlot(key);
        }

        internal ServerEndPoint AnyConnected(ServerType serverType, uint startOffset, RedisCommand command, CommandFlags flags)
        {
            var tmp = serverSnapshot;
            int len = tmp.Length;
            ServerEndPoint fallback = null;
            for (int i = 0; i < len; i++)
            {
                var server = tmp[(int)(((uint)i + startOffset) % len)];
                if (server != null && server.ServerType == serverType && server.IsSelectable(command))
                {
                    if (server.IsSlave)
                    {
                        switch (flags)
                        {
                            case CommandFlags.DemandSlave:
                            case CommandFlags.PreferSlave:
                                return server;
                            case CommandFlags.PreferMaster:
                                fallback = server;
                                break;
                        }
                    } else
                    {
                        switch (flags)
                        {
                            case CommandFlags.DemandMaster:
                            case CommandFlags.PreferMaster:
                                return server;
                            case CommandFlags.PreferSlave:
                                fallback = server;
                                break;
                        }
                    }
                }
            }
            return fallback;
        }

        volatile bool isDisposed;
        internal bool IsDisposed => isDisposed;

        static ConnectionMultiplexer CreateMultiplexer(object configuration, ServerType? serverType, EndPointCollection endpoints)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            ConfigurationOptions config;
            if (configuration is string)
            {
                config = ConfigurationOptions.Parse((string)configuration);
            } else if (configuration is ConfigurationOptions)
            {
                config = ((ConfigurationOptions)configuration).Clone();
            } else
            {
                throw new ArgumentException("configuration");
            }
            if (config.EndPoints.Count == 0) throw new ArgumentException("No endpoints specified", nameof(configuration));
            config.SetDefaultPorts();
            return new ConnectionMultiplexer(config, serverType, endpoints);
        }
        /// <summary>
        /// Create a new ConnectionMultiplexer instance
        /// </summary>
        public static ConnectionMultiplexer Connect(string configuration, Action<string> log = null, ServerType? serverType = null, EndPointCollection endpoints = null)
        {
            return Connect(ConfigurationOptions.Parse(configuration), log);
        }
        
        private static void Validate(ConfigurationOptions config)
        {
            if (config is null)
            {
                throw new ArgumentNullException(nameof(config));
            }
            if (config.EndPoints.Count == 0)
            {
                throw new ArgumentException("No endpoints specified", nameof(config));
            }
        }

        /// <summary>
        /// Create a new ConnectionMultiplexer instance
        /// </summary>
        public static ConnectionMultiplexer Connect(ConfigurationOptions configuration, Action<string> log = null, ServerType? serverType = null, EndPointCollection endpoints = null)
        {
            Validate(configuration);

            return configuration.IsSentinel
                ? SentinelPrimaryConnect(configuration, log)
                : ConnectImpl(configuration, log);
        }

        private static ConnectionMultiplexer ConnectImpl(ConfigurationOptions configuration, Action<string> log, ServerType? serverType = null, EndPointCollection endpoints = null)
        {
            IDisposable killMe = null;
            try
            {
                var muxer = CreateMultiplexer(configuration, serverType, endpoints);
                killMe = muxer;
                // note that task has timeouts internally, so it might take *just over* the regular timeout
                if (!muxer.Reconfigure(true, false, log, null, "connect"))
                {
                    var exception = ExceptionFactory.UnableToConnect(
                        muxer.RawConfig.AbortOnConnectFail,
                        muxer.failureMessage ?? "ConnectTimeout");

                    if (muxer.RawConfig.AbortOnConnectFail)
                    {
                        throw exception;
                    }
                    else
                    {
                        muxer.LastException = exception;
                    }
                }
                
                if (muxer.ServerSelectionStrategy.ServerType == ServerType.Sentinel)
                {
                    // Initialize the Sentinel handlers
                    muxer.InitializeSentinel(log);
                }

                killMe = null;
                return muxer;
            }
            finally
            {
                if (killMe != null)
                {
                    try
                    {
                        killMe.Dispose();
                    }
                    catch (Exception ex) when (!(ex is OutOfMemoryException))
                    {
                        TraceExceptionWithoutContext(ex);
                    }
                }
            }
        }

        private string failureMessage;
        private readonly Hashtable servers = new Hashtable();
        private volatile ServerEndPoint[] serverSnapshot = NilServers;

        private static readonly ServerEndPoint[] NilServers = new ServerEndPoint[0];

        internal ServerEndPoint GetServerEndPoint(EndPoint endpoint)
        {
            if (endpoint == null) return null;
            var server = (ServerEndPoint)servers[endpoint];
            if (server == null)
            {
                lock (servers)
                {
                    server = (ServerEndPoint)servers[endpoint];
                    if (server == null)
                    {
                        if (isDisposed) throw new ObjectDisposedException(ToString());

                        server = new ServerEndPoint(this, endpoint, null);
                        // ^^ this could indirectly cause servers to become changes, so treble-check!
                        if (!servers.ContainsKey(endpoint))
                        {
                            servers.Add(endpoint, server);
                        }

                        var newSnapshot = new ServerEndPoint[serverSnapshot.Length + 1];
                        serverSnapshot.CopyTo(newSnapshot, 0);
                        newSnapshot[newSnapshot.Length - 1] = server;
                        serverSnapshot = newSnapshot;
                    }

                }
            }
            return server;
        }

        internal readonly CommandMap CommandMap;
        private ConnectionMultiplexer(ConfigurationOptions configuration, ServerType? serverType = null, EndPointCollection endpoints = null)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            
            RawConfig = configuration ?? throw new ArgumentNullException(nameof(configuration));
            EndPoints = endpoints ?? RawConfig.EndPoints.Clone();
            EndPoints.SetDefaultPorts(serverType, ssl: RawConfig.Ssl);
            
            IncludeDetailInExceptions = true;
            IncludePerformanceCountersInExceptions = false;
            
            var map = CommandMap = configuration.GetCommandMap(serverType);
            if (!string.IsNullOrWhiteSpace(configuration.Password)) map.AssertAvailable(RedisCommand.AUTH);

            if(!map.IsAvailable(RedisCommand.ECHO) && !map.IsAvailable(RedisCommand.PING) && !map.IsAvailable(RedisCommand.TIME))
            { // I mean really, give me a CHANCE! I need *something* to check the server is available to me...
                // see also: SendTracer (matching logic)
                map.AssertAvailable(RedisCommand.EXISTS);
            }

            PreserveAsyncOrder = true; // safest default
            timeoutMilliseconds = configuration.SyncTimeout;

            OnCreateReaderWriter(configuration);
            unprocessableCompletionManager = new CompletionManager(this, "multiplexer");
            serverSelectionStrategy = new ServerSelectionStrategy(this);

            var configChannel = configuration.ConfigurationChannel;
            if (!string.IsNullOrWhiteSpace(configChannel))
            {
                ConfigurationChangedChannel = Encoding.UTF8.GetBytes(configChannel);
            }
            lastHeartbeatTicks = Environment.TickCount;
        }

        partial void OnCreateReaderWriter(ConfigurationOptions configuration);

        internal const int MillisecondsPerHeartbeat = 1000;

        private static readonly TimerCallback heartbeat = state =>
        {
            ((ConnectionMultiplexer)state).OnHeartbeat();
        };

        /// <summary>
        /// Performs the heartbeat manually when automatic ones disabled by setting the
        /// <see cref="ConfigurationOptions.HeartbeatInterval"/> to a zero value. It's
        /// expected that multiple heartbeats are called during the keep-alive time. By
        /// default it's expected to run every second.
        /// </summary>
        public void HeartbeatOnce()
        {
            OnHeartbeat();
        }

        private int _activeHeartbeatErrors;
        private void OnHeartbeat()
        {
            try
            {
                int now = Environment.TickCount;
                Interlocked.Exchange(ref lastHeartbeatTicks, now);
                Interlocked.Exchange(ref lastGlobalHeartbeatTicks, now);
                Trace("heartbeat");

                var tmp = serverSnapshot;
                for (int i = 0; i < tmp.Length; i++)
                    tmp[i].OnHeartbeat();
            }
            catch (Exception ex) when (!(ex is OutOfMemoryException))
            {
                if (Interlocked.CompareExchange(ref _activeHeartbeatErrors, 1, 0) == 0)
                {
                    try
                    {
                        OnInternalError(ex);
                    }
                    finally
                    {
                        Interlocked.Exchange(ref _activeHeartbeatErrors, 0);
                    }
                }
            }
        }

        private int lastHeartbeatTicks;
        private static int lastGlobalHeartbeatTicks = Environment.TickCount;
        internal long LastHeartbeatSecondsAgo {
            get {
                if (isDisposed) return -1;
                return unchecked(Environment.TickCount - VolatileWrapper.Read(ref lastHeartbeatTicks)) / 1000;
            }
        }

        internal Exception LastException { get; set; }

        internal static long LastGlobalHeartbeatSecondsAgo => unchecked(Environment.TickCount - VolatileWrapper.Read(ref lastGlobalHeartbeatTicks)) / 1000;

        internal CompletionManager UnprocessableCompletionManager => unprocessableCompletionManager;

        /// <summary>
        /// Obtain a pub/sub subscriber connection to the specified server
        /// </summary>
        public ISubscriber GetSubscriber(object asyncState = null)
        {
            if (RawConfig.Proxy == Proxy.Twemproxy) throw new NotSupportedException("The pub/sub API is not available via twemproxy");
            return new RedisSubscriber(this, asyncState);
        }
        /// <summary>
        /// Obtain an interactive connection to a database inside redis
        /// </summary>
        public IDatabase GetDatabase(int db = -1, object asyncState = null)
        {
            if (db == -1)
                db = RawConfig.DefaultDatabase ?? 0;

            if (db < 0) throw new ArgumentOutOfRangeException(nameof(db));
            if (db != 0 && RawConfig.Proxy == Proxy.Twemproxy) throw new NotSupportedException("Twemproxy only supports database 0");

            // if there's no async-state, and the DB is suitable, we can hand out a re-used instance
            return (asyncState == null && db <= MaxCachedDatabaseInstance)
                ? GetCachedDatabaseInstance(db) : new RedisDatabase(this, db, asyncState);
        }

        // DB zero is stored separately, since 0-only is a massively common use-case
        const int MaxCachedDatabaseInstance = 16; // 17 items - [0,16]
        // side note: "databases 16" is the default in redis.conf; happy to store one extra to get nice alignment etc
        private IDatabase dbCacheZero;
        private IDatabase[] dbCacheLow;
        private IDatabase GetCachedDatabaseInstance(int db) // note that we already trust db here; only caller checks range
        {
            // note we don't need to worry about *always* returning the same instance
            // - if two threads ask for db 3 at the same time, it is OK for them to get
            // different instances, one of which (arbitrarily) ends up cached for later use
            if(db == 0)
            {
                return dbCacheZero ?? (dbCacheZero = new RedisDatabase(this, 0, null));
            }
            var arr = dbCacheLow ?? (dbCacheLow = new IDatabase[MaxCachedDatabaseInstance]);
            return arr[db - 1] ?? (arr[db - 1] = new RedisDatabase(this, db, null));
        }

        /// <summary>
        /// Obtain a configuration API for an individual server
        /// </summary>
        public IServer GetServer(string host, int port, object asyncState = null)
        {
            return GetServer(Format.ParseEndPoint(host, port), asyncState);
        }
        /// <summary>
        /// Obtain a configuration API for an individual server
        /// </summary>
        public IServer GetServer(string hostAndPort, object asyncState = null)
        {
            return GetServer(Format.TryParseEndPoint(hostAndPort), asyncState);
        }
        /// <summary>
        /// Obtain a configuration API for an individual server
        /// </summary>
        public IServer GetServer(IPAddress host, int port)
        {
            return GetServer(new IPEndPoint(host, port));
        }

        /// <summary>
        /// Obtain a configuration API for an individual server
        /// </summary>
        public IServer GetServer(EndPoint endpoint, object asyncState = null)
        {
            if (endpoint == null) throw new ArgumentNullException(nameof(endpoint));
            if (RawConfig.Proxy == Proxy.Twemproxy) throw new NotSupportedException("The server API is not available via twemproxy");
            var server = (ServerEndPoint)servers[endpoint];
            if (server == null) throw new ArgumentException("The specified endpoint is not defined", nameof(endpoint));
            return new RedisServer(this, server, asyncState);
        }

        internal void Trace(string message, [System.Runtime.CompilerServices.CallerMemberName] string category = null)
        {
            OnTrace(message, category);
        }

        internal void Trace(bool condition, string message, [System.Runtime.CompilerServices.CallerMemberName] string category = null)
        {
            if (condition) OnTrace(message, category);
        }

        private static Action<string, Exception> Logger;

        /// <summary>
        /// Sets a global logging action for diagnostic purposes.
        /// </summary>
        public static void SetLoggingAction(Action<string, Exception> logger)
        {
            Logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        private void OnTrace(string message, string category)
        {
            try
            {
                if (!String.IsNullOrEmpty(category))
                {
                    Logger?.Invoke(category + ": " + message, null);
                }
                else
                {
                    Logger?.Invoke(message, null);
                }
            }
            catch
            {
                // 
            }
        }

        private static void OnTraceWithoutContext(string message, string category, Exception exception)
        {
            try
            {
                if (!String.IsNullOrEmpty(category))
                {
                    Logger?.Invoke(category + ": " + message, exception);
                }
                else
                {
                    Logger?.Invoke(message, exception);
                }
            }
            catch
            {
                // 
            }
        }

        internal static void TraceExceptionWithoutContext(Exception exception, string message = null, [System.Runtime.CompilerServices.CallerMemberName] string category = null)
        {
            OnTraceWithoutContext(message + (exception?.Message ?? "(no exception)"), category, exception);
        }

        internal static void TraceWithoutContext(string message, [System.Runtime.CompilerServices.CallerMemberName] string category = null)
        {
            OnTraceWithoutContext(message, category, null);
        }

        internal static void TraceWithoutContext(bool condition, string message, [System.Runtime.CompilerServices.CallerMemberName] string category = null)
        {
            if(condition) OnTraceWithoutContext(message, category, null);
        }

        private readonly CompletionManager unprocessableCompletionManager;

        /// <summary>
        /// The number of operations that have been performed on all connections
        /// </summary>
        public long OperationCount {
            get
            {
                long total = 0;
                var snapshot = serverSnapshot;
                for (int i = 0; i < snapshot.Length; i++) total += snapshot[i].OperationCount;
                return total;
            }
        }

        string activeConfigCause;

        internal bool ReconfigureIfNeeded(EndPoint blame, bool fromBroadcast, string cause, bool publishReconfigure = false, CommandFlags flags = CommandFlags.None)
        {
            if (fromBroadcast)
            {
                OnConfigurationChangedBroadcast(blame);
            }
            string activeCause = Interlocked.CompareExchange(ref activeConfigCause, null, null);
            if (activeCause == null)
            {
                bool reconfigureAll = fromBroadcast || publishReconfigure;
                Trace("Configuration change detected; checking nodes", "Configuration");

                var name = string.IsNullOrWhiteSpace(ClientName)
                    ? ClientName
                    : nameof(SocketManager);

                var thread = new Thread(
                    () =>
                    {
                        try
                        {
                            Reconfigure(false, reconfigureAll, null, blame, cause, publishReconfigure, flags);
                        }
                        catch (Exception ex)
                        {
                            TraceExceptionWithoutContext(ex);
                        }
                    }
#if !CORE_CLR
                    , 256 * 1024 // don't need a huge stack
#endif
                )
                {
                    IsBackground = true,
                    Name = name + ":Reconfig",
#if !CORE_CLR
                    Priority = RawConfig.HighPrioritySocketThreads
                        ? ThreadPriority.AboveNormal
                        : ThreadPriority.Normal
#endif
                };

                thread.Start();

                return true;
            } else
            {
                Trace("Configuration change skipped; already in progress via " + activeCause, "Configuration");
                return false;
            }
        }

        /// <summary>
        /// Reconfigure the current connections based on the existing configuration
        /// </summary>
        public bool Configure(Action<string> log = null)
        {
            // note we expect ReconfigureAsync to internally allow [n] duration,
            // so to avoid near misses, here we wait 2*[n]
            return Reconfigure(false, true, log, null, "configure");
        }

        internal int SyncConnectTimeout(bool forConnect)
        {
            int retryCount = forConnect ? RawConfig.ConnectRetry : 1;
            if (retryCount <= 0) retryCount = 1;

            int timeout = RawConfig.ConnectTimeout;            
            if (timeout >= int.MaxValue / retryCount) return int.MaxValue;

            timeout *= retryCount;
            if (timeout >= int.MaxValue - 500) return int.MaxValue;
            return timeout + Math.Min(500, timeout);
        }
        /// <summary>
        /// Provides a text overview of the status of all connections
        /// </summary>
        public string GetStatus()
        {
            var sb = new StringBuilder();
            void AppendLog(string msg) => sb.AppendLine(msg);

            GetStatus(AppendLog);
            return sb.ToString();
        }
        /// <summary>
        /// Provides a text overview of the status of all connections
        /// </summary>
        public void GetStatus(Action<string> log)
        {
            if (log == null) return;

            var tmp = serverSnapshot;
            foreach (var server in tmp)
            {
                LogLocked(log, server.Summary());
                LogLocked(log, server.GetCounters().ToString());
                LogLocked(log, server.GetProfile());
            }
            LogLocked(log, "Sync timeouts: {0}; fire and forget: {1}; last heartbeat: {2}s ago",
                Interlocked.Read(ref syncTimeouts), Interlocked.Read(ref fireAndForgets), LastHeartbeatSecondsAgo);
        }
        internal bool Reconfigure(bool first, bool reconfigureAll, Action<string> log, EndPoint blame, string cause, bool publishReconfigure = false, CommandFlags publishReconfigureFlags = CommandFlags.None)
        {
            if (isDisposed) return false;
            bool showStats = true;

            if (log == null)
            {
                showStats = false;
            }
            bool ranThisCall = false;
            try
            {   // note that "activeReconfigs" starts at one; we don't need to set it the first time
                ranThisCall = first || Interlocked.CompareExchange(ref activeConfigCause, cause, null) == null;

                if (!ranThisCall)
                {
                    LogLocked(log, "Reconfiguration was already in progress");
                    return false;
                }
                Trace("Starting reconfiguration...");
                Trace(blame != null, "Blaming: " + Format.ToString(blame));

                LogLocked(log, RawConfig.ToString(includePassword: false));


                if (first)
                {
                    if (RawConfig.ResolveDns && EndPoints.HasDnsEndPoints())
                    {
                        EndPoints.ResolveEndPoints(this, log);
                    }
                    int index = 0;
                    lock (servers)
                    {
                        serverSnapshot = new ServerEndPoint[EndPoints.Count];
                        foreach (var endpoint in EndPoints)
                        {
                            var server = (ServerEndPoint)servers[endpoint];
                            if (server == null)
                            {
                                server = new ServerEndPoint(this, endpoint, log);
                                // ^^ this could indirectly cause servers to become changes, so treble-check!
                                if (!servers.ContainsKey(endpoint))
                                {
                                    servers.Add(endpoint, server);
                                }
                            }
                            serverSnapshot[index++] = server;
                        }
                    }
                    foreach (var server in serverSnapshot)
                    {
                        server.Activate(ConnectionType.Interactive, log);
                        if (CommandMap.IsAvailable(RedisCommand.SUBSCRIBE))
                        {
                            server.Activate(ConnectionType.Subscription, null); // no need to log the SUB stuff
                        }
                    }
                }

                int attemptsLeft = first ? RawConfig.ConnectRetry : 1;

                bool healthy = false;
                do
                {
                    if (first)
                    {
                        attemptsLeft--;
                    }
                    int standaloneCount = 0, clusterCount = 0, sentinelCount = 0;
                    var endpoints = EndPoints;
                    LogLocked(log, "{0} unique nodes specified", endpoints.Count);

                    if (endpoints.Count == 0)
                    {
                        throw new InvalidOperationException("No nodes to consider");
                    }

                    const CommandFlags flags = CommandFlags.NoRedirect | CommandFlags.HighPriority;
                    List<ServerEndPoint> masters = new List<ServerEndPoint>(endpoints.Count);
                    bool useTieBreakers = RawConfig.TryGetTieBreaker(out var tieBreakerKey);
                    
                    ServerEndPoint[] servers = null;
                    Tuple<ResultBox<string>, ManualResetEvent>[] tieBreakers = null;
                    bool encounteredConnectedClusterServer = false;

                    int iterCount = first ? 2 : 1;
                    // this is fix for https://github.com/StackExchange/StackExchange.Redis/issues/300
                    // auto discoverability of cluster nodes is made synchronous. 
                    // we try to connect to endpoints specified inside the user provided configuration
                    // and when we encounter one such endpoint to which we are able to successfully connect,
                    // we get the list of cluster nodes from this endpoint and try to proactively connect
                    // to these nodes instead of relying on auto configure
                    for (int iter = 0; iter < iterCount; ++iter)
                    {
                        if (endpoints == null) break;

                        var available = new Tuple<ResultBox<bool>, ManualResetEvent>[endpoints.Count];
                        tieBreakers = useTieBreakers ? new Tuple<ResultBox<string>, ManualResetEvent>[endpoints.Count] : null;
                        servers = new ServerEndPoint[available.Length];
                        
                        for (int i = 0; i < available.Length; i++)
                        {
                            Trace("Testing: " + Format.ToString(endpoints[i]));
                            var server = GetServerEndPoint(endpoints[i]);
                            server.AutoConfigure(null, log);

                            servers[i] = server;

                            var mre = new ManualResetEvent(false);
                            var source = ResultBox<bool>.Get(mre);

                            var tracerMsg = server.GetTracerMessage(false);
                            tracerMsg = LoggingMessage.Create(log, tracerMsg);

                            tracerMsg.SetSource(ResultProcessor.Tracer, source);
                            if (!server.TryQueueDirect(tracerMsg))
                            {
                                source.SetException(ExceptionFactory.NoConnectionAvailable(IncludeDetailInExceptions, IncludePerformanceCountersInExceptions, tracerMsg.Command, tracerMsg, server, GetServerSnapshot()));
                                mre.Set();
                            }

                            available[i] = new Tuple<ResultBox<bool>, ManualResetEvent>(source, mre);
                        }
                        
                        var watch = Stopwatch.StartNew();
                        var remaining = RawConfig.ConnectTimeout - checked((int)watch.ElapsedMilliseconds);
                        LogLocked(log, "Allowing endpoints {0} to respond...", TimeSpan.FromMilliseconds(remaining));
                        Trace("Allowing endpoints " + TimeSpan.FromMilliseconds(remaining) + " to respond...");
                        WaitAllIgnoreErrors("available", available, remaining, log);

                        // Log current state after await
                        foreach (var server in serverSnapshot)
                        {
                            LogLocked(log, "{0} Endpoint: State is {1}", Format.ToString(server.EndPoint), server.ConnectionState);
                        }

                        if (useTieBreakers)
                        {
                            for (int i = 0; i < available.Length; i++)
                            {
                                var server = GetServerEndPoint(endpoints[i]);
                                LogLocked(log, "Requesting tie-break from {0} > {1}...", Format.ToString(server.EndPoint), RawConfig.TieBreaker);
                                Message msg = Message.Create(0, flags, RedisCommand.GET, tieBreakerKey);
                                msg.SetInternalCall();
                                msg = LoggingMessage.Create(log, msg);

                                var tieMre = new ManualResetEvent(false);
                                var tieSource = ResultBox<string>.Get(tieMre);

                                msg.SetSource(ResultProcessor.String, tieSource);
                                if (!server.TryQueueDirect(msg))
                                {
                                    tieSource.SetException(ExceptionFactory.NoConnectionAvailable(IncludeDetailInExceptions, IncludePerformanceCountersInExceptions, msg.Command, msg, server, GetServerSnapshot()));
                                    tieMre.Set();
                                }

                                tieBreakers[i] = new Tuple<ResultBox<string>, ManualResetEvent>(tieSource, tieMre);
                            }
                        }

                        EndPointCollection updatedClusterEndpointCollection = null;
                        for (int i = 0; i < available.Length; i++)
                        {
                            ResultBox<bool>.UnwrapAndRecycle(available[i].Item1, false, out var result, out var exception);
                            var server = servers[i];
                            if (exception != null)
                            {
                                server.SetUnselectable(UnselectableFlags.DidNotRespond);
                                LogLocked(log, "{0} Endpoint: Faulted: {1}", Format.ToString(server), exception.Message);
                                failureMessage = exception.Message;
                            }
                            else if (available[i].Item2.WaitOne(TimeSpan.Zero))
                            {
                                if (result)
                                {
                                    server.ClearUnselectable(UnselectableFlags.DidNotRespond);
                                    LogLocked(log, "{0} Endpoint: returned with success as {1} {2}", Format.ToString(server), server.ServerType, (server.IsSlave ? "replica" : "primary"));
                                    
                                    // count the server types
                                    switch (server.ServerType)
                                    {
                                        case ServerType.Twemproxy:
                                        case ServerType.Standalone:
                                            standaloneCount++;
                                            break;
                                        case ServerType.Sentinel:
                                            sentinelCount++;
                                            break;
                                        case ServerType.Cluster:
                                            clusterCount++;
                                            break;
                                    }

                                    if (clusterCount > 0 && !encounteredConnectedClusterServer)
                                    {
                                        // we have encountered a connected server with clustertype for the first time. 
                                        // so we will get list of other nodes from this server using "CLUSTER NODES" command
                                        // and try to connect to these other nodes in the next iteration
                                        encounteredConnectedClusterServer = true;
                                        updatedClusterEndpointCollection = GetEndpointsFromClusterNodes(server, log);
                                    }

                                    // set the server UnselectableFlags and update masters list
                                    switch (server.ServerType)
                                    {
                                        case ServerType.Twemproxy:
                                        case ServerType.Sentinel:
                                        case ServerType.Standalone:
                                        case ServerType.Cluster:
                                            server.ClearUnselectable(UnselectableFlags.ServerType);
                                            if (server.IsSlave)
                                            {
                                                server.ClearUnselectable(UnselectableFlags.RedundantMaster);
                                            }
                                            else
                                            {
                                                masters.Add(server);
                                            }
                                            break;
                                        default:
                                            server.SetUnselectable(UnselectableFlags.ServerType);
                                            break;
                                    }
                                }
                                else
                                {
                                    servers[i].SetUnselectable(UnselectableFlags.DidNotRespond);
                                    LogLocked(log, "{0} Endpoint: returned, but incorrectly", Format.ToString(server));
                                }
                            }
                            else
                            {
                                servers[i].SetUnselectable(UnselectableFlags.DidNotRespond);
                                LogLocked(log, "{0} Endpoint: did not respond", Format.ToString(server));
                            }
                        }

                        if (encounteredConnectedClusterServer)
                        {
                            endpoints = updatedClusterEndpointCollection;
                        }
                        else
                        {
                            break; // we do not want to repeat the second iteration
                        }
                    }

                    healthy = standaloneCount != 0 || clusterCount != 0 || sentinelCount != 0;
                    if (healthy)
                    {
                        if (clusterCount == 0)
                        {
                            // set the serverSelectionStrategy
                            if (RawConfig.Proxy == Proxy.Twemproxy)
                            {
                                ServerSelectionStrategy.ServerType = ServerType.Twemproxy;
                            }
                            else if (standaloneCount == 0 && sentinelCount > 0)
                            {
                                ServerSelectionStrategy.ServerType = ServerType.Sentinel;
                            }
                            else if (standaloneCount > 0)
                            {
                                ServerSelectionStrategy.ServerType = ServerType.Standalone;
                            }
                            var preferred = NominatePreferredMaster(log, servers, useTieBreakers, tieBreakers, masters);
                            foreach (var master in masters)
                            {
                                if (master == preferred)
                                {
                                    LogLocked(log, $"{Format.ToString(master)}: Clearing as RedundantMaster");
                                    master.ClearUnselectable(UnselectableFlags.RedundantMaster);
                                }
                                else
                                {
                                    LogLocked(log, $"{Format.ToString(master)}: Setting as RedundantMaster");
                                    master.SetUnselectable(UnselectableFlags.RedundantMaster);
                                }
                            }
                        }
                        else
                        {
                            ServerSelectionStrategy.ServerType = ServerType.Cluster;
                            long coveredSlots = ServerSelectionStrategy.CountCoveredSlots();
                            LogLocked(log, "Cluster: {0} of {1} slots covered", coveredSlots, serverSelectionStrategy.TotalSlots);
                        }
                    }

                    if (!first)
                    {
                        long subscriptionChanges = ValidateSubscriptions();
                        if (subscriptionChanges == 0)
                        {
                            LogLocked(log, "No subscription changes necessary");
                        }
                        else
                        {
                            LogLocked(log, "Subscriptions reconfigured: {0}", subscriptionChanges);
                        }
                    }
                    if (showStats)
                    {
                        GetStatus(log);
                    }

                    string stormLog = GetStormLog();
                    if (!string.IsNullOrWhiteSpace(stormLog))
                    {
                        LogLocked(log, stormLog);
                    }

                    if (first && !healthy && attemptsLeft > 0)
                    {
                        LogLocked(log, "resetting failing connections to retry...");
                        ResetAllNonConnected();
                        LogLocked(log, "retrying; attempts left: " + attemptsLeft + "...");
                    }
                    //WTF("?: " + attempts);
                } while (first && !healthy && attemptsLeft > 0);

                if(first && RawConfig.AbortOnConnectFail && !healthy)
                {
                    return false;
                }
                if (first && RawConfig.HeartbeatInterval > 0)
                {
                    LogLocked(log, "Starting heartbeat...");
                    pulse = new Timer(heartbeat, this, RawConfig.HeartbeatInterval, RawConfig.HeartbeatInterval);
                }
                if(publishReconfigure)
                {
                    try
                    {
                        LogLocked(log, "Broadcasting reconfigure...");
                        PublishReconfigureImpl(publishReconfigureFlags);
                    }
                    catch (Exception ex) when (!(ex is OutOfMemoryException))
                    {
                        TraceExceptionWithoutContext(ex);
                    }
                }
                return true;

            } catch (Exception ex) when (!(ex is OutOfMemoryException))
            {
                TraceExceptionWithoutContext(ex);
                throw;
            }
            finally
            {
                Trace("Exiting reconfiguration...");
                OnTraceLog(log);
                if (ranThisCall) Interlocked.Exchange(ref activeConfigCause, null);
                if (!first) OnConfigurationChanged(blame);
                Trace("Reconfiguration exited");
            }
        }

        private EndPointCollection GetEndpointsFromClusterNodes(ServerEndPoint server, Action<string> log)
        {
            var message = Message.Create(-1, CommandFlags.None, RedisCommand.CLUSTER, RedisLiterals.NODES);
            try
            {
                var clusterConfig = ExecuteSyncImpl(message, ResultProcessor.ClusterNodes, server);
                return new EndPointCollection(clusterConfig.Nodes.Select(node => node.EndPoint).ToList());
            }
            catch (Exception ex) when (!(ex is OutOfMemoryException))
            {
                LogLocked(log, "Encountered error while updating cluster config: " + ex.Message);
                return null;
            }
        }


        private void ResetAllNonConnected()
        {
            var snapshot = serverSnapshot;
            foreach(var server in snapshot)
            {
                server.ResetNonConnected();
            }
        }

        partial void OnTraceLog(Action<string> log, [System.Runtime.CompilerServices.CallerMemberName] string caller = null);
        private ServerEndPoint NominatePreferredMaster(Action<string> log, ServerEndPoint[] servers, bool useTieBreakers, Tuple<ResultBox<string>, ManualResetEvent>[] tieBreakers, List<ServerEndPoint> masters)
        {
            Dictionary<string, int> uniques = null;
            if (useTieBreakers)
            {   // count the votes
                uniques = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                WaitAllIgnoreErrors("tiebreaker", tieBreakers, 50, log);
                for (int i = 0; i < tieBreakers.Length; i++)
                {
                    ResultBox<string>.UnwrapAndRecycle(tieBreakers[i].Item1, false, out var result, out var exception);
                    var ep = servers[i].EndPoint;
                    if (exception != null)
                    {
                        LogLocked(log, "{0} failed to nominate (faulted)", Format.ToString(ep));
                        if (exception.Message.StartsWith("MOVED ") || exception.Message.StartsWith("ASK ")) continue;
                        LogLocked(log, "> {0}", exception.Message);
                    }
                    else if (tieBreakers[i].Item2.WaitOne(TimeSpan.Zero))
                    {
                        string s = result;
                        if (string.IsNullOrWhiteSpace(s))
                        {
                            LogLocked(log, "Election: {0} had no tiebreaker set", Format.ToString(ep));
                        }
                        else
                        {
                            LogLocked(log, "Election: {0} nominates: {1}", Format.ToString(ep), s);
                            int count;
                            if (!uniques.TryGetValue(s, out count)) count = 0;
                            uniques[s] = count + 1;
                        }
                    }
                    else
                    {
                        LogLocked(log, "Election: {0} failed to nominate (not completed)", Format.ToString(ep));
                    }
                }
            }


            switch (masters.Count)
            {
                case 0:
                    LogLocked(log, "No masters detected");
                    return null;
                case 1:
                    LogLocked(log, "Single master detected: " + Format.ToString(masters[0].EndPoint));
                    return masters[0];
                default:
                    LogLocked(log, "Multiple masters detected...");
                    if (useTieBreakers && uniques != null)
                    {
                        switch (uniques.Count)
                        {
                            case 0:
                                LogLocked(log, "nobody nominated a tie-breaker");
                                break;
                            case 1:
                                string unanimous = uniques.Keys.Single();
                                LogLocked(log, "tie-break is unanimous at {0}", unanimous);
                                var found = SelectServerByElection(servers, unanimous, log);
                                if (found != null)
                                {
                                    LogLocked(log, "Elected: {0}", Format.ToString(found.EndPoint));
                                    return found;
                                }
                                break;
                            default:
                                LogLocked(log, "tie-break is contested:");
                                ServerEndPoint highest = null;
                                bool arbitrary = false;
                                foreach (var pair in uniques.OrderByDescending(x => x.Value))
                                {
                                    LogLocked(log, "{0} has {1} votes", pair.Key, pair.Value);
                                    if (highest == null)
                                    {
                                        highest = SelectServerByElection(servers, pair.Key, log);
                                        if (highest != null)
                                        {
                                            // any more with this vote? if so: arbitrary
                                            arbitrary = uniques.Where(x => x.Value == pair.Value).Skip(1).Any();
                                        }
                                    }
                                }
                                if (highest != null)
                                {
                                    if (arbitrary)
                                    {
                                        LogLocked(log, "Choosing master arbitrarily: {0}", Format.ToString(highest.EndPoint));
                                    }
                                    else
                                    {
                                        LogLocked(log, "Elected: {0}", Format.ToString(highest.EndPoint));
                                    }
                                    return highest;
                                }
                                break;

                        }

                    }
                    break;
            }

            LogLocked(log, "Choosing master arbitrarily: {0}", Format.ToString(masters[0].EndPoint));
            return masters[0];

        }

        private ServerEndPoint SelectServerByElection(ServerEndPoint[] servers, string endpoint, Action<string> log)
        {
            if (servers == null || string.IsNullOrWhiteSpace(endpoint)) return null;
            for (int i = 0; i < servers.Length; i++)
            {
                if (string.Equals(Format.ToString(servers[i].EndPoint), endpoint, StringComparison.OrdinalIgnoreCase))
                    return servers[i];
            }
            LogLocked(log, "...but we couldn't find that");
            var deDottedEndpoint = DeDotifyHost(endpoint);
            for (int i = 0; i < servers.Length; i++)
            {
                if (string.Equals(DeDotifyHost(Format.ToString(servers[i].EndPoint)), deDottedEndpoint, StringComparison.OrdinalIgnoreCase))
                {
                    LogLocked(log, "...but we did find instead: {0}", deDottedEndpoint);
                    return servers[i];
                }
            }
            return null;
        }

        static string DeDotifyHost(string input)
        {
            if (string.IsNullOrWhiteSpace(input)) return input; // GIGO

            if (!char.IsLetter(input[0])) return input; // need first char to be alpha for this to work

            int periodPosition = input.IndexOf('.');
            if (periodPosition <= 0) return input; // no period or starts with a period? nothing useful to split

            int colonPosition = input.IndexOf(':');
            if (colonPosition > 0)
            { // has a port specifier
                return input.Substring(0, periodPosition) + input.Substring(colonPosition);
            }
            else
            {
                return input.Substring(0, periodPosition);
            }
        }

        internal void UpdateClusterRange(ClusterConfiguration configuration)
        {
            if (configuration == null) return;
            foreach (var node in configuration.Nodes)
            {
                if (node.IsSlave || node.Slots.Count == 0) continue;
                foreach (var slot in node.Slots)
                {
                    var server = GetServerEndPoint(node.EndPoint);
                    if (server != null) serverSelectionStrategy.UpdateClusterRange(slot.From, slot.To, server);
                }
            }
        }

        private Timer pulse;

        private readonly ServerSelectionStrategy serverSelectionStrategy;

        internal ServerEndPoint[] GetServerSnapshot()
        {
            var tmp = serverSnapshot;
            return tmp;
        }

        internal ServerEndPoint SelectServer(Message message)
        {
            if (message == null) return null;
            return serverSelectionStrategy.Select(message);
        }

        internal ServerEndPoint SelectServer(int db, RedisCommand command, CommandFlags flags, RedisKey key)
        {
            return serverSelectionStrategy.Select(db, command, key, flags);
        }
        private bool TryPushMessageToBridge<T>(Message message, ResultProcessor<T> processor, ResultBox<T> resultBox, ref ServerEndPoint server)
        {
            message.SetSource(processor, resultBox);

            if (server == null)
            {   // infer a server automatically
                server = SelectServer(message);
            }
            else // a server was specified; do we trust their choice, though?
            {

                if (message.IsMasterOnly() && server.IsSlave)
                {
                    throw ExceptionFactory.MasterOnly(IncludeDetailInExceptions, message.Command, message, server);
                }

                switch(server.ServerType)
                {
                    case ServerType.Cluster:
                    case ServerType.Twemproxy: // strictly speaking twemproxy uses a different hashing algo, but the hash-tag behavior is
                                               // the same, so this does a pretty good job of spotting illegal commands before sending them
                        if (message.GetHashSlot(ServerSelectionStrategy) == ServerSelectionStrategy.MultipleSlots)
                        {
                            throw ExceptionFactory.MultiSlot(IncludeDetailInExceptions, message);
                        }
                        break;
                }
                if (!server.IsConnected)
                {
                    // well, that's no use!
                    server = null;
                }
            }
            
            if (server != null)
            {
                var profCtx = profiler?.GetContext();
                if (profCtx != null)
                {
                    ConcurrentProfileStorageCollection inFlightForCtx;
                    if (profiledCommands.TryGetValue(profCtx, out inFlightForCtx))
                    {
                        message.SetProfileStorage(ProfileStorage.NewWithContext(inFlightForCtx, server));
                    }
                }

                if (message.Db >= 0)
                {
                    int availableDatabases = server.Databases;
                    if (availableDatabases > 0 && message.Db >= availableDatabases) throw ExceptionFactory.DatabaseOutfRange(
                        IncludeDetailInExceptions, message.Db, message, server);
                }

                Trace("Queueing on server: " + message);
                if (server.TryEnqueue(message)) return true;
            }
            Trace("No server or server unavailable - aborting: " + message);
            return false;
        }


        /// <summary>
        /// See Object.ToString()
        /// </summary>
        public override string ToString()
        {
            string s = ClientName;
            if (string.IsNullOrWhiteSpace(s)) s = GetType().Name;
            return s;
        }

        internal readonly byte[] ConfigurationChangedChannel; // this gets accessed for every received event; let's make sure we can process it "raw"
        internal readonly byte[] UniqueId = Guid.NewGuid().ToByteArray(); // unique identifier used when tracing


        /// <summary>
        /// Gets or sets whether asynchronous operations should be invoked in a way that guarantees their original delivery order
        /// </summary>
        public bool PreserveAsyncOrder { get; set; }

        /// <summary>
        /// Indicates whether any servers are connected
        /// </summary>
        public bool IsConnected
        {
            get
            {
                var tmp = serverSnapshot;
                for (int i = 0; i < tmp.Length; i++)
                    if (tmp[i].IsConnected) return true;
                return false;
            }
        }

        internal ConfigurationOptions RawConfig { get; }

        internal ServerSelectionStrategy ServerSelectionStrategy => serverSelectionStrategy;


        /// <summary>
        /// Close all connections and release all resources associated with this object
        /// </summary>
        public void Close(bool allowCommandsToComplete = true)
        {
            isDisposed = true;
            using (var tmp = pulse)
            {
                pulse = null;
            }

            try
            {
                if (allowCommandsToComplete)
                {
                    QuitAllServers();
                }
            }
            catch
            {
                // We are ignoring possible exceptions thrown from the QuitAllServers,
                // since that method can thrown an exception when closing connection
                // while physical bridge wasn't established yet. Otherwise exception
                // will prevent sockets from being closed.
            }

            DisposeAndClearServers();
            OnCloseReaderWriter();
        }
        partial void OnCloseReaderWriter();

        private void DisposeAndClearServers()
        {
            lock (servers)
            {
                var iter = servers.GetEnumerator();
                while (iter.MoveNext())
                {
                    var server = (ServerEndPoint)iter.Value;
                    server.Dispose();
                }
                servers.Clear();
            }
        }

        private bool QuitAllServers()
        {
            var waitHandles = new List<WaitHandle>(servers.Count);
            try
            {
                lock (servers)
                {
                    var iter = servers.GetEnumerator();
                    while (iter.MoveNext())
                    {
                        var server = (ServerEndPoint)iter.Value;
                        var message = server.Close(out var bridge);

                        if (message != null)
                        {
                            var quitEvent = new ManualResetEvent(false);
                            var source = ResultBox<bool>.Get(quitEvent);
                            message.SetSource(ResultProcessor.DemandOK, source);
                            if (!server.TryQueueDirect(message, bridge: bridge))
                            {
                                quitEvent.Set();
                            }

                            waitHandles.Add(quitEvent);
                        }
                    }
                }

                return WaitHandle.WaitAll(waitHandles.ToArray(), timeoutMilliseconds);
            }
            finally
            {
                foreach (var waitHandle in waitHandles)
                {
                    waitHandle.Dispose();
                }
            }
        }

        private Task[] QuitAllServersAsync()
        {
            Task[] quits = new Task[servers.Count];
            lock (servers)
            {
                var iter = servers.GetEnumerator();
                int index = 0;
                while (iter.MoveNext())
                {
                    var server = (ServerEndPoint)iter.Value;
                    var message = server.Close(out var bridge);

                    quits[index++] = message != null
                        ? server.QueueDirectAsync(message, ResultProcessor.DemandOK, bridge: bridge)
                        : CompletedTask<bool>.Default(null);
                }
            }
            return quits;
        }

        /// <summary>
        /// Close all connections and release all resources associated with this object
        /// </summary>
        public async Task CloseAsync(bool allowCommandsToComplete = true)
        {
            isDisposed = true;
            using (var tmp = pulse)
            {
                pulse = null;
            }

            if (allowCommandsToComplete)
            {
                var quits = QuitAllServersAsync();
                await WaitAllIgnoreErrorsAsync(quits, RawConfig.SyncTimeout, null).ForAwait();
            }

            DisposeAndClearServers();
        }

        /// <summary>
        /// Release all resources associated with this object
        /// </summary>
        public void Dispose()
        {
            Close(!isDisposed);
            sentinelConnection?.Dispose();
            var oldTimer = Interlocked.Exchange(ref sentinelPrimaryReconnectTimer, null);
            oldTimer?.Dispose();
        }


        internal Task<T> ExecuteAsyncImpl<T>(Message message, ResultProcessor<T> processor, object state, ServerEndPoint server)
        {
            if (isDisposed) throw new ObjectDisposedException(ToString());

            if (message == null)
            {
                return CompletedTask<T>.Default(state);
            }
            
            if (message.IsFireAndForget)
            {
                TryPushMessageToBridge(message, processor, null, ref server);
                return CompletedTask<T>.Default(null); // F+F explicitly does not get async-state
            }
            else
            {
                var tcs = TaskSource.CreateDenyExecSync<T>(state);
                var source = ResultBox<T>.Get(tcs);
                if (!TryPushMessageToBridge(message, processor, source, ref server))
                {
                    ThrowFailed(tcs, ExceptionFactory.NoConnectionAvailable(IncludeDetailInExceptions, IncludePerformanceCountersInExceptions, message.Command, message, server, GetServerSnapshot()));
                }
                return tcs.Task;
            }
        }

        internal static void ThrowFailed<T>(TaskCompletionSource<T> source, Exception unthrownException)
        {
            try
            {
                throw unthrownException;
            } catch (Exception ex) when (!(ex is OutOfMemoryException))
            {
                source.TrySetException(ex);
                GC.KeepAlive(source.Task.Exception);
                GC.SuppressFinalize(source.Task);
            }
        }
        internal T ExecuteSyncImpl<T>(Message message, ResultProcessor<T> processor, ServerEndPoint server)
        {
            if (isDisposed) throw new ObjectDisposedException(ToString());

            if (message == null) // fire-and forget could involve a no-op, represented by null - for example Increment by 0
            {
                return default(T);
            }

            var manager = this.socketManager;

            if (message.IsFireAndForget)
            {
                TryPushMessageToBridge(message, processor, null, ref server);
                Interlocked.Increment(ref fireAndForgets);
                return default(T);
            }
            else
            {
                using (var completedEvent = new ManualResetEventSlim(false))
                {
                    var source = ResultBox<T>.Get(completedEvent);

                    if (!TryPushMessageToBridge(message, processor, source, ref server))
                    {
                        throw ExceptionFactory.NoConnectionAvailable(IncludeDetailInExceptions, IncludePerformanceCountersInExceptions, message.Command, message, server, GetServerSnapshot());
                    }

                    if (completedEvent.Wait(timeoutMilliseconds))
                    {
                        Trace("Timeley response to " + message.ToString());
                    }
                    else
                    {
                        message.sendingCanceled = true;
                        ThrowTimeoutException(message, server);
                        // very important not to return "source" to the pool here
                    }

                    // snapshot these so that we can recycle the box
                    Exception ex;
                    T val;
                    ResultBox<T>.UnwrapAndRecycle(source, true, out val, out ex); // now that we aren't locking it...
                    if (ex != null) throw ex;
                    Trace(message + " received " + val);
                    return val;
                }
            }
        }

        internal void ThrowTimeoutException(Message message, ServerEndPoint server)
        {
            Trace("Timeout performing " + message.ToString());
            Interlocked.Increment(ref syncTimeouts);
            string errMessage;
            List<Tuple<string, string>> data = null;
            if (server == null || !IncludeDetailInExceptions)
            {
                errMessage = "Timeout performing " + message.Command.ToString();
            }
            else
            {
                int inst, qu, qs, qc, wr, wq, @in, ar;

                var sb = new StringBuilder("Timeout performing ").Append(message.CommandAndKey);
                data = new List<Tuple<string, string>> {Tuple.Create("Message", message.CommandAndKey)};
                Action<string, string, string> add = (lk, sk, v) =>
                {
                    data.Add(Tuple.Create(lk, v));
                    sb.Append(", " + sk + ": " + v);
                };

                int queue = server.GetOutstandingCount(message.Command, out inst, out qu, out qs, out qc, out wr, out wq,
                    out @in, out ar);
                add("Instantaneous", "inst", inst.ToString());

                add("Queue-Length", "queue", queue.ToString());
                add("Queue-Outstanding", "qu", qu.ToString());
                add("Queue-Awaiting-Response", "qs", qs.ToString());
                add("Queue-Completion-Outstanding", "qc", qc.ToString());
                add("Active-Writers", "wr", wr.ToString());
                add("Write-Queue", "wq", wq.ToString());
                add("Inbound-Bytes", "in", @in.ToString());
                add("Active-Readers", "ar", ar.ToString());

                add("Client-Name", "clientName", ClientName);
                add("Server-Endpoint", "serverEndpoint", server.EndPoint.ToString());
                var hashSlot = message.GetHashSlot(this.ServerSelectionStrategy);
                // only add keyslot if its a valid cluster key slot
                if (hashSlot != ServerSelectionStrategy.NoSlot)
                {
                    add("Key-HashSlot", "keyHashSlot", message.GetHashSlot(this.ServerSelectionStrategy).ToString());
                }
#if !CORE_CLR
                string iocp, worker;
                int busyWorkerCount = GetThreadPoolStats(out iocp, out worker);
                add("ThreadPool-IO-Completion", "IOCP", iocp);
                add("ThreadPool-Workers", "WORKER", worker);
                data.Add(Tuple.Create("Busy-Workers", busyWorkerCount.ToString()));

                if (IncludePerformanceCountersInExceptions)
                {
                    add("Local-CPU", "Local-CPU", GetSystemCpuPercent());
                }
#endif
                errMessage = sb.ToString();
                if (stormLogThreshold >= 0 && queue >= stormLogThreshold &&
                    Interlocked.CompareExchange(ref haveStormLog, 1, 0) == 0)
                {
                    var log = server.GetStormLog(message.Command);
                    if (string.IsNullOrWhiteSpace(log)) Interlocked.Exchange(ref haveStormLog, 0);
                    else Interlocked.Exchange(ref stormLogSnapshot, log);
                }
            }

            var timeoutEx = ExceptionFactory.Timeout(IncludeDetailInExceptions, errMessage, message, server);
            timeoutEx.HelpLink = timeoutHelpLink;

            if (data != null)
            {
                foreach (var kv in data)
                {
                    timeoutEx.Data["Redis-" + kv.Item1] = kv.Item2;
                }
            }

            throw timeoutEx;
        }

#if !CORE_CLR
        internal static string GetThreadPoolAndCPUSummary(bool includePerformanceCounters)
        {
            string iocp, worker;
            GetThreadPoolStats(out iocp, out worker);
            var cpu = includePerformanceCounters ? GetSystemCpuPercent() : "n/a";
            return $"IOCP: {iocp}, WORKER: {worker}, Local-CPU: {cpu}";
        }

        private static string GetSystemCpuPercent()
        {
#if FEATURE_PERFCOUNTERS
            float systemCPU;
            if (PerfCounterHelper.TryGetSystemCPU(out systemCPU))
            {
                return Math.Round(systemCPU, 2) + "%";
            }
#endif
            return "unavailable"; 
        }

        private static int GetThreadPoolStats(out string iocp, out string worker)
        {
            //BusyThreads =  TP.GetMaxThreads() –TP.GetAVailable();
            //If BusyThreads >= TP.GetMinThreads(), then threadpool growth throttling is possible.

            int maxIoThreads, maxWorkerThreads;
            ThreadPool.GetMaxThreads(out maxWorkerThreads, out maxIoThreads);

            int freeIoThreads, freeWorkerThreads;
            ThreadPool.GetAvailableThreads(out freeWorkerThreads, out freeIoThreads);

            int minIoThreads, minWorkerThreads;
            ThreadPool.GetMinThreads(out minWorkerThreads, out minIoThreads);

            int busyIoThreads = maxIoThreads - freeIoThreads;
            int busyWorkerThreads = maxWorkerThreads - freeWorkerThreads;

            iocp = $"(Busy={busyIoThreads},Free={freeIoThreads},Min={minIoThreads},Max={maxIoThreads})";
            worker = $"(Busy={busyWorkerThreads},Free={freeWorkerThreads},Min={minWorkerThreads},Max={maxWorkerThreads})";
            return busyWorkerThreads;
        }
#endif

            /// <summary>
            /// Should exceptions include identifiable details? (key names, additional .Data annotations)
            /// </summary>
        public bool IncludeDetailInExceptions { get; set; }

        /// <summary>
        /// Should exceptions include performance counter details? (CPU usage, etc - note that this can be problematic on some platforms)
        /// </summary>
        public bool IncludePerformanceCountersInExceptions { get; set; }

        int haveStormLog = 0, stormLogThreshold = 15;
        string stormLogSnapshot;
        /// <summary>
        /// Limit at which to start recording unusual busy patterns (only one log will be retained at a time;
        /// set to a negative value to disable this feature)
        /// </summary>
        public int StormLogThreshold { get { return stormLogThreshold; } set { stormLogThreshold = value; } }
        /// <summary>
        /// Obtains the log of unusual busy patterns
        /// </summary>
        public string GetStormLog()
        {
            var result = Interlocked.CompareExchange(ref stormLogSnapshot, null, null);
            return result;
        }
        /// <summary>
        /// Resets the log of unusual busy patterns
        /// </summary>
        public void ResetStormLog()
        {
            Interlocked.Exchange(ref stormLogSnapshot, null);
            Interlocked.Exchange(ref haveStormLog, 0);
        }
        private long syncTimeouts, fireAndForgets;

        /// <summary>
        /// Request all compatible clients to reconfigure or reconnect
        /// </summary>
        /// <returns>The number of instances known to have received the message (however, the actual number can be higher; returns -1 if the operation is pending)</returns>
        public long PublishReconfigure(CommandFlags flags = CommandFlags.None)
        {
            byte[] channel = ConfigurationChangedChannel;
            if (channel == null) return 0;
            if (ReconfigureIfNeeded(null, false, "PublishReconfigure", true, flags))
            {
                return -1;
            }
            else
            {
                return PublishReconfigureImpl(flags);
            }
        }
        private long PublishReconfigureImpl(CommandFlags flags)
        {
            byte[] channel = ConfigurationChangedChannel;
            if (channel == null) return 0;
            return GetSubscriber().Publish(channel, RedisLiterals.Wildcard, flags);
        }

        /// <summary>
        /// Request all compatible clients to reconfigure or reconnect
        /// </summary>
        /// <returns>The number of instances known to have received the message (however, the actual number can be higher)</returns>
        public Task<long> PublishReconfigureAsync(CommandFlags flags = CommandFlags.None)
        {
            byte[] channel = ConfigurationChangedChannel;
            if (channel == null) return CompletedTask<long>.Default(null);

            return GetSubscriber().PublishAsync(channel, RedisLiterals.Wildcard, flags);
        }
    }   
}
