using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace StackExchange.Redis
{
    internal class InlineSynchronizationContext : SynchronizationContext, IDisposable
    {
        private readonly ConcurrentQueue<Tuple<SendOrPostCallback, object>> _queue = new ConcurrentQueue<Tuple<SendOrPostCallback, object>>();
        private readonly Semaphore _semaphore = new Semaphore(0, Int32.MaxValue);

        public WaitHandle WaitHandle => _semaphore;

        public Tuple<SendOrPostCallback, object> Dequeue()
        {
            _queue.TryDequeue(out var tuple);
            return tuple;
        }

        public void Dispose()
        {
            _semaphore.Dispose();
        }

        public override void Post(SendOrPostCallback callback, object state)
        {
            try
            {
                _queue.Enqueue(Tuple.Create(callback, state));
                _semaphore.Release();
            }
            catch (ObjectDisposedException)
            {
                base.Post(callback, state);
            }
        }

        public static bool InvokeAsync<T>(Func<Task<T>> asyncCallback, int timeoutMs, out Task<T> task)
        {
            if (asyncCallback == null) throw new ArgumentNullException(nameof(asyncCallback));
            var oldSyncContext = SynchronizationContext.Current;

            try
            {
                using (var syncContext = new InlineSynchronizationContext())
                {
                    SynchronizationContext.SetSynchronizationContext(syncContext);

                    task = asyncCallback();
                    var asyncResult = (IAsyncResult)task;

                    var waitHandles = new[] { syncContext.WaitHandle, asyncResult.AsyncWaitHandle };
                    var waitResult = 1;

                    while (!asyncResult.IsCompleted && (waitResult = WaitHandle.WaitAny(waitHandles, timeoutMs)) == 0)
                    {
                        var workItem = syncContext.Dequeue();
                        workItem.Item1(workItem.Item2);
                    }

                    return waitResult != WaitHandle.WaitTimeout;
                }
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(oldSyncContext);
            }
        }
    }
}