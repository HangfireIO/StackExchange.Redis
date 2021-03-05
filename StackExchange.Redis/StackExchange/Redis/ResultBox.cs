using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace StackExchange.Redis
{
    abstract partial class ResultBox
    {
        protected Exception exception;
        protected object stateOrCompletionSource;

        public void SetState(object state)
        {
            stateOrCompletionSource = state;
        }

        public void SetException(Exception exception)
        {
            this.exception = exception;
            //try
            //{
            //    throw exception;
            //}
            //catch (Exception caught)
            //{ // stacktrace etc
            //    this.exception = caught;
            //}
        }

        public abstract bool TryComplete(bool isAsync);
    }
    sealed class ResultBox<T> : ResultBox
    {
        private T value;

        public ResultBox(object stateOrCompletionSource)
        {
            this.stateOrCompletionSource = stateOrCompletionSource;
        }

        public static ResultBox<T> Get(object stateOrCompletionSource)
        {
            return new ResultBox<T>(stateOrCompletionSource);
        }

        public static void UnwrapAndRecycle(ResultBox<T> box, bool recycle, out T value, out Exception exception)
        {
            if (box == null)
            {
                value = default(T);
                exception = null;
            }
            else
            {
                value = box.value;
                exception = box.exception;
                box.value = default(T);
                box.exception = null;
                box.stateOrCompletionSource = null;
            }
        }

        public void SetResult(T value)
        {
            this.value = value;
        }

        public override bool TryComplete(bool isAsync)
        {
            if (stateOrCompletionSource is TaskCompletionSource<T>)
            {
                var tcs = (TaskCompletionSource<T>)stateOrCompletionSource;
#if !PLAT_SAFE_CONTINUATIONS // we don't need to check in this scenario
                if (isAsync || TaskSource.IsSyncSafe(tcs.Task))
#endif
                {
                    T val;
                    Exception ex;
                    UnwrapAndRecycle(this, true, out val, out ex);

                    if (ex == null) tcs.TrySetResult(val);
                    else
                    {
                        if (ex is TaskCanceledException) tcs.TrySetCanceled();
                        else tcs.TrySetException(ex);
                        // mark it as observed
                        GC.KeepAlive(tcs.Task.Exception);
                        GC.SuppressFinalize(tcs.Task);
                    }
                    return true;
                }
#if !PLAT_SAFE_CONTINUATIONS
                else
                { // looks like continuations; push to async to preserve the reader thread
                    return false;
                }
#endif
            }
            else if (stateOrCompletionSource is ManualResetEventSlim resetEvent)
            {
                try
                {
                    resetEvent.Set();
                }
                catch (ObjectDisposedException)
                {
                }

                ConnectionMultiplexer.TraceWithoutContext("Pulsed", "Result");
                return true;
            }
            else if (stateOrCompletionSource is ManualResetEvent waitHandle)
            {
                try
                {
                    waitHandle.Set();
                }
                catch (ObjectDisposedException)
                {
                }

                ConnectionMultiplexer.TraceWithoutContext("Pulsed", "Result");
                return true;
            }
            else
            {
                ConnectionMultiplexer.TraceWithoutContext("Completed", "result");
                return true;
            }
        }
    }

}
