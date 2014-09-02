﻿//
//      Copyright (C) 2012-2014 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

﻿using System;
using System.Reflection;
using System.Threading.Tasks;

namespace Cassandra
{
    internal static class TaskHelper
    {
        private static readonly MethodInfo PreserveStackMethod;
        private static readonly Action<Exception> PreserveStackHandler = (ex) => { };

        static TaskHelper()
        {
            try
            {
                PreserveStackMethod = typeof(Exception).GetMethod("InternalPreserveStackTrace", BindingFlags.Instance | BindingFlags.NonPublic);
                if (PreserveStackMethod == null)
                {
                    return;
                }
                //Only under .NET Framework
                PreserveStackHandler = (ex) =>
                {
                    try
                    {
                        //This could result in a MemberAccessException
                        PreserveStackMethod.Invoke(ex, null);
                    }
                    catch
                    {
                        //Tried to preserve the stack trace, failed.
                        //Move on on.
                    }
                };
            }
            catch
            {
                //Do nothing
                //Do not throw exceptions on static constructors
            }
        }

        /// <summary>
        /// Returns an AsyncResult according to the .net async programming model (Begin)
        /// </summary>
        public static Task<TResult> ToApm<TResult>(this Task<TResult> task, AsyncCallback callback, object state)
        {
            if (task.AsyncState == state)
            {
                if (callback != null)
                {
                    task.ContinueWith((t) => callback(t), TaskContinuationOptions.ExecuteSynchronously);
                }
                return task;
            }

            var tcs = new TaskCompletionSource<TResult>(state);
            task.ContinueWith(delegate
            {
                if (task.IsFaulted)
                {
                    tcs.TrySetException(task.Exception.InnerExceptions);
                }
                else if (task.IsCanceled)
                {
                    tcs.TrySetCanceled();
                }
                else
                {
                    tcs.TrySetResult(task.Result);
                }

                if (callback != null)
                {
                    callback(tcs.Task);
                }

            }, TaskContinuationOptions.ExecuteSynchronously);
            return tcs.Task;
        }

        /// <summary>
        /// Returns a faulted task with the provided exception
        /// </summary>
        public static Task<TResult> FromException<TResult>(Exception exception)
        {
            var tcs = new TaskCompletionSource<TResult>();
            tcs.SetException(exception);
            return tcs.Task;
        }

        /// <summary>
        /// Waits the task to transition to RanToComplete.
        /// It throws the inner exception of the AggregateException in case there is a single exception.
        /// It throws the Aggregate exception when there is more than 1 inner exception.
        /// It throws a TimeoutException when the task didn't complete in the expected time.
        /// </summary>
        /// <param name="task">the task to wait upon</param>
        /// <param name="timeout">timeout in milliseconds</param>
        /// <exception cref="TimeoutException" />
        /// <exception cref="AggregateException" />
        public static T WaitToComplete<T>(Task<T> task, int timeout = System.Threading.Timeout.Infinite)
        {
            //It should wait and throw any exception
            try
            {
                task.Wait(timeout);
            }
            catch (AggregateException ex)
            {
                ex = ex.Flatten();
                //throw the actual exception when there was a single exception
                if (ex.InnerExceptions.Count == 1)
                {
                    throw PreserveStackTrace(ex.InnerExceptions[0]);
                }
                throw;
            }
            if (task.Status != TaskStatus.RanToCompletion)
            {
                throw new TimeoutException("The task didn't complete before timeout.");
            }
            return task.Result;
        }
        
        /// <summary>
        /// Waits the task to transition to RanToComplete.
        /// It throws the inner exception of the AggregateException in case there is a single exception.
        /// It throws the Aggregate exception when there is more than 1 inner exception.
        /// It throws a TimeoutException when the task didn't complete in the expected time.
        /// </summary>
        /// <param name="task">the task to wait upon</param>
        /// <param name="timeout">timeout in milliseconds</param>
        /// <exception cref="TimeoutException" />
        /// <exception cref="AggregateException" />
        public static void WaitToComplete(Task task, int timeout = System.Threading.Timeout.Infinite)
        {
            //It should wait and throw any exception
            try
            {
                task.Wait(timeout);
            }
            catch (AggregateException ex)
            {
                ex = ex.Flatten();
                //throw the actual exception when there was a single exception
                if (ex.InnerExceptions.Count == 1)
                {
                    throw PreserveStackTrace(ex.InnerExceptions[0]);
                }
                throw;
            }
            if (task.Status != TaskStatus.RanToCompletion)
            {
                throw new TimeoutException("The task didn't complete before timeout.");
            }
        }

        /// <summary>
        /// Waits the task to transition to RanToComplete.
        /// It throws the inner exception of the AggregateException in case there is a single exception.
        /// It throws the Aggregate exception when there is more than 1 inner exception.
        /// It throws a TimeoutException when the task didn't complete in the expected time.
        /// </summary>
        /// <param name="task">the task to wait upon</param>
        /// <param name="timeout">timeout</param>
        /// <exception cref="TimeoutException" />
        /// <exception cref="AggregateException" />
        public static T WaitToComplete<T>(Task<T> task, TimeSpan timeout)
        {
            return WaitToComplete(task, (int)timeout.TotalMilliseconds);
        }
        
        /// <summary>
        /// Waits the task to transition to RanToComplete.
        /// It throws the inner exception of the AggregateException in case there is a single exception.
        /// It throws the Aggregate exception when there is more than 1 inner exception.
        /// It throws a TimeoutException when the task didn't complete in the expected time.
        /// </summary>
        /// <param name="task">the task to wait upon</param>
        /// <param name="timeout">timeout</param>
        /// <exception cref="TimeoutException" />
        /// <exception cref="AggregateException" />
        public static void WaitToComplete(Task task, TimeSpan timeout)
        {
            WaitToComplete(task, (int)timeout.TotalMilliseconds);
        }

        /// <summary>
        /// Attempts to transition the underlying Task to RanToCompletion or Faulted state.
        /// </summary>
        public static void TrySet<T>(this TaskCompletionSource<T> tcs, Exception ex, T result)
        {
            if (ex != null)
            {
                tcs.TrySetException(ex);
            }
            else
            {
                tcs.TrySetResult(result);
            }
        }

        /// <summary>
        /// Required when retrowing exceptions to maintain the stack trace of the original exception
        /// </summary>
        internal static Exception PreserveStackTrace(Exception ex)
        {
            PreserveStackHandler(ex);
            return ex;
        }

        /// <summary>
        /// Throws an exception when task execution time exceeded delay.
        /// </summary>
        /// <param name="task">the task to wait upon</param>
        /// <param name="delay">delay interval</param>
        /// <param name="exception">exception to throw</param>
        /// <exception cref="TimeoutException" />
        /// <exception cref="AggregateException" />
        public static async Task SetTimeout(this Task task, TimeSpan delay, Func<Exception> exception)
        {
            var timedout = false;
            await Task.WhenAny(new[]
            {
                task,
                Task.Delay(delay).ContinueWith(p => { timedout = true; })
            }).ConfigureAwait(false);

            if (timedout)
            {
                throw exception();
            }
        }

        /// <summary>
        /// Throws an exception when task execution time exceeded delay.
        /// </summary>
        /// <param name="task">the task to wait upon</param>
        /// <param name="delay">delay interval</param>
        /// <param name="exception">exception to throw</param>
        /// <exception cref="TimeoutException" />
        /// <exception cref="AggregateException" />
        public static async Task<T> SetTimeout<T>(this Task<T> task, TimeSpan delay, Func<Exception> exception)
        {
            var timedout = false;
            await Task.WhenAny(new[]
            {
                task,
                Task.Delay(delay).ContinueWith(p => { timedout = true; })
            }).ConfigureAwait(false);

            if (timedout)
            {
                throw exception();
            }

            return task.Result;
        }
    }
}
