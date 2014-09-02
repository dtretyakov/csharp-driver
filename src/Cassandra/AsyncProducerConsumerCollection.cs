using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cassandra
{
    public class AsyncProducerConsumerCollection<T> : IEnumerable<T>
    {
        private readonly ConcurrentQueue<T> _collection;
        private readonly Queue<TaskCompletionSource<T>> _waiting = new Queue<TaskCompletionSource<T>>();

        public int Count
        {
            get { return _collection.Count; }
        }

        public AsyncProducerConsumerCollection()
        {
            _collection = new ConcurrentQueue<T>();
        }

        public AsyncProducerConsumerCollection(IEnumerable<T> collection)
        {
            _collection = new ConcurrentQueue<T>(collection);
        }

        public IEnumerator<T> GetEnumerator()
        {
            return _collection.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Add(T item)
        {
            TaskCompletionSource<T> tcs = null;
            lock (_collection)
            {
                if (_waiting.Count > 0)
                {
                    tcs = _waiting.Dequeue();
                }
                else
                {
                    _collection.Enqueue(item);
                }
            }
            if (tcs != null)
            {
                tcs.TrySetResult(item);
            }
        }

        public Task<T> TakeAsync()
        {
            lock (_collection)
            {
                T item;
                if (_collection.TryDequeue(out item))
                {
                    return Task.FromResult(item);
                }
                var tcs = new TaskCompletionSource<T>();
                _waiting.Enqueue(tcs);
                return tcs.Task;
            }
        }

        public override string ToString()
        {
            return string.Format("Count = {0}", _collection.Count);
        }
    }
}