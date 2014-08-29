using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cassandra
{
    public class AsyncProducerConsumerCollection<T> : IEnumerable<T>
    {
        private readonly Queue<T> _collection;
        private readonly Queue<TaskCompletionSource<T>> _waiting = new Queue<TaskCompletionSource<T>>();

        public int Count
        {
            get { return _collection.Count; }
        }

        public AsyncProducerConsumerCollection()
        {
            _collection = new Queue<T>();
        }

        public AsyncProducerConsumerCollection(IEnumerable<T> collection)
        {
            _collection = new Queue<T>(collection);
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
                if (_collection.Count > 0)
                {
                    return Task.FromResult(_collection.Dequeue());
                }
                var tcs = new TaskCompletionSource<T>();
                _waiting.Enqueue(tcs);
                return tcs.Task;
            }
        }
    }
}