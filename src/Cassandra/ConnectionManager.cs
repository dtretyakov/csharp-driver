using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Cassandra
{
    internal class ConnectionManager : IConnectionManager
    {
        private readonly AsyncProducerConsumerCollection<short> _availableStreams;

        public ConnectionManager(byte protocolVersion)
        {
            //Protocol 3 supports up to 32K concurrent request without waiting a response
            //Allowing larger amounts of concurrent requests will cause large memory consumption
            //Limit to 2K per connection sounds reasonable.
            MaxConcurrentRequests = protocolVersion < 3 ? 128 : 2048;

            IEnumerable<short> streamIds = Enumerable.Range(0, MaxConcurrentRequests).Select(s => (short)s);
            _availableStreams = new AsyncProducerConsumerCollection<short>(streamIds);
        }

        public int MaxConcurrentRequests { get; private set; }

        public Task<short> GetStreamId()
        {
            return _availableStreams.TakeAsync();
        }

        public void AddStreamId(short streamId)
        {
            _availableStreams.Add(streamId);
        }
    }
}