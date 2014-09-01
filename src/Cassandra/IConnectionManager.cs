using System.Threading.Tasks;

namespace Cassandra
{
    internal interface IConnectionManager
    {
        int MaxConcurrentRequests { get; }

        Task<short> GetStreamId();

        void AddStreamId(short streamId);
    }
}