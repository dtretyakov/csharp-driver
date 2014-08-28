using System.Net;

namespace Cassandra
{
    public class TopologyChangeEventArgs : CassandraEventArgs
    {
        public enum Reason
        {
            NewNode,
            RemovedNode
        };

        public IPEndPoint Address;
        public Reason What;
    }
}