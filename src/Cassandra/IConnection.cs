using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;

namespace Cassandra
{
    internal interface IConnection : IDisposable
    {
        IFrameCompressor Compressor { get; }
        IPEndPoint Address { get; }

        /// <summary>
        ///     Determines the amount of operations that are not finished.
        /// </summary>
        int InFlight { get; }

        /// <summary>
        ///     Determine if the Connection is closed
        /// </summary>
        bool IsClosed { get; }

        /// <summary>
        ///     Determine if the Connection has been explicitly disposed
        /// </summary>
        bool IsDisposed { get; }

        /// <summary>
        ///     Gets or sets the keyspace.
        ///     When setting the keyspace, it will issue a Query Request and wait to complete.
        /// </summary>
        string Keyspace { get; set; }

        ProtocolOptions Options { get; }
        byte ProtocolVersion { get; }
        Configuration Configuration { get; }

        /// <summary>
        ///     The event that represents a event RESPONSE from a Cassandra node
        /// </summary>
        event CassandraEventHandler CassandraEventResponse;

        Task ConnectAsync();
        
        /// <summary>
        ///     Sends a new request if possible. If it is not possible it queues it up.
        /// </summary>
        Task<AbstractResponse> SendAsync(IRequest request);

        /// <summary>
        /// Retrieves a list of pending operations.
        /// </summary>
        /// <returns>Pending tasks.</returns>
        IEnumerable<Task> GetPending();
    }
}