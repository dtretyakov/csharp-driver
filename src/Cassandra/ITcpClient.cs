using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Cassandra
{
    internal interface ITcpClient : IDisposable
    {
        IPEndPoint EndPoint { get; }
        SocketOptions Options { get; }
        int ReceiveBufferSize { get; }

        /// <summary>
        ///     Event that is fired when the host is closing the connection.
        /// </summary>
        event Action Disconnected;

        /// <summary>
        ///     Connects synchronously to the host and starts reading
        /// </summary>
        /// <exception cref="SocketException">Throws a SocketException when the connection could not be established with the host</exception>
        Task ConnectAsync();

        /// <summary>
        ///     Begins an asynchronous request to receive data from a connected Socket object.
        ///     It handles the exceptions in case there is one.
        /// </summary>
        Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        ///     Sends data asynchronously
        /// </summary>
        Task WriteAsync(Stream stream, CancellationToken cancellationToken = default(CancellationToken));
    }
}