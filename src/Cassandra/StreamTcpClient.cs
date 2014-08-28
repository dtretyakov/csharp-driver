using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Cassandra
{
    /// <summary>
    ///     Represents a Tcp connection to a host.
    ///     It emits Read and WriteCompleted events when data is received.
    ///     Similar to Netty's Channel or Node.js's net.Socket
    ///     It handles TLS validation and encryption when required.
    /// </summary>
    internal class StreamTcpClient : ITcpClient
    {
        protected static readonly Logger Logger = new Logger(typeof (StreamTcpClient));
        private readonly TcpClient _tcpClient;
        private readonly Timer _idleTimer;

        /// <summary>
        /// Gets a socket stream.
        /// </summary>
        protected Stream SocketStream { get; set; }

        /// <summary>
        ///     Creates a new instance of TcpSocket using the endpoint and options provided.
        /// </summary>
        public StreamTcpClient(IPEndPoint endPoint, SocketOptions options)
        {
            EndPoint = endPoint;
            Options = options;

            _tcpClient = new TcpClient(endPoint)
            {
                SendTimeout = (int) Options.SendTimeout.TotalMilliseconds,
                ReceiveTimeout = (int) Options.ReceiveTimeout.TotalMilliseconds
            };

            if (Options.KeepAlive != null)
            {
                _tcpClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, Options.KeepAlive.Value);
            }
            if (Options.SoLinger != null)
            {
                _tcpClient.LingerState = new LingerOption(true, Options.SoLinger.Value);
            }
            if (Options.ReceiveBufferSize != null)
            {
                _tcpClient.ReceiveBufferSize = Options.ReceiveBufferSize.Value;
            }
            if (Options.SendBufferSize != null)
            {
                _tcpClient.SendBufferSize = Options.SendBufferSize.Value;
            }
            if (Options.TcpNoDelay != null)
            {
                _tcpClient.NoDelay = Options.TcpNoDelay.Value;
            }
            if (Options.IdleTimeout > TimeSpan.Zero)
            {
                _idleTimer = _idleTimer ?? new Timer(Disconnect);
            }
        }

        public IPEndPoint EndPoint { get; private set; }

        public SocketOptions Options { get; private set; }

        public int ReceiveBufferSize
        {
            get { return _tcpClient.ReceiveBufferSize; }
        }

        /// <summary>
        ///     Event that is fired when the host is closing the connection.
        /// </summary>
        public event Action Disconnected;

        /// <summary>
        ///     Connects synchronously to the host and starts reading
        /// </summary>
        /// <exception cref="SocketException">Throws a SocketException when the connection could not be established with the host</exception>
        public virtual async Task ConnectAsync()
        {
            await _tcpClient.ConnectAsync(EndPoint.Address, EndPoint.Port);
            Logger.Verbose(String.Format("Connection {0}: Socket connected, start reading using Stream interface.", EndPoint));
            SocketStream = _tcpClient.GetStream();
        }

        /// <summary>
        ///     Sends data asynchronously
        /// </summary>
        public Task WriteAsync(Stream stream)
        {
            ResetIdleTimer();
            return stream.CopyToAsync(SocketStream);
        }

        private void Disconnect(object state)
        {
            Logger.Verbose(String.Format("Connection {0}: idle timeout has been expired.", EndPoint));

            Action disconnected = Disconnected;
            if (disconnected != null)
            {
                disconnected();
            }
        }

        private void ResetIdleTimer()
        {
            Timer idleTimer = _idleTimer;
            if (Options.IdleTimeout > TimeSpan.Zero && idleTimer != null)
            {
                idleTimer.Change(Options.IdleTimeout, TimeSpan.FromMilliseconds(Timeout.Infinite));
                Logger.Verbose(String.Format("Connection {0}: idle timer has been resetted.", EndPoint));
            }
        }

        private void StopIdleTimer()
        {
            Timer idleTimer = _idleTimer;
            if (Options.IdleTimeout > TimeSpan.Zero && idleTimer != null)
            {
                idleTimer.Change(Timeout.Infinite, Timeout.Infinite);
                Logger.Verbose(String.Format("Connection {0}: idle timer has been stopped.", EndPoint));
            }
        }

        /// <summary>
        ///     Begins an asynchronous request to receive data from a connected Socket object.
        ///     It handles the exceptions in case there is one.
        /// </summary>
        public Task<int> ReadAsync(byte[] buffer, int offset, int count)
        {
            ResetIdleTimer();
            return SocketStream.ReadAsync(buffer, offset, count);
        }

        public void Dispose()
        {
            StopIdleTimer();

            try
            {
                if (_tcpClient == null)
                {
                    return;
                }

                //Try to close it.
                //Some operations could make the socket to dispose itself
                _tcpClient.Close();
            }
            catch
            {
                //We should not mind if the socket shutdown or close methods throw an exception
            }
        }
    }
}