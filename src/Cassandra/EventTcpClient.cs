using System;
using System.IO;
using System.Net;
using System.Net.Security;
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
    internal class EventTcpClient : ITcpClient
    {
        private static readonly Logger Logger = new Logger(typeof (EventTcpClient));
        private Timer _idleTimer;
        private volatile bool _isClosing;
        private byte[] _receiveBuffer;
        private SocketAsyncEventArgs _receiveSocketEvent;
        private SocketAsyncEventArgs _sendSocketEvent;
        private Socket _socket;
        private Stream _socketStream;
        public SSLOptions SSLOptions { get; set; }

        /// <summary>
        ///     Creates a new instance of TcpSocket using the endpoint and options provided.
        /// </summary>
        public EventTcpClient(IPEndPoint endPoint, SocketOptions options, SSLOptions sslOptions)
        {
            EndPoint = endPoint;
            Options = options;
            SSLOptions = sslOptions;

            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
            {
                SendTimeout = (int)Options.SendTimeout.TotalMilliseconds,
                ReceiveTimeout = (int)Options.ReceiveTimeout.TotalMilliseconds
            };

            if (Options.KeepAlive != null)
            {
                _socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, Options.KeepAlive.Value);
            }
            if (Options.SoLinger != null)
            {
                _socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Linger, new LingerOption(true, Options.SoLinger.Value));
            }
            if (Options.ReceiveBufferSize != null)
            {
                _socket.ReceiveBufferSize = Options.ReceiveBufferSize.Value;
            }
            if (Options.SendBufferSize != null)
            {
                _socket.ReceiveBufferSize = Options.SendBufferSize.Value;
            }
            if (Options.TcpNoDelay != null)
            {
                _socket.NoDelay = Options.TcpNoDelay.Value;
            }
            if (Options.IdleTimeout > TimeSpan.Zero)
            {
                _idleTimer = _idleTimer ?? new Timer(Disconnect);
            }

            _receiveBuffer = new byte[_socket.ReceiveBufferSize];
        }

        public IPEndPoint EndPoint { get; protected set; }

        public SocketOptions Options { get; protected set; }
        public int ReceiveBufferSize { get; private set; }

        /// <summary>
        ///     Event that is fired when the host is closing the connection.
        /// </summary>
        public event Action Disconnected;

        /// <summary>
        ///     Event that gets fired when new data is received.
        /// </summary>
        public event Action<byte[], int> DataReceived;

        /// <summary>
        ///     Event that gets fired when a write async request have been completed.
        /// </summary>
        public event Action WriteCompleted;

        public event Action<Exception, SocketError?> Error;

        private void Disconnect(object state)
        {
            Logger.Verbose(String.Format("Connection {0}: idle timeout has been expired.", EndPoint));
            Dispose();
        }

        /// <summary>
        ///     Connects synchronously to the host and starts reading
        /// </summary>
        /// <exception cref="SocketException">Throws a SocketException when the connection could not be established with the host</exception>
        public Task ConnectAsync()
        {
            IAsyncResult connectResult = _socket.BeginConnect(EndPoint, null, null);
            bool connectSignaled = connectResult.AsyncWaitHandle.WaitOne(Options.SendTimeout);

            if (!connectSignaled)
            {
                //It timed out: Close the socket and throw the exception
                _socket.Close();
                throw new SocketException((int) SocketError.TimedOut);
            }
            //End the connect process
            //It will throw exceptions in case there was a problem
            _socket.EndConnect(connectResult);

            //Prepare read and write
            //There are 2 modes: using SocketAsyncEventArgs (most performant) and Stream mode with APM methods
            if (SSLOptions == null && !Options.UseStreamMode)
            {
                Logger.Verbose(String.Format("Connection {0}: socket connected, start reading using SocketEventArgs interface.", EndPoint));
                //using SocketAsyncEventArgs
                _receiveSocketEvent = new SocketAsyncEventArgs();
                _receiveSocketEvent.SetBuffer(_receiveBuffer, 0, _receiveBuffer.Length);
                _receiveSocketEvent.Completed += OnReceiveCompleted;
                _sendSocketEvent = new SocketAsyncEventArgs();
                _sendSocketEvent.Completed += OnSendCompleted;
            }
            else
            {
                Logger.Verbose(String.Format("Connection {0}: Socket connected, start reading using Stream interface.", EndPoint));
                //Stream mode: not the most performant but it has ssl support
                _socketStream = new NetworkStream(_socket);
                if (SSLOptions != null)
                {
                    string targetHost = EndPoint.Address.ToString();
                    try
                    {
                        targetHost = SSLOptions.HostNameResolver(EndPoint.Address);
                    }
                    catch (Exception ex)
                    {
                        Logger.Error(
                            String.Format(
                                "SSL connection: Can not resolve host name for address {0}. Using the IP address instead of the host name. This may cause RemoteCertificateNameMismatch error during Cassandra host authentication. Note that the Cassandra node SSL certificate's CN(Common Name) must match the Cassandra node hostname.",
                                targetHost), ex);
                    }
                    var sslStream = new SslStream(_socketStream, false, SSLOptions.RemoteCertValidationCallback, null);
                    IAsyncResult sslAuthResult = sslStream.BeginAuthenticateAsClient(targetHost, SSLOptions.CertificateCollection,
                        SSLOptions.SslProtocol, SSLOptions.CheckCertificateRevocation, null, null);
                    bool sslAuthSignaled = sslAuthResult.AsyncWaitHandle.WaitOne(Options.SendTimeout);
                    if (!sslAuthSignaled)
                    {
                        //It timed out: Close the socket and throw the exception
                        _socket.Close();
                        throw new SocketException((int) SocketError.TimedOut);
                    }
                    sslStream.EndAuthenticateAsClient(sslAuthResult);
                    _socketStream = sslStream;
                }
            }

            return Task.FromResult(0);
        }

        public Task<int> ReadAsync(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public Task WriteAsync(Stream stream)
        {
            throw new NotImplementedException();
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
        protected virtual void ReceiveAsync()
        {
            //Receive the next bytes
            if (_receiveSocketEvent != null)
            {
                bool willRaiseEvent = true;
                try
                {
                    willRaiseEvent = _socket.ReceiveAsync(_receiveSocketEvent);
                }
                catch (Exception ex)
                {
                    OnError(ex);
                }
                if (!willRaiseEvent)
                {
                    OnReceiveCompleted(this, _receiveSocketEvent);
                }
            }
            else
            {
                //Stream mode
                _socketStream.BeginRead(_receiveBuffer, 0, _receiveBuffer.Length, OnReceiveStreamCallback, null);
            }
        }

        protected virtual void OnError(Exception ex, SocketError? socketError = null)
        {
            StopIdleTimer();

            if (Error != null)
            {
                Error(ex, socketError);
            }
        }

        /// <summary>
        ///     Handles the receive completed event
        /// </summary>
        protected void OnReceiveCompleted(object sender, SocketAsyncEventArgs e)
        {
            if (e.SocketError != SocketError.Success)
            {
                //There was a socket error or the connection is being closed.
                OnError(null, e.SocketError);
                return;
            }
            if (e.BytesTransferred == 0)
            {
                OnClosing();
                return;
            }

            //Emit event
            if (DataReceived != null)
            {
                DataReceived(e.Buffer, e.BytesTransferred);
            }

            ResetIdleTimer();
            ReceiveAsync();
        }

        /// <summary>
        ///     Handles the callback for BeginRead on Stream mode
        /// </summary>
        protected void OnReceiveStreamCallback(IAsyncResult ar)
        {
            try
            {
                int bytesRead = _socketStream.EndRead(ar);
                if (bytesRead == 0)
                {
                    OnClosing();
                    return;
                }
                //Emit event
                if (DataReceived != null)
                {
                    DataReceived(_receiveBuffer, bytesRead);
                }

                ResetIdleTimer();
                ReceiveAsync();
            }
            catch (Exception ex)
            {
                if (ex is IOException && ex.InnerException is SocketException)
                {
                    OnError(ex.InnerException);
                }
                else
                {
                    OnError(ex);
                }
            }
        }

        /// <summary>
        ///     Handles the send completed event
        /// </summary>
        protected void OnSendCompleted(object sender, SocketAsyncEventArgs e)
        {
            if (e.SocketError != SocketError.Success)
            {
                OnError(null, e.SocketError);
            }
            if (WriteCompleted != null)
            {
                WriteCompleted();
            }
        }

        /// <summary>
        ///     Handles the callback for BeginWrite on Stream mode
        /// </summary>
        protected void OnSendStreamCallback(IAsyncResult ar)
        {
            try
            {
                _socketStream.EndWrite(ar);
            }
            catch (Exception ex)
            {
                if (ex is IOException && ex.InnerException is SocketException)
                {
                    OnError(ex.InnerException);
                }
                else
                {
                    OnError(ex);
                }
            }
            if (WriteCompleted != null)
            {
                WriteCompleted();
            }
        }

        protected virtual void OnClosing()
        {
            _isClosing = true;
            if (Disconnected != null)
            {
                Disconnected();
            }

            StopIdleTimer();
        }

        /// <summary>
        ///     Sends data asynchronously
        /// </summary>
        public virtual void Write(Stream stream)
        {
            if (_isClosing)
            {
                OnError(new SocketException((int) SocketError.Shutdown));
            }
            //This can result in OOM
            //A neat improvement would be to write this sync in small buffers when buffer.length > X
            byte[] buffer = Utils.ReadAllBytes(stream, 0);
            if (_sendSocketEvent != null)
            {
                _sendSocketEvent.SetBuffer(buffer, 0, buffer.Length);
                bool willRaiseEvent = false;
                try
                {
                    willRaiseEvent = _socket.SendAsync(_sendSocketEvent);
                }
                catch (Exception ex)
                {
                    OnError(ex);
                }
                if (!willRaiseEvent)
                {
                    OnSendCompleted(this, _sendSocketEvent);
                }
            }
            else
            {
                _socketStream.BeginWrite(buffer, 0, buffer.Length, OnSendStreamCallback, null);
            }
        }

        public void Dispose()
        {
            // Dispose connection idle timer if required
            Timer idleTimer = Interlocked.Exchange(ref _idleTimer, null);
            if (idleTimer != null)
            {
                idleTimer.Change(Timeout.Infinite, Timeout.Infinite);
            }

            try
            {
                if (_socket == null)
                {
                    return;
                }
                //Try to close it.
                //Some operations could make the socket to dispose itself
                _socket.Shutdown(SocketShutdown.Both);
                _socket.Close();
            }
            catch
            {
                //We should not mind if the socket shutdown or close methods throw an exception
            }
        }
    }
}