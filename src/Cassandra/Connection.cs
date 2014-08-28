using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Cassandra
{
    /// <summary>
    /// Represents a TCP connection to a Cassandra Node
    /// </summary>
    internal class Connection : IDisposable
    {
        private static readonly Logger _logger = new Logger(typeof(Connection));
        private readonly ITcpClient _tcpClient;
        private int _disposed;
        /// <summary>
        /// Determines that the connection canceled pending operations.
        /// It could be because its being closed or there was a socket error.
        /// </summary>
        private volatile bool _isCanceled;
        /// <summary>
        /// Stores the available stream ids.
        /// </summary>
        private ConcurrentStack<short> _freeOperations;
        /// <summary>
        /// Contains the requests that were sent through the wire and that hasn't been received yet.
        /// </summary>
        private ConcurrentDictionary<short, OperationState> _pendingOperations;
        /// <summary>
        /// It determines if the write queue can process the next (if it is not in-flight).
        /// It has to be volatile as it can not be cached by the thread.
        /// </summary>
        private volatile bool _canWriteNext = true;
        /// <summary>
        /// Its for processing the next item in the write queue.
        /// It can not be replaced by a Interlocked Increment as it must allow rollbacks (when there are no stream ids left).
        /// </summary>
        private readonly object _writeQueueLock = new object();
        private ConcurrentQueue<OperationState> _writeQueue;
        
        private volatile string _keyspace;
        private readonly object _keyspaceLock = new object();

        /// <summary>
        /// The event that represents a event RESPONSE from a Cassandra node
        /// </summary>
        public event CassandraEventHandler CassandraEventResponse;

        public IFrameCompressor Compressor { get; set; }

        public IPEndPoint Address
        {
            get { return _tcpClient.EndPoint; }
        }

        /// <summary>
        /// Determines the amount of operations that are not finished.
        /// </summary>
        public int InFlight
        {
            get { return _pendingOperations.Count; }
        }

        /// <summary>
        /// Determine if the Connection is closed
        /// </summary>
        public bool IsClosed
        {
            //if the connection attempted to cancel pending operations
            get { return _isCanceled; }
        }

        /// <summary>
        /// Determine if the Connection has been explicitly disposed
        /// </summary>
        public bool IsDisposed
        {
            get { return Thread.VolatileRead(ref _disposed) > 0; }
        }

        /// <summary>
        /// Gets or sets the keyspace.
        /// When setting the keyspace, it will issue a Query Request and wait to complete.
        /// </summary>
        public string Keyspace
        {
            get
            {
                return _keyspace;
            }
            set
            {
                if (String.IsNullOrEmpty(value))
                {
                    return;
                }
                if (_keyspace != null && value == _keyspace)
                {
                    return;
                }
                lock (_keyspaceLock)
                {
                    if (value == _keyspace)
                    {
                        return;
                    }
                    _logger.Info("Connection to host " + Address + " switching to keyspace " + value);
                    _keyspace = value;
                    var request = new QueryRequest(ProtocolVersion, String.Format("USE \"{0}\"", value), false, QueryProtocolOptions.Default);
                    TaskHelper.WaitToComplete(SendAsync(request), Configuration.SocketOptions.SendTimeout);
                }
            }
        }

        /// <summary>
        /// Gets the amount of concurrent requests depending on the protocol version
        /// </summary>
        public int MaxConcurrentRequests
        {
            get
            {
                if (ProtocolVersion < 3)
                {
                    return 128;
                }
                //Protocol 3 supports up to 32K concurrent request without waiting a response
                //Allowing larger amounts of concurrent requests will cause large memory consumption
                //Limit to 2K per connection sounds reasonable.
                return 2048;
            }
        }

        public ProtocolOptions Options { get { return Configuration.ProtocolOptions; } }

        public byte ProtocolVersion { get; set; }

        public Configuration Configuration { get; set; }

        public Connection(byte protocolVersion, IPEndPoint endpoint, Configuration configuration)
        {
            ProtocolVersion = protocolVersion;
            Configuration = configuration;

            _freeOperations = new ConcurrentStack<short>(Enumerable.Range(0, MaxConcurrentRequests).Select(s => (short) s).Reverse());
            _pendingOperations = new ConcurrentDictionary<short, OperationState>();
            _writeQueue = new ConcurrentQueue<OperationState>();

            if (Options.CustomCompressor != null)
            {
                Compressor = Options.CustomCompressor;
            }
            else if (Options.Compression == CompressionType.LZ4)
            {
                Compressor = new LZ4Compressor();
            }
            else if (Options.Compression == CompressionType.Snappy)
            {
                Compressor = new SnappyCompressor();
            }

            //Init TcpSocket
            _tcpClient = configuration.ProtocolOptions.SslOptions != null
                ? new SslStreamTcpClient(endpoint, configuration.SocketOptions, configuration.ProtocolOptions.SslOptions)
                : new StreamTcpClient(endpoint, configuration.SocketOptions);
        }

        public async Task ConnectAsync()
        {
            await _tcpClient.ConnectAsync();

            AbstractResponse response;
            try
            {
                response = await StartupAsync();
            }
            catch (ProtocolErrorException ex)
            {
                //As we are starting up, check for protocol version errors
                //There is no other way than checking the error message from Cassandra
                if (ex.Message.Contains("Invalid or unsupported protocol version"))
                {
                    throw new UnsupportedProtocolVersionException(ProtocolVersion, ex);
                }
                throw;
            }

            if (response is AuthenticateResponse)
            {
                await AuthenticateAsync();
            }
            else if (!(response is ReadyResponse))
            {
                throw new DriverInternalError("Expected READY or AUTHENTICATE, obtained " + response.GetType().Name);
            }
        }

        /// <summary>
        /// Starts the authentication flow
        /// </summary>
        /// <exception cref="AuthenticationException" />
        private async Task AuthenticateAsync()
        {
            //Determine which authentication flow to use.
            //Check if its using a C* 1.2 with authentication patched version (like DSE 3.1)
            var isPatchedVersion = ProtocolVersion == 1 && !(Configuration.AuthProvider is NoneAuthProvider) && Configuration.AuthInfoProvider == null;
            if (ProtocolVersion >= 2 || isPatchedVersion)
            {
                //Use protocol v2+ authentication flow

                //NewAuthenticator will throw AuthenticationException when NoneAuthProvider
                var authenticator = Configuration.AuthProvider.NewAuthenticator(Address);

                var initialResponse = authenticator.InitialResponse() ?? new byte[0];
                await AuthenticateAsync(initialResponse, authenticator);
            }
            else
            {
                //Use protocol v1 authentication flow
                if (Configuration.AuthInfoProvider == null)
                {
                    throw new AuthenticationException(
                        String.Format("Host {0} requires authentication, but no credentials provided in Cluster configuration", Address),
                        Address);
                }
                var credentialsProvider = Configuration.AuthInfoProvider;
                var credentials = credentialsProvider.GetAuthInfos(Address);
                var request = new CredentialsRequest(ProtocolVersion, credentials);
                var response = await SendAsync(request);
                //If Cassandra replied with a auth response error
                //The task already is faulted and the exception was already thrown.
                if (response is ReadyResponse)
                {
                    return;
                }

                throw new ProtocolErrorException("Expected SASL response, obtained " + response.GetType().Name);
            }
        }

        /// <exception cref="AuthenticationException" />
        private async Task AuthenticateAsync(byte[] token, IAuthenticator authenticator)
        {
            var request = new AuthResponseRequest(ProtocolVersion, token);
            var response = await SendAsync(request);
            if (response is AuthSuccessResponse)
            {
                //It is now authenticated
                return;
            }

            if (response is AuthChallengeResponse)
            {
                token = authenticator.EvaluateChallenge((response as AuthChallengeResponse).Token);
                if (token == null)
                {
                    // If we get a null response, then authentication has completed
                    //return without sending a further response back to the server.
                    return;
                }

                await AuthenticateAsync(token, authenticator);
                return;
            }
            throw new ProtocolErrorException("Expected SASL response, obtained " + response.GetType().Name);
        }

        /// <summary>
        /// It callbacks all operations already sent / or to be written, that do not have a response.
        /// </summary>
        internal void CancelPending(Exception ex)
        {
            _isCanceled = true;

            var pendingOperations = Interlocked.Exchange(ref _pendingOperations, new ConcurrentDictionary<short, OperationState>());
            var writeQueue = Interlocked.Exchange(ref _writeQueue, new ConcurrentQueue<OperationState>());

            _logger.Info("Canceling pending operations " + pendingOperations.Count + " and write queue " + writeQueue.Count);

            foreach (var operation in pendingOperations)
            {
                operation.Value.Response.SetException(ex);
            }

            foreach (var write in writeQueue)
            {
                write.Response.SetException(ex);
            }
        }

        public virtual void Dispose()
        {
            if (Interlocked.Increment(ref _disposed) != 1)
            {
                //Only dispose once
                return;
            }
            _tcpClient.Dispose();
        }

        /// <summary>
        /// Parses the bytes received into a frame. Uses the internal operation state to do the callbacks.
        /// Returns true if a full operation (streamId) has been processed and there is one available.
        /// </summary>
        protected async void ReadParse()
        {
            OperationState state = null;
            byte[] minimalBuffer = null;
            var receiveBuffer = new byte[_tcpClient.ReceiveBufferSize];

            do
            {
                int bytesRead, offset, count;
                try
                {
                    bytesRead = await _tcpClient.ReadAsync(receiveBuffer, 0, receiveBuffer.Length);
                }
                catch (Exception e)
                {
                    CancelPending(e);
                    return;
                }

                if (bytesRead == 0)
                {
                    CancelPending(new SocketException((int)SocketError.Disconnecting));
                }

                if (state == null)
                {
                    if (minimalBuffer != null)
                    {
                        receiveBuffer = Utils.JoinBuffers(minimalBuffer, 0, minimalBuffer.Length, buffer, offset, count);
                        offset = 0;
                        count = buffer.Length;
                    }
                    var headerSize = FrameHeader.GetSize(ProtocolVersion);
                    if (count < headerSize)
                    {
                        //There is not enough data to read the header
                        minimalBuffer = Utils.SliceBuffer(buffer, offset, count);
                        return;
                    }
                    minimalBuffer = null;
                    var header = FrameHeader.ParseResponseHeader(ProtocolVersion, buffer, offset);
                    if (!header.IsValidResponse())
                    {
                        _logger.Error("Not a response header");
                    }
                    offset += headerSize;
                    count -= headerSize;
                    if (header.Opcode != EventResponse.OpCode)
                    {
                        //Its a response to a previous request
                        state = _pendingOperations[header.StreamId];
                    }
                    else
                    {
                        //Its an event
                        state = new OperationState();
                        state.Callback = EventHandler;
                    }
                    state.Header = header;
                    _receivingOperation = state;
                }

                var countAdded = state.AppendBody(buffer, offset, count);

                if (state.IsBodyComplete)
                {
                    _logger.Verbose("Read #" + state.Header.StreamId + " for Opcode " + state.Header.Opcode);
                    //Stop reference it as the current receiving operation
                    _receivingOperation = null;
                    AbstractResponse response;

                    try
                    {
                        response = ReadParseResponse(state.Header, state.BodyStream);
                        state.InvokeCallback(null, response);
                    }
                    catch (Exception ex)
                    {
                        state.Response.SetException(ex);
                    }

                    if (state.Header.Opcode != EventResponse.OpCode)
                    {
                        //Remove from pending
                        _pendingOperations.TryRemove(state.Header.StreamId, out state);
                        //Release the streamId
                        _freeOperations.Push(state.Header.StreamId);
                    }
                    else
                    {
                        ProcessEventResponse(response);
                    }


                    if (countAdded < count)
                    {
                        //There is more data, from the next frame
                        ReadParse(buffer, offset + countAdded, count - countAdded);
                    }
                }
            } while (!_isCanceled);
        }

        private void ProcessEventResponse(AbstractResponse response)
        {
            if (!(response is EventResponse))
            {
                _logger.Error("Unexpected response type for event: " + response.GetType().Name);
                return;
            }
            if (CassandraEventResponse != null)
            {
                CassandraEventResponse(this, (response as EventResponse).CassandraEventArgs);
            }
        }

        private AbstractResponse ReadParseResponse(FrameHeader header, Stream body)
        {
            //Start at the first byte
            body.Position = 0;
            if ((header.Flags & 0x01) > 0)
            {
                body = Compressor.Decompress(body);
            }
            var frame = new ResponseFrame(header, body);
            var response = FrameParser.Parse(frame);
            return response;
        }

        /// <summary>
        /// Sends a protocol STARTUP message
        /// </summary>
        private Task<AbstractResponse> StartupAsync()
        {
            var startupOptions = new Dictionary<string, string>();
            startupOptions.Add("CQL_VERSION", "3.0.0");
            if (Options.Compression == CompressionType.LZ4)
            {
                startupOptions.Add("COMPRESSION", "lz4");
            }
            else if (Options.Compression == CompressionType.Snappy)
            {
                startupOptions.Add("COMPRESSION", "snappy");
            }
            var request = new StartupRequest(ProtocolVersion, startupOptions);
            var tcs = new TaskCompletionSource<AbstractResponse>();
            Send(request, tcs.TrySet);
            return tcs.Task;
        }

        /// <summary>
        /// Sends a new request if possible. If it is not possible it queues it up.
        /// </summary>
        public Task<AbstractResponse> SendAsync(IRequest request)
        {
            var tcs = new TaskCompletionSource<AbstractResponse>();
            Send(request, tcs.TrySet);
            return tcs.Task;
        }

        /// <summary>
        /// Sends a new request if possible and executes the callback when the response is parsed. If it is not possible it queues it up.
        /// </summary>
        public void Send(IRequest request, Action<Exception, AbstractResponse> callback)
        {
            if (_isCanceled)
            {
                callback(new SocketException((int)SocketError.NotConnected), null);
            }
            //thread safe write queue
            var state = new OperationState
            {
                Request = request,
                Callback = callback
            };
            SendQueueProcess(state);
        }

        /// <summary>
        /// Try to write the item provided. Thread safe.
        /// </summary>
        private async Task SendQueueProcess(OperationState state)
        {
            if (!_canWriteNext)
            {
                //Double-checked locking for best performance
                _writeQueue.Enqueue(state);
                return;
            }
            short streamId;
            lock (_writeQueueLock)
            {
                if (!_canWriteNext)
                {
                    //We have to recheck as the world can change since the last instruction
                    _writeQueue.Enqueue(state);
                    return;
                }
                //Check if Cassandra can process a new operation
                if (!_freeOperations.TryPop(out streamId))
                {
                    //Queue it up for later.
                    //When receiving the next complete message, we can process it.
                    _writeQueue.Enqueue(state);
                    _logger.Info("Enqueued: " + _writeQueue.Count + ", if this message is recurrent consider configuring more connections per host or lower the pressure");
                    return;
                }
                //Prevent the next to process
                _canWriteNext = false;
            }
            
            //At this point:
            //We have a valid stream id
            //Only 1 thread at a time can be here.
            try
            {
                _logger.Verbose("Sending #" + streamId + " for " + state.Request.GetType().Name);
                var frameStream = state.Request.GetFrame(streamId).Stream;
                _pendingOperations.AddOrUpdate(streamId, state, (k, oldValue) => state);
                //We will not use the request, stop reference it.
                state.Request = null;
                //Start sending it
                await _tcpClient.WriteAsync(frameStream);
            }
            catch (Exception ex)
            {
                //Prevent dead locking
                _canWriteNext = true;
                _logger.Error(ex);
                //The request was not written
                _pendingOperations.TryRemove(streamId, out state);
                _freeOperations.Push(streamId);
                throw;
            }
        }

        /// <summary>
        /// Try to write the next item in the write queue. Thread safe.
        /// </summary>
        protected virtual void SendQueueNext()
        {
            if (!_canWriteNext)
            {
                return;
            }
            OperationState state;
            if (_writeQueue.TryDequeue(out state))
            {
                SendQueueProcess(state);
            }
        }

        /// <summary>
        /// Method that gets executed when a write request has been completed.
        /// </summary>
        protected virtual void WriteCompletedHandler()
        {
            //There is no need to lock
            //Only 1 thread can be here at the same time.
            _canWriteNext = true;
            SendQueueNext();
        }

        internal WaitHandle WaitPending()
        {
            if (_pendingWaitHandle == null)
            {
                _pendingWaitHandle = new AutoResetEvent(false);
            }
            if (_pendingOperations.Count == 0 && _writeQueue.Count == 0)
            {
                _pendingWaitHandle.Set();
            }
            return _pendingWaitHandle;
        }
    }
}
