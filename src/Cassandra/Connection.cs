//
//      Copyright (C) 2012-2014 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

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
    ///     Represents a TCP connection to a Cassandra Node
    /// </summary>
    internal class Connection : IConnection
    {
        private static readonly Logger Logger = new Logger(typeof (Connection));

        /// <summary>
        ///     Contains list of available stream ids.
        /// </summary>
        private readonly AsyncProducerConsumerCollection<short> _availableStreams;

        private readonly object _keyspaceLock = new object();

        private readonly ITcpClient _tcpClient;

        /// <summary>
        ///     Determines that the connection canceled pending operations.
        ///     It could be because its being closed or there was a socket error.
        /// </summary>
        private readonly CancellationTokenSource _tokenSource = new CancellationTokenSource();

        private int _disposed;
        private Timer _idleTimer;
        private volatile string _keyspace;

        /// <summary>
        ///     Contains the requests that were sent through the wire and that hasn't been received yet.
        /// </summary>
        private ConcurrentDictionary<short, OperationState> _pendingOperations;

        /// <summary>
        ///     Contains pending request messages for sending.
        /// </summary>
        private AsyncProducerConsumerCollection<OperationState> _writeQueue;

        public Connection(byte protocolVersion, IPEndPoint endpoint, Configuration configuration)
        {
            ProtocolVersion = protocolVersion;
            Configuration = configuration;

            //Protocol 3 supports up to 32K concurrent request without waiting a response
            //Allowing larger amounts of concurrent requests will cause large memory consumption
            //Limit to 2K per connection sounds reasonable.
            MaxConcurrentRequests = protocolVersion < 3 ? 128 : 2048;
            IEnumerable<short> streamIds = Enumerable.Range(0, MaxConcurrentRequests).Select(s => (short) s);
            _availableStreams = new AsyncProducerConsumerCollection<short>(streamIds);

            _pendingOperations = new ConcurrentDictionary<short, OperationState>();
            _writeQueue = new AsyncProducerConsumerCollection<OperationState>();

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

            //Init TCP client
            _tcpClient = configuration.ProtocolOptions.SslOptions != null
                ? new SslStreamTcpClient(endpoint, configuration.SocketOptions, configuration.ProtocolOptions.SslOptions)
                : configuration.SocketOptions.UseStreamMode
                    ? (ITcpClient) new StreamTcpClient(endpoint, configuration.SocketOptions)
                    : new EventTcpClient(endpoint, configuration.SocketOptions);
        }

        /// <summary>
        ///     The event that represents a event RESPONSE from a Cassandra node
        /// </summary>
        public event CassandraEventHandler CassandraEventResponse;

        public IFrameCompressor Compressor { get; private set; }

        public IPEndPoint Address
        {
            get { return _tcpClient.EndPoint; }
        }

        /// <summary>
        ///     Determines the amount of operations that are not finished.
        /// </summary>
        public int InFlight
        {
            get { return _pendingOperations.Count; }
        }

        /// <summary>
        ///     Determine if the Connection is closed
        /// </summary>
        public bool IsClosed
        {
            //if the connection attempted to cancel pending operations
            get { return _tokenSource.IsCancellationRequested; }
        }

        /// <summary>
        ///     Determine if the Connection has been explicitly disposed
        /// </summary>
        public bool IsDisposed
        {
            get { return Thread.VolatileRead(ref _disposed) > 0; }
        }

        /// <summary>
        ///     Gets or sets the keyspace.
        ///     When setting the keyspace, it will issue a Query Request and wait to complete.
        /// </summary>
        public string Keyspace
        {
            get { return _keyspace; }
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
                    Logger.Info(string.Format("Host {0}: connection established, switching to keyspace {1}", Address, value));
                    _keyspace = value;
                    var request = new QueryRequest(ProtocolVersion, String.Format("USE \"{0}\"", value), false, QueryProtocolOptions.Default);
                    TaskHelper.WaitToComplete(SendAsync(request), Configuration.SocketOptions.SendTimeout);
                }
            }
        }

        public ProtocolOptions Options
        {
            get { return Configuration.ProtocolOptions; }
        }

        public byte ProtocolVersion { get; private set; }

        public Configuration Configuration { get; private set; }

        public int MaxConcurrentRequests { get; private set; }

        public async Task ConnectAsync()
        {
            await _tcpClient.ConnectAsync().ConfigureAwait(false);
            await Task.Factory.StartNew(ReadHandler).ConfigureAwait(false);
            await Task.Factory.StartNew(WriteHandler).ConfigureAwait(false);

            AbstractResponse response;
            try
            {
                response = await StartupAsync()
                    .SetTimeout(Configuration.SocketOptions.SendTimeout, () => new TimeoutException())
                    .ConfigureAwait(false);
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
                await AuthenticateAsync().ConfigureAwait(false);
            }
            else if (!(response is ReadyResponse))
            {
                throw new DriverInternalError("Expected READY or AUTHENTICATE, obtained " + response.GetType().Name);
            }

            if (Configuration.SocketOptions.IdleTimeout > TimeSpan.Zero)
            {
                _idleTimer = new Timer(PingServer);
                ResetIdleTimer();
            }
        }

        public virtual void Dispose()
        {
            if (Interlocked.Increment(ref _disposed) != 1)
            {
                //Only dispose once
                return;
            }

            StopIdleTimer();
            _tcpClient.Dispose();
        }

        /// <summary>
        ///     Sends a new request if possible. If it is not possible it queues it up.
        /// </summary>
        public Task<AbstractResponse> SendAsync(IRequest request)
        {
            var tcs = new TaskCompletionSource<AbstractResponse>();
            if (_tokenSource.IsCancellationRequested)
            {
                tcs.SetException(new SocketException((int) SocketError.NotConnected));
                return tcs.Task;
            }

            // thread safe write queue
            var state = new OperationState
            {
                Request = request
            };

            _writeQueue.Add(state);

            return state.Response.Task;
        }

        public IEnumerable<Task> GetPending()
        {
            var distinctTasks = new HashSet<Task>();
            foreach (OperationState state in _writeQueue)
            {
                distinctTasks.Add(state.Response.Task);
            }

            foreach (var state in _pendingOperations)
            {
                distinctTasks.Add(state.Value.Response.Task);
            }

            return distinctTasks;
        }

        private async void PingServer(object state)
        {
            var request = new OptionsRequest(ProtocolVersion);
            try
            {
                await SendAsync(request).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logger.Error(e);
            }
        }

        private void ResetIdleTimer()
        {
            if (Configuration.SocketOptions.IdleTimeout > TimeSpan.Zero && _idleTimer != null)
            {
                _idleTimer.Change(Configuration.SocketOptions.IdleTimeout, TimeSpan.FromMilliseconds(Timeout.Infinite));
                Logger.Verbose(String.Format("Host {0}: idle timer has been resetted.", Address));
            }
        }

        private void StopIdleTimer()
        {
            if (Configuration.SocketOptions.IdleTimeout > TimeSpan.Zero && _idleTimer != null)
            {
                _idleTimer.Change(Timeout.Infinite, Timeout.Infinite);
                Logger.Verbose(String.Format("Host {0}: idle timer has been stopped.", Address));
            }
        }

        /// <summary>
        ///     Starts the authentication flow
        /// </summary>
        /// <exception cref="AuthenticationException" />
        private async Task AuthenticateAsync()
        {
            //Determine which authentication flow to use.
            //Check if its using a C* 1.2 with authentication patched version (like DSE 3.1)
            bool isPatchedVersion = ProtocolVersion == 1 && !(Configuration.AuthProvider is NoneAuthProvider) &&
                                    Configuration.AuthInfoProvider == null;
            if (ProtocolVersion >= 2 || isPatchedVersion)
            {
                //Use protocol v2+ authentication flow

                //NewAuthenticator will throw AuthenticationException when NoneAuthProvider
                IAuthenticator authenticator = Configuration.AuthProvider.NewAuthenticator(Address);

                byte[] initialResponse = authenticator.InitialResponse() ?? new byte[0];
                await AuthenticateAsync(initialResponse, authenticator).ConfigureAwait(false);
            }
            else
            {
                //Use protocol v1 authentication flow
                if (Configuration.AuthInfoProvider == null)
                {
                    throw new AuthenticationException(
                        String.Format("Host {0}: authentication required, but no credentials provided in Cluster configuration", Address),
                        Address);
                }
                IAuthInfoProvider credentialsProvider = Configuration.AuthInfoProvider;
                IDictionary<string, string> credentials = credentialsProvider.GetAuthInfos(Address);
                var request = new CredentialsRequest(ProtocolVersion, credentials);
                AbstractResponse response = await SendAsync(request).ConfigureAwait(false);
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
            AbstractResponse response = await SendAsync(request).ConfigureAwait(false);
            if (response is AuthSuccessResponse)
            {
                //It is now authenticated
                return;
            }

            if (!(response is AuthChallengeResponse))
            {
                throw new ProtocolErrorException("Expected SASL response, obtained " + response.GetType().Name);
            }

            token = authenticator.EvaluateChallenge((response as AuthChallengeResponse).Token);
            if (token == null)
            {
                // If we get a null response, then authentication has completed
                //return without sending a further response back to the server.
                return;
            }

            await AuthenticateAsync(token, authenticator).ConfigureAwait(false);
        }

        /// <summary>
        ///     It callbacks all operations already sent / or to be written, that do not have a response.
        /// </summary>
        internal void CancelPending(Exception ex)
        {
            _tokenSource.Cancel();

            ConcurrentDictionary<short, OperationState> pendingOperations = Interlocked.Exchange(ref _pendingOperations,
                new ConcurrentDictionary<short, OperationState>());
            AsyncProducerConsumerCollection<OperationState> writeQueue = Interlocked.Exchange(ref _writeQueue,
                new AsyncProducerConsumerCollection<OperationState>());

            Logger.Info(string.Format("Host {0}: canceling pending operations {1} and write queue {2}", Address, pendingOperations.Count,
                writeQueue.Count));

            foreach (var operation in pendingOperations)
            {
                operation.Value.Response.SetException(ex);
            }

            foreach (OperationState write in writeQueue)
            {
                write.Response.SetException(ex);
            }
        }

        /// <summary>
        ///     Parses the bytes received into a frame. Uses the internal operation state to do the callbacks.
        ///     Returns true if a full operation (streamId) has been processed and there is one available.
        /// </summary>
        private async void ReadHandler()
        {
            OperationState state = null;
            byte[] minimalBuffer = null;
            var buffer = new byte[_tcpClient.ReceiveBufferSize];

            do
            {
                // Receive data from connection
                int offset = 0, count;
                try
                {
                    count = await _tcpClient.ReadAsync(buffer, offset, buffer.Length, _tokenSource.Token).ConfigureAwait(false);
                    ResetIdleTimer();
                }
                catch (Exception e)
                {
                    CancelPending(e.GetBaseException());
                    return;
                }

                if (count == 0)
                {
                    CancelPending(new SocketException((int) SocketError.Disconnecting));
                    return;
                }

                Logger.Verbose(string.Format("Host {0}: received {1} bytes", Address, count));

                // Process received data
                do
                {
                    if (state == null)
                    {
                        if (minimalBuffer != null)
                        {
                            buffer = Utils.JoinBuffers(minimalBuffer, 0, minimalBuffer.Length, buffer, 0, count);
                            offset = 0;
                            count = buffer.Length;
                        }

                        byte headerSize = FrameHeader.GetSize(ProtocolVersion);
                        if (count < headerSize)
                        {
                            //There is not enough data to read the header
                            minimalBuffer = Utils.SliceBuffer(buffer, offset, count);
                            continue;
                        }

                        minimalBuffer = null;

                        FrameHeader header = FrameHeader.ParseResponseHeader(ProtocolVersion, buffer, offset);
                        if (!header.IsValidResponse())
                        {
                            Logger.Error("Not a response header");
                        }

                        Logger.Verbose(string.Format("Host {0}, stream #{1}: processing frame with header {2} and body {3}", Address, header.StreamId,
                            headerSize, header.BodyLength));

                        offset += headerSize;
                        count -= headerSize;

                        if (header.Operation == FrameOperation.Event)
                        {
                            state = new OperationState();
                        }
                        else
                        {
                            if (!_pendingOperations.TryGetValue(header.StreamId, out state))
                            {
                                Logger.Error(string.Format("Host {0}, stream #{1}: unable to find response handler", Address, header.StreamId));
                                continue;
                            }
                        }

                        state.Header = header;
                    }

                    int appendedBytes = state.AppendBody(buffer, offset, count);
                    if (state.IsBodyComplete)
                    {
                        Logger.Verbose(string.Format("Host {0}, stream #{1}: received response {2}",
                            Address, state.Header.StreamId, state.Header.Operation));

                        AbstractResponse response = null;
                        try
                        {
                            response = ReadParseResponse(state.Header, state.BodyStream);
                        }
                        catch (Exception ex)
                        {
                            state.Response.SetException(ex);
                        }

                        var errorResponse = response as ErrorResponse;
                        if (errorResponse != null)
                        {
                            state.Response.SetException(errorResponse.Output.CreateException());
                        }
                        else
                        {
                            state.Response.SetResult(response);
                        }

                        switch (state.Header.Operation)
                        {
                            case FrameOperation.Event:
                                if (response != null)
                                {
                                    ProcessEventResponse(response);
                                }
                                break;
                            default:
                                //Remove from pending
                                _pendingOperations.TryRemove(state.Header.StreamId, out state);
                                //Release the streamId
                                _availableStreams.Add(state.Header.StreamId);
                                break;
                        }

                        // Response was processed
                        state = null;
                    }

                    count -= appendedBytes;
                    offset += appendedBytes;
                } while (count > 0);
            } while (!_tokenSource.IsCancellationRequested);
        }

        private void ProcessEventResponse(AbstractResponse response)
        {
            if (!(response is EventResponse))
            {
                Logger.Error(string.Format("Host {0}: unexpected response type for event: {1}", Address, response.GetType().Name));
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
            if (header.Flags.HasFlag(FrameFlags.Compressed))
            {
                body = Compressor.Decompress(body);
            }
            var frame = new ResponseFrame(header, body);
            AbstractResponse response = FrameParser.Parse(frame);
            return response;
        }

        /// <summary>
        ///     Sends a protocol STARTUP message
        /// </summary>
        private Task<AbstractResponse> StartupAsync()
        {
            var startupOptions = new Dictionary<string, string> {{"CQL_VERSION", "3.0.0"}};
            if (Options.Compression == CompressionType.LZ4)
            {
                startupOptions.Add("COMPRESSION", "lz4");
            }
            else if (Options.Compression == CompressionType.Snappy)
            {
                startupOptions.Add("COMPRESSION", "snappy");
            }
            var request = new StartupRequest(ProtocolVersion, startupOptions);
            return SendAsync(request);
        }

        /// <summary>
        ///     Try to write the item provided. Thread safe.
        /// </summary>
        private async void WriteHandler()
        {
            do
            {
                OperationState state = await _writeQueue.TakeAsync().ConfigureAwait(false);
                if (_tokenSource.IsCancellationRequested)
                {
                    state.Response.SetException(new SocketException((int) SocketError.NotConnected));
                    return;
                }

                short streamId = await _availableStreams.TakeAsync().ConfigureAwait(false);

                Logger.Verbose(string.Format("Host {0}, stream #{1}: sending request {2}", Address, streamId, state.Request.GetType().Name));

                _pendingOperations.AddOrUpdate(streamId, state, (k, oldValue) => state);

                //At this point:
                //We have a valid stream id
                //Only 1 thread at a time can be here.
                try
                {
                    Stream frameStream = state.Request.GetFrame(streamId).Stream;
                    frameStream.Position = 0;
                    //We will not use the request, stop reference it.
                    state.Request = null;
                    //Start sending it
                    await _tcpClient.WriteAsync(frameStream, _tokenSource.Token).ConfigureAwait(false);
                    ResetIdleTimer();
                }
                catch (Exception ex)
                {
                    Logger.Error(ex);

                    // The request was not written
                    if (_pendingOperations.TryRemove(streamId, out state))
                    {
                        state.Response.SetException(ex.GetBaseException());
                    }

                    _availableStreams.Add(streamId);
                }
            } while (!_tokenSource.IsCancellationRequested);
        }
    }
}