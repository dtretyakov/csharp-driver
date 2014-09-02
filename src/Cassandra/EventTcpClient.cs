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
    internal class EventTcpClient : ITcpClient
    {
        private static readonly Logger Logger = new Logger(typeof (EventTcpClient));
        private readonly Socket _socket;

        /// <summary>
        ///     Creates a new instance of TcpSocket using the endpoint and options provided.
        /// </summary>
        public EventTcpClient(IPEndPoint endPoint, SocketOptions options)
        {
            EndPoint = endPoint;
            Options = options;

            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
            {
                SendTimeout = (int) Options.SendTimeout.TotalMilliseconds,
                ReceiveTimeout = (int) Options.ReceiveTimeout.TotalMilliseconds
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
        }

        public IPEndPoint EndPoint { get; protected set; }

        public SocketOptions Options { get; protected set; }

        public int ReceiveBufferSize
        {
            get { return _socket.ReceiveBufferSize; }
        }

        /// <summary>
        ///     Connects synchronously to the host and starts reading
        /// </summary>
        /// <exception cref="SocketException">Throws a SocketException when the connection could not be established with the host</exception>
        public async Task ConnectAsync()
        {
            await Task.Factory.FromAsync(_socket.BeginConnect, _socket.EndConnect, EndPoint, null)
                      .SetTimeout(Options.SendTimeout, () => new SocketException((int) SocketError.TimedOut))
                      .ConfigureAwait(false);

            if (!_socket.Connected)
            {
                return;
            }

            Logger.Verbose(String.Format("Connection {0}: socket connected, start reading using SocketEventArgs interface.", EndPoint));
        }

        public Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken = default(CancellationToken))
        {
            var tcs = new TaskCompletionSource<int>();
            if (_socket == null || !_socket.Connected)
            {
                tcs.SetException(new SocketException((int)SocketError.NotConnected));
                return tcs.Task;
            }

            var receiveSocketEvent = new SocketAsyncEventArgs();
            receiveSocketEvent.SetBuffer(buffer, offset, count);
            receiveSocketEvent.Completed += (s, e) => HandleSocketEvent(e, tcs);

            try
            {
                if (!_socket.ReceiveAsync(receiveSocketEvent))
                {
                    HandleSocketEvent(receiveSocketEvent, tcs);
                }
            }
            catch (Exception e)
            {
                tcs.SetException(e);
            }

            return tcs.Task;
        }

        public Task WriteAsync(Stream stream, CancellationToken cancellationToken = default(CancellationToken))
        {
            var tcs = new TaskCompletionSource<int>();
            if (_socket == null || !_socket.Connected)
            {
                tcs.SetException(new SocketException((int)SocketError.NotConnected));
                return tcs.Task;
            }

            var sendSocketEvent = new SocketAsyncEventArgs();
            sendSocketEvent.Completed += (s, e) => HandleSocketEvent(e, tcs);

            //This can result in OOM
            //A neat improvement would be to write this sync in small buffers when buffer.length > X
            byte[] buffer = Utils.ReadAllBytes(stream, 0);
            sendSocketEvent.SetBuffer(buffer, 0, buffer.Length);
            try
            {
                if (!_socket.SendAsync(sendSocketEvent))
                {
                    HandleSocketEvent(sendSocketEvent, tcs);
                }
            }
            catch (Exception e)
            {
                tcs.SetException(e);
            }

            return tcs.Task;
        }

        public void Dispose()
        {
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

        private static void HandleSocketEvent(SocketAsyncEventArgs e, TaskCompletionSource<int> tcs)
        {
            if (e.SocketError != SocketError.Success)
            {
                tcs.SetException(new SocketException((int) e.SocketError));
            }
            else
            {
                tcs.SetResult(e.BytesTransferred);
            }
        }
    }
}