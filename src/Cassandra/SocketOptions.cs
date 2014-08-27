//
//      Copyright (C) 2012 DataStax Inc.
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

namespace Cassandra
{
    /// <summary>
    ///  Options to configure low-level socket options for the connections kept to the
    ///  Cassandra hosts.
    /// </summary>
    public class SocketOptions
    {
        [Obsolete("This constant will be removed in the upcoming version")]
        public const int DefaultConnectTimeoutMillis = 5000;

        private TimeSpan _idleTimeout = TimeSpan.Zero;
        private TimeSpan _receiveTimeout = TimeSpan.Zero;
        private TimeSpan _sendTimeout = TimeSpan.FromSeconds(5);
        
        private bool? _keepAlive = true;
        private int? _receiveBufferSize;
        private bool? _reuseAddress;
        private int? _sendBufferSize;
        private int? _soLinger;
        private bool? _tcpNoDelay;
        private bool _useStreamMode;

        [Obsolete("This property will be removed in the upcoming version")]
        public int ConnectTimeoutMillis
        {
            get { return (int) _sendTimeout.TotalMilliseconds; }
        }

        /// <summary>
        /// Gets an idle timeout for connection.
        /// </summary>
        public TimeSpan IdleTimeout
        {
            get { return _idleTimeout; }
        }
        
        /// <summary>
        /// Gets a receive timeout for connection.
        /// </summary>
        public TimeSpan ReceiveTimeout
        {
            get { return _receiveTimeout; }
        }
        
        /// <summary>
        /// Gets a send timeout for connection.
        /// </summary>
        public TimeSpan SendTimeout
        {
            get { return _sendTimeout; }
        }

        public bool? KeepAlive
        {
            get { return _keepAlive; }
        }

        public bool? ReuseAddress
        {
            get { return _reuseAddress; }
        }

        public int? SoLinger
        {
            get { return _soLinger; }
        }

        public bool? TcpNoDelay
        {
            get { return _tcpNoDelay; }
        }

        public int? ReceiveBufferSize
        {
            get { return _receiveBufferSize; }
        }

        public int? SendBufferSize
        {
            get { return _sendBufferSize; }
        }

        /// <summary>
        /// Determines if the driver should use either .NET NetworkStream interface (true) or SocketEventArgs interface (false, default)
        /// to handle the reading and writing
        /// </summary>
        public bool UseStreamMode
        {
            get { return _useStreamMode; }
        }

        [Obsolete("This method will be removed in the upcoming version")]
        public SocketOptions SetConnectTimeoutMillis(int connectTimeoutMillis)
        {
            _sendTimeout = TimeSpan.FromMilliseconds(connectTimeoutMillis);
            return this;
        }
        
        /// <summary>
        /// Sets an idle timeout for connection.
        /// </summary>
        /// <remarks>If set <c>TimeSpan.Zero</c> or negative value infinite timeout will be used.</remarks>
        /// <param name="timeout">Timeout value.</param>
        /// <returns>This socket options.</returns>
        public SocketOptions SetIdleTimeout(TimeSpan timeout)
        {
            _idleTimeout = timeout;
            return this;
        }

        /// <summary>
        /// Sets a receive timeout for connection.
        /// </summary>
        /// <remarks>If set <c>TimeSpan.Zero</c> or negative value infinite timeout will be used.</remarks>
        /// <param name="timeout">Timeout value.</param>
        /// <returns>This socket options.</returns>
        public SocketOptions SetReceiveTimeout(TimeSpan timeout)
        {
            _receiveTimeout = timeout;
            return this;
        }
        
        /// <summary>
        /// Sets a send timeout for connection.
        /// </summary>
        /// <remarks>If set <c>TimeSpan.Zero</c> or negative value infinite timeout will be used.</remarks>
        /// <param name="timeout">Timeout value.</param>
        /// <returns>This socket options.</returns>
        public SocketOptions SetSendTimeout(TimeSpan timeout)
        {
            _sendTimeout = timeout;
            return this;
        }

        public SocketOptions SetKeepAlive(bool keepAlive)
        {
            _keepAlive = keepAlive;
            return this;
        }

        public SocketOptions SetReuseAddress(bool reuseAddress)
        {
            _reuseAddress = reuseAddress;
            return this;
        }

        public SocketOptions SetSoLinger(int soLinger)
        {
            _soLinger = soLinger;
            return this;
        }

        public SocketOptions SetTcpNoDelay(bool tcpNoDelay)
        {
            _tcpNoDelay = tcpNoDelay;
            return this;
        }

        public SocketOptions SetReceiveBufferSize(int receiveBufferSize)
        {
            _receiveBufferSize = receiveBufferSize;
            return this;
        }

        public SocketOptions SetSendBufferSize(int sendBufferSize)
        {
            _sendBufferSize = sendBufferSize;
            return this;
        }

        /// <summary>
        /// Sets if the driver should use either .NET NetworkStream (true) interface or SocketEventArgs interface (false, default)
        /// to handle the reading and writing
        /// </summary>
        public SocketOptions SetStreamMode(bool useStreamMode)
        {
            _useStreamMode = useStreamMode;
            return this;
        }
    }
}