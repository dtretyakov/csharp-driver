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

namespace Cassandra
{
    /// <summary>
    ///  Options of the Cassandra __native__ binary protocol.
    /// </summary>
    public class ProtocolOptions
    {
        /// <summary>
        ///  The default port for Cassandra __native__ binary protocol: 9042.
        /// </summary>
        public const int DefaultPort = 9042;

        private readonly int _port;
        private readonly SSLOptions _sslOptions;
        private CompressionType _compression = CompressionType.NoCompression;
        private IFrameCompressor _compressor;

        /// <summary>
        ///  The port used to connect to the Cassandra hosts.
        /// </summary>
        /// 
        /// <returns>the port used to connect to the Cassandra hosts.</returns>
        public int Port
        {
            get { return _port; }
        }

        /// <summary>
        /// Specified SSL options used to connect to the Cassandra hosts.
        /// </summary>
        /// 
        /// <returns>SSL options used to connect to the Cassandra hosts.</returns>
        public SSLOptions SslOptions
        {
            get { return _sslOptions; }
        }

        /// <summary>
        ///  Returns the compression used by the protocol. <p> The default compression is
        ///  <c>Compression.SNAPPY</c>.</p>
        /// </summary>
        /// <returns>the compression used.</returns>
        public CompressionType Compression
        {
            get { return _compression; }
        }

        /// <summary>
        ///  Gets the custom compressor specified to be used for the compression type.
        /// </summary>
        public IFrameCompressor CustomCompressor
        {
            get { return _compressor; }
        }

        /// <summary>
        ///  Creates a new <c>ProtocolOptions</c> instance using the
        ///  <c>DEFAULT_PORT</c>.
        /// </summary>
        public ProtocolOptions()
            : this(DefaultPort)
        {
        }

        /// <summary>
        ///  Creates a new <c>ProtocolOptions</c> instance using the provided port.
        /// </summary>
        /// <param name="port"> the port to use for the binary protocol.</param>
        public ProtocolOptions(int port)
        {
            _port = port;
        }


        /// <summary>       
        /// Creates a new ProtocolOptions instance using the provided port and SSL context.        
        /// </summary>
        /// <param name="port">the port to use for the binary protocol.</param>
        /// <param name="sslOptions">sslOptions the SSL options to use. Use null if SSL is not to be used.</param>
        public ProtocolOptions(int port, SSLOptions sslOptions)
        {
            _port = port;
            _sslOptions = sslOptions;
        }

        /// <summary>
        ///  Sets the compression to use. <p> Note that while this setting can be changed
        ///  at any time, it will only apply to newly created connections.</p>
        /// </summary>
        /// <param name="compression"> the compression algorithm to use (or <c>Compression.NONE</c> to disable compression).
        ///  </param>
        /// <returns>this <c>ProtocolOptions</c> object.</returns>
        public ProtocolOptions SetCompression(CompressionType compression)
        {
            _compression = compression;
            return this;
        }

        /// <summary>
        /// Sets a custom compressor to be used for the compression type.
        /// If specified, the compression type is mandatory.
        /// If not specified the driver default compressor will be use for the compression type.
        /// </summary>
        /// <param name="compressor">Implementation of IFrameCompressor</param>
        public ProtocolOptions SetCustomCompressor(IFrameCompressor compressor)
        {
            _compressor = compressor;
            return this;
        }
    }
}