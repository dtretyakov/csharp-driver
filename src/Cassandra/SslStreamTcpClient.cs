using System;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace Cassandra
{
    /// <summary>
    ///     Represents a Tcp connection to a host.
    ///     It emits Read and WriteCompleted events when data is received.
    ///     Similar to Netty's Channel or Node.js's net.Socket
    ///     It handles TLS validation and encryption when required.
    /// </summary>
    internal sealed class SslStreamTcpClient : StreamTcpClient
    {
        private readonly SSLOptions _sslOptions;

        public SslStreamTcpClient(IPEndPoint endPoint, SocketOptions options, SSLOptions sslOptions)
            : base(endPoint, options)
        {
            _sslOptions = sslOptions;
        }

        /// <summary>
        ///     Connects synchronously to the host and starts reading
        /// </summary>
        /// <exception cref="SocketException">Throws a SocketException when the connection could not be established with the host</exception>
        public override async Task ConnectAsync()
        {
            await base.ConnectAsync();

            string targetHost = EndPoint.Address.ToString();
            try
            {
                targetHost = _sslOptions.HostNameResolver(EndPoint.Address);
            }
            catch (Exception ex)
            {
                Logger.Error(
                    String.Format(
                        "SSL connection: Can not resolve host name for address {0}. Using the IP address instead of the host name. This may cause RemoteCertificateNameMismatch error during Cassandra host authentication. Note that the Cassandra node SSL certificate's CN(Common Name) must match the Cassandra node hostname.",
                        targetHost), ex);
            }

            var sslStream = new SslStream(SocketStream, false, _sslOptions.RemoteCertValidationCallback, null);
            await sslStream.AuthenticateAsClientAsync(targetHost,
                _sslOptions.CertificateCollection,
                _sslOptions.SslProtocol,
                _sslOptions.CheckCertificateRevocation);

            SocketStream = sslStream;
        }
    }
}