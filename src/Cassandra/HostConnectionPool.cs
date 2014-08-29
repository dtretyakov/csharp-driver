using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;

namespace Cassandra
{
    /// <summary>
    /// Represents a pool of connections to a host
    /// </summary>
    internal class HostConnectionPool
    {
        private readonly static Logger _logger = new Logger(typeof(HostConnectionPool));
        private ConcurrentBag<IConnection> _connections;
        private readonly object _poolCreationLock = new object();
        private int _creating;

        private Configuration Configuration { get; set; }

        /// <summary>
        /// Gets a list of connections already opened to the host
        /// </summary>
        public IEnumerable<IConnection> OpenConnections 
        { 
            get
            {
                if (_connections == null)
                {
                    return new Connection[] { };
                }
                return _connections;
            }
        }

        private Host Host { get; set; }

        private HostDistance HostDistance { get; set; }

        public byte ProtocolVersion { get; set; }

        public HostConnectionPool(Host host, HostDistance hostDistance, Configuration configuration)
        {
            Host = host;
            HostDistance = hostDistance;
            Configuration = configuration;
        }

        /// <summary>
        /// Gets an open connection from the host pool (creating if necessary).
        /// It returns null if the load balancing policy didn't allow connections to this host.
        /// </summary>
        public IConnection BorrowConnection()
        {
            MaybeCreateCorePool();
            if (_connections.Count == 0)
            {
                //The load balancing policy stated no connections for this host
                return null;
            }
            var connection = _connections.OrderBy(c => c.InFlight).First();
            MaybeSpawnNewConnection(connection.InFlight);
            return connection;
        }

        private IConnection CreateConnection()
        {
            _logger.Info("Creating a new connection to the host " + Host);
            var connection = new Connection(ProtocolVersion, Host.Address, Configuration);
            TaskHelper.WaitToComplete(connection.ConnectAsync(), Configuration.SocketOptions.SendTimeout);
            return connection;
        }

        /// <summary>
        /// Create the min amount of connections, if the pool is empty
        /// </summary>
        private void MaybeCreateCorePool()
        {
            var coreConnections = Configuration.GetPoolingOptions(ProtocolVersion).GetCoreConnectionsPerHost(HostDistance);
            if (_connections == null || _connections.All(c => c.IsClosed))
            {
                lock(_poolCreationLock)
                {
                    if (_connections != null && !_connections.All(c => c.IsClosed))
                    {
                        return;
                    }
                    _connections = new ConcurrentBag<IConnection>();
                    while (_connections.Count < coreConnections)
                    {
                        try
                        {
                            _connections.Add(CreateConnection());
                        }
                        catch
                        {
                            if (_connections.Count == 0)
                            {
                                //Leave the pool to its previous state
                                _connections = null;
                            }
                            throw;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Creates a new connection, if the conditions apply
        /// </summary>
        private void MaybeSpawnNewConnection(int inFlight)
        {
            var maxInFlight = Configuration.GetPoolingOptions(ProtocolVersion).GetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance);
            var maxConnections = Configuration.GetPoolingOptions(ProtocolVersion).GetMaxConnectionPerHost(HostDistance);
            if (inFlight > maxInFlight)
            {
                if (_connections.Count >= maxConnections)
                {
                    _logger.Warning("Max amount of connections and max amount of in-flight operations reached");
                    return;
                }
                //Only one creation at a time
                if (Interlocked.Increment(ref _creating) == 1)
                {
                    _connections.Add(CreateConnection());
                }
                Interlocked.Decrement(ref _creating);
            }
        }
    }
}
