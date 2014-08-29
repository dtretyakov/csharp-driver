﻿using Moq;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cassandra.IntegrationTests.Core
{
    [Timeout(600000)]
    public class ConnectionTests : SingleNodeClusterTest
    {
        protected override bool ConnectToCluster
        {
            get
            {
                //Do not create a session and cluster for the ccm cluster
                return false;
            }
        }

        public ConnectionTests()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
        }

        [Test]
        public void BasicStartupTest()
        {
            using (var connection = CreateConnection())
            {
                Assert.DoesNotThrow(connection.ConnectAsync().Wait);
            }
        }

        [Test]
        public void BasicQueryTest()
        {
            using (var connection = CreateConnection())
            {
                connection.ConnectAsync().Wait();
                //Start a query
                var request = new QueryRequest(connection.ProtocolVersion, "SELECT * FROM system.schema_keyspaces", false, QueryProtocolOptions.Default);
                var task = connection.SendAsync(request);
                task.Wait();
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                //Result from Cassandra
                var output = ValidateResult<OutputRows>(task.Result);
                var rs = output.RowSet;
                var rows = rs.ToList();
                Assert.Greater(rows.Count, 0);
                Assert.True(rows[0].GetValue<string>("keyspace_name") != null, "It should contain a keyspace name");
            }
        }

        [Test]
        public void PrepareQuery()
        {
            using (var connection = CreateConnection())
            {
                connection.ConnectAsync().Wait();
                var request = new PrepareRequest(connection.ProtocolVersion, "SELECT * FROM system.schema_keyspaces");
                var task = connection.SendAsync(request);
                task.Wait();
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                var output = ValidateResult<OutputPrepared>(task.Result);
            }
        }

        [Test]
        public void PrepareResponseErrorFaultsTask()
        {
            using (var connection = CreateConnection())
            {
                connection.ConnectAsync().Wait();
                var request = new PrepareRequest(connection.ProtocolVersion, "SELECT WILL FAIL");
                var task = connection.SendAsync(request);
                task.ContinueWith(t =>
                {
                    Assert.AreEqual(TaskStatus.Faulted, t.Status);
                    Assert.IsInstanceOf<SyntaxError>(t.Exception.InnerException);
                }, TaskContinuationOptions.ExecuteSynchronously).Wait();
            }
        }

        [Test]
        public void ExecutePreparedTest()
        {
            using (var connection = CreateConnection())
            {
                connection.ConnectAsync().Wait();

                //Prepare a query
                var prepareRequest = new PrepareRequest(connection.ProtocolVersion, "SELECT * FROM system.schema_keyspaces");
                var task = connection.SendAsync(prepareRequest);
                var prepareOutput = ValidateResult<OutputPrepared>(task.Result);
                
                //Execute the prepared query
                var executeRequest = new ExecuteRequest(connection.ProtocolVersion, prepareOutput.QueryId, null, false, QueryProtocolOptions.Default);
                task = connection.SendAsync(executeRequest);
                var output = ValidateResult<OutputRows>(task.Result);
                var rs = output.RowSet;
                var rows = rs.ToList();
                Assert.Greater(rows.Count, 0);
                Assert.True(rows[0].GetValue<string>("keyspace_name") != null, "It should contain a keyspace name");
            }
        }

        [Test]
        public void ExecutePreparedWithParamTest()
        {
            using (var connection = CreateConnection())
            {
                connection.ConnectAsync().Wait();

                var prepareRequest = new PrepareRequest(connection.ProtocolVersion, "SELECT * FROM system.schema_columnfamilies WHERE keyspace_name = ?");
                var task = connection.SendAsync(prepareRequest);
                var prepareOutput = ValidateResult<OutputPrepared>(task.Result);

                var options = new QueryProtocolOptions(ConsistencyLevel.One, new[] { "system" }, false, 100, null, ConsistencyLevel.Any);

                var executeRequest = new ExecuteRequest(connection.ProtocolVersion, prepareOutput.QueryId, null, false, options);
                task = connection.SendAsync(executeRequest);
                var output = ValidateResult<OutputRows>(task.Result);

                var rows = output.RowSet.ToList();
                Assert.Greater(rows.Count, 0);
                Assert.True(rows[0].GetValue<string>("columnfamily_name") != null, "It should contain a column family name");
            }
        }

        [Test]
        [TestCassandraVersion(2, 0)]
        public void QueryCompressionLZ4Test()
        {
            var protocolOptions = new ProtocolOptions().SetCompression(CompressionType.LZ4);
            using (var connection = CreateConnection(protocolOptions))
            {
                connection.ConnectAsync().Wait();

                //Start a query
                var task = Query(connection, "SELECT * FROM system.schema_keyspaces", QueryProtocolOptions.Default);
                task.Wait(360000);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                var output = ValidateResult<OutputRows>(task.Result);
                var rs = output.RowSet;
                var rows = rs.ToList();
                Assert.Greater(rows.Count, 0);
                Assert.True(rows[0].GetValue<string>("keyspace_name") != null, "It should contain a keyspace name");
            }
        }

        [Test]
        public void QueryCompressionSnappyTest()
        {
            var protocolOptions = new ProtocolOptions().SetCompression(CompressionType.Snappy);
            using (var connection = CreateConnection(protocolOptions))
            {
                connection.ConnectAsync().Wait();

                //Start a query
                var task = Query(connection, "SELECT * FROM system.schema_keyspaces", QueryProtocolOptions.Default);
                task.Wait(360000);
                Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                var output = ValidateResult<OutputRows>(task.Result);
                var rs = output.RowSet;
                var rows = rs.ToList();
                Assert.Greater(rows.Count, 0);
                Assert.True(rows[0].GetValue<string>("keyspace_name") != null, "It should contain a keyspace name");
            }
        }

        /// <summary>
        /// Test that a Response error from Cassandra results in a faulted task
        /// </summary>
        [Test]
        public void QueryResponseErrorFaultsTask()
        {
            using (var connection = CreateConnection())
            {
                connection.ConnectAsync().Wait();

                //Start a query
                var task = Query(connection, "SELECT WILL FAIL", QueryProtocolOptions.Default);
                task.ContinueWith(t =>
                {
                    Assert.AreEqual(TaskStatus.Faulted, t.Status);
                    Assert.IsInstanceOf<SyntaxError>(t.Exception.InnerException);
                }, TaskContinuationOptions.ExecuteSynchronously).Wait();
            }
        }

        [Test]
        public void QueryMultipleAsyncTest()
        {
            //Try to fragment the message
            var socketOptions = new SocketOptions().SetReceiveBufferSize(128);
            using (var connection = CreateConnection(null, socketOptions))
            {
                connection.ConnectAsync().Wait();
                var taskList = new List<Task<AbstractResponse>>();
                //Run a query multiple times
                for (var i = 0; i < 16; i++)
                {
                    //schema_columns
                    taskList.Add(Query(connection, "SELECT * FROM system.schema_keyspaces", QueryProtocolOptions.Default));
                }
                Task.WaitAll(taskList.ToArray());
                foreach (var t in taskList)
                {
                    Assert.AreEqual(TaskStatus.RanToCompletion, t.Status);
                    Assert.NotNull(t.Result);
                }
            }
        }

        [Test]
        public void QueryMultipleAsyncConsumeAllStreamIdsTest()
        {
            using (var connection = CreateConnection())
            {
                connection.ConnectAsync().Wait();
                var taskList = new List<Task>();
                //Run the query more times than the max allowed
                for (var i = 0; i < connection.MaxConcurrentRequests * 2; i++)
                {
                    taskList.Add(Query(connection, "SELECT * FROM system.schema_keyspaces", QueryProtocolOptions.Default));
                }
                Task.WaitAll(taskList.ToArray());
                Assert.True(taskList.All(t => t.Status == TaskStatus.RanToCompletion), "Not all task completed");
            }
        }

        [Test]
        public void QueryMultipleSyncTest()
        {
            using (var connection = CreateConnection())
            {
                connection.ConnectAsync().Wait();
                //Run a query multiple times
                for (var i = 0; i < 8; i++)
                {
                    var task = Query(connection, "SELECT * FROM system.schema_keyspaces", QueryProtocolOptions.Default);
                    task.Wait(1000);
                    Assert.AreEqual(TaskStatus.RanToCompletion, task.Status);
                    Assert.NotNull(task.Result);
                }
            }
        }

        [Test]
        public void RegisterForEvents()
        {
            var eventHandle = new AutoResetEvent(false);
            CassandraEventArgs eventArgs = null;
            using (var connection = CreateConnection())
            {
                connection.ConnectAsync().Wait();
                var eventTypes = CassandraEventType.TopologyChange | CassandraEventType.StatusChange | CassandraEventType.SchemaChange;
                var task = connection.SendAsync(new RegisterForEventRequest(connection.ProtocolVersion, eventTypes));
                TaskHelper.WaitToComplete(task, 1000);
                Assert.IsInstanceOf<ReadyResponse>(task.Result);
                connection.CassandraEventResponse += (o, e) =>
                {
                    eventArgs = e;
                    eventHandle.Set();
                };
                //create a keyspace and check if gets received as an event
                Query(connection, String.Format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, "test_events_kp", 1)).Wait(1000);
                eventHandle.WaitOne(2000);
                Assert.IsNotNull(eventArgs);
                Assert.IsInstanceOf<SchemaChangeEventArgs>(eventArgs);
                Assert.AreEqual(SchemaChangeEventArgs.Reason.Created, (eventArgs as SchemaChangeEventArgs).What);
                Assert.AreEqual("test_events_kp", (eventArgs as SchemaChangeEventArgs).Keyspace);
                Assert.IsNullOrEmpty((eventArgs as SchemaChangeEventArgs).Table);

                //create a table and check if gets received as an event
                Query(connection, String.Format(TestUtils.CREATE_TABLE_ALL_TYPES, "test_events_kp.test_table", 1)).Wait(1000);
                eventHandle.WaitOne(2000);
                Assert.IsNotNull(eventArgs);
                Assert.IsInstanceOf<SchemaChangeEventArgs>(eventArgs);

                Assert.AreEqual(SchemaChangeEventArgs.Reason.Created, (eventArgs as SchemaChangeEventArgs).What);
                Assert.AreEqual("test_events_kp", (eventArgs as SchemaChangeEventArgs).Keyspace);
                Assert.AreEqual("test_table", (eventArgs as SchemaChangeEventArgs).Table);

                if (connection.ProtocolVersion >= 3)
                {
                    //create a udt type
                    Query(connection, "CREATE TYPE test_events_kp.test_type (street text, city text, zip int);").Wait(1000);
                    eventHandle.WaitOne(2000);
                    Assert.IsNotNull(eventArgs);
                    Assert.IsInstanceOf<SchemaChangeEventArgs>(eventArgs);

                    Assert.AreEqual(SchemaChangeEventArgs.Reason.Created, (eventArgs as SchemaChangeEventArgs).What);
                    Assert.AreEqual("test_events_kp", (eventArgs as SchemaChangeEventArgs).Keyspace);
                    Assert.AreEqual("test_type", (eventArgs as SchemaChangeEventArgs).Type);
                }
            }
        }

        [Test]
        public void StreamModeReadAndWrite()
        {
            var socketOptions = new SocketOptions();
            using (var connection = CreateConnection(new ProtocolOptions(), new SocketOptions().SetStreamMode(true)))
            {
                connection.ConnectAsync().Wait();

                var taskList = new List<Task<AbstractResponse>>();
                //Run the query multiple times
                for (var i = 0; i < 129; i++)
                {
                    taskList.Add(Query(connection, "SELECT * FROM system.schema_columns", QueryProtocolOptions.Default));
                }
                Task.WaitAll(taskList.ToArray());
                Assert.True(taskList.All(t => t.Status == TaskStatus.RanToCompletion), "Not all task completed");

                //One last time
                var task = Query(connection, "SELECT * FROM system.schema_keyspaces");
                Assert.True(task.Result != null);
            }
        }

        [Test]
        [Explicit]
        public void SslTest()
        {
            var certs = new X509CertificateCollection();
            certs.Add(new X509Certificate(@"D:\var\ssl\cassandra_cert2.crt"));
            RemoteCertificateValidationCallback callback = (s, cert, chain, policyErrors) =>
            {
                if (policyErrors == SslPolicyErrors.None)
                {
                    return true; 
                }
                if (policyErrors == SslPolicyErrors.RemoteCertificateChainErrors && 
                    chain.ChainStatus.Length == 1 && 
                    chain.ChainStatus[0].Status == X509ChainStatusFlags.UntrustedRoot)
                {
                    //self issued
                    return true;
                }
                return false;
            };
            var sslOptions = new SSLOptions().SetCertificateCollection(certs).SetRemoteCertValidationCallback(callback);
            using (var connection = CreateConnection(new ProtocolOptions(ProtocolOptions.DefaultPort, sslOptions)))
            {
                connection.ConnectAsync().Wait();

                var taskList = new List<Task<AbstractResponse>>();
                //Run the query multiple times
                for (var i = 0; i < 129; i++)
                {
                    taskList.Add(Query(connection, "SELECT * FROM system.schema_columns", QueryProtocolOptions.Default));
                }
                Task.WaitAll(taskList.ToArray());
                Assert.True(taskList.All(t => t.Status == TaskStatus.RanToCompletion), "Not all task completed");

                //One last time
                var task = Query(connection, "SELECT * FROM system.schema_keyspaces");
                Assert.True(task.Result != null);
            }
        }
        
        [Test]
        [Explicit]
        public void AuthenticationWithV2Test()
        {
            byte protocolVersion = 2;
            var config = new Configuration(
                new Cassandra.Policies(),
                new ProtocolOptions(),
                new PoolingOptions(),
                new SocketOptions(),
                new ClientOptions(),
                new PlainTextAuthProvider("username", "password"),
                null,
                new QueryOptions(),
                new DefaultAddressTranslator());
            using (var connection = CreateConnection(protocolVersion, config))
            {
                //Authentication will happen on init
                connection.ConnectAsync().Wait();
                //Try to query
                var r = TaskHelper.WaitToComplete(Query(connection, "SELECT * FROM system.schema_keyspaces"), 60000);
                Assert.IsInstanceOf<ResultResponse>(r);
            }

            //Check that it throws an authentication exception when credentials are invalid.
            config = new Configuration(
                new Cassandra.Policies(),
                new ProtocolOptions(),
                new PoolingOptions(),
                new SocketOptions(),
                new ClientOptions(),
                new PlainTextAuthProvider("WRONGUSERNAME", "password"),
                null,
                new QueryOptions(),
                new DefaultAddressTranslator());
            using (var connection = CreateConnection(protocolVersion, config))
            {
                Assert.Throws<AuthenticationException>(() => connection.ConnectAsync().Wait());
            }
        }

        [Test]
        [Explicit]
        public void AuthenticationWithV1Test()
        {
            byte protocolVersion = 1;
            var config = new Configuration(
                new Cassandra.Policies(),
                new ProtocolOptions(),
                new PoolingOptions(),
                new SocketOptions(),
                new ClientOptions(),
                NoneAuthProvider.Instance,
                new SimpleAuthInfoProvider(new Dictionary<string, string> { { "username", "username" }, {"password", "password"} }),
                new QueryOptions(),
                new DefaultAddressTranslator());
            using (var connection = CreateConnection(protocolVersion, config))
            {
                //Authentication will happen on init
                connection.ConnectAsync().Wait();
                //Try to query
                var r = TaskHelper.WaitToComplete(Query(connection, "SELECT * FROM system.schema_keyspaces"), 60000);
                Assert.IsInstanceOf<ResultResponse>(r);
            }

            //Check that it throws an authentication exception when credentials are invalid.
            config = new Configuration(
                new Cassandra.Policies(),
                new ProtocolOptions(),
                new PoolingOptions(),
                new SocketOptions(),
                new ClientOptions(),
                NoneAuthProvider.Instance,
                new SimpleAuthInfoProvider(new Dictionary<string, string> { { "username", "WRONGUSERNAME" }, { "password", "password" } }),
                new QueryOptions(),
                new DefaultAddressTranslator());
            using (var connection = CreateConnection(protocolVersion, config))
            {
                Assert.Throws<AuthenticationException>(() => connection.ConnectAsync().Wait());
            }
        }

        [Test]
        public void UseKeyspaceTest()
        {
            using (var connection = CreateConnection())
            {
                connection.ConnectAsync().Wait();
                Assert.Null(connection.Keyspace);
                connection.Keyspace = "system";
                //If it was executed correctly, it should be set
                Assert.AreEqual("system", connection.Keyspace);
                //Execute a query WITHOUT the keyspace prefix
                TaskHelper.WaitToComplete(Query(connection, "SELECT * FROM schema_keyspaces", QueryProtocolOptions.Default));
            }
        }

        [Test]
        public void WrongIpInitThrowsException()
        {
            var socketOptions = new SocketOptions();
            socketOptions.SetConnectTimeoutMillis(1000);
            var config = new Configuration(
                new Cassandra.Policies(), 
                new ProtocolOptions(), 
                new PoolingOptions(), 
                socketOptions, 
                new ClientOptions(), 
                NoneAuthProvider.Instance,
                null,
                new QueryOptions(),
                new DefaultAddressTranslator());
            try
            {
                using (var connection = new Connection(1, new IPEndPoint(new IPAddress(new byte[] { 1, 1, 1, 1 }), 9042), config))
                {
                    connection.ConnectAsync().Wait();
                    Assert.Fail("It must throw an exception");
                }
            }
            catch (SocketException ex)
            {
                //It should have timed out
                Assert.AreEqual(SocketError.TimedOut, ex.SocketErrorCode);
            }
            try
            {
                using (var connection = new Connection(1, new IPEndPoint(new IPAddress(new byte[] { 255, 255, 255, 255 }), 9042), config))
                {
                    connection.ConnectAsync().Wait();
                    Assert.Fail("It must throw an exception");
                }
            }
            catch (SocketException)
            {
                //Socket exception is just fine.
            }
        }

        [Test]
        public void ConnectionCloseFaultsAllPendingTasks()
        {
            var connection = CreateConnection();
            connection.ConnectAsync().Wait();
            //Queue a lot of read and writes
            var taskList = new List<Task<AbstractResponse>>();
            for (var i = 0; i < 1024; i++)
            {
                taskList.Add(Query(connection, "SELECT * FROM system.schema_keyspaces"));
            }
            //Wait for the first to finish
            ValidateResult<OutputRows>(taskList[0].Result);
            Assert.Greater(connection.InFlight, 0);

            //Close the socket, this would trigger all pending ops to be called back
            connection.Dispose();
            try
            {
                Task.WaitAll(taskList.ToArray());
            }
            catch (AggregateException)
            {
                //Its alright, it will fail
            }

            Assert.Greater(taskList.Count(t => t.Status == TaskStatus.RanToCompletion), 0);
            Assert.Greater(taskList.Count(t => t.Status == TaskStatus.Faulted), 0);
            Assert.True(!taskList.Any(t => t.Status != TaskStatus.RanToCompletion && t.Status != TaskStatus.Faulted), "Must be only completed and faulted task");

            //A new call to write will be called back immediately with an exception
            var task = Query(connection, "SELECT * FROM system.schema_keyspaces");
            //It will throw
            Assert.Throws<AggregateException>(() => task.Wait(50));
        }

        /// <summary>
        /// It checks that the connection startup method throws an exception when using a greater protocol version
        /// </summary>
        [Test]
        [TestCassandraVersion(2, 0, IntegrationTests.Comparison.LessThan)]
        public void StartupGreaterProtocolVersionThrows()
        {
            const byte protocolVersion = 2;
            using (var connection = CreateConnection(protocolVersion, new Configuration()))
            {
                Assert.Throws<UnsupportedProtocolVersionException>(() => connection.ConnectAsync().Wait());
            }
        }

        private Connection CreateConnection(ProtocolOptions protocolOptions = null, SocketOptions socketOptions = null)
        {
            if (socketOptions == null)
            {
                socketOptions = new SocketOptions();
            }
            if (protocolOptions == null)
            {
                protocolOptions = new ProtocolOptions();
            }
            var config = new Configuration(
                new Cassandra.Policies(),
                protocolOptions,
                null,
                socketOptions,
                new ClientOptions(),
                NoneAuthProvider.Instance,
                null,
                new QueryOptions(),
                new DefaultAddressTranslator());
            return CreateConnection(GetLatestProtocolVersion(), config);
        }

        /// <summary>
        /// Gets the latest protocol depending on the Cassandra Version running the tests
        /// </summary>
        private byte GetLatestProtocolVersion()
        {
            var cassandraVersion = Options.Default.CassandraVersion;
            byte protocolVersion = 1;
            if (cassandraVersion >= Version.Parse("2.1"))
            {
                protocolVersion = 3;
            }
            else if (cassandraVersion > Version.Parse("2.0"))
            {
                protocolVersion = 2;
            }
            return protocolVersion;
        }

        private Connection CreateConnection(byte protocolVersion, Configuration config)
        {
            Trace.TraceInformation("Creating test connection using protocol v{0}", protocolVersion);
            return new Connection(protocolVersion, new IPEndPoint(new IPAddress(new byte[] { 127, 0, 0, 1 }), 9042), config);
        }

        private static Task<AbstractResponse> Query(Connection connection, string query, QueryProtocolOptions options = null)
        {
            if (options == null)
            {
                options = QueryProtocolOptions.Default;
            }
            var request = new QueryRequest(connection.ProtocolVersion, query, false, options);
            return connection.SendAsync(request);
        }

        private static T ValidateResult<T>(AbstractResponse response)
        {
            Assert.IsInstanceOf<ResultResponse>(response);
            Assert.IsInstanceOf<T>(((ResultResponse)response).Output);
            return (T)((ResultResponse)response).Output;
        }
    }
}
