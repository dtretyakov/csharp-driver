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
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace Cassandra
{
    /// <summary>
    /// Handles a request to cassandra, dealing with host failover and retries on error
    /// </summary>
    internal class RequestHandler<T>
    {
        private readonly static Logger Logger = new Logger(typeof(Session));

        private IConnection _connection;
        private Host _currentHost;
        private readonly IRequest _request;
        private readonly IRetryPolicy _retryPolicy;
        private readonly Session _session;
        private readonly IStatement _statement;
        private int _retryCount;
        private readonly Dictionary<IPEndPoint, Exception> _triedHosts = new Dictionary<IPEndPoint, Exception>();

        /// <summary>
        /// Creates a new instance of the RequestHandler that deals with host failover and retries on error
        /// </summary>
        public RequestHandler(Session session, IRequest request, IStatement statement)
        {
            _session = session;
            _request = request;
            _statement = statement;
            _retryPolicy = Policies.DefaultRetryPolicy;
            //Don't use Argument null check to allow unit test
            if (session != null && session.Policies != null && session.Policies.RetryPolicy != null)
            {
                _retryPolicy = session.Policies.RetryPolicy;
            }
            if (statement != null && statement.RetryPolicy != null)
            {
                _retryPolicy = statement.RetryPolicy;
            }
        }

        /// <summary>
        /// Determines if the host, due to the connection error can be resurrected if no other host is alive.
        /// </summary>
        private static bool CanBeResurrected(SocketException ex, IConnection connection)
        {
            if (connection == null || connection.IsDisposed)
            {
                //It was never connected or the connection is being disposed manually
                return false;
            }
            var isNetworkReset = false;
            switch (ex.SocketErrorCode)
            {
                case SocketError.ConnectionRefused:
                case SocketError.TimedOut:
                case SocketError.ConnectionReset:
                case SocketError.ConnectionAborted:
                case SocketError.Fault:
                case SocketError.Interrupted:
                    isNetworkReset = true;
                    break;
            }
            return isNetworkReset;
        }

        /// <summary>
        /// Fills the common properties of the RowSet
        /// </summary>
        private RowSet FillRowSet(RowSet rs)
        {
            rs.Info.SetTriedHosts(_triedHosts.Keys.ToList());
            if (_request is ICqlRequest)
            {
                rs.Info.SetAchievedConsistency(((ICqlRequest)_request).Consistency);
            }
            if (rs.PagingState != null && _request is IQueryRequest && typeof(T) == typeof(RowSet))
            {
                rs.FetchNextPage = pagingState =>
                {
                    if (_session.IsDisposed)
                    {
                        Logger.Warning("Trying to page results using a Session already disposed.");
                        return new RowSet();
                    }
                    ((IQueryRequest)_request).PagingState = pagingState;
                    var task = new RequestHandler<RowSet>(_session, _request, _statement).SendAsync();
                    TaskHelper.WaitToComplete(task, _session.Configuration.ClientOptions.QueryAbortTimeout);
                    return task.Result;
                };
            }
            return rs;
        }

        /// <summary>
        /// Gets a connection from the next host according to the load balancing policy
        /// </summary>
        /// <exception cref="NoHostAvailableException"/>
        /// <exception cref="InvalidQueryException">When keyspace does not exist</exception>
        /// <exception cref="UnsupportedProtocolVersionException"/>
        internal IConnection GetNextConnection(IStatement statement, bool isLastChance = false)
        {
            var hostEnumerable = _session.Policies.LoadBalancingPolicy.NewQueryPlan(statement);
            Host lastChanceHost = null;
            //hostEnumerable GetEnumerator will return a NEW enumerator, making this call thread safe
            foreach (var host in hostEnumerable)
            {
                if (!host.IsConsiderablyUp)
                {
                    if (!isLastChance && host.Resurrect)
                    {
                        lastChanceHost = host;
                    }
                    continue;
                }
                _currentHost = host;
                _triedHosts[host.Address] = null;
                IConnection connection = null;
                try
                {
                    var distance = _session.Policies.LoadBalancingPolicy.Distance(host);
                    var hostPool = _session.GetConnectionPool(host, distance);
                    connection = hostPool.BorrowConnection();
                    if (connection == null)
                    {
                        continue;
                    }
                    connection.Keyspace = _session.Keyspace;
                    return connection;
                }
                catch (SocketException ex)
                {
                    _session.SetHostDown(host, connection);
                    _triedHosts[host.Address] = ex;
                    host.Resurrect = CanBeResurrected(ex, connection);
                    if (!isLastChance && host.Resurrect)
                    {
                        lastChanceHost = host;
                    }
                }
                catch (InvalidQueryException)
                {
                    //The keyspace does not exist
                    throw;
                }
                catch (UnsupportedProtocolVersionException)
                {
                    //The version of the protocol is not supported
                    throw;
                }
                catch (Exception ex)
                {
                    Logger.Error(ex);
                    _triedHosts[host.Address] = ex;
                }
            }
            _currentHost = null;
            if (lastChanceHost != null)
            {
                //There are no host available and some of them are due to network events.
                //Probably there was a network event that reset all connections and it does not mean the connection
                Logger.Warning("Suspected network reset. Getting one host up and retrying for a last chance");
                lastChanceHost.BringUpIfDown();
                return GetNextConnection(statement, true);
            }
            throw new NoHostAvailableException(_triedHosts);
        }

        /// <summary>
        /// Gets the retry decision based on the exception from Cassandra
        /// </summary>
        public RetryDecision GetRetryDecision(Exception ex)
        {
            RetryDecision decision = RetryDecision.Rethrow();
            if (ex is SocketException)
            {
                decision = RetryDecision.Retry(null);
            }
            else if (ex is OverloadedException || ex is IsBootstrappingException || ex is TruncateException)
            {
                decision = RetryDecision.Retry(null);
            }
            else if (ex is ReadTimeoutException)
            {
                var e = ex as ReadTimeoutException;
                decision = _retryPolicy.OnReadTimeout(_statement, e.ConsistencyLevel, e.RequiredAcknowledgements, e.ReceivedAcknowledgements, e.WasDataRetrieved, _retryCount);
            }
            else if (ex is WriteTimeoutException)
            {
                var e = ex as WriteTimeoutException;
                decision = _retryPolicy.OnWriteTimeout(_statement, e.ConsistencyLevel, e.WriteType, e.RequiredAcknowledgements, e.ReceivedAcknowledgements, _retryCount);
            }
            else if (ex is UnavailableException)
            {
                var e = ex as UnavailableException;
                decision = _retryPolicy.OnUnavailable(_statement, e.Consistency, e.RequiredReplicas, e.AliveReplicas, _retryCount);
            }
            return decision;
        }

        /// <summary>
        /// Creates the prepared statement and transitions the task to completed
        /// </summary>
        private T HandlePreparedResult(AbstractResponse response)
        {
            ValidateResult(response);
            var output = ((ResultResponse)response).Output;
            if (!(output is OutputPrepared))
            {
                throw new DriverInternalError("Expected prepared response, obtained " + output.GetType().FullName);
            }
            if (!(_request is PrepareRequest))
            {
                throw new DriverInternalError("Obtained PREPARED response for " + _request.GetType().FullName + " request");
            }
            var prepared = (OutputPrepared)output;
            var statement = new PreparedStatement(prepared.Metadata, prepared.QueryId, ((PrepareRequest)_request).Query, prepared.ResultMetadata);
            return (T)(object)statement;
        }

        /// <summary>
        /// Gets the resulting RowSet and transitions the task to completed.
        /// </summary>
        private T HandleRowSetResult(AbstractResponse response)
        {
            ValidateResult(response);
            var output = ((ResultResponse)response).Output;
            RowSet rs;
            if (output is OutputRows)
            {
                rs = ((OutputRows)output).RowSet;
            }
            else
            {
                if (output is OutputSetKeyspace)
                {
                    _session.Keyspace = ((OutputSetKeyspace)output).Value;
                }
                rs = new RowSet();
            }
            if (output.TraceId != null)
            {
                rs.Info.SetQueryTrace(new QueryTrace(output.TraceId.Value, _session));
            }
            FillRowSet(rs);
            return (T)(object)rs;
        }

        private async Task PrepareQueryAsync(byte[] id)
        {
            Logger.Info(String.Format("Query {0} is not prepared on {1}, preparing before retrying executing.", BitConverter.ToString(id),
                _currentHost.Address));

            BoundStatement boundStatement = null;
            if (_statement is BoundStatement)
            {
                boundStatement = (BoundStatement) _statement;
            }
            else if (_statement is BatchStatement)
            {
                var batch = (BatchStatement) _statement;
                Func<Statement, bool> search = s => s is BoundStatement && ((BoundStatement) s).PreparedStatement.Id.SequenceEqual(id);
                boundStatement = (BoundStatement) batch.Queries.FirstOrDefault(search);
            }
            if (boundStatement == null)
            {
                throw new DriverInternalError("Expected Bound or batch statement");
            }

            var request = new PrepareRequest(_request.ProtocolVersion, boundStatement.PreparedStatement.Cql);
            var response = await _connection.SendAsync(request).ConfigureAwait(false);
            ValidateResult(response);

            var output = ((ResultResponse)response).Output;
            if (!(output is OutputPrepared))
            {
                throw new DriverInternalError("Expected prepared response, obtained " + output.GetType().FullName);
            }

            response = await _connection.SendAsync(_request).ConfigureAwait(false);
            HandleResponse(response);
        }

        /// <summary>
        /// Generic handler for all the responses
        /// </summary>
        public T HandleResponse(AbstractResponse response)
        {
            if (typeof (T) == typeof (RowSet))
            {
                return HandleRowSetResult(response);
            }

            if (typeof (T) == typeof (PreparedStatement))
            {
                return HandlePreparedResult(response);
            }

            throw new DriverInternalError(string.Format("Invalid response {0}", response.GetType().FullName));
        }

        /// <summary>
        /// Sends a request asynchronously.
        /// </summary>
        /// <returns>Execution result.</returns>
        public async Task<T> SendAsync()
        {
            T result = default (T);
            bool retry;

            do
            {
                byte[] prepareQuery = null;
                Exception exception;
                try
                {
                    _connection = GetNextConnection(_statement);
                    AbstractResponse response = await _connection.SendAsync(_request).ConfigureAwait(false);
                    return HandleResponse(response);
                }
                catch (PreparedQueryNotFoundException e)
                {
                    exception = e;
                    if (_statement is BoundStatement || _statement is BatchStatement)
                    {
                        prepareQuery = e.UnknownId;
                    }
                }
                catch (SocketException e)
                {
                    exception = e;
                    Logger.Verbose("Socket error " + e.SocketErrorCode);
                    _session.SetHostDown(_currentHost, _connection);
                    if (!_currentHost.IsUp)
                    {
                        _currentHost.Resurrect = CanBeResurrected(e, _connection);
                    }
                }
                catch (Exception e)
                {
                    exception = e;
                }

                if (prepareQuery != null)
                {
                    await PrepareQueryAsync(prepareQuery).ConfigureAwait(false);
                    retry = true;
                }
                else
                {
                    RetryDecision decision = GetRetryDecision(exception);
                    retry = HandleRetryDecision(decision, exception, out result);    
                }
                
            } while (retry);

            return result;
        }

        private bool HandleRetryDecision(RetryDecision decision, Exception exception, out T result)
        {
            result = default(T);
            switch (decision.DecisionType)
            {
                case RetryDecision.RetryDecisionType.Rethrow:
                    throw TaskHelper.PreserveStackTrace(exception);

                case RetryDecision.RetryDecisionType.Ignore:
                    //The error was ignored by the RetryPolicy
                    //Try to give a decent response
                    if (typeof(T).IsAssignableFrom(typeof(RowSet)))
                    {
                        var rs = new RowSet();
                        result = (T)(object)FillRowSet(rs);
                    }
                    return false;

                case RetryDecision.RetryDecisionType.Retry:
                    //Retry the Request using the new consistency level
                    _retryCount++;
                    if (decision.RetryConsistencyLevel != null && _request is ICqlRequest)
                    {
                        //Set the new consistency to be used for the new request
                        ((ICqlRequest) _request).Consistency = decision.RetryConsistencyLevel.Value;
                    }
                    return true;

                default:
                    throw new DriverInternalError(string.Format("Invalid retry decision {0}", decision));
            }
        }

        private void ValidateResult(AbstractResponse response)
        {
            if (response == null)
            {
                throw new DriverInternalError("Response can not be null");
            }
            if (!(response is ResultResponse))
            {
                throw new DriverInternalError("Excepted ResultResponse, obtained " + response.GetType().FullName);
            }
        }
    }
}
