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

using NUnit.Framework;
using System;

namespace Cassandra.IntegrationTests.Core
{
    public class RequestHandlerTests : SingleNodeClusterTest
    {
        private new Session Session
        {
            get
            {
                //We will need the solid session for the internal methods.
                return (Session)base.Session;
            }
        }

        [Test]
        public void RequestHandlerRetryDecisionTest()
        {
            var statement = new SimpleStatement("SELECT WILL FAIL");
            var request = Session.GetRequest(statement);
            var requestHandler = new RequestHandler<RowSet>(Session, request, statement);

            //Using default retry policy the decision will always be to rethrow on read/write timeout
            var expected = RetryDecision.RetryDecisionType.Rethrow;
            var decision = requestHandler.GetRetryDecision(new ReadTimeoutException(ConsistencyLevel.Quorum, 1, 2, true));
            Assert.AreEqual(expected, decision.DecisionType);
            
            decision = requestHandler.GetRetryDecision(new WriteTimeoutException(ConsistencyLevel.Quorum, 1, 2, "SIMPLE"));
            Assert.AreEqual(expected, decision.DecisionType);

            decision = requestHandler.GetRetryDecision(new UnavailableException(ConsistencyLevel.Quorum, 2, 1));
            Assert.AreEqual(expected, decision.DecisionType);

            decision = requestHandler.GetRetryDecision(new Exception());
            Assert.AreEqual(expected, decision.DecisionType);

            //Expecting to retry when a Cassandra node is Bootstrapping/overloaded
            expected = RetryDecision.RetryDecisionType.Retry;
            decision = requestHandler.GetRetryDecision(new OverloadedException(null));
            Assert.AreEqual(expected, decision.DecisionType);
            decision = requestHandler.GetRetryDecision(new IsBootstrappingException(null));
            Assert.AreEqual(expected, decision.DecisionType);
            decision = requestHandler.GetRetryDecision(new TruncateException(null));
            Assert.AreEqual(expected, decision.DecisionType);
        }
    }
}
