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
using System.Collections.Generic;

namespace Cassandra
{
    /// <summary>
    /// 
    /// </summary>
    internal class FrameParser
    {
        /// <summary>
        /// A factory to get the response handlers 
        /// </summary>
        private static readonly Dictionary<FrameOperation, Func<ResponseFrame, AbstractResponse>> ResponseHandlerFactory =
            new Dictionary<FrameOperation, Func<ResponseFrame, AbstractResponse>>
        {
            {FrameOperation.Authenticate, AuthenticateResponse.Create},
            {FrameOperation.Error, ErrorResponse.Create},
            {FrameOperation.Event, EventResponse.Create},
            {FrameOperation.Ready, ReadyResponse.Create},
            {FrameOperation.Result, ResultResponse.Create},
            {FrameOperation.Supported, SupportedResponse.Create},
            {FrameOperation.AuthSuccess, AuthSuccessResponse.Create},
            {FrameOperation.AuthChallenge, AuthChallengeResponse.Create}
        };

        /// <summary>
        /// Parses the response frame
        /// </summary>
        public static AbstractResponse Parse(ResponseFrame frame)
        {
            Func<ResponseFrame, AbstractResponse> handler;
            if (!ResponseHandlerFactory.TryGetValue(frame.Header.Operation, out handler))
            {
                throw new DriverInternalError("Unknown Response Frame type " + frame.Header.Operation);
            }
            return handler(frame);
        }
    }
}