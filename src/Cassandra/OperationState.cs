﻿using System.IO;
using System.Threading.Tasks;

namespace Cassandra
{
    /// <summary>
    ///     Represents the state of the ongoing operation for the Connection
    /// </summary>
    internal class OperationState
    {
        /// <summary>
        ///     Gets a readable stream representing the body
        /// </summary>
        public Stream BodyStream { get; private set; }

        /// <summary>
        ///     Returns true if there are enough data to parse body
        /// </summary>
        public bool IsBodyComplete
        {
            get
            {
                if (BodyStream is MemoryStream)
                {
                    return true;
                }
                if (BodyStream is ListBackedStream)
                {
                    return BodyStream.Length == Header.BodyLength;
                }
                return false;
            }
        }

        /// <summary>
        ///     8 byte header of the frame
        /// </summary>
        public FrameHeader Header { get; set; }

        public IRequest Request { get; set; }

        public TaskCompletionSource<AbstractResponse> Response { get; private set; }

        public OperationState()
        {
            Response = new TaskCompletionSource<AbstractResponse>();
        }

        /// <summary>
        ///     Appends to the body stream
        /// </summary>
        /// <returns>The total amount of bytes added</returns>
        public int AppendBody(byte[] value, int offset, int count)
        {
            if (Header == null)
            {
                throw new DriverInternalError("To add a response body you must specify the header");
            }
            if (BodyStream == null)
            {
                if (Header.BodyLength <= count)
                {
                    //There is no need to copy the buffer: Use the inner buffer
                    BodyStream = new MemoryStream(value, offset, Header.BodyLength, false, false);
                    return Header.BodyLength;
                }
                BodyStream = new ListBackedStream();
            }
            if (BodyStream.Position + count > Header.BodyLength)
            {
                count = Header.BodyLength - (int) BodyStream.Position;
            }
            if (count > 0)
            {
                BodyStream.Write(value, offset, count);
            }
            return count;
        }
    }
}