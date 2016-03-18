/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandraBloomFilters.streaming.messages;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.UUID;

import org.apache.cassandraBloomFilters.io.util.DataInputPlus;
import org.apache.cassandraBloomFilters.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandraBloomFilters.io.util.DataOutputStreamPlus;
import org.apache.cassandraBloomFilters.net.MessagingService;
import org.apache.cassandraBloomFilters.streaming.StreamSession;
import org.apache.cassandraBloomFilters.utils.UUIDSerializer;

public class RetryMessage extends StreamMessage
{
    public static Serializer<RetryMessage> serializer = new Serializer<RetryMessage>()
    {
        @SuppressWarnings("resource") // Not closing constructed DataInputPlus's as the channel needs to remain open.
        public RetryMessage deserialize(ReadableByteChannel in, int version, StreamSession session) throws IOException
        {
            DataInputPlus input = new DataInputStreamPlus(Channels.newInputStream(in));
            return new RetryMessage(UUIDSerializer.serializer.deserialize(input, MessagingService.current_version), input.readInt());
        }

        public void serialize(RetryMessage message, DataOutputStreamPlus out, int version, StreamSession session) throws IOException
        {
            UUIDSerializer.serializer.serialize(message.cfId, out, MessagingService.current_version);
            out.writeInt(message.sequenceNumber);
        }
    };

    public final UUID cfId;
    public final int sequenceNumber;

    public RetryMessage(UUID cfId, int sequenceNumber)
    {
        super(Type.RETRY);
        this.cfId = cfId;
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("Retry (");
        sb.append(cfId).append(", #").append(sequenceNumber).append(')');
        return sb.toString();
    }
}
