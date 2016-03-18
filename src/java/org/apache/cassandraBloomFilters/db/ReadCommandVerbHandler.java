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
package org.apache.cassandraBloomFilters.db;

import org.apache.cassandraBloomFilters.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandraBloomFilters.io.IVersionedSerializer;
import org.apache.cassandraBloomFilters.net.IVerbHandler;
import org.apache.cassandraBloomFilters.net.MessageIn;
import org.apache.cassandraBloomFilters.net.MessageOut;
import org.apache.cassandraBloomFilters.net.MessagingService;
import org.apache.cassandraBloomFilters.service.StorageService;
import org.apache.cassandraBloomFilters.tracing.Tracing;

public class ReadCommandVerbHandler implements IVerbHandler<ReadCommand>
{
    protected IVersionedSerializer<ReadResponse> serializer()
    {
        return ReadResponse.serializer;
    }

    public void doVerb(MessageIn<ReadCommand> message, int id)
    {
        if (StorageService.instance.isBootstrapMode())
        {
            throw new RuntimeException("Cannot service reads while bootstrapping!");
        }

        ReadCommand command = message.payload;
        command.setMonitoringTime(message.constructionTime, message.getTimeout());

        ReadResponse response;
        try (ReadExecutionController executionController = command.executionController();
             UnfilteredPartitionIterator iterator = command.executeLocally(executionController))
        {
            response = command.createResponse(iterator);
        }

        if (!command.complete())
        {
            Tracing.trace("Discarding partial response to {} (timed out)", message.from);
            MessagingService.instance().incrementDroppedMessages(message, System.currentTimeMillis() - message.constructionTime.timestamp);
            return;
        }

        Tracing.trace("Enqueuing response to {}", message.from);
        MessageOut<ReadResponse> reply = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE, response, serializer());
        MessagingService.instance().sendReply(reply, id, message.from);
    }
}
