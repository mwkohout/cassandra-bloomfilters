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
package org.apache.cassandraBloomFilters.streaming;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandraBloomFilters.net.IVerbHandler;
import org.apache.cassandraBloomFilters.net.MessageIn;
import org.apache.cassandraBloomFilters.net.MessageOut;
import org.apache.cassandraBloomFilters.net.MessagingService;
import org.apache.cassandraBloomFilters.service.StorageService;

public class ReplicationFinishedVerbHandler implements IVerbHandler
{
    private static final Logger logger = LoggerFactory.getLogger(ReplicationFinishedVerbHandler.class);

    public void doVerb(MessageIn msg, int id)
    {
        StorageService.instance.confirmReplication(msg.from);
        MessageOut response = new MessageOut(MessagingService.Verb.INTERNAL_RESPONSE);
        if (logger.isDebugEnabled())
            logger.debug("Replying to {}@{}", id, msg.from);
        MessagingService.instance().sendReply(response, id, msg.from);
    }
}