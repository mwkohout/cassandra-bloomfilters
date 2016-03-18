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
package org.apache.cassandraBloomFilters.io.sstable.metadata;

import java.io.IOException;

import org.apache.cassandraBloomFilters.io.sstable.format.Version;
import org.apache.cassandraBloomFilters.io.util.DataInputPlus;
import org.apache.cassandraBloomFilters.io.util.DataOutputPlus;

/**
 * Metadata component serializer
 */
public interface IMetadataComponentSerializer<T extends MetadataComponent>
{
    /**
     * Calculate and return serialized size.
     *
     *
     *
     * @param version
     * @param component MetadataComponent to calculate serialized size
     * @return serialized size of this component
     * @throws IOException
     */
    int serializedSize(Version version, T component) throws IOException;

    /**
     * Serialize metadata component to given output.
     *
     *
     *
     * @param version
     * @param component MetadataComponent to serialize
     * @param out  serialize destination
     * @throws IOException
     */
    void serialize(Version version, T component, DataOutputPlus out) throws IOException;

    /**
     * Deserialize metadata component from given input.
     *
     * @param version serialize version
     * @param in deserialize source
     * @return Deserialized component
     * @throws IOException
     */
    T deserialize(Version version, DataInputPlus in) throws IOException;
}