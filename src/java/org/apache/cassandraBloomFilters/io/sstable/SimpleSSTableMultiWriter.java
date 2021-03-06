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

package org.apache.cassandraBloomFilters.io.sstable;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import org.apache.cassandraBloomFilters.config.CFMetaData;
import org.apache.cassandraBloomFilters.db.RowIndexEntry;
import org.apache.cassandraBloomFilters.db.SerializationHeader;
import org.apache.cassandraBloomFilters.db.lifecycle.LifecycleTransaction;
import org.apache.cassandraBloomFilters.db.rows.UnfilteredRowIterator;
import org.apache.cassandraBloomFilters.index.Index;
import org.apache.cassandraBloomFilters.io.sstable.format.SSTableReader;
import org.apache.cassandraBloomFilters.io.sstable.format.SSTableWriter;
import org.apache.cassandraBloomFilters.io.sstable.metadata.MetadataCollector;

public class SimpleSSTableMultiWriter implements SSTableMultiWriter
{
    private final SSTableWriter writer;
    private final LifecycleTransaction txn;

    protected SimpleSSTableMultiWriter(SSTableWriter writer, LifecycleTransaction txn)
    {
        this.txn = txn;
        this.writer = writer;
    }

    public boolean append(UnfilteredRowIterator partition)
    {
        RowIndexEntry<?> indexEntry = writer.append(partition);
        return indexEntry != null;
    }

    public Collection<SSTableReader> finish(long repairedAt, long maxDataAge, boolean openResult)
    {
        return Collections.singleton(writer.finish(repairedAt, maxDataAge, openResult));
    }

    public Collection<SSTableReader> finish(boolean openResult)
    {
        return Collections.singleton(writer.finish(openResult));
    }

    public Collection<SSTableReader> finished()
    {
        return Collections.singleton(writer.finished());
    }

    public SSTableMultiWriter setOpenResult(boolean openResult)
    {
        writer.setOpenResult(openResult);
        return this;
    }

    public String getFilename()
    {
        return writer.getFilename();
    }

    public long getFilePointer()
    {
        return writer.getFilePointer();
    }

    public UUID getCfId()
    {
        return writer.metadata.cfId;
    }

    public Throwable commit(Throwable accumulate)
    {
        return writer.commit(accumulate);
    }

    public Throwable abort(Throwable accumulate)
    {
        txn.untrackNew(writer);
        return writer.abort(accumulate);
    }

    public void prepareToCommit()
    {
        writer.prepareToCommit();
    }

    public void close()
    {
        writer.close();
    }

    @SuppressWarnings("resource") // SimpleSSTableMultiWriter closes writer
    public static SSTableMultiWriter create(Descriptor descriptor,
                                            long keyCount,
                                            long repairedAt,
                                            CFMetaData cfm,
                                            MetadataCollector metadataCollector,
                                            SerializationHeader header,
                                            Collection<Index> indexes,
                                            LifecycleTransaction txn)
    {
        SSTableWriter writer = SSTableWriter.create(descriptor, keyCount, repairedAt, cfm, metadataCollector, header, indexes, txn);
        return new SimpleSSTableMultiWriter(writer, txn);
    }
}
