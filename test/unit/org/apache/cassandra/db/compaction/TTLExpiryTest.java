/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandraBloomFilters.db.compaction;

import org.apache.cassandraBloomFilters.config.CFMetaData;
import org.apache.cassandraBloomFilters.db.lifecycle.SSTableSet;
import org.apache.cassandraBloomFilters.db.rows.UnfilteredRowIterator;
import org.apache.cassandraBloomFilters.db.marshal.AsciiType;
import org.apache.cassandraBloomFilters.io.sstable.format.SSTableReader;
import org.junit.BeforeClass;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandraBloomFilters.OrderedJUnit4ClassRunner;
import org.apache.cassandraBloomFilters.SchemaLoader;
import org.apache.cassandraBloomFilters.Util;
import org.apache.cassandraBloomFilters.db.*;
import org.apache.cassandraBloomFilters.db.filter.ColumnFilter;
import org.apache.cassandraBloomFilters.exceptions.ConfigurationException;
import org.apache.cassandraBloomFilters.io.sstable.ISSTableScanner;
import org.apache.cassandraBloomFilters.schema.KeyspaceParams;
import org.apache.cassandraBloomFilters.tools.SSTableExpiredBlockers;
import org.apache.cassandraBloomFilters.utils.ByteBufferUtil;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class TTLExpiryTest
{
    public static final String KEYSPACE1 = "TTLExpiryTest";
    private static final String CF_STANDARD1 = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    CFMetaData.Builder.create(KEYSPACE1, CF_STANDARD1)
                                                      .addPartitionKey("pKey", AsciiType.instance)
                                                      .addRegularColumn("col1", AsciiType.instance)
                                                      .addRegularColumn("col", AsciiType.instance)
                                                      .addRegularColumn("col311", AsciiType.instance)
                                                      .addRegularColumn("col2", AsciiType.instance)
                                                      .addRegularColumn("col3", AsciiType.instance)
                                                      .addRegularColumn("col7", AsciiType.instance)
                                                      .addRegularColumn("shadow", AsciiType.instance)
                                                      .build().gcGraceSeconds(0));
    }

    @Test
    public void testAggressiveFullyExpired()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore("Standard1");
        cfs.disableAutoCompaction();
        cfs.metadata.gcGraceSeconds(0);
        String key = "ttl";
        new RowUpdateBuilder(cfs.metadata, 1L, 1, key)
                    .add("col1", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();

        new RowUpdateBuilder(cfs.metadata, 3L, 1, key)
                    .add("col2", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();
        cfs.forceBlockingFlush();
        new RowUpdateBuilder(cfs.metadata, 2L, 1, key)
                    .add("col1", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();

        new RowUpdateBuilder(cfs.metadata, 5L, 1, key)
                    .add("col2", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();

        cfs.forceBlockingFlush();

        new RowUpdateBuilder(cfs.metadata, 4L, 1, key)
                    .add("col1", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();

        new RowUpdateBuilder(cfs.metadata, 7L, 1, key)
                    .add("shadow", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();

        cfs.forceBlockingFlush();


        new RowUpdateBuilder(cfs.metadata, 6L, 3, key)
                    .add("shadow", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();

        new RowUpdateBuilder(cfs.metadata, 8L, 1, key)
                    .add("col2", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();

        cfs.forceBlockingFlush();

        Set<SSTableReader> sstables = Sets.newHashSet(cfs.getLiveSSTables());
        int now = (int)(System.currentTimeMillis() / 1000);
        int gcBefore = now + 2;
        Set<SSTableReader> expired = CompactionController.getFullyExpiredSSTables(
                cfs,
                sstables,
                Collections.EMPTY_SET,
                gcBefore);
        assertEquals(2, expired.size());

        cfs.clearUnsafe();
    }

    @Test
    public void testSimpleExpire() throws InterruptedException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore("Standard1");
        cfs.disableAutoCompaction();
        cfs.metadata.gcGraceSeconds(0);
        long timestamp = System.currentTimeMillis();
        String key = "ttl";
        new RowUpdateBuilder(cfs.metadata, timestamp, 1, key)
                        .add("col", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                        .add("col7", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                        .build()
                        .applyUnsafe();

        cfs.forceBlockingFlush();

        new RowUpdateBuilder(cfs.metadata, timestamp, 1, key)
            .add("col2", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();


        cfs.forceBlockingFlush();
        new RowUpdateBuilder(cfs.metadata, timestamp, 1, key)
                    .add("col3", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();


        cfs.forceBlockingFlush();
        new RowUpdateBuilder(cfs.metadata, timestamp, 1, key)
                            .add("col311", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                            .build()
                            .applyUnsafe();


        cfs.forceBlockingFlush();
        Thread.sleep(2000); // wait for ttl to expire
        assertEquals(4, cfs.getLiveSSTables().size());
        cfs.enableAutoCompaction(true);
        assertEquals(0, cfs.getLiveSSTables().size());
    }

    @Test
    public void testNoExpire() throws InterruptedException, IOException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore("Standard1");
        cfs.disableAutoCompaction();
        cfs.metadata.gcGraceSeconds(0);
        long timestamp = System.currentTimeMillis();
        String key = "ttl";
        new RowUpdateBuilder(cfs.metadata, timestamp, 1, key)
            .add("col", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .add("col7", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();

        cfs.forceBlockingFlush();
        new RowUpdateBuilder(cfs.metadata, timestamp, 1, key)
            .add("col2", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        cfs.forceBlockingFlush();
        new RowUpdateBuilder(cfs.metadata, timestamp, 1, key)
            .add("col3", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        cfs.forceBlockingFlush();
        String noTTLKey = "nottl";
        new RowUpdateBuilder(cfs.metadata, timestamp, noTTLKey)
            .add("col311", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();

        cfs.forceBlockingFlush();
        Thread.sleep(2000); // wait for ttl to expire
        assertEquals(4, cfs.getLiveSSTables().size());
        cfs.enableAutoCompaction(true);
        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        ISSTableScanner scanner = sstable.getScanner(ColumnFilter.all(sstable.metadata), DataRange.allData(cfs.getPartitioner()), false);
        assertTrue(scanner.hasNext());
        while(scanner.hasNext())
        {
            UnfilteredRowIterator iter = scanner.next();
            assertEquals(Util.dk(noTTLKey), iter.partitionKey());
        }
        scanner.close();
    }

    @Test
    public void testCheckForExpiredSSTableBlockers() throws InterruptedException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore("Standard1");
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        cfs.metadata.gcGraceSeconds(0);

        new RowUpdateBuilder(cfs.metadata, System.currentTimeMillis(), "test")
                .noRowMarker()
                .add("col1", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                .build()
                .applyUnsafe();

        cfs.forceBlockingFlush();
        SSTableReader blockingSSTable = cfs.getSSTables(SSTableSet.LIVE).iterator().next();
        for (int i = 0; i < 10; i++)
        {
            new RowUpdateBuilder(cfs.metadata, System.currentTimeMillis(), "test")
                            .noRowMarker()
                            .delete("col1")
                            .build()
                            .applyUnsafe();
            cfs.forceBlockingFlush();
        }
        Multimap<SSTableReader, SSTableReader> blockers = SSTableExpiredBlockers.checkForExpiredSSTableBlockers(cfs.getSSTables(SSTableSet.LIVE), (int) (System.currentTimeMillis() / 1000) + 100);
        assertEquals(1, blockers.keySet().size());
        assertTrue(blockers.keySet().contains(blockingSSTable));
        assertEquals(10, blockers.get(blockingSSTable).size());
    }
}
