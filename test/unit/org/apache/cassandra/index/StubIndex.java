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

package org.apache.cassandraBloomFilters.index;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;

import org.apache.cassandraBloomFilters.Util;
import org.apache.cassandraBloomFilters.config.ColumnDefinition;
import org.apache.cassandraBloomFilters.cql3.Operator;
import org.apache.cassandraBloomFilters.db.*;
import org.apache.cassandraBloomFilters.db.filter.RowFilter;
import org.apache.cassandraBloomFilters.db.marshal.AbstractType;
import org.apache.cassandraBloomFilters.db.marshal.UTF8Type;
import org.apache.cassandraBloomFilters.db.partitions.PartitionIterator;
import org.apache.cassandraBloomFilters.db.partitions.PartitionUpdate;
import org.apache.cassandraBloomFilters.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandraBloomFilters.db.rows.Row;
import org.apache.cassandraBloomFilters.exceptions.InvalidRequestException;
import org.apache.cassandraBloomFilters.index.transactions.IndexTransaction;
import org.apache.cassandraBloomFilters.schema.IndexMetadata;
import org.apache.cassandraBloomFilters.utils.Pair;
import org.apache.cassandraBloomFilters.utils.concurrent.OpOrder;

/**
 * Basic custom index implementation for testing.
 * During indexing by default it just records the updates for later inspection.
 * At query time, the Searcher implementation simply performs a local scan of the entire target table
 * with no further filtering applied.
 */
public class StubIndex implements Index
{
    public List<DeletionTime> partitionDeletions = new ArrayList<>();
    public List<RangeTombstone> rangeTombstones = new ArrayList<>();
    public List<Row> rowsInserted = new ArrayList<>();
    public List<Row> rowsDeleted = new ArrayList<>();
    public List<Pair<Row,Row>> rowsUpdated = new ArrayList<>();
    private IndexMetadata indexMetadata;
    private ColumnFamilyStore baseCfs;

    public void reset()
    {
        rowsInserted.clear();
        rowsDeleted.clear();
        rowsUpdated.clear();
        partitionDeletions.clear();
        rangeTombstones.clear();
    }

    public StubIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
    {
        this.baseCfs = baseCfs;
        this.indexMetadata = metadata;
    }

    public boolean shouldBuildBlocking()
    {
        return false;
    }

    public boolean dependsOn(ColumnDefinition column)
    {
        return false;
    }

    public boolean supportsExpression(ColumnDefinition column, Operator operator)
    {
        return operator == Operator.EQ;
    }

    public AbstractType<?> customExpressionValueType()
    {
        return UTF8Type.instance;
    }

    public RowFilter getPostIndexQueryFilter(RowFilter filter)
    {
        return filter;
    }

    public Indexer indexerFor(final DecoratedKey key,
                              PartitionColumns columns,
                              int nowInSec,
                              OpOrder.Group opGroup,
                              IndexTransaction.Type transactionType)
    {
        return new Indexer()
        {
            public void begin()
            {
            }

            public void partitionDelete(DeletionTime deletionTime)
            {
                partitionDeletions.add(deletionTime);
            }

            public void rangeTombstone(RangeTombstone tombstone)
            {
                rangeTombstones.add(tombstone);
            }

            public void insertRow(Row row)
            {
                rowsInserted.add(row);
            }

            public void removeRow(Row row)
            {
                rowsDeleted.add(row);
            }

            public void updateRow(Row oldRowData, Row newRowData)
            {
                rowsUpdated.add(Pair.create(oldRowData, newRowData));
            }

            public void finish()
            {
            }
        };
    }

    public Callable<?> getInitializationTask()
    {
        return null;
    }

    public IndexMetadata getIndexMetadata()
    {
        return indexMetadata;
    }

    public void register(IndexRegistry registry){
        registry.registerIndex(this);
    }

    public Optional<ColumnFamilyStore> getBackingTable()
    {
        return Optional.empty();
    }

    public Collection<ColumnDefinition> getIndexedColumns()
    {
        return Collections.emptySet();
    }

    public Callable<?> getBlockingFlushTask()
    {
        return null;
    }

    public Callable<?> getTruncateTask(long truncatedAt)
    {
        return null;
    }

    public Callable<?> getInvalidateTask()
    {
        return null;
    }

    public Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata)
    {
        return null;
    }

    public long getEstimatedResultRows()
    {
        return 0;
    }

    public void validate(PartitionUpdate update) throws InvalidRequestException
    {

    }

    public Searcher searcherFor(final ReadCommand command)
    {
        return (controller) -> Util.executeLocally((PartitionRangeReadCommand)command, baseCfs, controller);
    }

    public BiFunction<PartitionIterator, ReadCommand, PartitionIterator> postProcessorFor(ReadCommand readCommand)
    {
        return (iter, command) -> iter;
    }
}
