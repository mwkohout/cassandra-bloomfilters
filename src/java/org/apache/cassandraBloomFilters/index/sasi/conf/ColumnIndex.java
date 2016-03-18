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
package org.apache.cassandraBloomFilters.index.sasi.conf;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandraBloomFilters.config.ColumnDefinition;
import org.apache.cassandraBloomFilters.cql3.Operator;
import org.apache.cassandraBloomFilters.db.DecoratedKey;
import org.apache.cassandraBloomFilters.db.Memtable;
import org.apache.cassandraBloomFilters.db.marshal.AbstractType;
import org.apache.cassandraBloomFilters.db.marshal.AsciiType;
import org.apache.cassandraBloomFilters.db.marshal.UTF8Type;
import org.apache.cassandraBloomFilters.db.rows.Cell;
import org.apache.cassandraBloomFilters.db.rows.Row;
import org.apache.cassandraBloomFilters.index.sasi.analyzer.AbstractAnalyzer;
import org.apache.cassandraBloomFilters.index.sasi.conf.view.View;
import org.apache.cassandraBloomFilters.index.sasi.disk.Token;
import org.apache.cassandraBloomFilters.index.sasi.memory.IndexMemtable;
import org.apache.cassandraBloomFilters.index.sasi.plan.Expression;
import org.apache.cassandraBloomFilters.index.sasi.plan.Expression.Op;
import org.apache.cassandraBloomFilters.index.sasi.utils.RangeIterator;
import org.apache.cassandraBloomFilters.index.sasi.utils.RangeUnionIterator;
import org.apache.cassandraBloomFilters.io.sstable.Component;
import org.apache.cassandraBloomFilters.io.sstable.format.SSTableReader;
import org.apache.cassandraBloomFilters.schema.IndexMetadata;
import org.apache.cassandraBloomFilters.utils.FBUtilities;

public class ColumnIndex
{
    private static final String FILE_NAME_FORMAT = "SI_%s.db";

    private final AbstractType<?> keyValidator;

    private final ColumnDefinition column;
    private final Optional<IndexMetadata> config;

    private final AtomicReference<IndexMemtable> memtable;
    private final ConcurrentMap<Memtable, IndexMemtable> pendingFlush = new ConcurrentHashMap<>();

    private final IndexMode mode;

    private final Component component;
    private final DataTracker tracker;

    private final boolean isTokenized;

    public ColumnIndex(AbstractType<?> keyValidator, ColumnDefinition column, IndexMetadata metadata)
    {
        this.keyValidator = keyValidator;
        this.column = column;
        this.config = metadata == null ? Optional.empty() : Optional.of(metadata);
        this.mode = IndexMode.getMode(column, config);
        this.memtable = new AtomicReference<>(new IndexMemtable(this));
        this.tracker = new DataTracker(keyValidator, this);
        this.component = new Component(Component.Type.SECONDARY_INDEX, String.format(FILE_NAME_FORMAT, getIndexName()));
        this.isTokenized = getAnalyzer().isTokenizing();
    }

    /**
     * Initialize this column index with specific set of SSTables.
     *
     * @param sstables The sstables to be used by index initially.
     *
     * @return A collection of sstables which don't have this specific index attached to them.
     */
    public Iterable<SSTableReader> init(Set<SSTableReader> sstables)
    {
        return tracker.update(Collections.emptySet(), sstables);
    }

    public AbstractType<?> keyValidator()
    {
        return keyValidator;
    }

    public long index(DecoratedKey key, Row row)
    {
        return getCurrentMemtable().index(key, getValueOf(column, row, FBUtilities.nowInSeconds()));
    }

    public void switchMemtable()
    {
        // discard current memtable with all of it's data, useful on truncate
        memtable.set(new IndexMemtable(this));
    }

    public void switchMemtable(Memtable parent)
    {
        pendingFlush.putIfAbsent(parent, memtable.getAndSet(new IndexMemtable(this)));
    }

    public void discardMemtable(Memtable parent)
    {
        pendingFlush.remove(parent);
    }

    @VisibleForTesting
    public IndexMemtable getCurrentMemtable()
    {
        return memtable.get();
    }

    @VisibleForTesting
    public Collection<IndexMemtable> getPendingMemtables()
    {
        return pendingFlush.values();
    }

    public RangeIterator<Long, Token> searchMemtable(Expression e)
    {
        RangeIterator.Builder<Long, Token> builder = new RangeUnionIterator.Builder<>();
        builder.add(getCurrentMemtable().search(e));
        for (IndexMemtable memtable : getPendingMemtables())
            builder.add(memtable.search(e));

        return builder.build();
    }

    public void update(Collection<SSTableReader> oldSSTables, Collection<SSTableReader> newSSTables)
    {
        tracker.update(oldSSTables, newSSTables);
    }

    public ColumnDefinition getDefinition()
    {
        return column;
    }

    public AbstractType<?> getValidator()
    {
        return column.cellValueType();
    }

    public Component getComponent()
    {
        return component;
    }

    public IndexMode getMode()
    {
        return mode;
    }

    public String getColumnName()
    {
        return column.name.toString();
    }

    public String getIndexName()
    {
        return config.isPresent() ? config.get().name : "undefined";
    }

    public AbstractAnalyzer getAnalyzer()
    {
        AbstractAnalyzer analyzer = mode.getAnalyzer(getValidator());
        analyzer.init(config.isPresent() ? config.get().options : Collections.emptyMap(), column.cellValueType());
        return analyzer;
    }

    public View getView()
    {
        return tracker.getView();
    }

    public boolean hasSSTable(SSTableReader sstable)
    {
        return tracker.hasSSTable(sstable);
    }

    public void dropData(long truncateUntil)
    {
        switchMemtable();
        tracker.dropData(truncateUntil);
    }

    public boolean isIndexed()
    {
        return mode != IndexMode.NOT_INDEXED;
    }

    public boolean isLiteral()
    {
        AbstractType<?> validator = getValidator();
        return isIndexed() ? mode.isLiteral : (validator instanceof UTF8Type || validator instanceof AsciiType);
    }

    public boolean supports(Operator op)
    {
        Op operator = Op.valueOf(op);
        return !(isTokenized && operator == Op.EQ) // EQ is only applicable to non-tokenized indexes
            && !(isLiteral() && operator == Op.RANGE) // RANGE only applicable to indexes non-literal indexes
            && mode.supports(operator); // for all other cases let's refer to index itself

    }

    public static ByteBuffer getValueOf(ColumnDefinition column, Row row, int nowInSecs)
    {
        switch (column.kind)
        {
            case CLUSTERING:
                return row.clustering().get(column.position());

            case REGULAR:
                Cell cell = row.getCell(column);
                return cell == null || !cell.isLive(nowInSecs) ? null : cell.value();

            default:
                return null;
        }
    }
}