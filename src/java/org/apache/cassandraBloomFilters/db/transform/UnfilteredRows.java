package org.apache.cassandraBloomFilters.db.transform;

import org.apache.cassandraBloomFilters.db.DeletionTime;
import org.apache.cassandraBloomFilters.db.rows.EncodingStats;
import org.apache.cassandraBloomFilters.db.rows.Unfiltered;
import org.apache.cassandraBloomFilters.db.rows.UnfilteredRowIterator;

final class UnfilteredRows extends BaseRows<Unfiltered, UnfilteredRowIterator> implements UnfilteredRowIterator
{
    private DeletionTime partitionLevelDeletion;

    public UnfilteredRows(UnfilteredRowIterator input)
    {
        super(input);
        partitionLevelDeletion = input.partitionLevelDeletion();
    }

    @Override
    void add(Transformation add)
    {
        super.add(add);
        partitionLevelDeletion = add.applyToDeletion(partitionLevelDeletion);
    }

    public DeletionTime partitionLevelDeletion()
    {
        return partitionLevelDeletion;
    }

    public EncodingStats stats()
    {
        return input.stats();
    }

    @Override
    public boolean isEmpty()
    {
        return staticRow().isEmpty() && partitionLevelDeletion().isLive() && !hasNext();
    }
}
