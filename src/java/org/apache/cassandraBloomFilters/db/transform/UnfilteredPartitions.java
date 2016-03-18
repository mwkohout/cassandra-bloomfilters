package org.apache.cassandraBloomFilters.db.transform;

import org.apache.cassandraBloomFilters.config.CFMetaData;
import org.apache.cassandraBloomFilters.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandraBloomFilters.db.rows.UnfilteredRowIterator;

final class UnfilteredPartitions extends BasePartitions<UnfilteredRowIterator, UnfilteredPartitionIterator> implements UnfilteredPartitionIterator
{
    final boolean isForThrift;

    // wrap an iterator for transformation
    public UnfilteredPartitions(UnfilteredPartitionIterator input)
    {
        super(input);
        this.isForThrift = input.isForThrift();
    }

    public boolean isForThrift()
    {
        return isForThrift;
    }

    public CFMetaData metadata()
    {
        return input.metadata();
    }
}
