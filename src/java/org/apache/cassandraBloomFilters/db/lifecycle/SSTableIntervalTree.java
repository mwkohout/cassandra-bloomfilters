package org.apache.cassandraBloomFilters.db.lifecycle;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Iterables;

import org.apache.cassandraBloomFilters.db.PartitionPosition;
import org.apache.cassandraBloomFilters.io.sstable.format.SSTableReader;
import org.apache.cassandraBloomFilters.utils.Interval;
import org.apache.cassandraBloomFilters.utils.IntervalTree;

public class SSTableIntervalTree extends IntervalTree<PartitionPosition, SSTableReader, Interval<PartitionPosition, SSTableReader>>
{
    private static final SSTableIntervalTree EMPTY = new SSTableIntervalTree(null);

    SSTableIntervalTree(Collection<Interval<PartitionPosition, SSTableReader>> intervals)
    {
        super(intervals);
    }

    public static SSTableIntervalTree empty()
    {
        return EMPTY;
    }

    public static SSTableIntervalTree build(Iterable<SSTableReader> sstables)
    {
        return new SSTableIntervalTree(buildIntervals(sstables));
    }

    public static List<Interval<PartitionPosition, SSTableReader>> buildIntervals(Iterable<SSTableReader> sstables)
    {
        List<Interval<PartitionPosition, SSTableReader>> intervals = new ArrayList<>(Iterables.size(sstables));
        for (SSTableReader sstable : sstables)
            intervals.add(Interval.<PartitionPosition, SSTableReader>create(sstable.first, sstable.last, sstable));
        return intervals;
    }
}
