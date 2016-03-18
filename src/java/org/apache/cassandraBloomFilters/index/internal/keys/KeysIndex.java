package org.apache.cassandraBloomFilters.index.internal.keys;

import java.nio.ByteBuffer;

import org.apache.cassandraBloomFilters.config.CFMetaData;
import org.apache.cassandraBloomFilters.config.ColumnDefinition;
import org.apache.cassandraBloomFilters.db.*;
import org.apache.cassandraBloomFilters.db.rows.Cell;
import org.apache.cassandraBloomFilters.db.rows.CellPath;
import org.apache.cassandraBloomFilters.db.rows.Row;
import org.apache.cassandraBloomFilters.index.internal.CassandraIndex;
import org.apache.cassandraBloomFilters.index.internal.IndexEntry;
import org.apache.cassandraBloomFilters.schema.IndexMetadata;

public class KeysIndex extends CassandraIndex
{
    public KeysIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef)
    {
        super(baseCfs, indexDef);
    }

    public CFMetaData.Builder addIndexClusteringColumns(CFMetaData.Builder builder,
                                                        CFMetaData baseMetadata,
                                                        ColumnDefinition cfDef)
    {
        // no additional clustering columns required
        return builder;
    }

    protected CBuilder buildIndexClusteringPrefix(ByteBuffer partitionKey,
                                               ClusteringPrefix prefix,
                                               CellPath path)
    {
        CBuilder builder = CBuilder.create(getIndexComparator());
        builder.add(partitionKey);
        return builder;
    }

    protected ByteBuffer getIndexedValue(ByteBuffer partitionKey,
                                      Clustering clustering,
                                      CellPath path, ByteBuffer cellValue)
    {
        return cellValue;
    }

    public IndexEntry decodeEntry(DecoratedKey indexedValue, Row indexEntry)
    {
        throw new UnsupportedOperationException("KEYS indexes do not use a specialized index entry format");
    }

    public boolean isStale(Row row, ByteBuffer indexValue, int nowInSec)
    {
        if (row == null)
            return true;

        Cell cell = row.getCell(indexedColumn);

        return (cell == null
             || !cell.isLive(nowInSec)
             || indexedColumn.type.compare(indexValue, cell.value()) != 0);
    }
}
