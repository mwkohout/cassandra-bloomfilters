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
package org.apache.cassandraBloomFilters.db.rows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Objects;

import org.apache.cassandraBloomFilters.io.util.DataOutputPlus;
import org.apache.cassandraBloomFilters.io.util.DataInputPlus;
import org.apache.cassandraBloomFilters.utils.ByteBufferUtil;
import org.apache.cassandraBloomFilters.utils.ObjectSizes;
import org.apache.cassandraBloomFilters.utils.memory.AbstractAllocator;

/**
 * A path for a cell belonging to a complex column type (non-frozen collection or UDT).
 */
public abstract class CellPath
{
    public static final CellPath BOTTOM = new EmptyCellPath();
    public static final CellPath TOP = new EmptyCellPath();

    public abstract int size();
    public abstract ByteBuffer get(int i);

    // The only complex we currently have are collections that have only one value.
    public static CellPath create(ByteBuffer value)
    {
        assert value != null;
        return new CollectionCellPath(value);
    }

    public int dataSize()
    {
        int size = 0;
        for (int i = 0; i < size(); i++)
            size += get(i).remaining();
        return size;
    }

    public void digest(MessageDigest digest)
    {
        for (int i = 0; i < size(); i++)
            digest.update(get(i).duplicate());
    }

    public abstract CellPath copy(AbstractAllocator allocator);

    public abstract long unsharedHeapSizeExcludingData();

    @Override
    public final int hashCode()
    {
        int result = 31;
        for (int i = 0; i < size(); i++)
            result += 31 * Objects.hash(get(i));
        return result;
    }

    @Override
    public final boolean equals(Object o)
    {
        if(!(o instanceof CellPath))
            return false;

        CellPath that = (CellPath)o;
        if (this.size() != that.size())
            return false;

        for (int i = 0; i < size(); i++)
            if (!Objects.equals(this.get(i), that.get(i)))
                return false;

        return true;
    }

    public interface Serializer
    {
        public void serialize(CellPath path, DataOutputPlus out) throws IOException;
        public CellPath deserialize(DataInputPlus in) throws IOException;
        public long serializedSize(CellPath path);
        public void skip(DataInputPlus in) throws IOException;
    }

    private static class CollectionCellPath extends CellPath
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new CollectionCellPath(ByteBufferUtil.EMPTY_BYTE_BUFFER));

        protected final ByteBuffer value;

        private CollectionCellPath(ByteBuffer value)
        {
            this.value = value;
        }

        public int size()
        {
            return 1;
        }

        public ByteBuffer get(int i)
        {
            assert i == 0;
            return value;
        }

        public CellPath copy(AbstractAllocator allocator)
        {
            return new CollectionCellPath(allocator.clone(value));
        }

        public long unsharedHeapSizeExcludingData()
        {
            return EMPTY_SIZE + ObjectSizes.sizeOnHeapExcludingData(value);
        }
    }

    private static class EmptyCellPath extends CellPath
    {
        public int size()
        {
            return 0;
        }

        public ByteBuffer get(int i)
        {
            throw new UnsupportedOperationException();
        }

        public CellPath copy(AbstractAllocator allocator)
        {
            return this;
        }

        public long unsharedHeapSizeExcludingData()
        {
            return 0;
        }
    }
}
