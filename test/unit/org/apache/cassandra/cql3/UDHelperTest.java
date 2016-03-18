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

package org.apache.cassandraBloomFilters.cql3;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandraBloomFilters.cql3.functions.UDHelper;
import org.apache.cassandraBloomFilters.db.marshal.AbstractType;
import org.apache.cassandraBloomFilters.db.marshal.AsciiType;
import org.apache.cassandraBloomFilters.db.marshal.BooleanType;
import org.apache.cassandraBloomFilters.db.marshal.ByteType;
import org.apache.cassandraBloomFilters.db.marshal.BytesType;
import org.apache.cassandraBloomFilters.db.marshal.CounterColumnType;
import org.apache.cassandraBloomFilters.db.marshal.DateType;
import org.apache.cassandraBloomFilters.db.marshal.DecimalType;
import org.apache.cassandraBloomFilters.db.marshal.DoubleType;
import org.apache.cassandraBloomFilters.db.marshal.FloatType;
import org.apache.cassandraBloomFilters.db.marshal.InetAddressType;
import org.apache.cassandraBloomFilters.db.marshal.Int32Type;
import org.apache.cassandraBloomFilters.db.marshal.IntegerType;
import org.apache.cassandraBloomFilters.db.marshal.LongType;
import org.apache.cassandraBloomFilters.db.marshal.ReversedType;
import org.apache.cassandraBloomFilters.db.marshal.ShortType;
import org.apache.cassandraBloomFilters.db.marshal.SimpleDateType;
import org.apache.cassandraBloomFilters.db.marshal.TimeType;
import org.apache.cassandraBloomFilters.db.marshal.TimeUUIDType;
import org.apache.cassandraBloomFilters.db.marshal.TimestampType;
import org.apache.cassandraBloomFilters.db.marshal.UTF8Type;
import org.apache.cassandraBloomFilters.db.marshal.UUIDType;
import org.apache.cassandraBloomFilters.serializers.MarshalException;
import org.apache.cassandraBloomFilters.serializers.TypeSerializer;
import org.apache.cassandraBloomFilters.utils.ByteBufferUtil;

public class UDHelperTest
{
    static class UFTestCustomType extends AbstractType<String>
    {
        protected UFTestCustomType()
        {
            super(ComparisonType.CUSTOM);
        }

        public ByteBuffer fromString(String source) throws MarshalException
        {
            return ByteBuffer.wrap(source.getBytes());
        }

        public Term fromJSONObject(Object parsed) throws MarshalException
        {
            throw new UnsupportedOperationException();
        }

        public TypeSerializer<String> getSerializer()
        {
            return UTF8Type.instance.getSerializer();
        }

        public int compareCustom(ByteBuffer o1, ByteBuffer o2)
        {
            return o1.compareTo(o2);
        }
    }

    @Test
    public void testEmptyVariableLengthTypes()
    {
        AbstractType<?>[] types = new AbstractType<?>[]{
                                                       AsciiType.instance,
                                                       BytesType.instance,
                                                       UTF8Type.instance,
                                                       new UFTestCustomType()
        };

        for (AbstractType<?> type : types)
        {
            Assert.assertFalse("type " + type.getClass().getName(),
                               UDHelper.isNullOrEmpty(type, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        }
    }

    @Test
    public void testNonEmptyPrimitiveTypes()
    {
        AbstractType<?>[] types = new AbstractType<?>[]{
                                                       TimeType.instance,
                                                       SimpleDateType.instance,
                                                       ByteType.instance,
                                                       ShortType.instance
        };

        for (AbstractType<?> type : types)
        {
            try
            {
                type.getSerializer().validate(ByteBufferUtil.EMPTY_BYTE_BUFFER);
                Assert.fail(type.getClass().getSimpleName());
            }
            catch (MarshalException e)
            {
                //
            }
        }
    }

    @Test
    public void testEmptiableTypes()
    {
        AbstractType<?>[] types = new AbstractType<?>[]{
                                                       BooleanType.instance,
                                                       CounterColumnType.instance,
                                                       DateType.instance,
                                                       DecimalType.instance,
                                                       DoubleType.instance,
                                                       FloatType.instance,
                                                       InetAddressType.instance,
                                                       Int32Type.instance,
                                                       IntegerType.instance,
                                                       LongType.instance,
                                                       TimestampType.instance,
                                                       TimeUUIDType.instance,
                                                       UUIDType.instance
        };

        for (AbstractType<?> type : types)
        {
            Assert.assertTrue(type.getClass().getSimpleName(), UDHelper.isNullOrEmpty(type, ByteBufferUtil.EMPTY_BYTE_BUFFER));
            Assert.assertTrue("reversed " + type.getClass().getSimpleName(),
                              UDHelper.isNullOrEmpty(ReversedType.getInstance(type), ByteBufferUtil.EMPTY_BYTE_BUFFER));
        }
    }
}
