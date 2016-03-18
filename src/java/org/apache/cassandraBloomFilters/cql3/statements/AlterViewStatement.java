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
package org.apache.cassandraBloomFilters.cql3.statements;

import org.apache.cassandraBloomFilters.auth.Permission;
import org.apache.cassandraBloomFilters.config.CFMetaData;
import org.apache.cassandraBloomFilters.config.Schema;
import org.apache.cassandraBloomFilters.config.ViewDefinition;
import org.apache.cassandraBloomFilters.cql3.CFName;
import org.apache.cassandraBloomFilters.db.view.View;
import org.apache.cassandraBloomFilters.exceptions.InvalidRequestException;
import org.apache.cassandraBloomFilters.exceptions.RequestValidationException;
import org.apache.cassandraBloomFilters.exceptions.UnauthorizedException;
import org.apache.cassandraBloomFilters.schema.TableParams;
import org.apache.cassandraBloomFilters.service.ClientState;
import org.apache.cassandraBloomFilters.service.MigrationManager;
import org.apache.cassandraBloomFilters.transport.Event;

import static org.apache.cassandraBloomFilters.thrift.ThriftValidation.validateColumnFamily;

public class AlterViewStatement extends SchemaAlteringStatement
{
    private final TableAttributes attrs;

    public AlterViewStatement(CFName name, TableAttributes attrs)
    {
        super(name);
        this.attrs = attrs;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        CFMetaData baseTable = View.findBaseTable(keyspace(), columnFamily());
        if (baseTable != null)
            state.hasColumnFamilyAccess(keyspace(), baseTable.cfName, Permission.ALTER);
    }

    public void validate(ClientState state)
    {
        // validated in announceMigration()
    }

    public Event.SchemaChange announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        CFMetaData meta = validateColumnFamily(keyspace(), columnFamily());
        if (!meta.isView())
            throw new InvalidRequestException("Cannot use ALTER MATERIALIZED VIEW on Table");

        ViewDefinition viewCopy = Schema.instance.getView(keyspace(), columnFamily()).copy();

        if (attrs == null)
            throw new InvalidRequestException("ALTER MATERIALIZED VIEW WITH invoked, but no parameters found");

        attrs.validate();

        TableParams params = attrs.asAlteredTableParams(viewCopy.metadata.params);
        if (params.gcGraceSeconds == 0)
        {
            throw new InvalidRequestException("Cannot alter gc_grace_seconds of a materialized view to 0, since this " +
                                              "value is used to TTL undelivered updates. Setting gc_grace_seconds too " +
                                              "low might cause undelivered updates to expire before being replayed.");
        }
        viewCopy.metadata.params(params);

        MigrationManager.announceViewUpdate(viewCopy, isLocalOnly);
        return new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily());
    }

    public String toString()
    {
        return String.format("AlterViewStatement(name=%s)", cfName);
    }
}