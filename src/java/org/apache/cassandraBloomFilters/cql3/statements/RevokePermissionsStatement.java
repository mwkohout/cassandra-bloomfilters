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

import java.util.Set;

import org.apache.cassandraBloomFilters.auth.IResource;
import org.apache.cassandraBloomFilters.auth.Permission;
import org.apache.cassandraBloomFilters.config.DatabaseDescriptor;
import org.apache.cassandraBloomFilters.cql3.RoleName;
import org.apache.cassandraBloomFilters.exceptions.RequestExecutionException;
import org.apache.cassandraBloomFilters.exceptions.RequestValidationException;
import org.apache.cassandraBloomFilters.service.ClientState;
import org.apache.cassandraBloomFilters.transport.messages.ResultMessage;

public class RevokePermissionsStatement extends PermissionsManagementStatement
{
    public RevokePermissionsStatement(Set<Permission> permissions, IResource resource, RoleName grantee)
    {
        super(permissions, resource, grantee);
    }

    public ResultMessage execute(ClientState state) throws RequestValidationException, RequestExecutionException
    {
        DatabaseDescriptor.getAuthorizer().revoke(state.getUser(), permissions, resource, grantee);
        return null;
    }
}
