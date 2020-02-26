/*
 * Copyright 2016-2020 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.fram.standalone;


import org.polypheny.fram.remote.RemoteNode;
import org.polypheny.fram.remote.types.RemoteExecuteBatchResult;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.remote.types.RemoteStatementHandle;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.calcite.avatica.Meta.ConnectionHandle;
import org.apache.calcite.avatica.Meta.StatementHandle;


/**
 * Represents a (Prepared)Statement
 */
public class StatementInfos {

    private final ConnectionInfos connection;
    private final StatementHandle statementHandle;
    private final Map<RemoteNode, RemoteStatementHandle> origins = new LinkedHashMap<>();
    private ResultSetInfos resultSetInfos;
    private List<RemoteNode> executionTargets;
    private final Set<RemoteNode> accessedNodes = new HashSet<>();


    StatementInfos( final ConnectionInfos connection, final StatementHandle statementHandle ) {
        this.connection = connection;
        this.statementHandle = statementHandle;
    }


    public ConnectionHandle getConnectionHandle() {
        return this.connection.getConnectionHandle();
    }


    public StatementHandle getStatementHandle() {
        return this.statementHandle;
    }


    public RemoteStatementHandle getRemoteStatementHandle( final RemoteNode node ) {
        return this.origins.get( node );
    }


    public ResultSetInfos getResultSet() {
        return this.resultSetInfos;
    }


    public ResultSetInfos createResultSet( List<Entry<RemoteNode, RemoteExecuteResult>> remoteResults ) {
        synchronized ( this ) {
            this.resultSetInfos = new ResultSetInfos.SingleResult( this, remoteResults );
            return this.resultSetInfos;
        }
    }


    public ResultSetInfos createBatchResultSet( List<Entry<RemoteNode, RemoteExecuteBatchResult>> remoteBatchResults ) {
        synchronized ( this ) {
            this.resultSetInfos = new ResultSetInfos.BatchResult( this, remoteBatchResults );
            return this.resultSetInfos;
        }
    }


    public StatementInfos withExecutionTargets( Collection<RemoteNode> remoteNodes ) {
        this.executionTargets = new LinkedList<>( remoteNodes );
        this.addAccessedNodes( remoteNodes );
        return this;
    }


    public Collection<RemoteNode> getExecutionTargets() {
        return new LinkedList<>( this.executionTargets );
    }


    public void addAccessedNodes( Collection<RemoteNode> nodes ) {
        this.accessedNodes.addAll( nodes );
        connection.addAccessedNodes( nodes );
    }


    public Collection<RemoteNode> getAccessedNodes() {
        return Collections.unmodifiableCollection( accessedNodes );
    }


    public StatementInfos toPreparedStatement( final List<Entry<RemoteNode, RemoteStatementHandle>> remoteStatements, final Collection<RemoteNode> quorum ) {
        remoteStatements.forEach( entry -> StatementInfos.this.origins.put( entry.getKey(), entry.getValue() ) );
        // BEGIN HACK
        this.statementHandle.signature = remoteStatements.size() == 0 ? null : remoteStatements.get( 0 ).getValue().toStatementHandle().signature;
        // END HACK
        return this.withExecutionTargets( quorum );
    }
}