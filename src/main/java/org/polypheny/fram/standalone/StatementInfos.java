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


import java.util.AbstractMap.SimpleImmutableEntry;
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
import org.polypheny.fram.remote.AbstractRemoteNode;
import org.polypheny.fram.remote.types.RemoteConnectionHandle;
import org.polypheny.fram.remote.types.RemoteExecuteBatchResult;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.remote.types.RemoteStatementHandle;


/**
 * Represents a (Prepared)Statement
 */
public class StatementInfos {

    protected final ConnectionInfos connection;
    protected final StatementHandle statementHandle;
    protected final Map<AbstractRemoteNode, RemoteStatementHandle> remoteStatements = new LinkedHashMap<>();
    protected final Map<RemoteStatementHandle, Set<AbstractRemoteNode>> remoteNodes = new LinkedHashMap<>();
    protected ResultSetInfos resultSetInfos;


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


    public RemoteStatementHandle getRemoteStatementHandle( final AbstractRemoteNode node ) {
        return this.remoteStatements.get( node );
    }


    public ResultSetInfos getResultSet() {
        return this.resultSetInfos;
    }


    public ResultSetInfos createResultSet( List<Entry<AbstractRemoteNode, RemoteExecuteResult>> remoteResults ) {
        synchronized ( this ) {
            this.resultSetInfos = new ResultSetInfos.SingleResult( this, remoteResults );
            return this.resultSetInfos;
        }
    }


    public ResultSetInfos createBatchResultSet( List<Entry<AbstractRemoteNode, RemoteExecuteBatchResult>> remoteBatchResults ) {
        synchronized ( this ) {
            this.resultSetInfos = new ResultSetInfos.BatchResult( this, remoteBatchResults );
            return this.resultSetInfos;
        }
    }


    public void addAccessedNode( final AbstractRemoteNode node, RemoteStatementHandle remoteConnection ) {
        this.addAccessedNodes( Collections.singleton( new SimpleImmutableEntry<>( node, remoteConnection ) ) );
    }


    public void addAccessedNode( final Entry<AbstractRemoteNode, RemoteStatementHandle> node ) {
        this.addAccessedNodes( Collections.singleton( node ) );
    }


    public void addAccessedNodes( final Collection<Entry<AbstractRemoteNode, RemoteStatementHandle>> nodes ) {
        nodes.forEach( node -> {
            this.remoteStatements.put( node.getKey(), node.getValue() );
            this.remoteNodes.compute( node.getValue(), ( handle, set ) -> {
                if ( set == null ) {
                    set = new HashSet<>();
                }
                set.add( node.getKey() );
                return set;
            } );
        } );
    }


    public Collection<AbstractRemoteNode> getAccessedNodes() {
        return Collections.unmodifiableCollection( this.remoteStatements.keySet() );
    }


    public class PreparedStatementInfos extends StatementInfos {

        PreparedStatementInfos( final AbstractRemoteNode remoteNode, final RemoteStatementHandle remoteStatement ) {
            this( new SimpleImmutableEntry<>( remoteNode, remoteStatement ) );
        }


        PreparedStatementInfos( final Entry<AbstractRemoteNode, RemoteStatementHandle> remoteStatement ) {
            this( Collections.singletonList( remoteStatement ) );
        }


        PreparedStatementInfos( final List<Entry<AbstractRemoteNode, RemoteStatementHandle>> remoteStatements ) {
            super( StatementInfos.this.connection, StatementInfos.this.statementHandle );

            remoteStatements.forEach( entry -> {
                this.remoteStatements.put( entry.getKey(), entry.getValue() );
                this.remoteNodes.compute( entry.getValue(), ( sh, set ) -> {
                    if ( set == null ) {
                        set = new HashSet<>();
                    }
                    set.add( entry.getKey() );
                    return set;
                } );
                this.connection.addAccessedNode( entry.getKey(), RemoteConnectionHandle.fromConnectionHandle( new ConnectionHandle( entry.getValue().toStatementHandle().connectionId ) ) );
            } );

            // BEGIN HACK
            this.statementHandle.signature = remoteStatements.isEmpty() ? null : remoteStatements.get( 0 ).getValue().toStatementHandle().signature;
            // END HACK
        }


        public Collection<AbstractRemoteNode> getExecutionTargets() {
            return new LinkedList<>( this.remoteStatements.keySet() );
        }
    }
}
