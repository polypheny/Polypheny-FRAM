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


import io.vavr.Function1;
import io.vavr.Function4;
import io.vavr.Function5;
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
import lombok.EqualsAndHashCode;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.ConnectionHandle;
import org.apache.calcite.avatica.Meta.ExecuteBatchResult;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.proto.Common.TypedValue;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.polypheny.fram.remote.AbstractRemoteNode;
import org.polypheny.fram.remote.types.RemoteConnectionHandle;
import org.polypheny.fram.remote.types.RemoteExecuteBatchResult;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.remote.types.RemoteStatementHandle;
import org.polypheny.fram.standalone.ResultSetInfos.BatchResultSetInfos;
import org.polypheny.fram.standalone.ResultSetInfos.QueryResultSet;


/**
 * Represents a (Prepared)Statement
 */
@EqualsAndHashCode(doNotUseGetters = true, onlyExplicitlyIncluded = true)
public class StatementInfos {

    @EqualsAndHashCode.Include
    protected final ConnectionInfos connection;
    @EqualsAndHashCode.Include
    protected final StatementHandle statementHandle;
    protected final Map<AbstractRemoteNode, RemoteStatementHandle> remoteStatements = new LinkedHashMap<>();
    protected final Map<RemoteStatementHandle, Set<AbstractRemoteNode>> remoteNodes = new LinkedHashMap<>();
    protected final int[] primaryKeyColumnIndexes;
    protected ResultSetInfos resultSetInfos;


    StatementInfos( final ConnectionInfos connection, final StatementHandle statementHandle ) {
        this( connection, statementHandle, null );
    }


    StatementInfos( final ConnectionInfos connection, final StatementHandle statementHandle, final int[] primaryKeyColumnIndexes ) {
        this.connection = connection;
        this.statementHandle = statementHandle;
        if ( primaryKeyColumnIndexes == null ) {
            this.primaryKeyColumnIndexes = null;
        } else {
            this.primaryKeyColumnIndexes = new int[primaryKeyColumnIndexes.length];
            System.arraycopy( primaryKeyColumnIndexes, 0, this.primaryKeyColumnIndexes, 0, this.primaryKeyColumnIndexes.length );
        }
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


    public boolean hasPrimaryKeyColumnIndexes() {
        return this.primaryKeyColumnIndexes != null && this.primaryKeyColumnIndexes.length > 0;
    }


    public int[] getPrimaryKeyColumnIndexes() {
        if ( this.primaryKeyColumnIndexes == null ) {
            return null;
        }
        if ( this.primaryKeyColumnIndexes.length == 0 ) {
            return new int[0];
        }
        int[] primaryKeyColumnIndexes = new int[this.primaryKeyColumnIndexes.length];
        System.arraycopy( this.primaryKeyColumnIndexes, 0, primaryKeyColumnIndexes, 0, primaryKeyColumnIndexes.length );
        return primaryKeyColumnIndexes;
    }


    public QueryResultSet createResultSet( Map<AbstractRemoteNode, RemoteExecuteResult> remoteResults, Function1<Map<AbstractRemoteNode, RemoteExecuteResult>, ExecuteResult> resultsMergeFunction, Function5<Map<AbstractRemoteNode, RemoteExecuteResult>, ConnectionInfos, StatementInfos, Long, Integer, Frame> resultsFetchFunction ) {
        synchronized ( this ) {
            this.resultSetInfos = new QueryResultSet( this, remoteResults, resultsMergeFunction, resultsFetchFunction );
            return (QueryResultSet) this.resultSetInfos;
        }
    }


    public BatchResultSetInfos createBatchResultSet( Map<AbstractRemoteNode, RemoteExecuteBatchResult> remoteBatchResults, Function1<Map<AbstractRemoteNode, RemoteExecuteBatchResult>, ExecuteBatchResult> resultMergeFunction ) {
        synchronized ( this ) {
            this.resultSetInfos = new BatchResultSetInfos( this, remoteBatchResults, resultMergeFunction );
            return (BatchResultSetInfos) this.resultSetInfos;
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


    @EqualsAndHashCode(callSuper = true)
    public class PreparedStatementInfos extends StatementInfos {

        PreparedStatementInfos( final AbstractRemoteNode remoteNode, final RemoteStatementHandle remoteStatement ) {
            this( Collections.singletonMap( remoteNode, remoteStatement ), origins -> remoteStatement.toStatementHandle().signature );
        }


        PreparedStatementInfos( final Map<AbstractRemoteNode, RemoteStatementHandle> remoteStatements, final Function1<Map<AbstractRemoteNode, RemoteStatementHandle>, Meta.Signature> signatureMergeFunction ) {
            this( remoteStatements, signatureMergeFunction, null, null );
        }


        private final Function5<ConnectionInfos, TransactionInfos, StatementInfos, List<TypedValue>, Integer, QueryResultSet> executeFunction;
        private final Function4<ConnectionInfos, TransactionInfos, StatementInfos, List<UpdateBatch>, BatchResultSetInfos> executeBatchFunction;

        private Function5<ConnectionInfos, TransactionInfos, StatementInfos, List, ResultSetInfos, Void> workloadAnalysisFunction;


        public PreparedStatementInfos(
                final Map<AbstractRemoteNode, RemoteStatementHandle> remoteStatements,
                final Function1<Map<AbstractRemoteNode, RemoteStatementHandle>, Signature> signatureMergeFunction,
                final Function5<ConnectionInfos, TransactionInfos, StatementInfos, List<TypedValue>, Integer, QueryResultSet> executeFunction,
                final Function4<ConnectionInfos, TransactionInfos, StatementInfos, List<UpdateBatch>, BatchResultSetInfos> executeBatchFunction ) {
            this( remoteStatements, signatureMergeFunction, executeFunction, executeBatchFunction, null );
        }


        public PreparedStatementInfos(
                final Map<AbstractRemoteNode, RemoteStatementHandle> remoteStatements,
                final Function1<Map<AbstractRemoteNode, RemoteStatementHandle>, Signature> signatureMergeFunction,
                final Function5<ConnectionInfos, TransactionInfos, StatementInfos, List<TypedValue>, Integer, QueryResultSet> executeFunction,
                final Function4<ConnectionInfos, TransactionInfos, StatementInfos, List<UpdateBatch>, BatchResultSetInfos> executeBatchFunction,
                final int[] primaryKeyColumnIndexes ) {

            super( StatementInfos.this.connection, StatementInfos.this.statementHandle, primaryKeyColumnIndexes );

            remoteStatements.forEach( ( abstractRemoteNode, remoteStatementHandle ) -> {
                this.remoteStatements.put( abstractRemoteNode, remoteStatementHandle );
                this.remoteNodes.compute( remoteStatementHandle, ( sh, set ) -> {
                    if ( set == null ) {
                        set = new HashSet<>();
                    }
                    set.add( abstractRemoteNode );
                    return set;
                } );
                this.connection.addAccessedNode( abstractRemoteNode, RemoteConnectionHandle.fromConnectionHandle( new ConnectionHandle( remoteStatementHandle.toStatementHandle().connectionId ) ) );
            } );

            this.statementHandle.signature = signatureMergeFunction.apply( remoteStatements );

            this.executeFunction = executeFunction;
            this.executeBatchFunction = executeBatchFunction;
        }


        public Collection<AbstractRemoteNode> getExecutionTargets() {
            return new LinkedList<>( this.remoteStatements.keySet() );
        }


        public QueryResultSet execute( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<TypedValue> parameterValues, int maxRowsInFirstFrame ) {
            return this.executeFunction.apply( connection, transaction, statement, parameterValues, maxRowsInFirstFrame );
        }


        public BatchResultSetInfos executeBatch( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<UpdateBatch> parameterValues ) {
            return this.executeBatchFunction.apply( connection, transaction, statement, parameterValues );
        }


        public Function5<ConnectionInfos, TransactionInfos, StatementInfos, List, ResultSetInfos, Void> getWorkloadAnalysisFunction() {
            return this.workloadAnalysisFunction;
        }


        public void setWorkloadAnalysisFunction( Function5<ConnectionInfos, TransactionInfos, StatementInfos, List, ResultSetInfos, Void> workloadAnalysisFunction ) {
            this.workloadAnalysisFunction = workloadAnalysisFunction;
        }
    }
}
