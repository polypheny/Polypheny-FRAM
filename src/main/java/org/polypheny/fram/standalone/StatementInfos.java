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


import io.vavr.Function4;
import io.vavr.Function5;
import java.sql.SQLFeatureNotSupportedException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import lombok.EqualsAndHashCode;
import org.apache.calcite.avatica.Meta.ConnectionHandle;
import org.apache.calcite.avatica.Meta.ExecuteBatchResult;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.proto.Common.TypedValue;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.polypheny.fram.Node;
import org.polypheny.fram.remote.AbstractRemoteNode;
import org.polypheny.fram.remote.PhysicalNode;
import org.polypheny.fram.remote.types.RemoteConnectionHandle;
import org.polypheny.fram.remote.types.RemoteStatementHandle;
import org.polypheny.fram.standalone.Meta.Result;
import org.polypheny.fram.standalone.Meta.Statement;
import org.polypheny.fram.standalone.ResultSetInfos.BatchResultSet;
import org.polypheny.fram.standalone.ResultSetInfos.QueryResultSet;


/**
 * Represents a (Prepared)Statement
 */
@EqualsAndHashCode(doNotUseGetters = true, onlyExplicitlyIncluded = true)
public class StatementInfos implements Statement {

    @EqualsAndHashCode.Include
    protected final ConnectionInfos connection;
    @EqualsAndHashCode.Include
    protected final StatementHandle statementHandle;

    protected final Map<Node, Statement> remoteStatements = new LinkedHashMap<>();
    protected final Map<Statement, Set<Node>> remoteNodes = new LinkedHashMap<>();
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


    public Statement getRemoteStatementHandle( final AbstractRemoteNode node ) {
        synchronized ( this.remoteStatements ) {
            return this.remoteStatements.get( node );
        }
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


    public <NodeType extends Node, ResultType extends Result> QueryResultSet createResultSet( NodeType origin, ResultType remoteResult ) {
        synchronized ( this ) {
            this.resultSetInfos = new QueryResultSet<NodeType, ResultType>( this, origin, remoteResult );
            return (QueryResultSet) this.resultSetInfos;
        }
    }


    public <NodeType extends Node, ResultType extends Result> QueryResultSet createResultSet( Set<NodeType> sources, Supplier<ExecuteResult> executeResultSupplier, Supplier<ExecuteResult> generatedKeysSupplier ) {
        return this.<NodeType, ResultType>createResultSet( sources, executeResultSupplier, generatedKeysSupplier,
                ( _sources, _connection, _statement, _offset, _fetchMaxRowCount ) -> {
                    throw Utils.wrapException( new SQLFeatureNotSupportedException( "Fetch is not supported." ) );
                } );
    }


    public <NodeType extends Node, ResultType extends Result> QueryResultSet createResultSet( Set<NodeType> sources, Supplier<ExecuteResult> executeResultSupplier, Supplier<ExecuteResult> generatedKeysSupplier, Function5<Set<NodeType>, ConnectionInfos, StatementInfos, Long, Integer, Frame> resultsFetchFunction ) {
        synchronized ( this ) {
            this.resultSetInfos = new QueryResultSet<>( this, sources, executeResultSupplier, generatedKeysSupplier, resultsFetchFunction );
            return (QueryResultSet) this.resultSetInfos;
        }
    }


    public <NodeType extends Node, ResultType extends Result> BatchResultSet createBatchResultSet( NodeType source, ResultType result ) {
        synchronized ( this ) {
            this.resultSetInfos = new BatchResultSet<>( this, source, result );
            return (BatchResultSet) this.resultSetInfos;
        }
    }


    public <NodeType extends Node, ResultType extends Result> BatchResultSet createBatchResultSet( Set<NodeType> sources, Supplier<ExecuteBatchResult> executeBatchResultSupplier, Supplier<ExecuteResult> generatedKeysSupplier ) {
        synchronized ( this ) {
            this.resultSetInfos = new BatchResultSet<>( this, sources, executeBatchResultSupplier, generatedKeysSupplier );
            return (BatchResultSet) this.resultSetInfos;
        }
    }


    public void addAccessedNode( final PhysicalNode node ) {
        this.addAccessedNode( node, this );
    }


    public void addAccessedNode( final Node node, Statement statement ) {
        this.addAccessedNodes( Collections.singleton( new SimpleImmutableEntry<>( node, statement ) ) );
    }


    public void addAccessedNode( final Entry<? extends Node, ? extends Statement> node ) {
        this.addAccessedNodes( Collections.singleton( node ) );
    }


    public void addAccessedNodes( final Collection<Entry<? extends Node, ? extends Statement>> nodes ) {
        nodes.forEach( node -> {
            synchronized ( this.remoteStatements ) {
                this.remoteStatements.put( node.getKey(), node.getValue() );
            }
            synchronized ( this.remoteNodes ) {
                this.remoteNodes.compute( node.getValue(), ( handle, set ) -> {
                    if ( set == null ) {
                        set = new HashSet<>();
                    }
                    set.add( node.getKey() );
                    return set;
                } );
            }
        } );
    }


    public Collection<PhysicalNode> getAccessedPhysicalNodes() {
        final Set<PhysicalNode> physicalNodes = new HashSet<>();
        synchronized ( this.remoteStatements ) {
            for ( Node node : this.remoteStatements.keySet() ) {
                if ( node instanceof PhysicalNode ) {
                    physicalNodes.add( (PhysicalNode) node );
                }
            }
        }
        return Collections.unmodifiableCollection( physicalNodes );
    }


    @EqualsAndHashCode(callSuper = true)
    public class PreparedStatementInfos<NodeType extends Node, StatementType extends Statement> extends StatementInfos {

        private final Function5<ConnectionInfos, TransactionInfos, StatementInfos, List<TypedValue>, Integer, QueryResultSet> executeFunction;
        private final Function4<ConnectionInfos, TransactionInfos, StatementInfos, List<UpdateBatch>, BatchResultSet> executeBatchFunction;

        private Function5<ConnectionInfos, TransactionInfos, StatementInfos, List, ResultSetInfos, Void> workloadAnalysisFunction;


        PreparedStatementInfos( final NodeType remoteNode, final StatementType statement ) {
            this( remoteNode, statement, null, null );
        }


        PreparedStatementInfos( final Map<NodeType, StatementType> remoteStatements, final Supplier<Signature> signatureSupplier ) {
            this( remoteStatements, signatureSupplier, null, null );
        }


        PreparedStatementInfos(
                final NodeType remoteNode, final StatementType statement,
                final Function5<ConnectionInfos, TransactionInfos, StatementInfos, List<TypedValue>, Integer, QueryResultSet> executeFunction,
                final Function4<ConnectionInfos, TransactionInfos, StatementInfos, List<UpdateBatch>, BatchResultSet> executeBatchFunction ) {
            this( Collections.singletonMap( remoteNode, statement ), () -> {
                if ( statement instanceof StatementHandle ) {
                    return ((StatementHandle) statement).signature;
                }
                if ( statement instanceof RemoteStatementHandle ) {
                    return ((RemoteStatementHandle) statement).toStatementHandle().signature;
                }
                if ( statement instanceof StatementInfos ) {
                    return ((StatementInfos) statement).statementHandle.signature;
                }
                throw new IllegalArgumentException( "Type of statement is not of StatementHandle, RemoteStatementHandle, or StatementInfos." );
            }, executeFunction, executeBatchFunction, null );
        }


        PreparedStatementInfos(
                final Map<NodeType, StatementType> remoteStatements,
                final Supplier<Signature> signatureSupplier,
                final Function5<ConnectionInfos, TransactionInfos, StatementInfos, List<TypedValue>, Integer, QueryResultSet> executeFunction,
                final Function4<ConnectionInfos, TransactionInfos, StatementInfos, List<UpdateBatch>, BatchResultSet> executeBatchFunction ) {
            this( remoteStatements, signatureSupplier, executeFunction, executeBatchFunction, null );
        }


        PreparedStatementInfos(
                final Map<NodeType, StatementType> statements,
                final Supplier<Signature> signatureSupplier,
                final Function5<ConnectionInfos, TransactionInfos, StatementInfos, List<TypedValue>, Integer, QueryResultSet> executeFunction,
                final Function4<ConnectionInfos, TransactionInfos, StatementInfos, List<UpdateBatch>, BatchResultSet> executeBatchFunction,
                final int[] primaryKeyColumnIndexes ) {
            super( StatementInfos.this.connection, StatementInfos.this.statementHandle, primaryKeyColumnIndexes );

            statements.forEach( ( node, statement ) -> {
                synchronized ( this.remoteStatements ) {
                    this.remoteStatements.put( node, statement );
                }
                synchronized ( this.remoteNodes ) {
                    this.remoteNodes.compute( statement, ( sh, set ) -> {
                        if ( set == null ) {
                            set = new HashSet<>();
                        }
                        set.add( node );
                        return set;
                    } );
                }
                this.connection.addAccessedNode( node, RemoteConnectionHandle.fromConnectionHandle( new ConnectionHandle( Optional.ofNullable(
                        statement instanceof StatementHandle ? ((StatementHandle) statement).connectionId :
                                statement instanceof RemoteStatementHandle ? ((RemoteStatementHandle) statement).toStatementHandle().connectionId :
                                        statement instanceof StatementInfos ? ((StatementInfos) statement).statementHandle.connectionId : null
                ).orElseThrow( () -> new IllegalArgumentException( "Type of statement is not of StatementHandle, RemoteStatementHandle, or StatementInfos." ) ) ) ) );
            } );

            this.statementHandle.signature = signatureSupplier.get();

            this.executeFunction = executeFunction;
            this.executeBatchFunction = executeBatchFunction;
        }


        public Collection<Node> getExecutionTargets() {
            synchronized ( this.remoteStatements ) {
                return new LinkedList<>( this.remoteStatements.keySet() );
            }
        }


        public QueryResultSet execute( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<TypedValue> parameterValues, int maxRowsInFirstFrame ) {
            return this.executeFunction.apply( connection, transaction, statement, parameterValues, maxRowsInFirstFrame );
        }


        public BatchResultSet executeBatch( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<UpdateBatch> parameterValues ) {
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
