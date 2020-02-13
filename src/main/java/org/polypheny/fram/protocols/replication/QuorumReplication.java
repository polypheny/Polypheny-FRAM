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

package org.polypheny.fram.protocols.replication;


import org.polypheny.fram.protocols.AbstractProtocol;
import org.polypheny.fram.protocols.Protocol.ReplicationProtocol;
import org.polypheny.fram.remote.Cluster;
import org.polypheny.fram.remote.RemoteNode;
import org.polypheny.fram.remote.types.RemoteConnectionHandle;
import org.polypheny.fram.remote.types.RemoteExecuteBatchResult;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.remote.types.RemoteStatementHandle;
import org.polypheny.fram.remote.types.RemoteTransactionHandle;
import org.polypheny.fram.standalone.ConnectionInfos;
import org.polypheny.fram.standalone.ResultSetInfos;
import org.polypheny.fram.standalone.StatementInfos;
import org.polypheny.fram.standalone.TransactionInfos;
import java.rmi.RemoteException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.avatica.Meta.ConnectionProperties;
import org.apache.calcite.avatica.Meta.ExecuteBatchResult;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.proto.Common.TypedValue;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.apache.calcite.sql.SqlNode;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;


public class QuorumReplication extends AbstractProtocol implements ReplicationProtocol {

    public static final QuorumReplication ROWA = new QuorumReplication();

    private Map< /* ConnectionInfos = ConnectionHandle.id */ ConnectionInfos, List<RemoteNode>> openConnections = new HashMap<>();


    /**
     * For 1SR consistency, it is required that the read and the write quorum have at least one node in common.
     */
    protected Collection<RemoteNode> getReadQuorum( Cluster cluster ) {
        // THIS IS ROWA
        return Collections.singletonList( cluster.getLocalNodeAsRemoteNode() );
    }


    /**
     * For 1SR consistency, it is required that the read and the write quorum have at least one node in common.
     */
    protected Collection<RemoteNode> getWriteQuorum( Cluster cluster ) {
        // THIS IS ROWA
        return cluster.getAllMembers();
    }


    private Collection<RemoteNode> getAllNodes( final Cluster cluster ) {
        return cluster.getAllMembers();
    }


    @Override
    public ConnectionProperties connectionSync( Cluster cluster, ConnectionInfos connection, ConnectionProperties newConnectionProperties ) throws RemoteException {
        Collection<RemoteNode> nodesWhichHaveOpenConnections = openConnections.get( connection );
        nodesWhichHaveOpenConnections = connection.getAccessedNodes();
        if ( nodesWhichHaveOpenConnections == null ) {
            return null;
        }
        final RspList<Common.ConnectionProperties> responses = cluster.connectionSync( RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ), newConnectionProperties.toProto(), nodesWhichHaveOpenConnections );
        for ( Rsp<Common.ConnectionProperties> response : responses ) {
            if ( response.hasException() ) {
                // TODO: Check for errors, etc.
            }
        }
        return newConnectionProperties;
    }


    @Override
    public ExecuteResult prepareAndExecuteDataDefinition( Cluster cluster, ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        final String generatedSql = sql.toSqlString( cluster.getLocalNode().getSqlDialect() ).getSql();
        final Collection<RemoteNode> quorum = this.getAllNodes( cluster );

        connection.addAccessedNodes( quorum );
        transaction.addAccessedNodes( quorum );
        statement.withExecutionTargets( quorum );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepareAndExecute[DataDefinition] on {}", quorum );
        }

        final RspList<RemoteExecuteResult> responseList = cluster.prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), generatedSql, maxRowCount, maxRowsInFirstFrame, quorum );

        final List<Entry<RemoteNode, RemoteExecuteResult>> remoteResults = new LinkedList<>();
        responseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            remoteResults.add( new SimpleImmutableEntry<>( cluster.getRemoteNode( address ), remoteStatementHandleRsp.getValue() ) );
        } );

        final ResultSetInfos resultSetInfos = statement.createResultSet( remoteResults );

        return resultSetInfos.getExecuteResult();
    }


    @Override
    public ExecuteResult prepareAndExecuteDataManipulation( Cluster cluster, ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        final String generatedSql = sql.toSqlString( cluster.getLocalNode().getSqlDialect() ).getSql();
        final Collection<RemoteNode> quorum = this.getWriteQuorum( cluster );

        connection.addAccessedNodes( quorum );
        transaction.addAccessedNodes( quorum );
        statement.withExecutionTargets( quorum );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepareAndExecute[DataManipulation] on {}", quorum );
        }

        final RspList<RemoteExecuteResult> responseList = cluster.prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), generatedSql, maxRowCount, maxRowsInFirstFrame, quorum );

        final List<Entry<RemoteNode, RemoteExecuteResult>> remoteResults = new LinkedList<>();
        responseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            remoteResults.add( new SimpleImmutableEntry<>( cluster.getRemoteNode( address ), remoteStatementHandleRsp.getValue() ) );
        } );

        final ResultSetInfos resultSetInfos = statement.createResultSet( remoteResults );

        return resultSetInfos.getExecuteResult();
    }


    @Override
    public ExecuteResult prepareAndExecuteDataQuery( Cluster cluster, ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        final String generatedSql = sql.toSqlString( cluster.getLocalNode().getSqlDialect() ).getSql();
        final Collection<RemoteNode> quorum = this.getReadQuorum( cluster );

        connection.addAccessedNodes( quorum );
        transaction.addAccessedNodes( quorum );
        statement.withExecutionTargets( quorum );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepareAndExecute[DataQuery] on {}", quorum );
        }

        final RspList<RemoteExecuteResult> responseList = cluster.prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), generatedSql, maxRowCount, maxRowsInFirstFrame, quorum );

        final List<Entry<RemoteNode, RemoteExecuteResult>> remoteResults = new LinkedList<>();
        responseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            remoteResults.add( new SimpleImmutableEntry<>( cluster.getRemoteNode( address ), remoteStatementHandleRsp.getValue() ) );
        } );

        final ResultSetInfos resultSetInfos = statement.createResultSet( remoteResults );

        return resultSetInfos.getExecuteResult();
    }


    @Override
    public ExecuteResult prepareAndExecuteTransactionCommit( Cluster cluster, ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        final String generatedSql = sql.toSqlString( cluster.getLocalNode().getSqlDialect() ).getSql();

        final Collection<RemoteNode> accessedNodes = transaction.getAccessedNodes();

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepareAndExecute[TransactionCommit] on {}", accessedNodes );
        }

        final RspList<RemoteExecuteResult> responseList = cluster.prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), generatedSql, maxRowCount, maxRowsInFirstFrame, accessedNodes );

        final List<Entry<RemoteNode, RemoteExecuteResult>> remoteResults = new LinkedList<>();
        responseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            remoteResults.add( new SimpleImmutableEntry<>( cluster.getRemoteNode( address ), remoteStatementHandleRsp.getValue() ) );
        } );

        final ResultSetInfos resultSetInfos = statement.createResultSet( remoteResults );

        return resultSetInfos.getExecuteResult();
    }


    @Override
    public ExecuteResult prepareAndExecuteTransactionRollback( Cluster cluster, ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        final String generatedSql = sql.toSqlString( cluster.getLocalNode().getSqlDialect() ).getSql();

        final Collection<RemoteNode> accessedNodes = transaction.getAccessedNodes();

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepareAndExecute[TransactionRollback] on {}", accessedNodes );
        }

        final RspList<RemoteExecuteResult> responseList = cluster.prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), generatedSql, maxRowCount, maxRowsInFirstFrame, accessedNodes );

        final List<Entry<RemoteNode, RemoteExecuteResult>> remoteResults = new LinkedList<>();
        responseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            remoteResults.add( new SimpleImmutableEntry<>( cluster.getRemoteNode( address ), remoteStatementHandleRsp.getValue() ) );
        } );

        final ResultSetInfos resultSetInfos = statement.createResultSet( remoteResults );

        return resultSetInfos.getExecuteResult();
    }


    @Override
    public StatementInfos prepareDataManipulation( Cluster cluster, ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {
        final String generatedSql = sql.toSqlString( cluster.getLocalNode().getSqlDialect() ).getSql();
        final Collection<RemoteNode> quorum = this.getWriteQuorum( cluster );

        connection.addAccessedNodes( quorum );
        statement.withExecutionTargets( quorum );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepare[DataManipulation] on {}", quorum );
        }

        final RspList<RemoteStatementHandle> responseList = cluster.prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), generatedSql, maxRowCount, quorum );
        final List<Entry<RemoteNode, RemoteStatementHandle>> remoteStatements = new LinkedList<>();

        responseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            remoteStatements.add( new SimpleImmutableEntry<>( cluster.getRemoteNode( address ), remoteStatementHandleRsp.getValue() ) );
        } );
        return connection.createPreparedStatement( statement, remoteStatements, quorum );
        //result.getStatementHandle().signature = ... TODO (remove the assignment in StatementInfos?)
    }


    @Override
    public StatementInfos prepareDataQuery( Cluster cluster, ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {
        final String generatedSql = sql.toSqlString( cluster.getLocalNode().getSqlDialect() ).getSql();
        final Collection<RemoteNode> quorum = this.getReadQuorum( cluster );

        connection.addAccessedNodes( quorum );
        statement.withExecutionTargets( quorum );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepare[DataQuery] on {}", quorum );
        }

        final RspList<RemoteStatementHandle> responseList = cluster.prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), generatedSql, maxRowCount, quorum );
        final List<Entry<RemoteNode, RemoteStatementHandle>> remoteStatements = new LinkedList<>();

        responseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            remoteStatements.add( new SimpleImmutableEntry<>( cluster.getRemoteNode( address ), remoteStatementHandleRsp.getValue() ) );
        } );

        return connection.createPreparedStatement( statement, remoteStatements, quorum );
    }


    @Override
    public ExecuteResult execute( Cluster cluster, ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<TypedValue> parameterValues, int maxRowsInFirstFrame ) throws NoSuchStatementException, RemoteException {
        // Execute the statement on the nodes it was prepared
        final Collection<RemoteNode> quorum = statement.getExecutionTargets();

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "execute on {}", quorum );
        }

        connection.addAccessedNodes( quorum );
        transaction.addAccessedNodes( quorum );

        final RspList<RemoteExecuteResult> responseList = cluster.execute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), parameterValues, maxRowsInFirstFrame, quorum );
        final List<Entry<RemoteNode, RemoteExecuteResult>> remoteResults = new LinkedList<>();

        responseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            remoteResults.add( new SimpleImmutableEntry<>( cluster.getRemoteNode( address ), remoteStatementHandleRsp.getValue() ) );
        } );

        final ResultSetInfos resultSetInfos = statement.createResultSet( remoteResults );

        return resultSetInfos.getExecuteResult();
    }


    @Override
    public ExecuteBatchResult executeBatch( Cluster cluster, ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<UpdateBatch> parameterValues ) throws NoSuchStatementException, RemoteException {
        // Execute the statement on the nodes it was prepared
        final Collection<RemoteNode> quorum = statement.getExecutionTargets();

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "executeBatch on {}", quorum );
        }

        connection.addAccessedNodes( quorum );
        transaction.addAccessedNodes( quorum );

        final RspList<RemoteExecuteBatchResult> responseList = cluster.executeBatch( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), parameterValues, quorum );
        final List<Entry<RemoteNode, RemoteExecuteBatchResult>> remoteResults = new LinkedList<>();

        responseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            remoteResults.add( new SimpleImmutableEntry<>( cluster.getRemoteNode( address ), remoteStatementHandleRsp.getValue() ) );
        } );

        final ResultSetInfos resultSetInfos = statement.createBatchResultSet( remoteResults );

        return resultSetInfos.getExecuteResult();
    }


    @Override
    public Frame fetch( Cluster cluster, StatementHandle statementHandle, long offset, int fetchMaxRowCount ) throws NoSuchStatementException, MissingResultsException {
        return null;
    }


    @Override
    public void commit( Cluster cluster, ConnectionInfos connection, TransactionInfos transaction ) throws RemoteException {
        final Collection<RemoteNode> accessedNodes = transaction.getAccessedNodes();

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "commit on {}", accessedNodes );
        }

        if ( transaction.requires2pc() ) {
            final AtomicReference<Boolean> prepareCommitResultHolder = new AtomicReference<>( Boolean.TRUE );
            final RspList<Boolean> prepareCommitResponses = cluster.prepareCommit( RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ), RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), accessedNodes );
            prepareCommitResponses.forEach( ( address, remoteStatementHandleRsp ) -> {
                if ( remoteStatementHandleRsp.hasException() ) {
                    throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
                }
                prepareCommitResultHolder.getAndUpdate( prepareCommitResult -> Boolean.logicalAnd( prepareCommitResult, remoteStatementHandleRsp.getValue() ) );
            } );
            if ( prepareCommitResultHolder.get() == false ) {
                // one agent voted NO
                // According to 2PC rules --> ROLLBACK
                this.rollback( cluster, connection, transaction );
                throw new RuntimeException( "ROLLBACK performed since an agent voted NO in the 2PC prepare phase." );
            }
            // all YES --> continue commit
            cluster.commit( RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ), RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), accessedNodes );
        } else {
            cluster.onePhaseCommit( RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ), RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), accessedNodes );
        }
    }


    @Override
    public void rollback( Cluster cluster, ConnectionInfos connection, TransactionInfos transaction ) throws RemoteException {
        final Collection<RemoteNode> accessedNodes = transaction.getAccessedNodes();

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "rollback on {}", accessedNodes );
        }

        cluster.rollback( RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ), RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), accessedNodes );
    }


    @Override
    public void closeStatement( Cluster cluster, StatementInfos statement ) throws RemoteException {
        final Collection<RemoteNode> accessedNodes = statement.getAccessedNodes();

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "closeStatement on {}", accessedNodes );
        }

        cluster.closeStatement( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), accessedNodes );
    }


    @Override
    public void closeConnection( Cluster cluster, ConnectionInfos connection ) throws RemoteException {
        final Collection<RemoteNode> accessedNodes = connection.getAccessedNodes();

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "closeConnection on {}", accessedNodes );
        }

        cluster.closeConnection( RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ), accessedNodes );
    }


    @Override
    public FragmentationProtocol setFragmentationProtocol( FragmentationProtocol fragmentationProtocol ) {
        return null;
    }


    @Override
    public PlacementProtocol setAllocationProtocol( PlacementProtocol placementProtocol ) {
        return null;
    }
}
