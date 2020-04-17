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


import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.Meta.ConnectionHandle;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.proto.Common.TypedValue;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.apache.calcite.sql.SqlNode;
import org.jgroups.util.RspList;
import org.polypheny.fram.protocols.AbstractProtocol;
import org.polypheny.fram.protocols.Protocol.ReplicationProtocol;
import org.polypheny.fram.remote.AbstractRemoteNode;
import org.polypheny.fram.remote.Cluster;
import org.polypheny.fram.remote.types.RemoteConnectionHandle;
import org.polypheny.fram.remote.types.RemoteExecuteBatchResult;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.remote.types.RemoteStatementHandle;
import org.polypheny.fram.remote.types.RemoteTransactionHandle;
import org.polypheny.fram.standalone.ConnectionInfos;
import org.polypheny.fram.standalone.ResultSetInfos;
import org.polypheny.fram.standalone.StatementInfos;
import org.polypheny.fram.standalone.StatementInfos.PreparedStatementInfos;
import org.polypheny.fram.standalone.TransactionInfos;
import org.polypheny.fram.standalone.Utils;


public class QuorumReplication extends AbstractProtocol implements ReplicationProtocol {

    public static final QuorumReplication ROWA = new QuorumReplication();

    private Map<ConnectionInfos, List<AbstractRemoteNode>> openConnections = new HashMap<>();


    /**
     * For 1SR consistency, it is required that the read and the write quorum have at least one node in common.
     */
    protected Collection<AbstractRemoteNode> getReadQuorum( Cluster cluster ) {
        // THIS IS ROWA
        return Collections.singletonList( cluster.getLocalNodeAsRemoteNode() );
    }


    /**
     * For 1SR consistency, it is required that the read and the write quorum have at least one node in common.
     */
    protected Collection<AbstractRemoteNode> getWriteQuorum( Cluster cluster ) {
        // THIS IS ROWA
        return cluster.getAllMembers();
    }


    @Override
    public ResultSetInfos prepareAndExecuteDataManipulation( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        final Collection<AbstractRemoteNode> quorum = this.getWriteQuorum( connection.getCluster() );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepareAndExecute[DataManipulation] on {}", quorum );
        }

        final RspList<RemoteExecuteResult> responseList = connection.getCluster().prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, maxRowsInFirstFrame, quorum );

        final Map<AbstractRemoteNode, RemoteExecuteResult> remoteResults = new HashMap<>();
        responseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            final AbstractRemoteNode currentRemote = connection.getCluster().getRemoteNode( address );

            remoteResults.put( currentRemote, remoteStatementHandleRsp.getValue() );

            remoteStatementHandleRsp.getValue().toExecuteResult().resultSets.forEach( resultSet -> {
                connection.addAccessedNode( currentRemote, RemoteConnectionHandle.fromConnectionHandle( new ConnectionHandle( resultSet.connectionId ) ) );
                statement.addAccessedNode( currentRemote, RemoteStatementHandle.fromStatementHandle( new StatementHandle( resultSet.connectionId, resultSet.statementId, resultSet.signature ) ) );
            } );
        } );

        return statement.createResultSet( remoteResults, origins -> origins.entrySet().iterator().next().getValue().toExecuteResult(), ( origins, conn, stmt, offset, fetchMaxRowCount ) -> {
            try {
                return origins.entrySet().iterator().next().getKey().fetch( RemoteStatementHandle.fromStatementHandle( stmt.getStatementHandle() ), offset, fetchMaxRowCount ).toFrame();
            } catch ( RemoteException e ) {
                throw Utils.wrapException( e );
            }
        } );
    }


    @Override
    public ResultSetInfos prepareAndExecuteDataQuery( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        final Collection<AbstractRemoteNode> quorum = this.getReadQuorum( connection.getCluster() );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepareAndExecute[DataQuery] on {}", quorum );
        }

        final RspList<RemoteExecuteResult> responseList = connection.getCluster().prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, maxRowsInFirstFrame, quorum );

        final Map<AbstractRemoteNode, RemoteExecuteResult> remoteResults = new HashMap<>();
        responseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            final AbstractRemoteNode currentRemote = connection.getCluster().getRemoteNode( address );

            remoteResults.put( currentRemote, remoteStatementHandleRsp.getValue() );

            remoteStatementHandleRsp.getValue().toExecuteResult().resultSets.forEach( resultSet -> {
                connection.addAccessedNode( currentRemote, RemoteConnectionHandle.fromConnectionHandle( new ConnectionHandle( resultSet.connectionId ) ) );
                statement.addAccessedNode( currentRemote, RemoteStatementHandle.fromStatementHandle( new StatementHandle( resultSet.connectionId, resultSet.statementId, resultSet.signature ) ) );
            } );
        } );

        return statement.createResultSet( remoteResults, origins -> origins.entrySet().iterator().next().getValue().toExecuteResult(), ( origins, conn, stmt, offset, fetchMaxRowCount ) -> {
            try {
                return origins.entrySet().iterator().next().getKey().fetch( RemoteStatementHandle.fromStatementHandle( stmt.getStatementHandle() ), offset, fetchMaxRowCount ).toFrame();
            } catch ( RemoteException e ) {
                throw Utils.wrapException( e );
            }
        } );
    }


    @Override
    public StatementInfos prepareDataManipulation( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {
        final Collection<AbstractRemoteNode> quorum = this.getWriteQuorum( connection.getCluster() );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepare[DataManipulation] on {}", quorum );
        }

        final RspList<RemoteStatementHandle> responseList = connection.getCluster().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, quorum );
        final Map<AbstractRemoteNode, RemoteStatementHandle> preparedStatements = new HashMap<>();

        responseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            final AbstractRemoteNode currentRemote = connection.getCluster().getRemoteNode( address );

            preparedStatements.put( currentRemote, remoteStatementHandleRsp.getValue() );

            connection.addAccessedNode( currentRemote, RemoteConnectionHandle.fromConnectionHandle( new ConnectionHandle( remoteStatementHandleRsp.getValue().toStatementHandle().connectionId ) ) );
        } );

        return connection.createPreparedStatement( statement, preparedStatements, remoteStatements -> {
            // BEGIN HACK
            return remoteStatements.values().iterator().next().toStatementHandle().signature;
            // END HACK
        } );
    }


    @Override
    public StatementInfos prepareDataQuery( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {
        final Collection<AbstractRemoteNode> quorum = this.getReadQuorum( connection.getCluster() );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepare[DataQuery] on {}", quorum );
        }

        final RspList<RemoteStatementHandle> responseList = connection.getCluster().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, quorum );
        final Map<AbstractRemoteNode, RemoteStatementHandle> preparedStatements = new HashMap<>();

        responseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            final AbstractRemoteNode currentRemote = connection.getCluster().getRemoteNode( address );

            preparedStatements.put( currentRemote, remoteStatementHandleRsp.getValue() );

            connection.addAccessedNode( currentRemote, RemoteConnectionHandle.fromConnectionHandle( new ConnectionHandle( remoteStatementHandleRsp.getValue().toStatementHandle().connectionId ) ) );
        } );

        return connection.createPreparedStatement( statement, preparedStatements, remoteStatements -> {
            // BEGIN HACK
            return remoteStatements.values().iterator().next().toStatementHandle().signature;
            // END HACK
        } );
    }


    @Override
    public ResultSetInfos execute( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<TypedValue> parameterValues, int maxRowsInFirstFrame ) throws NoSuchStatementException, RemoteException {
        if ( !(statement instanceof PreparedStatementInfos) ) {
            throw new IllegalArgumentException( "The provided statement is not a PreparedStatement." );
        }

        // Execute the statement on the nodes it was prepared
        final Collection<AbstractRemoteNode> quorum = ((PreparedStatementInfos) statement).getExecutionTargets();

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "execute on {}", quorum );
        }

        final RspList<RemoteExecuteResult> responseList = connection.getCluster().execute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), parameterValues, maxRowsInFirstFrame, quorum );
        final Map<AbstractRemoteNode, RemoteExecuteResult> remoteResults = new HashMap<>();

        responseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            final AbstractRemoteNode currentRemote = connection.getCluster().getRemoteNode( address );

            remoteResults.put( currentRemote, remoteStatementHandleRsp.getValue() );

            remoteStatementHandleRsp.getValue().toExecuteResult().resultSets.forEach( resultSet -> {
                connection.addAccessedNode( currentRemote, RemoteConnectionHandle.fromConnectionHandle( new ConnectionHandle( resultSet.connectionId ) ) );
                statement.addAccessedNode( currentRemote, RemoteStatementHandle.fromStatementHandle( new StatementHandle( resultSet.connectionId, resultSet.statementId, resultSet.signature ) ) );
            } );
        } );

        return statement.createResultSet( remoteResults, origins -> origins.entrySet().iterator().next().getValue().toExecuteResult(), ( origins, conn, stmt, offset, fetchMaxRowCount ) -> {
            try {
                return origins.entrySet().iterator().next().getKey().fetch( RemoteStatementHandle.fromStatementHandle( stmt.getStatementHandle() ), offset, fetchMaxRowCount ).toFrame();
            } catch ( RemoteException e ) {
                throw Utils.wrapException( e );
            }
        } );
    }


    @Override
    public ResultSetInfos executeBatch( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<UpdateBatch> parameterValues ) throws NoSuchStatementException, RemoteException {
        if ( !(statement instanceof PreparedStatementInfos) ) {
            throw new IllegalArgumentException( "The provided statement is not a PreparedStatement." );
        }

        // Execute the statement on the nodes it was prepared
        final Collection<AbstractRemoteNode> quorum = ((PreparedStatementInfos) statement).getExecutionTargets();

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "executeBatch on {}", quorum );
        }

        final RspList<RemoteExecuteBatchResult> responseList = connection.getCluster().executeBatch( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), parameterValues, quorum );
        final Map<AbstractRemoteNode, RemoteExecuteBatchResult> remoteResults = new HashMap<>();

        responseList.forEach( ( address, remoteExecuteBatchResultRsp ) -> {
            if ( remoteExecuteBatchResultRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteExecuteBatchResultRsp.getException() );
            }
            final AbstractRemoteNode currentRemote = connection.getCluster().getRemoteNode( address );

            remoteResults.put( currentRemote, remoteExecuteBatchResultRsp.getValue() );

            connection.addAccessedNode( currentRemote, RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ) );
        } );

        return statement.createBatchResultSet( remoteResults, origins -> origins.entrySet().iterator().next().getValue().toExecuteBatchResult() );
    }


    @Override
    public Iterable<Serializable> createIterable( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, QueryState state, Signature signature, List<TypedValue> parameterValues, Frame firstFrame ) throws RemoteException {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean syncResults( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, QueryState state, long offset ) throws RemoteException {
        throw new UnsupportedOperationException();
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
