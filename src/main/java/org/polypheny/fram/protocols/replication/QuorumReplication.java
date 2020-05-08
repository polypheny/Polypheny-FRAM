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


import io.vavr.Function1;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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


/**
 * """
 * > Read and write quorums must fulfill the following constraints:
 * > 2 · wq > n and rq + wq > n, being n the number of sites.
 * """
 * -- Ricardo Jiménez-Peris, Marta Patiño-Martínez, Gustavo Alonso, and Bettina Kemme. 2003. Are Quorums an Alternative for Data Replication? ACM Trans. Database Syst. 28, 3 (September 2003), 257–294. DOI:https://doi.org/10.1145/937598.937601
 */
public class QuorumReplication extends AbstractProtocol implements ReplicationProtocol {

    /**
     *
     */
    public static final QuorumReplication ROWA = new ROWA();

    /**
     * """
     * > The minimum quorum sizes satisfying the constraints are:
     * > 2 · wq = n + 1 and rq + wq = n + 1 and therefore,
     * > wq = floor(n / 2) + 1 and rq = ceil(n / 2 ) = floor((n+1) / 2).
     * """
     * -- Ricardo Jiménez-Peris, Marta Patiño-Martínez, Gustavo Alonso, and Bettina Kemme. 2003. Are Quorums an Alternative for Data Replication? ACM Trans. Database Syst. 28, 3 (September 2003), 257–294. DOI:https://doi.org/10.1145/937598.937601
     */
    public static final QuorumReplication MAJORITY = new QuorumReplication(
            connection -> {
                List<AbstractRemoteNode> candidates = new LinkedList<>( connection.getCluster().getMembers() );
                Collections.shuffle( candidates );
                Set<AbstractRemoteNode> readQuorum = new LinkedHashSet<>(); // floor((n+1) / 2)
                final int n = candidates.size();
                final int rq = Math.floorDiv( n + 1, 2 );
                for ( AbstractRemoteNode candidate : candidates ) {
                    readQuorum.add( candidate );
                    if ( readQuorum.size() >= rq ) {
                        break;
                    }
                }
                return Collections.unmodifiableSet( readQuorum );
            },
            connection -> {
                List<AbstractRemoteNode> candidates = new LinkedList<>( connection.getCluster().getMembers() );
                Collections.shuffle( candidates );
                Set<AbstractRemoteNode> writeQuorum = new LinkedHashSet<>(); // floor(n / 2) + 1
                final int n = candidates.size();
                final int wq = Math.floorDiv( n, 2 ) + 1;
                for ( AbstractRemoteNode candidate : candidates ) {
                    writeQuorum.add( candidate );
                    if ( writeQuorum.size() >= wq ) {
                        break;
                    }
                }
                return Collections.unmodifiableSet( writeQuorum );
            }
    );

    public final Function1<ConnectionInfos, Collection<AbstractRemoteNode>> readQuorumFunction;
    public final Function1<ConnectionInfos, Collection<AbstractRemoteNode>> writeQuorumFunction;


    public QuorumReplication( final Function1<ConnectionInfos, Collection<AbstractRemoteNode>> readQuorumFunction, final Function1<ConnectionInfos, Collection<AbstractRemoteNode>> writeQuorumFunction ) {
        this.readQuorumFunction = readQuorumFunction;
        this.writeQuorumFunction = writeQuorumFunction;
    }


    /**
     * For 1SR consistency, it is required that the read and the write quorum have at least one node in common.
     */
    protected Collection<AbstractRemoteNode> getReadQuorum( final ConnectionInfos connection ) {
        return this.readQuorumFunction.apply( connection );
    }


    /**
     * For 1SR consistency, it is required that the read and the write quorum have at least one node in common.
     */
    protected Collection<AbstractRemoteNode> getWriteQuorum( final ConnectionInfos connection ) {
        return this.writeQuorumFunction.apply( connection );
    }


    @Override
    public ResultSetInfos prepareAndExecuteDataDefinition( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        throw new UnsupportedOperationException();
    }


    @Override
    public ResultSetInfos prepareAndExecuteDataManipulation( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        final Collection<AbstractRemoteNode> quorum = this.getWriteQuorum( connection );
        LOGGER.trace( "prepareAndExecute[DataManipulation] on {}", quorum );

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
        final Collection<AbstractRemoteNode> quorum = this.getReadQuorum( connection );
        LOGGER.trace( "prepareAndExecute[DataQuery] on {}", quorum );

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
        final Collection<AbstractRemoteNode> quorum = this.getWriteQuorum( connection );
        LOGGER.trace( "prepare[DataManipulation] on {}", quorum );

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
        final Collection<AbstractRemoteNode> quorum = this.getReadQuorum( connection );
        LOGGER.trace( "prepare[DataQuery] on {}", quorum );

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
        LOGGER.trace( "execute on {}", quorum );

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
        LOGGER.trace( "executeBatch on {}", quorum );

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


    public static class ROWA extends QuorumReplication {

        public ROWA() {
            super(
                    connection -> Collections.singletonList( connection.getCluster().getLocalNodeAsRemoteNode() ), // read quorum
                    connection -> Collections.unmodifiableList( connection.getCluster().getMembers() ) // write quorum
            );
        }


        /**
         * For ROWA we do not need to alter the tables to include version columns.
         */
        @Override
        public ResultSetInfos prepareAndExecuteDataDefinition( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
            final Collection<AbstractRemoteNode> quorum = this.getAllNodes( connection.getCluster() );
            LOGGER.trace( "prepareAndExecute[DataDefinition] on {}", quorum );

            final RspList<RemoteExecuteResult> responseList = connection.getCluster().prepareAndExecuteDataDefinition( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, sql, maxRowCount, maxRowsInFirstFrame, quorum );

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
    }
}
