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


import java.rmi.RemoteException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.calcite.avatica.Meta.ExecuteBatchResult;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.proto.Common.TypedValue;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.apache.calcite.sql.SqlNode;
import org.jgroups.util.RspList;
import org.polypheny.fram.protocols.AbstractProtocol;
import org.polypheny.fram.protocols.Protocol.ReplicationProtocol;
import org.polypheny.fram.remote.AbstractRemoteNode;
import org.polypheny.fram.remote.Cluster;
import org.polypheny.fram.remote.types.RemoteExecuteBatchResult;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.remote.types.RemoteStatementHandle;
import org.polypheny.fram.remote.types.RemoteTransactionHandle;
import org.polypheny.fram.standalone.ConnectionInfos;
import org.polypheny.fram.standalone.ResultSetInfos;
import org.polypheny.fram.standalone.StatementInfos;
import org.polypheny.fram.standalone.TransactionInfos;


public class QuorumReplication extends AbstractProtocol implements ReplicationProtocol {

    public static final QuorumReplication ROWA = new QuorumReplication();

    private Map<ConnectionInfos /*= ConnectionHandle.id */, List<AbstractRemoteNode>> openConnections = new HashMap<>();

    //private final Executor cluster = new Executor();
    private final Cluster cluster = Cluster.getDefaultCluster(); // TODO: Replace me with the executor


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
    public ExecuteResult prepareAndExecuteDataManipulation( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        final Collection<AbstractRemoteNode> quorum = this.getWriteQuorum( connection.getCluster() );

        connection.addAccessedNodes( quorum );
        transaction.addAccessedNodes( quorum );
        statement.withExecutionTargets( quorum );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepareAndExecute[DataManipulation] on {}", quorum );
        }

        final RspList<RemoteExecuteResult> responseList = cluster.prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, maxRowsInFirstFrame, quorum );

        final List<Entry<AbstractRemoteNode, RemoteExecuteResult>> remoteResults = new LinkedList<>();
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
    public ExecuteResult prepareAndExecuteDataQuery( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        final Collection<AbstractRemoteNode> quorum = this.getReadQuorum( connection.getCluster() );

        connection.addAccessedNodes( quorum );
        transaction.addAccessedNodes( quorum );
        statement.withExecutionTargets( quorum );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepareAndExecute[DataQuery] on {}", quorum );
        }

        final RspList<RemoteExecuteResult> responseList = cluster.prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, maxRowsInFirstFrame, quorum );

        final List<Entry<AbstractRemoteNode, RemoteExecuteResult>> remoteResults = new LinkedList<>();
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
    public StatementInfos prepareDataManipulation( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {
        final Collection<AbstractRemoteNode> quorum = this.getWriteQuorum( connection.getCluster() );

        connection.addAccessedNodes( quorum );
        statement.withExecutionTargets( quorum );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepare[DataManipulation] on {}", quorum );
        }

        final RspList<RemoteStatementHandle> responseList = cluster.prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, quorum );
        final List<Entry<AbstractRemoteNode, RemoteStatementHandle>> remoteStatements = new LinkedList<>();

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
    public StatementInfos prepareDataQuery( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {
        final Collection<AbstractRemoteNode> quorum = this.getReadQuorum( connection.getCluster() );

        connection.addAccessedNodes( quorum );
        statement.withExecutionTargets( quorum );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepare[DataQuery] on {}", quorum );
        }

        final RspList<RemoteStatementHandle> responseList = cluster.prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, quorum );
        final List<Entry<AbstractRemoteNode, RemoteStatementHandle>> remoteStatements = new LinkedList<>();

        responseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            remoteStatements.add( new SimpleImmutableEntry<>( cluster.getRemoteNode( address ), remoteStatementHandleRsp.getValue() ) );
        } );

        return connection.createPreparedStatement( statement, remoteStatements, quorum );
    }


    @Override
    public ExecuteResult execute( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<TypedValue> parameterValues, int maxRowsInFirstFrame ) throws NoSuchStatementException, RemoteException {
        // Execute the statement on the nodes it was prepared
        final Collection<AbstractRemoteNode> quorum = statement.getExecutionTargets();

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "execute on {}", quorum );
        }

        connection.addAccessedNodes( quorum );
        transaction.addAccessedNodes( quorum );

        final RspList<RemoteExecuteResult> responseList = cluster.execute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), parameterValues, maxRowsInFirstFrame, quorum );
        final List<Entry<AbstractRemoteNode, RemoteExecuteResult>> remoteResults = new LinkedList<>();

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
    public ExecuteBatchResult executeBatch( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<UpdateBatch> parameterValues ) throws NoSuchStatementException, RemoteException {
        // Execute the statement on the nodes it was prepared
        final Collection<AbstractRemoteNode> quorum = statement.getExecutionTargets();

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "executeBatch on {}", quorum );
        }

        connection.addAccessedNodes( quorum );
        transaction.addAccessedNodes( quorum );

        final RspList<RemoteExecuteBatchResult> responseList = cluster.executeBatch( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), parameterValues, quorum );
        final List<Entry<AbstractRemoteNode, RemoteExecuteBatchResult>> remoteResults = new LinkedList<>();

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
    public FragmentationProtocol setFragmentationProtocol( FragmentationProtocol fragmentationProtocol ) {
        return null;
    }


    @Override
    public PlacementProtocol setAllocationProtocol( PlacementProtocol placementProtocol ) {
        return null;
    }
}
