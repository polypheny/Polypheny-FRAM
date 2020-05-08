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

package org.polypheny.fram.protocols;


import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.avatica.Meta.ConnectionHandle;
import org.apache.calcite.avatica.Meta.ConnectionProperties;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.sql.SqlNode;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.polypheny.fram.remote.AbstractRemoteNode;
import org.polypheny.fram.remote.Cluster;
import org.polypheny.fram.remote.types.RemoteConnectionHandle;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.remote.types.RemoteStatementHandle;
import org.polypheny.fram.remote.types.RemoteTransactionHandle;
import org.polypheny.fram.standalone.ConnectionInfos;
import org.polypheny.fram.standalone.ResultSetInfos;
import org.polypheny.fram.standalone.StatementInfos;
import org.polypheny.fram.standalone.TransactionInfos;
import org.polypheny.fram.standalone.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractProtocol implements Protocol {

    protected static final Logger LOGGER = LoggerFactory.getLogger( Protocol.class );

    protected Protocol up;
    protected Protocol down;


    @Override
    public Protocol setUp( Protocol protocol ) {
        Protocol old = up;
        up = protocol;
        return old;
    }


    @Override
    public Protocol setDown( Protocol protocol ) {
        Protocol old = down;
        down = protocol;
        return old;
    }


    /*
     *
     */


    protected Collection<AbstractRemoteNode> getAllNodes( final Cluster cluster ) {
        return cluster.getMembers();
    }


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


    @Override
    public ResultSetInfos prepareAndExecuteTransactionCommit( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        final Collection<AbstractRemoteNode> accessedNodes = transaction.getAccessedNodes();
        LOGGER.trace( "prepareAndExecute[TransactionCommit] on {}", accessedNodes );

        final RspList<RemoteExecuteResult> responseList = connection.getCluster().prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, maxRowsInFirstFrame, accessedNodes );

        final Map<AbstractRemoteNode, RemoteExecuteResult> remoteResults = new HashMap<>();
        responseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            remoteResults.put( connection.getCluster().getRemoteNode( address ), remoteStatementHandleRsp.getValue() );
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
    public ResultSetInfos prepareAndExecuteTransactionRollback( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        final Collection<AbstractRemoteNode> accessedNodes = transaction.getAccessedNodes();
        LOGGER.trace( "prepareAndExecute[TransactionRollback] on {}", accessedNodes );

        final RspList<RemoteExecuteResult> responseList = connection.getCluster().prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, maxRowsInFirstFrame, accessedNodes );

        final Map<AbstractRemoteNode, RemoteExecuteResult> remoteResults = new HashMap<>();
        responseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            remoteResults.put( connection.getCluster().getRemoteNode( address ), remoteStatementHandleRsp.getValue() );
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
    public ConnectionProperties connectionSync( ConnectionInfos connection, ConnectionProperties newConnectionProperties ) throws RemoteException {
        Collection<AbstractRemoteNode> nodesWhichHaveOpenConnections = connection.getAccessedNodes();
        if ( nodesWhichHaveOpenConnections == null ) {
            return null;
        }
        final RspList<Common.ConnectionProperties> responses = connection.getCluster().connectionSync( RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ), newConnectionProperties.toProto(), nodesWhichHaveOpenConnections );
        for ( Rsp<Common.ConnectionProperties> response : responses ) {
            if ( response.hasException() ) {
                // TODO: Check for errors, etc.
            }
        }
        return newConnectionProperties;
    }


    @Override
    public Frame fetch( final ConnectionInfos connection, StatementHandle statementHandle, long offset, int fetchMaxRowCount ) throws NoSuchStatementException, MissingResultsException, RemoteException {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }


    @Override
    public void commit( ConnectionInfos connection, TransactionInfos transaction ) throws RemoteException {
        final Collection<AbstractRemoteNode> accessedNodes = transaction.getAccessedNodes();
        LOGGER.trace( "commit on {}", accessedNodes );

        if ( transaction.requires2pc() ) {
            final AtomicReference<Boolean> prepareCommitResultHolder = new AtomicReference<>( Boolean.TRUE );
            final RspList<Boolean> prepareCommitResponses = connection.getCluster().prepareCommit( RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ), RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), accessedNodes );
            prepareCommitResponses.forEach( ( address, remoteStatementHandleRsp ) -> {
                if ( remoteStatementHandleRsp.hasException() ) {
                    throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
                }
                prepareCommitResultHolder.getAndUpdate( prepareCommitResult -> Boolean.logicalAnd( prepareCommitResult, remoteStatementHandleRsp.getValue() ) );
            } );
            if ( prepareCommitResultHolder.get() == false ) {
                // one agent voted NO
                // According to 2PC rules --> ROLLBACK
                this.rollback( connection, transaction );
                throw new RuntimeException( "ROLLBACK performed since an agent voted NO in the 2PC prepare phase." );
            }
            // all YES --> continue commit
            connection.getCluster().commit( RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ), RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), accessedNodes );
        } else {
            connection.getCluster().onePhaseCommit( RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ), RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), accessedNodes );
        }
    }


    @Override
    public void rollback( ConnectionInfos connection, TransactionInfos transaction ) throws RemoteException {
        final Collection<AbstractRemoteNode> accessedNodes = transaction.getAccessedNodes();
        LOGGER.trace( "rollback on {}", accessedNodes );

        connection.getCluster().rollback( RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ), RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), accessedNodes );
    }


    @Override
    public void closeStatement( ConnectionInfos connection, StatementInfos statement ) throws RemoteException {
        final Collection<AbstractRemoteNode> accessedNodes = statement.getAccessedNodes();
        LOGGER.trace( "closeStatement on {}", accessedNodes );
        connection.getCluster().closeStatement( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), accessedNodes );
    }


    @Override
    public void closeConnection( ConnectionInfos connection ) throws RemoteException {
        final Collection<AbstractRemoteNode> accessedNodes = connection.getAccessedNodes();
        LOGGER.trace( "closeConnection on {}", accessedNodes );
        connection.getCluster().closeConnection( RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ), accessedNodes );
    }
}
