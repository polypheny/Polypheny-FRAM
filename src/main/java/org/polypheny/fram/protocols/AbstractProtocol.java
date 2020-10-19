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


import java.io.Serializable;
import java.rmi.RemoteException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.avatica.Meta.ConnectionProperties;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.proto.Common.TypedValue;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.polypheny.fram.Node;
import org.polypheny.fram.remote.AbstractRemoteNode;
import org.polypheny.fram.remote.Cluster;
import org.polypheny.fram.remote.PhysicalNode;
import org.polypheny.fram.remote.types.RemoteConnectionHandle;
import org.polypheny.fram.remote.types.RemoteStatementHandle;
import org.polypheny.fram.remote.types.RemoteTransactionHandle;
import org.polypheny.fram.standalone.ConnectionInfos;
import org.polypheny.fram.standalone.ResultSetInfos;
import org.polypheny.fram.standalone.StatementInfos;
import org.polypheny.fram.standalone.StatementInfos.PreparedStatementInfos;
import org.polypheny.fram.standalone.TransactionInfos;
import org.polypheny.fram.standalone.Utils;
import org.polypheny.fram.standalone.Utils.WrappingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractProtocol implements Protocol {

    protected static final Logger LOGGER = LoggerFactory.getLogger( Protocol.class );

    protected AbstractProtocol up;
    protected AbstractProtocol down;


    public Protocol setUp( AbstractProtocol protocol ) {
        AbstractProtocol old = up;
        up = protocol;
        return old;
    }


    public Protocol setDown( AbstractProtocol protocol ) {
        AbstractProtocol old = down;
        down = protocol;
        return old;
    }


    /*
     *
     */


    protected Set<AbstractRemoteNode> getAllNodes( final Cluster cluster ) {
        return cluster.getMembers();
    }


    public final ResultSetInfos prepareAndExecute( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlSelect sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        if ( sql.isA( SqlKind.DDL ) ) {
            /*
             * DataDefinition cannot be validated (for now?). That's why we branch off here.
             */
            return prepareAndExecuteDataDefinition( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
        }
        if ( sql.isA( SqlKind.DML ) ) {
            /*
             * Branching off DML statements (writing statements)
             */
            return prepareAndExecuteDataManipulation( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
        }
        if ( sql.isA( SqlKind.QUERY ) ) {
            /*
             * Branching off QUERY statements (reading statements)
             */
            return prepareAndExecuteDataQuery( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
        }
        throw Utils.wrapException(
                new IllegalStateException( "Given SQL is none of DDL, DML, QUERY." )
        );
    }


    public <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecute( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlSelect sql, long maxRowCount, int maxRowsInFirstFrame, Set<NodeType> executionTargets ) throws RemoteException {
        if ( sql.isA( SqlKind.DDL ) ) {
            /*
             * DataDefinition cannot be validated (for now?). That's why we branch off here.
             */
            return (Map<NodeType, ResultSetInfos>) prepareAndExecuteDataDefinition( connection, transaction, statement, sql, sql, maxRowCount, maxRowsInFirstFrame, executionTargets );
        }
        if ( sql.isA( SqlKind.DML ) ) {
            /*
             * Branching off DML statements (writing statements)
             */
            return (Map<NodeType, ResultSetInfos>) prepareAndExecuteDataManipulation( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, executionTargets );
        }
        if ( sql.isA( SqlKind.QUERY ) ) {
            /*
             * Branching off QUERY statements (reading statements)
             */
            return (Map<NodeType, ResultSetInfos>) prepareAndExecuteDataQuery( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, executionTargets );
        }
        throw Utils.wrapException(
                new IllegalStateException( "Given SQL is none of DDL, DML, or QUERY." )
        );
    }


    @Override
    public final ResultSetInfos prepareAndExecuteDataDefinition( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        return this.prepareAndExecuteDataDefinition( connection, transaction, statement, sql, sql, maxRowCount, maxRowsInFirstFrame, callback );
    }


    public ResultSetInfos prepareAndExecuteDataDefinition( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, final SqlNode catalogSql, SqlNode storeSql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        throw new UnsupportedOperationException( "Not supported." );
    }


    public abstract <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteDataDefinition( final ConnectionInfos connection, final TransactionInfos transaction, final StatementInfos statement, final SqlNode catalogSql, SqlNode storeSql, final long maxRowCount, final int maxRowsInFirstFrame, final Set<NodeType> executionTargets ) throws RemoteException;


    @Override
    public ResultSetInfos prepareAndExecuteDataManipulation( final ConnectionInfos connection, final TransactionInfos transaction, final StatementInfos statement, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final PrepareCallback callback ) throws RemoteException {
        throw new UnsupportedOperationException( "Not supported." );
    }


    public abstract <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteDataManipulation( final ConnectionInfos connection, final TransactionInfos transaction, final StatementInfos statement, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final Set<NodeType> executionTargets ) throws RemoteException;


    @Override
    public ResultSetInfos prepareAndExecuteDataQuery( final ConnectionInfos connection, final TransactionInfos transaction, final StatementInfos statement, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final PrepareCallback callback ) throws RemoteException {
        throw new UnsupportedOperationException( "Not supported." );
    }


    public abstract <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteDataQuery( final ConnectionInfos connection, final TransactionInfos transaction, final StatementInfos statement, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final Set<NodeType> executionTargets ) throws RemoteException;


    @Override
    public ResultSetInfos prepareAndExecuteTransactionCommit( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {

        final Set<PhysicalNode> accessedNodes = transaction.getAccessedNodes();
        LOGGER.trace( "prepareAndExecute[TransactionCommit] on {}", accessedNodes );

        final Map<PhysicalNode, ResultSetInfos> remoteResults = this.prepareAndExecuteTransactionCommit( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame,
                accessedNodes
        );

        return statement.createResultSet( remoteResults.keySet(),
                /* executeResultSupplier */ () -> remoteResults.values().iterator().next().getExecuteResult(), // todo: merge the result of `COMMIT` properly
                /* generatedKeysSupplier */ () -> remoteResults.values().iterator().next().getGeneratedKeys()
        );
    }


    public <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteTransactionCommit( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, Set<NodeType> executionTargets ) throws RemoteException {

        final Map<NodeType, ResultSetInfos> results = new HashMap<>();
        try {
            executionTargets.parallelStream().forEach( ( executionTarget ) -> {
                if ( !(executionTarget instanceof PhysicalNode) ) {
                    throw new IllegalArgumentException( "Type of executionTargets is not supported!" );
                }
                final PhysicalNode physicalNode = ((PhysicalNode) executionTarget);
                try {
                    results.put( executionTarget, statement.createResultSet( physicalNode, physicalNode.prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), connection.getCluster().serializeSql( sql ), maxRowCount, maxRowsInFirstFrame ) ) );
                } catch ( RemoteException e ) {
                    throw Utils.wrapException( e );
                }
            } );
        } catch ( WrappingException we ) {
            final Throwable t = Utils.xtractException( we );
            if ( t instanceof RemoteException ) {
                throw (RemoteException) t;
            } else {
                throw we;
            }
        }

        return results;
    }


    @Override
    public ResultSetInfos prepareAndExecuteTransactionRollback( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {

        final Set<PhysicalNode> accessedNodes = transaction.getAccessedNodes();
        LOGGER.trace( "prepareAndExecute[TransactionRollback] on {}", accessedNodes );

        final Map<PhysicalNode, ResultSetInfos> remoteResults = this.prepareAndExecuteTransactionRollback( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame,
                accessedNodes
        );

        return statement.createResultSet( remoteResults.keySet(),
                /* executeResultSupplier */ () -> remoteResults.values().iterator().next().getExecuteResult(), // todo: merge the result of `ROLLBACK` properly
                /* generatedKeysSupplier */ () -> remoteResults.values().iterator().next().getGeneratedKeys()
        );
    }


    public <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteTransactionRollback( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, Set<NodeType> executionTargets ) throws RemoteException {

        final Map<NodeType, ResultSetInfos> results = new HashMap<>();
        try {
            executionTargets.parallelStream().forEach( ( executionTarget ) -> {
                if ( !(executionTarget instanceof PhysicalNode) ) {
                    throw new IllegalArgumentException( "Type of executionTargets is not supported!" );
                }
                final PhysicalNode physicalNode = ((PhysicalNode) executionTarget);
                try {
                    results.put( executionTarget, statement.createResultSet( physicalNode, physicalNode.prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), connection.getCluster().serializeSql( sql ), maxRowCount, maxRowsInFirstFrame ) ) );
                } catch ( RemoteException e ) {
                    throw Utils.wrapException( e );
                }
            } );
        } catch ( WrappingException we ) {
            final Throwable t = Utils.xtractException( we );
            if ( t instanceof RemoteException ) {
                throw (RemoteException) t;
            } else {
                throw we;
            }
        }

        return results;
    }


    public final PreparedStatementInfos prepare( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {

        if ( sql.isA( SqlKind.DDL ) ) {
            /*
             * Branching off DataDefinition
             */
            throw Utils.wrapException( new SQLFeatureNotSupportedException( "Cannot prepare DDL statements." ) );
        }
        if ( sql.isA( SqlKind.DML ) ) {
            /*
             * Branching off DML statements (writing statements)
             */
            return prepareDataManipulation( connection, connection.createStatement(), sql, maxRowCount );
        }
        if ( sql.isA( SqlKind.QUERY ) ) {
            /*
             * Branching off QUERY statements (reading statements)
             */
            return prepareDataQuery( connection, connection.createStatement(), sql, maxRowCount );
        }
        throw Utils.wrapException(
                new IllegalStateException( "Given SQL is none of DML, or QUERY." )
        );
    }


    public <NodeType extends Node> Map<NodeType, PreparedStatementInfos> prepare( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount, Set<NodeType> executionTargets ) throws RemoteException {

        if ( sql.isA( SqlKind.DDL ) ) {
            /*
             * Branching off DataDefinition
             */
            throw Utils.wrapException( new SQLFeatureNotSupportedException( "Cannot prepare DDL statements." ) );
        }
        if ( sql.isA( SqlKind.DML ) ) {
            /*
             * Branching off DML statements (writing statements)
             */
            return prepareDataManipulation( connection, connection.createStatement(), sql, maxRowCount, executionTargets );
        }
        if ( sql.isA( SqlKind.QUERY ) ) {
            /*
             * Branching off QUERY statements (reading statements)
             */
            return prepareDataQuery( connection, connection.createStatement(), sql, maxRowCount, executionTargets );
        }
        throw Utils.wrapException(
                new IllegalStateException( "Given SQL is none of DML, or QUERY." )
        );
    }


    @Override
    public PreparedStatementInfos prepareDataManipulation( final ConnectionInfos connection, final StatementInfos statement, final SqlNode sql, final long maxRowCount ) throws RemoteException {
        throw new UnsupportedOperationException( "Not supported." );
    }


    public abstract <NodeType extends Node> Map<NodeType, PreparedStatementInfos> prepareDataManipulation( final ConnectionInfos connection, final StatementInfos statement, final SqlNode sql, final long maxRowCount, final Set<NodeType> executionTargets ) throws RemoteException;


    @Override
    public PreparedStatementInfos prepareDataQuery( final ConnectionInfos connection, final StatementInfos statement, final SqlNode sql, final long maxRowCount ) throws RemoteException {
        throw new UnsupportedOperationException( "Not supported." );
    }


    public abstract <NodeType extends Node> Map<NodeType, PreparedStatementInfos> prepareDataQuery( final ConnectionInfos connection, final StatementInfos statement, final SqlNode sql, final long maxRowCount, final Set<NodeType> executionTargets ) throws RemoteException;


    @Override
    public ResultSetInfos execute( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<TypedValue> parameterValues, int maxRowsInFirstFrame ) throws NoSuchStatementException, RemoteException {
        if ( !(statement instanceof PreparedStatementInfos) ) {
            throw new IllegalArgumentException( "The provided statement is not a PreparedStatement." );
        }

        try {
            return ((PreparedStatementInfos) statement).execute( connection, transaction, statement, parameterValues, maxRowsInFirstFrame );
        } catch ( WrappingException we ) {
            final Throwable t = Utils.xtractException( we );
            if ( t instanceof NoSuchStatementException ) {
                throw (NoSuchStatementException) t;
            }
            if ( t instanceof RemoteException ) {
                throw (RemoteException) t;
            }
            throw we;
        }
    }


    @Override
    public ResultSetInfos executeBatch( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<UpdateBatch> parameterValues ) throws NoSuchStatementException, RemoteException {
        if ( !(statement instanceof PreparedStatementInfos) ) {
            throw new IllegalArgumentException( "The provided statement is not a PreparedStatement." );
        }

        try {
            return ((PreparedStatementInfos) statement).executeBatch( connection, transaction, statement, parameterValues );
        } catch ( WrappingException we ) {
            final Throwable t = Utils.xtractException( we );
            if ( t instanceof NoSuchStatementException ) {
                throw (NoSuchStatementException) t;
            }
            if ( t instanceof RemoteException ) {
                throw (RemoteException) t;
            }
            throw we;
        }
    }


    @Override
    public Frame fetch( final ConnectionInfos connection, StatementHandle statementHandle, long offset, int fetchMaxRowCount ) throws NoSuchStatementException, MissingResultsException, RemoteException {
        throw new UnsupportedOperationException( "FETCH is not implemented yet." );
    }


    @Override
    public void commit( ConnectionInfos connection, TransactionInfos transaction ) throws RemoteException {
        final Collection<PhysicalNode> accessedNodes = transaction.getAccessedNodes();
        LOGGER.trace( "commit on {}", accessedNodes );

        if ( transaction.requires2pc() ) {
            final AtomicReference<Boolean> prepareCommitResultHolder = new AtomicReference<>( Boolean.TRUE );
            final RspList<Boolean> prepareCommitResponses = connection.getCluster().prepareCommit( RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ), RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), accessedNodes );
            prepareCommitResponses.forEach( ( address, remoteStatementHandleRsp ) -> {
                if ( remoteStatementHandleRsp.hasException() ) {
                    throw Utils.wrapException( remoteStatementHandleRsp.getException() );
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
        final Collection<PhysicalNode> accessedNodes = transaction.getAccessedNodes();
        LOGGER.trace( "rollback on {}", accessedNodes );

        connection.getCluster().rollback( RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ), RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), accessedNodes );
    }


    @Override
    public void closeStatement( ConnectionInfos connection, StatementInfos statement ) throws RemoteException {
        final Collection<PhysicalNode> accessedNodes = statement.getAccessedPhysicalNodes();
        LOGGER.trace( "closeStatement on {}", accessedNodes );
        connection.getCluster().closeStatement( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), accessedNodes );
    }


    @Override
    public void closeConnection( ConnectionInfos connection ) throws RemoteException {
        final Collection<PhysicalNode> accessedNodes = connection.getAccessedNodes();
        LOGGER.trace( "closeConnection on {}", accessedNodes );
        connection.getCluster().closeConnection( RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ), accessedNodes );
    }


    @Override
    public Iterable<Serializable> createIterable( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, QueryState state, Signature signature, List<TypedValue> parameterValues, Frame firstFrame ) throws RemoteException {
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    @Override
    public boolean syncResults( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, QueryState state, long offset ) throws RemoteException {
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    @Override
    public ConnectionProperties connectionSync( ConnectionInfos connection, ConnectionProperties newConnectionProperties ) throws RemoteException {

        Collection<PhysicalNode> nodesWhichHaveOpenConnections = connection.getAccessedNodes();
        if ( nodesWhichHaveOpenConnections == null ) {
            return null;
        }
        final RspList<Common.ConnectionProperties> responses = connection.getCluster().connectionSync( RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ), newConnectionProperties.toProto(), nodesWhichHaveOpenConnections );
        for ( Rsp<Common.ConnectionProperties> response : responses ) {
            if ( response.hasException() ) {
                // todo: Check for errors, etc.
            }
        }
        return newConnectionProperties;
    }
}
