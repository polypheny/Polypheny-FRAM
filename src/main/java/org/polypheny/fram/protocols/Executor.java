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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.sql.SqlNode;
import org.polypheny.fram.Node;
import org.polypheny.fram.remote.PhysicalNode;
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
import org.polypheny.fram.standalone.Utils.WrappingException;


/**
 * Executes the given Statement on the given Node
 */
public class Executor extends AbstractProtocol implements Protocol {

    @Override
    public AbstractProtocol setDown( AbstractProtocol protocol ) {
        throw new ProtocolException( "Executor cannot have a protocol down the chain." );
    }


    @Override
    public <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteDataDefinition( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, final SqlNode catalogSql, SqlNode storeSql, long maxRowCount, int maxRowsInFirstFrame, Set<NodeType> executionTargets ) throws RemoteException {

        final Map<NodeType, ResultSetInfos> prepareAndExecuteResult = new LinkedHashMap<>();

        try {
            executionTargets.parallelStream().forEach( executionTarget -> {
                        if ( !(executionTarget instanceof PhysicalNode) ) {
                            throw new IllegalArgumentException( "Type of executionTargets is not supported!" );
                        }
                        final PhysicalNode physicalNode = ((PhysicalNode) executionTarget);

                        final RemoteExecuteResult executeResult;
                        try {
                            executeResult = physicalNode.prepareAndExecuteDataDefinition(
                                    RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ),
                                    RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ),
                                    connection.getCluster().serializeSql( catalogSql ),
                                    connection.getCluster().serializeSql( storeSql ),
                                    maxRowCount,
                                    maxRowsInFirstFrame
                            );
                            statement.addAccessedNode( physicalNode );
                            connection.addAccessedNode( physicalNode );
                        } catch ( RemoteException e ) {
                            throw Utils.wrapException( e );
                        }

                        prepareAndExecuteResult.put( executionTarget, statement.createResultSet( physicalNode, executeResult ) );
                    }
            );
        } catch ( WrappingException we ) {
            final Throwable t = Utils.xtractException( we );
            if ( t instanceof RemoteException ) {
                throw (RemoteException) t;
            } else {
                throw we;
            }
        }

        return prepareAndExecuteResult;
    }


    @Override
    public <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteDataManipulation( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, Set<NodeType> executionTargets ) throws RemoteException {

        final Map<NodeType, ResultSetInfos> prepareAndExecuteResult = new LinkedHashMap<>();

        try {
            executionTargets.parallelStream().forEach( executionTarget -> {
                        if ( !(executionTarget instanceof PhysicalNode) ) {
                            throw new IllegalArgumentException( "Type of executionTargets is not supported!" );
                        }
                        final PhysicalNode physicalNode = ((PhysicalNode) executionTarget);

                        final RemoteExecuteResult executeResult;
                        try {
                            executeResult = physicalNode.prepareAndExecute(
                                    RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ),
                                    RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ),
                                    connection.getCluster().serializeSql( sql ),
                                    maxRowCount,
                                    maxRowsInFirstFrame
                            );
                            statement.addAccessedNode( physicalNode );
                            connection.addAccessedNode( physicalNode );
                        } catch ( RemoteException e ) {
                            throw Utils.wrapException( e );
                        }

                        prepareAndExecuteResult.put( executionTarget, statement.createResultSet( physicalNode, executeResult ) );
                    }
            );
        } catch ( WrappingException we ) {
            final Throwable t = Utils.xtractException( we );
            if ( t instanceof RemoteException ) {
                throw (RemoteException) t;
            } else {
                throw we;
            }
        }

        return prepareAndExecuteResult;
    }


    @Override
    public <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteDataQuery( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, Set<NodeType> executionTargets ) throws RemoteException {

        final Map<NodeType, ResultSetInfos> prepareAndExecuteResult = new LinkedHashMap<>();

        try {
            executionTargets.parallelStream().forEach( executionTarget -> {
                        if ( !(executionTarget instanceof PhysicalNode) ) {
                            throw new IllegalArgumentException( "Type of executionTargets is not supported!" );
                        }
                        final PhysicalNode physicalNode = ((PhysicalNode) executionTarget);

                        final RemoteExecuteResult executeResult;
                        try {
                            executeResult = physicalNode.prepareAndExecute(
                                    RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ),
                                    RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ),
                                    connection.getCluster().serializeSql( sql ),
                                    maxRowCount,
                                    maxRowsInFirstFrame
                            );
                            statement.addAccessedNode( physicalNode );
                            connection.addAccessedNode( physicalNode );
                        } catch ( RemoteException e ) {
                            throw Utils.wrapException( e );
                        }

                        prepareAndExecuteResult.put( executionTarget, statement.createResultSet( physicalNode, executeResult ) );
                    }
            );
        } catch ( WrappingException we ) {
            final Throwable t = Utils.xtractException( we );
            if ( t instanceof RemoteException ) {
                throw (RemoteException) t;
            } else {
                throw we;
            }
        }

        return prepareAndExecuteResult;
    }


    @Override
    public <NodeType extends Node> Map<NodeType, PreparedStatementInfos> prepareDataManipulation( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount, Set<NodeType> executionTargets ) throws RemoteException {

        final Map<NodeType, PreparedStatementInfos> prepareResult = new LinkedHashMap<>();

        try {
            executionTargets.parallelStream().forEach( executionTarget -> {
                        if ( !(executionTarget instanceof PhysicalNode) ) {
                            throw new IllegalArgumentException( "Type of executionTargets is not supported!" );
                        }
                        final PhysicalNode physicalNode = ((PhysicalNode) executionTarget);

                        final RemoteStatementHandle preparedStatementHandle;
                        try {
                            preparedStatementHandle = physicalNode.prepare(
                                    RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ),
                                    connection.getCluster().serializeSql( sql ),
                                    maxRowCount
                            );
                            statement.addAccessedNode( physicalNode );
                        } catch ( RemoteException e ) {
                            throw Utils.wrapException( e );
                        }

                        prepareResult.put( executionTarget, connection.createPreparedStatement( statement, physicalNode, preparedStatementHandle,
                                //
                                /* execute */ ( _connection, _transaction, _statement, _parameterValues, _maxRowsInFirstFrame ) -> {
                                    //
                                    final RemoteExecuteResult executeResult;
                                    try {
                                        executeResult = physicalNode.execute( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _parameterValues, _maxRowsInFirstFrame );
                                        _statement.addAccessedNode( physicalNode );
                                        _connection.addAccessedNode( physicalNode );
                                    } catch ( RemoteException e ) {
                                        throw Utils.wrapException( e );
                                    }
                                    return _statement.createResultSet( physicalNode, executeResult );
                                },
                                //
                                /* executeBatch */ ( _connection, _transaction, _statement, _listOfUpdateBatches ) -> {
                                    //
                                    final RemoteExecuteBatchResult executeBatchResult;
                                    try {
                                        executeBatchResult = physicalNode.executeBatch( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _listOfUpdateBatches );
                                        _statement.addAccessedNode( physicalNode );
                                        _connection.addAccessedNode( physicalNode );
                                    } catch ( RemoteException e ) {
                                        throw Utils.wrapException( e );
                                    }
                                    return _statement.createBatchResultSet( physicalNode, executeBatchResult );
                                } ) );
                    }
            );
        } catch ( WrappingException we ) {
            final Throwable t = Utils.xtractException( we );
            if ( t instanceof RemoteException ) {
                throw (RemoteException) t;
            } else {
                throw we;
            }
        }

        return prepareResult;
    }


    @Override
    public <NodeType extends Node> Map<NodeType, PreparedStatementInfos> prepareDataQuery( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount, Set<NodeType> executionTargets ) throws RemoteException {

        final Map<NodeType, PreparedStatementInfos> prepareResult = new LinkedHashMap<>();

        try {
            executionTargets.parallelStream().forEach( executionTarget -> {
                        if ( !(executionTarget instanceof PhysicalNode) ) {
                            throw new IllegalArgumentException( "Type of executionTargets is not supported!" );
                        }
                        final PhysicalNode physicalNode = ((PhysicalNode) executionTarget);

                        final RemoteStatementHandle preparedStatementHandle;
                        try {
                            preparedStatementHandle = physicalNode.prepare(
                                    RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ),
                                    connection.getCluster().serializeSql( sql ),
                                    maxRowCount
                            );
                            statement.addAccessedNode( physicalNode );
                        } catch ( RemoteException e ) {
                            throw Utils.wrapException( e );
                        }

                        prepareResult.put( executionTarget, connection.createPreparedStatement( statement, physicalNode, preparedStatementHandle,
                                //
                                /* execute */ ( _connection, _transaction, _statement, _parameterValues, _maxRowsInFirstFrame ) -> {
                                    //
                                    final RemoteExecuteResult executeResult;
                                    try {
                                        executeResult = physicalNode.execute( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _parameterValues, _maxRowsInFirstFrame );
                                        _statement.addAccessedNode( physicalNode );
                                        _connection.addAccessedNode( physicalNode );
                                    } catch ( RemoteException e ) {
                                        throw Utils.wrapException( e );
                                    }
                                    return _statement.createResultSet( physicalNode, executeResult );
                                },
                                //
                                /* executeBatch */ ( _connection, _transaction, _statement, _listOfUpdateBatches ) -> {
                                    //
                                    final RemoteExecuteBatchResult executeBatchResult;
                                    try {
                                        executeBatchResult = physicalNode.executeBatch( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _listOfUpdateBatches );
                                        _statement.addAccessedNode( physicalNode );
                                        _connection.addAccessedNode( physicalNode );
                                    } catch ( RemoteException e ) {
                                        throw Utils.wrapException( e );
                                    }
                                    return _statement.createBatchResultSet( physicalNode, executeBatchResult );
                                } ) );
                    }
            );
        } catch ( WrappingException we ) {
            final Throwable t = Utils.xtractException( we );
            if ( t instanceof RemoteException ) {
                throw (RemoteException) t;
            } else {
                throw we;
            }
        }

        return prepareResult;
    }
}
