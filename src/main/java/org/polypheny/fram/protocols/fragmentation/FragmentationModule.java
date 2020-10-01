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

package org.polypheny.fram.protocols.fragmentation;


import io.vavr.Function2;
import io.vavr.Function5;
import java.io.Serializable;
import java.math.BigDecimal;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.ExecuteBatchResult;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.proto.Common.TypedValue;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.jgroups.Address;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.polypheny.fram.Node;
import org.polypheny.fram.datadistribution.RecordIdentifier;
import org.polypheny.fram.datadistribution.Transaction;
import org.polypheny.fram.datadistribution.Transaction.Operation;
import org.polypheny.fram.datadistribution.VirtualNode;
import org.polypheny.fram.protocols.AbstractProtocol;
import org.polypheny.fram.protocols.Protocol.FragmentationProtocol;
import org.polypheny.fram.protocols.fragmentation.HorizontalHashFragmentation.HorizontalExecuteResultMergeFunction;
import org.polypheny.fram.remote.AbstractRemoteNode;
import org.polypheny.fram.remote.ClusterUtils;
import org.polypheny.fram.remote.types.RemoteConnectionHandle;
import org.polypheny.fram.remote.types.RemoteExecuteBatchResult;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.remote.types.RemoteStatementHandle;
import org.polypheny.fram.remote.types.RemoteTransactionHandle;
import org.polypheny.fram.standalone.CatalogUtils;
import org.polypheny.fram.standalone.ConnectionInfos;
import org.polypheny.fram.standalone.ResultSetInfos;
import org.polypheny.fram.standalone.ResultSetInfos.QueryResultSet;
import org.polypheny.fram.standalone.StatementInfos;
import org.polypheny.fram.standalone.TransactionInfos;
import org.polypheny.fram.standalone.Utils;
import org.polypheny.fram.standalone.parser.sql.SqlNodeUtils;


public class FragmentationModule extends AbstractProtocol implements FragmentationProtocol {

    private FragmentationSchema currentSchema = new FragmentationSchema();

    @Override
    public ResultSetInfos prepareAndExecuteDataDefinition( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        // todo: for now send to all nodes
        return super.prepareAndExecuteDataDefinition( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
    }


    @Override
    public ResultSetInfos prepareAndExecuteDataManipulation( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    @Override
    public <NodeType extends Node> Map<NodeType, RemoteExecuteResult> prepareAndExecuteDataManipulation( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, Collection<NodeType> executionTargets ) throws RemoteException {
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    @Override
    public ResultSetInfos prepareAndExecuteDataQuery( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {

        LOGGER.trace( "prepareAndExecuteDataQuery( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {} )", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame );

        switch ( sql.getKind() ) {
            // See org.apache.calcite.sql.SqlKind.QUERY
            case SELECT:
                return prepareAndExecuteDataQuerySelect( connection, transaction, statement, (SqlSelect) sql, maxRowCount, maxRowsInFirstFrame, callback );

            case UNION:
            case INTERSECT:
            case EXCEPT:
            case VALUES:
            case WITH:
            case ORDER_BY:
            case EXPLICIT_TABLE:
            default:
                throw new UnsupportedOperationException( "Not supported" );
        }
    }


    public <NodeType extends Node> Map<NodeType, RemoteExecuteResult> prepareAndExecuteDataQuery( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, Collection<NodeType> executionTargets ) throws RemoteException {

        switch ( sql.getKind() ) {
            // See org.apache.calcite.sql.SqlKind.QUERY
            case SELECT:
                return prepareAndExecuteDataQuerySelect( connection, transaction, statement, (SqlSelect) sql, maxRowCount, maxRowsInFirstFrame, executionTargets );

            case UNION:
            case INTERSECT:
            case EXCEPT:
            case VALUES:
            case WITH:
            case ORDER_BY:
            case EXPLICIT_TABLE:
            default:
                throw new UnsupportedOperationException( "Not supported" );
        }
    }


    protected ResultSetInfos prepareAndExecuteDataQuerySelect( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlSelect sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        LOGGER.trace( "prepareAndExecuteDataQuery( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {} )", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame );

        if ( SqlNodeUtils.SELECT_UTILS.selectListContainsAggregate( sql ) ) {
            // SELECT MIN/MAX/AVG/ FROM ...
            return prepareAndExecuteDataQuerySelectAggregate( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
        }

        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    protected <NodeType extends Node> Map<NodeType, RemoteExecuteResult> prepareAndExecuteDataQuerySelect( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlSelect sql, long maxRowCount, int maxRowsInFirstFrame, Collection<NodeType> executionTargets ) throws RemoteException {
        LOGGER.trace( "prepareAndExecuteDataQuery( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {} )", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame );

        if ( SqlNodeUtils.SELECT_UTILS.selectListContainsAggregate( sql ) ) {
            // SELECT MIN/MAX/AVG/ FROM ...
            return prepareAndExecuteDataQuerySelectAggregate( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, executionTargets );
        }

        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    protected ResultSetInfos prepareAndExecuteDataQuerySelectAggregate( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlSelect sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        LOGGER.trace( "prepareAndExecuteDataQueryAggregate( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {} )", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame );

        // FAIL-FAST
        final SqlNodeList selectList = sql.getSelectList();
        if ( selectList.size() > 1 ) {
            throw new UnsupportedOperationException( "Not implemented yet." );
        }

        // more than one table and especially the required join is currently not supported!
        final SqlIdentifier targetTable = SqlNodeUtils.SELECT_UTILS.getTargetTable( sql );

        final SqlNode asExpressionOrAggregateNode = selectList.get( 0 );
        final SqlNode aggregateNode;
        if ( asExpressionOrAggregateNode.getKind() == SqlKind.AS ) {
            aggregateNode = ((SqlBasicCall) asExpressionOrAggregateNode).operand( 0 );
        } else {
            aggregateNode = asExpressionOrAggregateNode;
        }

        final List<Fragment> targetFragements;
        if ( sql.hasWhere() ) {
            // todo: know where to send it
            throw new UnsupportedOperationException( "Not implemented yet." );
        } else {
            targetFragements = this.currentSchema.allFragments();
        }

        // handle the responses of the execution
        final Map<Fragment, RemoteExecuteResult> prepareAndExecuteResults = down.prepareAndExecute( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame,
                targetFragements
        );

        switch ( aggregateNode.getKind() ) {
            case MAX:
                /*
                    YCSB:
                        SELECT MAX(`USERTABLE`.`YCSB_KEY`)
                        FROM `PUBLIC`.`USERTABLE` AS `USERTABLE`
                 */
                return statement.createResultSet( prepareAndExecuteResults,
                        /* mergeResults */ ( origins ) -> {
                            int maxValue = Integer.MIN_VALUE;
                            for ( RemoteExecuteResult rex : origins.values() ) {
                                for ( MetaResultSet rs : rex.toExecuteResult().resultSets ) {
                                    Object row = rs.firstFrame.rows.iterator().next();
                                    switch ( rs.signature.cursorFactory.style ) {
                                        case LIST:
                                            maxValue = Math.max( maxValue, (int) ((List) row).get( 0 ) );
                                            break;

                                        default:
                                            throw new UnsupportedOperationException( "Not implemented yet." );
                                    }
                                }
                            }
                            Signature signature = origins.values().iterator().next().toExecuteResult().resultSets.iterator().next().signature;
                            Frame frame = Frame.create( 0, true, Collections.singletonList( new Object[]{ maxValue } ) );
                            return new Meta.ExecuteResult( Collections.singletonList( MetaResultSet.create( statement.getStatementHandle().connectionId, statement.getStatementHandle().id, false, signature, frame ) ) );
                        },
                        /* fetch */ ( origins, conn, stmt, offset, fetchMaxRowCount ) -> {
                            throw new UnsupportedOperationException( "Not implemented yet." );
                        } );

            default:
                throw new UnsupportedOperationException( "Not implemented yet." );
        }
    }


    protected <NodeType extends Node> Map<NodeType, RemoteExecuteResult> prepareAndExecuteDataQuerySelectAggregate( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlSelect sql, long maxRowCount, int maxRowsInFirstFrame, Collection<NodeType> executionTargets ) throws RemoteException {
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    @Override
    public ResultSetInfos prepareAndExecuteTransactionCommit( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        // todo: for now
        return super.prepareAndExecuteTransactionCommit( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
    }


    @Override
    public ResultSetInfos prepareAndExecuteTransactionRollback( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        // todo: for now
        return super.prepareAndExecuteTransactionRollback( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
    }


    @Override
    public StatementInfos prepareDataManipulation( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {
        LOGGER.trace( "prepareDataManipulation( connection: {}, statement: {}, sql: {}, maxRowCount: {} )", connection, statement, sql, maxRowCount );

        switch ( sql.getKind() ) {
            // See org.apache.calcite.sql.SqlKind.DML
            case INSERT:
                return this.prepareDataManipulationInsert( connection, statement, (SqlInsert) sql, maxRowCount );

            case DELETE:
                return this.prepareDataManipulationDelete( connection, statement, (SqlDelete) sql, maxRowCount );

            case UPDATE:
                return this.prepareDataManipulationUpdate( connection, statement, (SqlUpdate) sql, maxRowCount );

            case MERGE:
            case PROCEDURE_CALL:
            default:
                throw new UnsupportedOperationException( "Not supported" );
        }
    }


    protected StatementInfos prepareDataManipulationInsert( ConnectionInfos connection, StatementInfos statement, SqlInsert sql, long maxRowCount ) throws RemoteException {
        LOGGER.trace( "prepareDataManipulationInsert( connection: {}, statement: {}, sql: {}, maxRowCount: {} )", connection, statement, sql, maxRowCount );

        // inserts have to be PREPARED on ALL nodes
        // todo: replace this with a call to the protocol down
        final RspList<RemoteStatementHandle> prepareResponseList = connection.getCluster().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount );
        // handle the responses of the PREPARE execution
        final Map<AbstractRemoteNode, RemoteStatementHandle> prepareRemoteResults = ClusterUtils.getRemoteResults( connection.getCluster(), prepareResponseList );

        final SqlIdentifier targetTable = SqlNodeUtils.INSERT_UTILS.getTargetTable( sql );
        final SqlNodeList targetColumns = SqlNodeUtils.INSERT_UTILS.getTargetColumns( sql );

        final String catalogName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 2 );
        final String schemaName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 1 );
        final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 0 );
        final Map<String, Integer> primaryKeyColumnNamesAndIndexes = CatalogUtils.lookupPrimaryKeyColumnNamesAndIndexes( connection, catalogName, schemaName, tableName );

        final SortedMap<Integer, Integer> primaryKeyColumnsIndexesToParametersIndexes = SqlNodeUtils.INSERT_UTILS.getPrimaryKeyColumnsIndexesToParametersIndexesMap( sql, primaryKeyColumnNamesAndIndexes );

        // construct the prepared statement together with the execution functions
        return connection.createPreparedStatement( statement, prepareRemoteResults,
                //
                /* signatureMerge */ ( _remoteStatements ) -> {
                    // BEGIN HACK - get the first signature
                    return _remoteStatements.values().iterator().next().toStatementHandle().signature;
                    // END HACK
                },
                //
                /* execute */ ( _connection, _transaction, _statement, _parameterValues, _maxRowsInFirstFrame ) -> {
                    //
                    // nodes on which the insert is EXECUTED
                    // todo: replace this with a lookup in the Schema on where to execute the inserts
                    // use something like
                    /*
                        ( clusterMembers, parameterValues ) -> {
                            final AbstractRemoteNode[] executionTargets = clusterMembers.toArray( new AbstractRemoteNode[clusterMembers.size()] );
                            final Object[] primaryKeyValues = new Object[primaryKeyColumnsIndexes.size()];
                            int primaryKeyValueIndex = 0;
                            for ( Iterator<Integer> primaryKeyColumnsIndexIterator = primaryKeyColumnsIndexes.iterator(); primaryKeyColumnsIndexIterator.hasNext(); ) {
                                primaryKeyValues[primaryKeyValueIndex++] = parameterValues.get( primaryKeyColumnsIndexesToParametersIndexes.get( primaryKeyColumnsIndexIterator.next() ) );
                            }
                            final int executionTargetIndex = Math.abs( Objects.hash( primaryKeyValues ) % executionTargets.length );
                            return Arrays.asList( executionTargets[executionTargetIndex] );
                        }
                     */
                    List<AbstractRemoteNode> _executionTargets = // this.executionTargetsFunctions.get( _statement ).apply( _connection.getCluster().getMembers(), _parameterValues );
                            _connection.getCluster().getMembers();

                    final Map<AbstractRemoteNode, RemoteExecuteResult> _remoteResults;
                    try {
                        // execute the insert statement
                        // todo: replace with a call to the next protocol (down)
                        final RspList<RemoteExecuteResult> _responseList = _connection.getCluster().execute( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _parameterValues, -1/*_maxRowsInFirstFrame*/, _executionTargets );

                        // handle the responses
                        _remoteResults = ClusterUtils.getRemoteResults( _connection.getCluster(), _responseList );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    for ( AbstractRemoteNode _node : _remoteResults.keySet() ) {
                        _connection.addAccessedNode( _node, RemoteConnectionHandle.fromConnectionHandle( _connection.getConnectionHandle() ) );
                    }

                    return _statement.createResultSet( _remoteResults,
                            /* mergeResults */ ( origins ) -> {
                                // todo: replace with a proper merge of the results
                                return new HorizontalExecuteResultMergeFunction().apply( _statement, origins, _maxRowsInFirstFrame );
                            },
                            /* fetch */ ( origins, conn, stmt, offset, fetchMaxRowCount ) -> {
                                throw new UnsupportedOperationException( "Not implemented yet." );
                            } );
                },
                //
                /* executeBatch */ ( _connection, _transaction, _statement, _parameterValues ) -> {
                    //
                    // nodes on which the insert is EXECUTED
                    // todo: replace this with a lookup in the Schema on where to execute the inserts
                    // use something like
                    /*
                        ( clusterMembers, parameterValues ) -> {
                            final AbstractRemoteNode[] executionTargets = clusterMembers.toArray( new AbstractRemoteNode[clusterMembers.size()] );
                            final Object[] primaryKeyValues = new Object[primaryKeyColumnsIndexes.size()];
                            int primaryKeyValueIndex = 0;
                            for ( Iterator<Integer> primaryKeyColumnsIndexIterator = primaryKeyColumnsIndexes.iterator(); primaryKeyColumnsIndexIterator.hasNext(); ) {
                                primaryKeyValues[primaryKeyValueIndex++] = parameterValues.get( primaryKeyColumnsIndexesToParametersIndexes.get( primaryKeyColumnsIndexIterator.next() ) );
                            }
                            final int executionTargetIndex = Math.abs( Objects.hash( primaryKeyValues ) % executionTargets.length );
                            return Arrays.asList( executionTargets[executionTargetIndex] );
                        }
                     */
                    List<AbstractRemoteNode> _executionTargets = // this.executionTargetsFunctions.get( _statement ).apply( _connection.getCluster().getMembers(), _parameterValues );
                            _connection.getCluster().getMembers();

                    final Map<AbstractRemoteNode, RemoteExecuteBatchResult> _remoteResults;
                    try {
                        // execute the insert statement
                        // todo: replace with a call to the next protocol (down)
                        final RspList<RemoteExecuteBatchResult> _responseList = _connection.getCluster().executeBatch( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _parameterValues, _executionTargets );

                        // handle the responses
                        _remoteResults = ClusterUtils.getRemoteResults( _connection.getCluster(), _responseList );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    for ( AbstractRemoteNode _node : _remoteResults.keySet() ) {
                        _connection.addAccessedNode( _node, RemoteConnectionHandle.fromConnectionHandle( _connection.getConnectionHandle() ) );
                    }

                    return _statement.createBatchResultSet( _remoteResults,
                            /* mergeResults */ ( origins ) -> {
                                return new ExecuteBatchResult( origins.values().iterator().next().toExecuteBatchResult().updateCounts );
                            } );
                }
        );
    }


    protected StatementInfos prepareDataManipulationDelete( ConnectionInfos connection, StatementInfos statement, SqlDelete sql, long maxRowCount ) throws RemoteException {
        LOGGER.trace( "prepareDataManipulationDelete( connection: {}, statement: {}, sql: {}, maxRowCount: {} )", connection, statement, sql, maxRowCount );

        // FAIL-FAST
        if ( SqlNodeUtils.DELETE_UTILS.whereConditionContainsOnlyEquals( sql ) == false ) {
            // Currently only primary_key EQUALS('=') ? is supported
            throw new UnsupportedOperationException( "Not supported yet." );
        }

        // deletes have to be PREPARED on ALL nodes
        // todo: replace this with a call to the protocol down
        final RspList<RemoteStatementHandle> prepareResponseList = connection.getCluster().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount );
        // handle the responses of the PREPARE execution
        final Map<AbstractRemoteNode, RemoteStatementHandle> prepareRemoteResults = ClusterUtils.getRemoteResults( connection.getCluster(), prepareResponseList );

        final SqlIdentifier targetTable = SqlNodeUtils.DELETE_UTILS.getTargetTable( sql );

        // do now most of the work required for later execution
        final String catalogName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 2 );
        final String schemaName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 1 );
        final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 0 );
        final Map<String, Integer> primaryKeyColumnNamesAndIndexes = CatalogUtils.lookupPrimaryKeyColumnNamesAndIndexes( connection, catalogName, schemaName, tableName );

        final Set<Integer> primaryKeyColumnsIndexes = new TreeSet<>( primaryKeyColumnNamesAndIndexes.values() ); // naturally ordered and thus the indexes of the primary key columns are in the correct order
        final SortedMap<Integer, Integer> primaryKeyColumnsIndexesToParametersIndexes = new TreeMap<>();
        final Map<String, Integer> columnNamesToParameterIndexes = SqlNodeUtils.getColumnsNameToDynamicParametersIndexMap( sql );

        // Check the WHERE condition if the primary key is included
        boolean allPrimaryKeyColumnsAreInTheCondition = true;
        for ( Entry<String, Integer> primaryKeyColumnNameAndIndex : primaryKeyColumnNamesAndIndexes.entrySet() ) {
            // for every primary key and its index in the table
            final Integer parameterIndex = columnNamesToParameterIndexes.get( primaryKeyColumnNameAndIndex.getKey() );
            if ( parameterIndex != null ) {
                // the primary key is present in the condition
                primaryKeyColumnsIndexesToParametersIndexes.put( primaryKeyColumnNameAndIndex.getValue(), parameterIndex );
            } else {
                // the primary key is NOT in the condition
                allPrimaryKeyColumnsAreInTheCondition = false;
                throw new UnsupportedOperationException( "Not implemented yet." );
            }
        }
        final boolean final_allPrimaryKeyColumnsAreInTheCondition = allPrimaryKeyColumnsAreInTheCondition;

        // construct the prepared statement together with the execution functions
        return connection.createPreparedStatement( statement, prepareRemoteResults,
                //
                /* signatureMerge */ ( _remoteStatements ) -> {
                    // BEGIN HACK - get the first signature
                    return _remoteStatements.values().iterator().next().toStatementHandle().signature;
                    // END HACK
                },
                //
                /* execute */ ( _connection, _transaction, _statement, _parameterValues, _maxRowsInFirstFrame ) -> {
                    //
                    // nodes on which the insert is EXECUTED
                    // todo: replace this with a lookup in the Schema on where to execute the inserts
                    // use something like
                    /*
                    ( clusterMembers, parameterValues ) -> {
                        final AbstractRemoteNode[] executionTargets = clusterMembers.toArray( new AbstractRemoteNode[clusterMembers.size()] );
                        final Object[] primaryKeyValues = new Object[primaryKeyColumnsIndexes.size()];
                        int primaryKeyValueIndex = 0;
                        for ( Iterator<Integer> primaryKeyColumnsIndexIterator = primaryKeyColumnsIndexes.iterator(); primaryKeyColumnsIndexIterator.hasNext(); ) {
                            primaryKeyValues[primaryKeyValueIndex++] = parameterValues.get( primaryKeyColumnsIndexesToParametersIndexes.get( primaryKeyColumnsIndexIterator.next() ) );
                        }
                        final int executionTargetIndex = Math.abs( Objects.hash( primaryKeyValues ) % executionTargets.length );
                        return Arrays.asList( executionTargets[executionTargetIndex] );
                    }
                    */
                    List<AbstractRemoteNode> _executionTargets = // this.executionTargetsFunctions.get( _statement ).apply( _connection.getCluster().getMembers(), _parameterValues );
                            _connection.getCluster().getMembers();

                    final Map<AbstractRemoteNode, RemoteExecuteResult> _remoteResults;
                    try {
                        // execute the delete statement
                        // todo: replace with a call to the next protocol (down)
                        final RspList<RemoteExecuteResult> _responseList = _connection.getCluster().execute( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _parameterValues, -1/*_maxRowsInFirstFrame*/, _executionTargets );

                        // handle the responses
                        _remoteResults = ClusterUtils.getRemoteResults( _connection.getCluster(), _responseList );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    for ( AbstractRemoteNode _node : _remoteResults.keySet() ) {
                        _connection.addAccessedNode( _node, RemoteConnectionHandle.fromConnectionHandle( _connection.getConnectionHandle() ) );
                    }

                    return _statement.createResultSet( _remoteResults,
                            /* mergeResults */ ( origins ) -> {
                                //
/*                                if ( final_allPrimaryKeyColumnsAreInTheCondition ) {
                                    // the full primary key is present in the where condition of the query
                                    // we can use this to add to the workload

                                    // collect all values of the (combined) primary key
                                    final Serializable[] primaryKeyValues = new Serializable[primaryKeyColumnsIndexes.size()];
                                    int primaryKeyValueIndex = 0;
                                    for ( Iterator<Integer> primaryKeyColumnsIndexIterator = primaryKeyColumnsIndexes.iterator(); primaryKeyColumnsIndexIterator.hasNext(); ) {
                                        primaryKeyValues[primaryKeyValueIndex++] = _parameterValues.get( primaryKeyColumnsIndexesToParametersIndexes.get( primaryKeyColumnsIndexIterator.next() ) );
                                    }

                                    // add the requested record to the transaction
                                    //final Transaction wlTrx = Transaction.getTransaction( _transaction.getTransactionId() );
                                    //wlTrx.addAction( Operation.WRITE, new RecordIdentifier( catalogName, schemaName, tableName, primaryKeyValues ) );

                                } else {
                                    // we need to scan the result set to measure the workload
                                    throw new UnsupportedOperationException( "Not implemented yet." );
                                }*/

                                // todo: replace with a proper merge of the results
                                return new HorizontalExecuteResultMergeFunction().apply( _statement, origins, _maxRowsInFirstFrame );
                            },
                            /* fetch */ ( origins, conn, stmt, offset, fetchMaxRowCount ) -> {
                                throw new UnsupportedOperationException( "Not implemented yet." );
                            } );
                },
                //
                /* executeBatch */ ( connectionInfos, transactionInfos, statementInfos, updateBatches ) -> {
                    throw new UnsupportedOperationException( "Not implemented yet." );
                }
        );
    }


    protected StatementInfos prepareDataManipulationUpdate( ConnectionInfos connection, StatementInfos statement, SqlUpdate sql, long maxRowCount ) throws RemoteException {
        LOGGER.trace( "prepareDataManipulationUpdate( connection: {}, statement: {}, sql: {}, maxRowCount: {} )", connection, statement, sql, maxRowCount );

        // FAIL-FAST
        if ( SqlNodeUtils.UPDATE_UTILS.whereConditionContainsOnlyEquals( sql ) == false ) {
            // Currently only primary_key EQUALS('=') ? is supported
            throw new UnsupportedOperationException( "Not supported yet." );
        }

        final SqlIdentifier targetTable = SqlNodeUtils.UPDATE_UTILS.getTargetTable( sql );
        final SqlNodeList targetColumns = SqlNodeUtils.UPDATE_UTILS.getTargetColumns( sql );

        // do now most of the work required for later execution
        final String catalogName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 2 );
        final String schemaName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 1 );
        final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 0 );
        final Map<String, Integer> primaryKeyColumnNamesAndIndexes = CatalogUtils.lookupPrimaryKeyColumnNamesAndIndexes( connection, catalogName, schemaName, tableName );

        final Map<Integer, Integer> primaryKeyColumnsIndexesToParametersIndexes = new HashMap<>();

        if ( targetColumns.accept( new SqlBasicVisitor<Boolean>() {
            @Override
            public Boolean visit( SqlNodeList nodeList ) {
                boolean b = false;
                for ( SqlNode node : nodeList ) {
                    b |= node.accept( this );
                }
                return b;
            }


            @Override
            public Boolean visit( SqlIdentifier id ) {
                return primaryKeyColumnNamesAndIndexes.containsKey( id.names.reverse().get( 0 ) );
            }
        } ) ) {
            // At least one primary key column is in the targetColumns list and will be updated
            throw new UnsupportedOperationException( "Not implemented yet." );
        }

        final Map<String, Integer> columnNamesToParameterIndexes = SqlNodeUtils.getColumnsNameToDynamicParametersIndexMap( sql );

        // updates have to be PREPARED on ALL nodes
        // todo: replace this with a call to the protocol down
        final RspList<RemoteStatementHandle> prepareResponseList = connection.getCluster().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount );
        // handle the responses of the PREPARE execution
        final Map<AbstractRemoteNode, RemoteStatementHandle> prepareRemoteResults = ClusterUtils.getRemoteResults( connection.getCluster(), prepareResponseList );

        // do now most of the work required for later execution
        boolean allPrimaryKeyColumnsAreInTheCondition = true;
        for ( Entry<String, Integer> primaryKeyColumnNameAndIndex : primaryKeyColumnNamesAndIndexes.entrySet() ) {
            // for every primary key and its index in the table
            final Integer parameterIndex = columnNamesToParameterIndexes.get( primaryKeyColumnNameAndIndex.getKey() );
            if ( parameterIndex != null ) {
                // the primary key is present in the condition
                primaryKeyColumnsIndexesToParametersIndexes.put( primaryKeyColumnNameAndIndex.getValue(), parameterIndex );
            } else {
                // the primary key is NOT in the condition
                // Thus, during execution, the statement has to be executed on all nodes
                allPrimaryKeyColumnsAreInTheCondition = false;
            }
        }
        final boolean final_allPrimaryKeyColumnsAreInTheCondition = allPrimaryKeyColumnsAreInTheCondition;

        // construct the prepared statement together with the execution functions
        return connection.createPreparedStatement( statement, prepareRemoteResults,
                //
                /* signatureMerge */ ( _remoteStatements ) -> {
                    // BEGIN HACK - get the first signature
                    return _remoteStatements.values().iterator().next().toStatementHandle().signature;
                    // END HACK
                },
                //
                /* execute */ ( _connection, _transaction, _statement, _parameterValues, _maxRowsInFirstFrame ) -> {
                    //
                    // nodes on which the insert is EXECUTED
                    // todo: replace this with a lookup in the Schema on where to execute the inserts
                    // use something like

                    /*
                    final Function2<List<AbstractRemoteNode>, List<TypedValue>, List<AbstractRemoteNode>> executionTargetsFunction;
                    if ( allPrimaryKeyColumnsAreInTheCondition ) {
                        executionTargetsFunction = ( clusterMembers, parameterValues ) -> {
                            final AbstractRemoteNode[] executionTargets = clusterMembers.toArray( new AbstractRemoteNode[clusterMembers.size()] );
                            final Object[] primaryKeyValues = new Object[primaryKeyColumnsIndexes.size()];
                            int primaryKeyValueIndex = 0;
                            for ( Iterator<Integer> primaryKeyColumnsIndexIterator = primaryKeyColumnsIndexes.iterator(); primaryKeyColumnsIndexIterator.hasNext(); ) {
                                primaryKeyValues[primaryKeyValueIndex++] = parameterValues.get( primaryKeyColumnsIndexesToParametersIndexes.get( primaryKeyColumnsIndexIterator.next() ) );
                            }
                            final int executionTargetIndex = Math.abs( Objects.hash( primaryKeyValues ) % executionTargets.length );
                            return Arrays.asList( executionTargets[executionTargetIndex] );
                        };
                    } else {
                        executionTargetsFunction = ( clusterMembers, typedValues ) -> clusterMembers;
                    }
                    */

                    List<AbstractRemoteNode> _executionTargets = // this.executionTargetsFunctions.get( _statement ).apply( _connection.getCluster().getMembers(), _parameterValues );
                            _connection.getCluster().getMembers();

                    final Map<AbstractRemoteNode, RemoteExecuteResult> _remoteResults;
                    try {
                        // execute the update statement
                        // todo: replace with a call to the next protocol (down)
                        final RspList<RemoteExecuteResult> _responseList = _connection.getCluster().execute( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _parameterValues, -1/*_maxRowsInFirstFrame*/, _executionTargets );

                        // handle the responses
                        _remoteResults = ClusterUtils.getRemoteResults( _connection.getCluster(), _responseList );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    for ( AbstractRemoteNode _node : _remoteResults.keySet() ) {
                        _connection.addAccessedNode( _node, RemoteConnectionHandle.fromConnectionHandle( _connection.getConnectionHandle() ) );
                    }

                    return _statement.createResultSet( _remoteResults,
                            /* mergeResults */ ( origins ) -> {
                                //
                                if ( final_allPrimaryKeyColumnsAreInTheCondition ) {
                                    // the full primary key is present in the where condition of the query
                                    // we can use this to add to the workload

                                    final Set<Integer> primaryKeyColumnsIndexes = new TreeSet<>( primaryKeyColumnNamesAndIndexes.values() ); // naturally ordered and thus the indexes of the primary key columns are in the correct order

                                    // collect all values of the (combined) primary key
                                    final Serializable[] primaryKeyValues = new Serializable[primaryKeyColumnsIndexes.size()];
                                    int primaryKeyValueIndex = 0;
                                    for ( Iterator<Integer> primaryKeyColumnsIndexIterator = primaryKeyColumnsIndexes.iterator(); primaryKeyColumnsIndexIterator.hasNext(); ) {
                                        primaryKeyValues[primaryKeyValueIndex++] = _parameterValues.get( primaryKeyColumnsIndexesToParametersIndexes.get( primaryKeyColumnsIndexIterator.next() ) );
                                    }

                                    // add the requested record to the transaction
                                    //final Transaction wlTrx = Transaction.getTransaction( _transaction.getTransactionId() );
                                    //wlTrx.addAction( Operation.WRITE, new RecordIdentifier( catalogName, schemaName, tableName, primaryKeyValues ) );

                                } else {
                                    // we need to scan the result set to measure the workload
                                    throw new UnsupportedOperationException( "Not implemented yet." );
                                }

                                // todo: replace with a proper merge of the results
                                return new HorizontalExecuteResultMergeFunction().apply( _statement, origins, _maxRowsInFirstFrame );
                            },
                            /* fetch */ ( origins, conn, stmt, offset, fetchMaxRowCount ) -> {
                                throw new UnsupportedOperationException( "Not implemented yet." );
                            } );
                },
                //
                /* executeBatch */ ( connectionInfos, transactionInfos, statementInfos, updateBatches ) -> {
                    throw new UnsupportedOperationException( "Not implemented yet." );
                }
        );
    }


    @Override
    public Map<AbstractRemoteNode, RemoteStatementHandle> prepareDataManipulation( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount, Collection<AbstractRemoteNode> executionTargets ) throws RemoteException {
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    @Override
    public StatementInfos prepareDataQuery( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {
        LOGGER.trace( "prepareDataQuery( connection: {}, statement: {}, sql: {}, maxRowCount: {} )", connection, statement, sql, maxRowCount );

        switch ( sql.getKind() ) {
            // See org.apache.calcite.sql.SqlKind.QUERY
            case SELECT:
                return prepareDataQuerySelect( connection, statement, (SqlSelect) sql, maxRowCount );

            case UNION:
            case INTERSECT:
            case EXCEPT:
            case VALUES:
            case WITH:
            case ORDER_BY:
            case EXPLICIT_TABLE:
            default:
                throw new UnsupportedOperationException( "Not supported" );
        }
    }


    @Override
    public Map<AbstractRemoteNode, RemoteStatementHandle> prepareDataQuery( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount, Collection<AbstractRemoteNode> executionTargets ) throws RemoteException {
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    protected StatementInfos prepareDataQuerySelect( ConnectionInfos connection, StatementInfos statement, SqlSelect sql, long maxRowCount ) throws RemoteException {
        LOGGER.trace( "prepareDataQuerySelect( connection: {}, statement: {}, sql: {}, maxRowCount: {} )", connection, statement, sql, maxRowCount );

        if ( SqlNodeUtils.SELECT_UTILS.selectListContainsAggregate( sql ) ) {
            // SELECT MIN/MAX/AVG/ FROM ...
            return prepareDataQuerySelectAggregate( connection, statement, sql, maxRowCount );
        }

        // more than one table and especially the required join is currently not supported!
        final SqlIdentifier targetTable = SqlNodeUtils.SELECT_UTILS.getTargetTable( sql );

        // do now most of the work required for later execution
        final String catalogName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 2 );
        final String schemaName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 1 );
        final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 0 );
        final Map<String, Integer> primaryKeyColumnNamesAndIndexes = CatalogUtils.lookupPrimaryKeyColumnNamesAndIndexes( connection, catalogName, schemaName, tableName );
        final Set<Integer> primaryKeyColumnsIndexes = new TreeSet<>( primaryKeyColumnNamesAndIndexes.values() ); // naturally ordered and thus the indexes of the primary key columns are in the correct order
        final Map<Integer, Integer> primaryKeyColumnsIndexesToParametersIndexes = new HashMap<>();

        final Map<String, Integer> columnNamesToParameterIndexes = SqlNodeUtils.getColumnsNameToDynamicParametersIndexMap( sql );

        boolean incompletePrimaryKey = false;
        for ( Entry<String, Integer> primaryKeyColumnNameAndIndex : primaryKeyColumnNamesAndIndexes.entrySet() ) {
            // for every primary key and its index in the table
            final Integer parameterIndex = columnNamesToParameterIndexes.get( primaryKeyColumnNameAndIndex.getKey() );
            if ( parameterIndex != null ) {
                // the primary key is present in the condition
                primaryKeyColumnsIndexesToParametersIndexes.put( primaryKeyColumnNameAndIndex.getValue(), parameterIndex );
            } else {
                // the primary key is NOT in the condition
                incompletePrimaryKey = true;
            }
        }

        final boolean final_incompletePrimaryKey = incompletePrimaryKey;

        // selects might have to be PREPARED on ALL nodes
        // todo: replace this with a call to the protocol down
        final RspList<RemoteStatementHandle> prepareResponseList = connection.getCluster().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount );
        // handle the responses of the PREPARE execution
        final Map<AbstractRemoteNode, RemoteStatementHandle> prepareRemoteResults = ClusterUtils.getRemoteResults( connection.getCluster(), prepareResponseList );

        // construct the prepared statement together with the execution functions
        return connection.createPreparedStatement( statement, prepareRemoteResults,
                //
                /* signatureMerge */ ( _remoteStatements ) -> {
                    // BEGIN HACK - get the first signature
                    return _remoteStatements.values().iterator().next().toStatementHandle().signature;
                    // END HACK
                },
                //
                /* execute */ ( _connection, _transaction, _statement, _parameterValues, _maxRowsInFirstFrame ) -> {
                    //
                    // nodes on which the insert is EXECUTED
                    // todo: replace this with a lookup in the Schema on where to execute the inserts
                    // use something like

                    /*
                    final Function2<List<AbstractRemoteNode>, List<TypedValue>, List<AbstractRemoteNode>> executionTargetsFunction;
                    // todo: improve. Only the primary keys are relevant
                    if ( SqlNodeUtils.SELECT_UTILS.whereConditionContainsOnlyEquals( sql ) ) {
                        // It seems that we only have EQUALS in our WHERE condition
                        if ( final_incompletePrimaryKey ) {
                            executionTargetsFunction = ( clusterMembers, typedValues ) -> clusterMembers;
                        } else {
                            executionTargetsFunction = ( clusterMembers, parameterValues ) -> {
                                final AbstractRemoteNode[] executionTargets = clusterMembers.toArray( new AbstractRemoteNode[clusterMembers.size()] );
                                final Object[] primaryKeyValues = new Object[primaryKeyColumnsIndexes.size()];
                                int primaryKeyValueIndex = 0;
                                for ( Iterator<Integer> primaryKeyColumnsIndexIterator = primaryKeyColumnsIndexes.iterator(); primaryKeyColumnsIndexIterator.hasNext(); ) {
                                    primaryKeyValues[primaryKeyValueIndex++] = parameterValues.get( primaryKeyColumnsIndexesToParametersIndexes.get( primaryKeyColumnsIndexIterator.next() ) );
                                }
                                final int executionTargetIndex = Math.abs( Objects.hash( primaryKeyValues ) % executionTargets.length );
                                return Arrays.asList( executionTargets[executionTargetIndex] );
                            };
                        }
                    } else {
                        // We need to scan and thus the query needs to go to all nodes
                        executionTargetsFunction = ( clusterMembers, typedValues ) -> clusterMembers;
                    }
                    */

                    List<AbstractRemoteNode> _executionTargets = // this.executionTargetsFunctions.get( _statement ).apply( _connection.getCluster().getMembers(), _parameterValues );
                            _connection.getCluster().getMembers();

                    final Map<AbstractRemoteNode, RemoteExecuteResult> _remoteResults;
                    try {
                        // execute the select statement
                        // todo: replace with a call to the next protocol (down)
                        final RspList<RemoteExecuteResult> _responseList = _connection.getCluster().execute( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _parameterValues, -1/*_maxRowsInFirstFrame*/, _executionTargets );

                        // handle the responses
                        _remoteResults = ClusterUtils.getRemoteResults( _connection.getCluster(), _responseList );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    for ( AbstractRemoteNode _node : _remoteResults.keySet() ) {
                        _connection.addAccessedNode( _node, RemoteConnectionHandle.fromConnectionHandle( _connection.getConnectionHandle() ) );
                    }

                    return _statement.createResultSet( _remoteResults,
                            /* mergeResults */ ( origins ) -> {
                                //
                                if ( final_incompletePrimaryKey == false ) {
                                    // the full primary key is present in the query
                                    // we can use this to add to the workload

                                    // collect all values of the (combined) primary key
                                    final Serializable[] primaryKeyValues = new Serializable[primaryKeyColumnsIndexes.size()];
                                    int primaryKeyValueIndex = 0;
                                    for ( Iterator<Integer> primaryKeyColumnsIndexIterator = primaryKeyColumnsIndexes.iterator(); primaryKeyColumnsIndexIterator.hasNext(); ) {
                                        primaryKeyValues[primaryKeyValueIndex++] = _parameterValues.get( primaryKeyColumnsIndexesToParametersIndexes.get( primaryKeyColumnsIndexIterator.next() ) );
                                    }

                                    // add the requested record to the transaction
                                    final Transaction wlTrx = Transaction.getTransaction( _transaction.getTransactionId() );
                                    wlTrx.addAction( Operation.READ, new RecordIdentifier( catalogName, schemaName, tableName, primaryKeyValues ) );

                                } else {
                                    // we need to scan the result set to measure the workload
                                    throw new UnsupportedOperationException( "Not implemented yet." );
                                }

                                // todo: replace with a proper merge of the results
                                return new HorizontalExecuteResultMergeFunction().apply( _statement, origins, _maxRowsInFirstFrame );
                            },
                            /* fetch */ ( origins, conn, stmt, offset, fetchMaxRowCount ) -> {
                                throw new UnsupportedOperationException( "Not implemented yet." );
                            } );
                },
                //
                /* executeBatch */ ( connectionInfos, transactionInfos, statementInfos, updateBatches ) -> {
                    throw Utils.wrapException( new SQLException( "SELECT statements cannot be executed in a batch context." ) );
                }
        );
    }


    protected StatementInfos prepareDataQuerySelectAggregate( ConnectionInfos connection, StatementInfos statement, SqlSelect sql, long maxRowCount ) throws RemoteException {
        LOGGER.trace( "prepareDataQuerySelect( connection: {}, statement: {}, sql: {}, maxRowCount: {} )", connection, statement, sql, maxRowCount );

        // todo: MIN(pk_column) SUM(foreign_column) - both have incomplete primary keys
        // SELECT MIN(`NEW_ORDER`.`NO_O_ID`)
        //FROM `PUBLIC`.`NEW_ORDER` AS `NEW_ORDER`
        //WHERE `NEW_ORDER`.`NO_D_ID` = ? AND `NEW_ORDER`.`NO_W_ID` = ?

        // FAIL-FAST
        final SqlNodeList selectList = sql.getSelectList();
        if ( selectList.size() > 1 ) {
            throw new UnsupportedOperationException( "Not implemented yet." );
        }

        // more than one table and especially the required join is currently not supported!
        final SqlIdentifier targetTable = SqlNodeUtils.SELECT_UTILS.getTargetTable( sql );

        final Map<String, Integer> primaryKeyColumnNamesAndIndexes = CatalogUtils.lookupPrimaryKeyColumnNamesAndIndexes( connection, SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 2 ), SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 1 ), SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 0 ) );
        final Set<Integer> primaryKeyColumnsIndexes = new TreeSet<>( primaryKeyColumnNamesAndIndexes.values() ); // naturally ordered and thus the indexes of the primary key columns are in the correct order
        final Map<Integer, Integer> primaryKeyColumnsIndexesToParametersIndexes = new HashMap<>();

        final Map<String, Integer> columnNamesToParameterIndexes = SqlNodeUtils.getColumnsNameToDynamicParametersIndexMap( sql );

        // selects might have to be PREPARED on ALL nodes (so we do it now)
        // todo: replace this with a call to the protocol down
        final RspList<RemoteStatementHandle> prepareResponseList = connection.getCluster().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount );

        // handle the responses of the PREPARE execution
        final Map<AbstractRemoteNode, RemoteStatementHandle> prepareRemoteResults = ClusterUtils.getRemoteResults( connection.getCluster(), prepareResponseList );

        boolean incompletePrimaryKey = false;
        for ( Entry<String, Integer> primaryKeyColumnNameAndIndex : primaryKeyColumnNamesAndIndexes.entrySet() ) {
            // for every primary key and its index in the table
            final Integer parameterIndex = columnNamesToParameterIndexes.get( primaryKeyColumnNameAndIndex.getKey() );
            if ( parameterIndex != null ) {
                // the primary key is present in the condition
                primaryKeyColumnsIndexesToParametersIndexes.put( primaryKeyColumnNameAndIndex.getValue(), parameterIndex );
            } else {
                // the primary key is NOT in the condition
                incompletePrimaryKey = true;
            }
        }
        final boolean final_incompletePrimaryKey = incompletePrimaryKey;

        final Function2<List<AbstractRemoteNode>, List<TypedValue>, List<AbstractRemoteNode>> executionTargetsFunction;
        // todo: improve. Only the primary keys are relevant
        if ( SqlNodeUtils.SELECT_UTILS.whereConditionContainsOnlyEquals( sql ) ) {
            // It seems that we only have EQUALS in our WHERE condition
            if ( final_incompletePrimaryKey ) {
                executionTargetsFunction = ( clusterMembers, typedValues ) -> clusterMembers;
            } else {
                throw new UnsupportedOperationException( "Not implemented yet." );
            }
        } else {
            // We need to scan and thus the query needs to go to all nodes
            executionTargetsFunction = ( clusterMembers, typedValues ) -> clusterMembers;
        }

        final SqlNode asExpressionOrAggregateNode = selectList.get( 0 );
        final SqlNode aggregateNode;
        if ( asExpressionOrAggregateNode.getKind() == SqlKind.AS ) {
            aggregateNode = ((SqlBasicCall) asExpressionOrAggregateNode).operand( 0 );
        } else {
            aggregateNode = asExpressionOrAggregateNode;
        }

        final Function5<ConnectionInfos, TransactionInfos, StatementInfos, List<TypedValue>, Integer, QueryResultSet> executeFunction;
        switch ( aggregateNode.getKind() ) {
            case MIN:
                executeFunction = ( _connection, _transaction, _statement, _parameterValues, _maxRowsInFirstFrame ) -> {

                    List<AbstractRemoteNode> _executionTargets = // this.executionTargetsFunctions.get( _statement ).apply( _connection.getCluster().getMembers(), _parameterValues );
                            _connection.getCluster().getMembers();
                    LOGGER.trace( "execute on {}", _executionTargets );

                    final RspList<RemoteExecuteResult> executeResponseList;
                    try {
                        executeResponseList = _connection.getCluster().execute( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _parameterValues, -1/*maxRowsInFirstFrame*/, _executionTargets );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    final Map<AbstractRemoteNode, RemoteExecuteResult> executeRemoteResults = new HashMap<>();
                    for ( Entry<Address, Rsp<RemoteExecuteResult>> e : executeResponseList.entrySet() ) {
                        final Address address = e.getKey();
                        final Rsp<RemoteExecuteResult> remoteExecuteResultRsp = e.getValue();

                        if ( remoteExecuteResultRsp.hasException() ) {
                            throw Utils.wrapException( remoteExecuteResultRsp.getException() );
                        }

                        final AbstractRemoteNode currentRemote = _connection.getCluster().getRemoteNode( address );

                        executeRemoteResults.put( currentRemote, remoteExecuteResultRsp.getValue() );

                        _connection.addAccessedNode( currentRemote, RemoteConnectionHandle.fromConnectionHandle( _connection.getConnectionHandle() ) );
                    }

                    return statement.createResultSet( executeRemoteResults,
                            /* merge */ origins -> {
                                int minValue = Integer.MAX_VALUE;
                                for ( RemoteExecuteResult rex : origins.values() ) {
                                    for ( MetaResultSet rs : rex.toExecuteResult().resultSets ) {
                                        Object row = rs.firstFrame.rows.iterator().next();
                                        switch ( rs.signature.cursorFactory.style ) {
                                            case LIST:
                                                if ( row instanceof List ) {
                                                    minValue = Math.min( minValue, (int) ((List) row).get( 0 ) );
                                                } else if ( row instanceof Object[] ) {
                                                    minValue = Math.min( minValue, (int) ((Object[]) row)[0] );
                                                } else {
                                                    throw new UnsupportedOperationException( "Not implemented yet." );
                                                }
                                                break;

                                            default:
                                                throw new UnsupportedOperationException( "Not implemented yet." );
                                        }
                                    }
                                }
                                Signature signature = origins.values().iterator().next().toExecuteResult().resultSets.iterator().next().signature;
                                Frame frame = Frame.create( 0, true, Collections.singletonList( new Object[]{ minValue } ) );
                                return new Meta.ExecuteResult( Collections.singletonList( MetaResultSet.create( statement.getStatementHandle().connectionId, statement.getStatementHandle().id, false, signature, frame ) ) );
                            },
                            /* fetch */ ( origins, conn, stmt, offset, fetchMaxRowCount ) -> Frame.EMPTY );
                };
                break;

            case SUM:
                executeFunction = ( _connection, _transaction, _statement, _parameterValues, _maxRowsInFirstFrame ) -> {

                    List<AbstractRemoteNode> _executionTargets = // this.executionTargetsFunctions.get( _statement ).apply( _connection.getCluster().getMembers(), _parameterValues );
                            _connection.getCluster().getMembers();
                    LOGGER.trace( "execute on {}", _executionTargets );

                    final RspList<RemoteExecuteResult> executeResponseList;
                    try {
                        executeResponseList = _connection.getCluster().execute( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _parameterValues, -1/*maxRowsInFirstFrame*/, _executionTargets );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    final Map<AbstractRemoteNode, RemoteExecuteResult> executeRemoteResults = new HashMap<>();
                    for ( Entry<Address, Rsp<RemoteExecuteResult>> e : executeResponseList.entrySet() ) {
                        final Address address = e.getKey();
                        final Rsp<RemoteExecuteResult> remoteExecuteResultRsp = e.getValue();

                        if ( remoteExecuteResultRsp.hasException() ) {
                            throw Utils.wrapException( remoteExecuteResultRsp.getException() );
                        }

                        final AbstractRemoteNode currentRemote = _connection.getCluster().getRemoteNode( address );

                        executeRemoteResults.put( currentRemote, remoteExecuteResultRsp.getValue() );

                        _connection.addAccessedNode( currentRemote, RemoteConnectionHandle.fromConnectionHandle( _connection.getConnectionHandle() ) );
                    }

                    return statement.createResultSet( executeRemoteResults,
                            /* merge */ origins -> {
                                BigDecimal sumValue = BigDecimal.ZERO;
                                for ( RemoteExecuteResult rex : origins.values() ) {
                                    for ( MetaResultSet rs : rex.toExecuteResult().resultSets ) {
                                        final Object row = rs.firstFrame.rows.iterator().next();
                                        final Object summand;
                                        switch ( rs.signature.cursorFactory.style ) {
                                            case LIST:
                                                if ( row instanceof List ) {
                                                    summand = ((List) row).get( 0 );
                                                } else if ( row instanceof Object[] ) {
                                                    summand = ((Object[]) row)[0];
                                                } else {
                                                    throw new UnsupportedOperationException( "Not implemented yet." );
                                                }
                                                break;

                                            default:
                                                throw new UnsupportedOperationException( "Not implemented yet." );
                                        }
                                        if ( summand instanceof BigDecimal ) {
                                            sumValue = sumValue.add( (BigDecimal) summand );
                                        } else {
                                            throw new UnsupportedOperationException( "Not implemented yet." );
                                        }
                                    }
                                }
                                Signature signature = origins.values().iterator().next().toExecuteResult().resultSets.iterator().next().signature;
                                Frame frame = Frame.create( 0, true, Collections.singletonList( new Object[]{ sumValue } ) );
                                return new Meta.ExecuteResult( Collections.singletonList( MetaResultSet.create( statement.getStatementHandle().connectionId, statement.getStatementHandle().id, false, signature, frame ) ) );
                            },
                            /* fetch */ ( origins, conn, stmt, offset, fetchMaxRowCount ) -> Frame.EMPTY );
                };
                break;

            default:
                throw new UnsupportedOperationException( "Not implemented yet." );
        }

        // construct the prepared statement together with the execution functions
        return connection.createPreparedStatement( statement, prepareRemoteResults,
                //
                /* signatureMerge */ ( _remoteStatements ) -> {
                    // BEGIN HACK - get the first signature
                    return _remoteStatements.values().iterator().next().toStatementHandle().signature;
                    // END HACK
                },
                //
                /* execute */ executeFunction,
                //
                /* executeBatch */ ( connectionInfos, transactionInfos, statementInfos, updateBatches ) -> {
                    throw Utils.wrapException( new SQLException( "SELECT statements cannot be executed in a batch context." ) );
                }
        );
    }


    @Override
    public ReplicationProtocol setReplicationProtocol( ReplicationProtocol replicationProtocol ) {
        return null;
    }


    public static class FragmentationSchema {

        public Fragment lookupFragment( RecordIdentifier record ) {
            throw new UnsupportedOperationException( "Not implemented yet." );
        }


        public List<Fragment> allFragments() {
            throw new UnsupportedOperationException( "Not implemented yet." );
        }
    }


    public static class Fragment extends VirtualNode {

    }
}
