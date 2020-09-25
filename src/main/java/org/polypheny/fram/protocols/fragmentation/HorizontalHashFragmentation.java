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
import io.vavr.Function3;
import java.io.Serializable;
import java.math.BigDecimal;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.ExecuteBatchResult;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.proto.Common.TypedValue;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.jgroups.Address;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.polypheny.fram.protocols.AbstractProtocol;
import org.polypheny.fram.protocols.Protocol.FragmentationProtocol;
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
import org.polypheny.fram.standalone.StatementInfos;
import org.polypheny.fram.standalone.StatementInfos.PreparedStatementInfos;
import org.polypheny.fram.standalone.TransactionInfos;
import org.polypheny.fram.standalone.Utils;
import org.polypheny.fram.standalone.Utils.WrappingException;
import org.polypheny.fram.standalone.parser.sql.SqlNodeUtils;


public class HorizontalHashFragmentation extends AbstractProtocol implements FragmentationProtocol {


    private final Function2<Object[], Integer, Integer> hashFunction =
            ( Object[] values, Integer modulo ) -> Objects.hash( values ) % modulo;

    private final Function3<List<AbstractRemoteNode>, List<TypedValue>, SortedMap<Integer, Integer>, List<AbstractRemoteNode>> getExecutionTargetsFunction =
            ( List<AbstractRemoteNode> remotes, List<TypedValue> parameterValues, SortedMap<Integer, Integer> sortedPrimaryKeyColumnsIndexesToParametersIndexes ) -> {
                final Object[] keyValues = new Object[sortedPrimaryKeyColumnsIndexesToParametersIndexes.size()];
                int keyValueIndex = 0;
                for ( Entry<Integer, Integer> primaryKeyColumnIndexToParameterIndex : sortedPrimaryKeyColumnsIndexesToParametersIndexes.entrySet() ) {
                    final int parameterIndex = primaryKeyColumnIndexToParameterIndex.getValue();
                    if ( parameterValues.size() > parameterIndex ) {
                        keyValues[keyValueIndex++] = parameterValues.get( primaryKeyColumnIndexToParameterIndex.getValue() /* index of the parameter */ );
                    } else {
                        // incomplete key; return all!
                        return remotes;
                    }
                }
                return Collections.singletonList( remotes.get(
                        HorizontalHashFragmentation.this.hashFunction.apply( keyValues, remotes.size() )
                ) );
            };


    public HorizontalHashFragmentation() {
    }


    /*
     *
     */


    @Override
    public ResultSetInfos prepareAndExecuteDataManipulation( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        LOGGER.trace( "prepareAndExecuteDataManipulation( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, callback: {} )", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );

        switch ( sql.getKind() ) {
            // See org.apache.calcite.sql.SqlKind.DML
            case INSERT:
                return prepareAndExecuteDataManipulationInsert( connection, transaction, statement, (SqlInsert) sql, maxRowCount, maxRowsInFirstFrame, callback );

            case DELETE:
                return prepareAndExecuteDataManipulationDelete( connection, transaction, statement, (SqlDelete) sql, maxRowCount, maxRowsInFirstFrame, callback );

            case UPDATE:
                return prepareAndExecuteDataManipulationUpdate( connection, transaction, statement, (SqlUpdate) sql, maxRowCount, maxRowsInFirstFrame, callback );

            case MERGE:
            case PROCEDURE_CALL:
            default:
                throw new UnsupportedOperationException( "Not supported" );
        }
    }


    protected ResultSetInfos prepareAndExecuteDataManipulationInsert( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlInsert sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        LOGGER.trace( "prepareAndExecuteDataManipulationInsert( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, callback: {} )", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );

        final SqlIdentifier table = (SqlIdentifier) sql.getTargetTable();
        final SqlNodeList targetColumns = sql.getTargetColumnList();
        final SqlBasicCall source = (SqlBasicCall) sql.getSource();

        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    protected ResultSetInfos prepareAndExecuteDataManipulationDelete( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlDelete sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        LOGGER.trace( "prepareAndExecuteDataManipulationInsert( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, callback: {} )", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );

        final SqlIdentifier table = (SqlIdentifier) sql.getTargetTable();

        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    protected ResultSetInfos prepareAndExecuteDataManipulationUpdate( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlUpdate sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        LOGGER.trace( "prepareAndExecuteDataManipulationInsert( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, callback: {} )", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );

        final SqlIdentifier targetTable = (SqlIdentifier) sql.getTargetTable();

        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    @Override
    public ResultSetInfos prepareAndExecuteDataManipulation( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, int[] columnIndexes, PrepareCallback callback ) throws RemoteException {
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


    public ResultSetInfos prepareAndExecuteDataQuerySelect( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlSelect sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        LOGGER.trace( "prepareAndExecuteDataQuery( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {} )", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame );

        if ( SqlNodeUtils.SELECT_UTILS.selectListContainsAggregate( sql ) ) {
            // SELECT MIN/MAX/AVG/ FROM ...
            return prepareAndExecuteDataQuerySelectAggregate( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
        }

        // TODO: Refactor below

        final SqlIdentifier targetTable = SqlNodeUtils.SELECT_UTILS.getTargetTable( sql );

        final String catalogName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 2 );
        final String schemaName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 1 );
        final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 0 );

        final Map<String, Integer> primaryKeysColumnIndexes = CatalogUtils.lookupPrimaryKeyColumnNamesAndIndexes( connection, catalogName, schemaName, tableName );
        if ( primaryKeysColumnIndexes.size() > 1 ) {
            // Composite primary key
            throw new UnsupportedOperationException( "Not implemented yet." );
        }
        final String primaryKey = primaryKeysColumnIndexes.keySet().iterator().next();

        final SqlNode condition = sql.getWhere();

        if ( condition == null ) {
            // Table scan
            final RspList<RemoteExecuteResult> responseList = connection.getCluster().prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, maxRowsInFirstFrame );

            final Map<AbstractRemoteNode, RemoteExecuteResult> remoteResults = new HashMap<>();
            final Map<AbstractRemoteNode, Throwable> throwables = new HashMap<>();
            for ( Entry<Address, Rsp<RemoteExecuteResult>> responseEntry : responseList.entrySet() ) {
                final AbstractRemoteNode node = connection.getCluster().getRemoteNode( responseEntry.getKey() );
                final Rsp<RemoteExecuteResult> response = responseEntry.getValue();
                if ( response.hasException() ) {
                    throwables.put( node, response.getException() );
                } else {
                    remoteResults.put( node, response.getValue() );
                }
            }
            if ( !throwables.isEmpty() ) {
                final RemoteException ex = new RemoteException( "Exception at " + throwables.keySet().toString() + " occurred." );
                for ( Throwable suppressed : throwables.values() ) {
                    ex.addSuppressed( suppressed );
                }
                throw ex;
            }
            return statement.createResultSet( remoteResults, origins -> new HorizontalExecuteResultMergeFunction().apply( statement, origins, maxRowsInFirstFrame ), ( origins, conn, stmt, offset, fetchMaxRowCount ) -> {
                // fetch
                throw new UnsupportedOperationException( "Not implemented yet." );
            } );
        }

        if ( condition.getKind() == SqlKind.EQUALS
                && ((SqlCall) condition).<SqlIdentifier>operand( 0 ).names.reverse().get( 0 ).equalsIgnoreCase( primaryKey )
                && ((SqlCall) condition).<SqlNode>operand( 1 ).getKind() == SqlKind.LITERAL ) {
            // ... WHERE primary_key = ?

            final List<SqlLiteral> primaryKeyValues = new LinkedList<>();
            primaryKeyValues.add( (SqlLiteral) ((SqlCall) condition).<SqlNode>operand( 1 ) );

            final AbstractRemoteNode[] remotes = connection.getCluster().getMembers().toArray( new AbstractRemoteNode[0] );
            final int hash = Objects.hash( primaryKeyValues.toArray() );
            final int winner = Math.abs( hash % remotes.length );
            final AbstractRemoteNode executionTarget = remotes[winner];

            final RspList<RemoteExecuteResult> responseList = connection.getCluster().prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, maxRowsInFirstFrame, executionTarget );

            final Map<AbstractRemoteNode, RemoteExecuteResult> remoteResults = new HashMap<>();
            for ( Entry<Address, Rsp<RemoteExecuteResult>> responseEntry : responseList.entrySet() ) {
                AbstractRemoteNode remoteNode = connection.getCluster().getRemoteNode( responseEntry.getKey() );
                Rsp<RemoteExecuteResult> response = responseEntry.getValue();
                if ( response.hasException() ) {
                    throw new RemoteException( "Exception at " + remoteNode + " occurred.", response.getException() );
                }
                remoteResults.put( remoteNode, response.getValue() );
            }

            return statement.createResultSet( remoteResults, origins -> origins.entrySet().iterator().next().getValue().toExecuteResult(), ( origins, conn, stmt, offset, fetchMaxRowCount ) -> {
                try {
                    return origins.entrySet().iterator().next().getKey().fetch( RemoteStatementHandle.fromStatementHandle( stmt.getStatementHandle() ), offset, fetchMaxRowCount ).toFrame();
                } catch ( RemoteException e ) {
                    throw Utils.wrapException( e );
                }
            } );
        }

        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    private ResultSetInfos prepareAndExecuteDataQuerySelectAggregate( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlSelect sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        LOGGER.trace( "prepareAndExecuteDataQueryAggregate( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {} )", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame );

        // FAIL-FAST
        final SqlNodeList selectList = sql.getSelectList();
        if ( selectList.size() > 1 ) {
            throw new UnsupportedOperationException( "Not implemented yet." );
        }

        // more than one table and especially the required join is currently not supported!
        final SqlIdentifier targeTable = SqlNodeUtils.SELECT_UTILS.getTargetTable( sql );

        final SqlNode asExpressionOrAggregateNode = selectList.get( 0 );
        final SqlNode aggregateNode;
        if ( asExpressionOrAggregateNode.getKind() == SqlKind.AS ) {
            aggregateNode = ((SqlBasicCall) asExpressionOrAggregateNode).operand( 0 );
        } else {
            aggregateNode = asExpressionOrAggregateNode;
        }

        // send to all
        final RspList<RemoteExecuteResult> responseList = connection.getCluster().prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, maxRowsInFirstFrame );
        // handle the responses of the execution
        final Map<AbstractRemoteNode, RemoteExecuteResult> remoteResults = ClusterUtils.getRemoteResults( connection.getCluster(), responseList );

        switch ( aggregateNode.getKind() ) {
            case MAX:
                /*
                    YCSB:
                        SELECT MAX(`USERTABLE`.`YCSB_KEY`)
                        FROM `PUBLIC`.`USERTABLE` AS `USERTABLE`
                 */
                return statement.createResultSet( remoteResults,
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


    @Override
    public ResultSetInfos prepareAndExecuteDataQuery( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, int[] columnIndexes, PrepareCallback callback ) throws RemoteException {
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    /*
     *
     */


    @Override
    public StatementInfos prepareDataManipulation( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {
        LOGGER.trace( "prepareDataManipulation( connection: {}, statement: {}, sql: {}, maxRowCount: {} )", connection, statement, sql, maxRowCount );

        switch ( sql.getKind() ) {
            // See org.apache.calcite.sql.SqlKind.DML
            case INSERT:
                return prepareDataManipulationInsert( connection, statement, (SqlInsert) sql, maxRowCount );

            case DELETE:
                return prepareDataManipulationDelete( connection, statement, (SqlDelete) sql, maxRowCount );

            case UPDATE:
                return prepareDataManipulationUpdate( connection, statement, (SqlUpdate) sql, maxRowCount );

            case MERGE:
            case PROCEDURE_CALL:
            default:
                throw new UnsupportedOperationException( "Not supported" );
        }
    }


    protected StatementInfos prepareDataManipulationInsert( ConnectionInfos connection, StatementInfos statement, SqlInsert sql, long maxRowCount ) throws RemoteException {
        LOGGER.trace( "prepareDataManipulationInsert( connection: {}, statement: {}, sql: {}, maxRowCount: {} )", connection, statement, sql, maxRowCount );

        final SqlIdentifier targetTable = SqlNodeUtils.INSERT_UTILS.getTargetTable( sql );

        final String catalogName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 2 );
        final String schemaName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 1 );
        final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 0 );
        final Map<String, Integer> primaryKeyColumnNamesAndIndexes = CatalogUtils.lookupPrimaryKeyColumnNamesAndIndexes( connection, catalogName, schemaName, tableName );

        final SortedMap<Integer, Integer> primaryKeyColumnsIndexesToParametersIndexes = SqlNodeUtils.INSERT_UTILS.getPrimaryKeyColumnsIndexesToParametersIndexesMap( sql, primaryKeyColumnNamesAndIndexes );

        final boolean executeOnAllNodes = false;

        // inserts have to be PREPARED on all nodes
        final RspList<RemoteStatementHandle> prepareResponseList = connection.getCluster().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount );
        // handle the responses of the PREPARE execution
        final Map<AbstractRemoteNode, RemoteStatementHandle> prepareRemoteResults = ClusterUtils.getRemoteResults( connection.getCluster(), prepareResponseList );

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
                    final Map<AbstractRemoteNode, RemoteExecuteResult> _executeRemoteResults;
                    try {
                        // EXECUTE the insert statement
                        final RspList<RemoteExecuteResult> _executeResponseList = _connection.getCluster().execute( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _parameterValues, -1/*_maxRowsInFirstFrame*/,
                                executeOnAllNodes ? _connection.getCluster().getMembers() :
                                        HorizontalHashFragmentation.this.getExecutionTargetsFunction.apply(
                                                _connection.getCluster().getMembers(),
                                                _parameterValues,
                                                primaryKeyColumnsIndexesToParametersIndexes
                                        )
                        );

                        // handle the EXECUTE responses
                        _executeRemoteResults = ClusterUtils.getRemoteResults( _connection.getCluster(), _executeResponseList );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    for ( AbstractRemoteNode _node : _executeRemoteResults.keySet() ) {
                        _connection.addAccessedNode( _node, RemoteConnectionHandle.fromConnectionHandle( _connection.getConnectionHandle() ) );
                    }

                    return _statement.createResultSet( _executeRemoteResults,
                            /* mergeResults */ ( origins ) -> {
                                // todo: replace with a proper merge of the results
                                return new HorizontalExecuteResultMergeFunction().apply( _statement, origins, _maxRowsInFirstFrame );
                            },
                            /* fetch */ ( origins, conn, stmt, offset, fetchMaxRowCount ) -> {
                                throw new UnsupportedOperationException( "Not implemented yet." );
                            } );
                },
                //
                /* executeBatch */ ( _connection, _transaction, _statement, _listOfUpdateBatches ) -> {
                    //
                    final List<AbstractRemoteNode> _availableRemotes = _connection.getCluster().getMembers();
                    final Map<Integer, AbstractRemoteNode> _mapToMergeTheUpdateCounts = new HashMap<>();
                    final Map<AbstractRemoteNode, RemoteExecuteBatchResult> _remoteBatchResults;

                    if ( executeOnAllNodes ) {
                        for ( ListIterator<UpdateBatch> _updateBatchIterator = _listOfUpdateBatches.listIterator(); _updateBatchIterator.hasNext(); _updateBatchIterator.next() ) {
                            _mapToMergeTheUpdateCounts.put( _updateBatchIterator.nextIndex(), _availableRemotes.get( 0 ) );
                        }

                        try {
                            // EXECUTE the insert statements
                            final RspList<RemoteExecuteBatchResult> _executeBatchResponseList = _connection.getCluster().executeBatch( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _listOfUpdateBatches, _connection.getCluster().getMembers() );

                            // handle the EXECUTE responses
                            _remoteBatchResults = ClusterUtils.getRemoteResults( _connection.getCluster(), _executeBatchResponseList );
                        } catch ( RemoteException ex ) {
                            throw Utils.wrapException( ex );
                        }
                    } else {
                        final Map<AbstractRemoteNode, List<UpdateBatch>> _parameterValuesForRemoteNode = new HashMap<>();

                        for ( ListIterator<UpdateBatch> _updateBatchIterator = _listOfUpdateBatches.listIterator(); _updateBatchIterator.hasNext(); ) {
                            final UpdateBatch ub = _updateBatchIterator.next();
                            final List<TypedValue> _parameterValues = ub.getParameterValuesList();

                            for ( AbstractRemoteNode executionTarget :
                                    HorizontalHashFragmentation.this.getExecutionTargetsFunction.apply( _availableRemotes, _parameterValues, primaryKeyColumnsIndexesToParametersIndexes )
                            ) {
                                final List<UpdateBatch> pVs = _parameterValuesForRemoteNode.getOrDefault( executionTarget, new LinkedList<>() );
                                pVs.add( ub );
                                _parameterValuesForRemoteNode.put( executionTarget, pVs );
                                _mapToMergeTheUpdateCounts.put( _updateBatchIterator.previousIndex(), executionTarget ); // Here we have then the last of the list providing the updateCount. For now acceptable.
                            }
                        }

                        final RspList<RemoteExecuteBatchResult> _responseList = new RspList<>();
                        _parameterValuesForRemoteNode.entrySet().parallelStream().forEach( target -> {
                            try {
                                final RemoteExecuteBatchResult response = target.getKey().executeBatch( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), target.getValue() );
                                _responseList.addRsp( target.getKey().getNodeAddress(), response );
                            } catch ( RemoteException e ) {
                                _responseList.put( target.getKey().getNodeAddress(), new Rsp<>( e ) );
                            }
                        } );

                        try {
                            _remoteBatchResults = ClusterUtils.getRemoteResults( _connection.getCluster(), _responseList );
                        } catch ( RemoteException ex ) {
                            throw Utils.wrapException( ex );
                        }
                    }

                    for ( AbstractRemoteNode _node : _remoteBatchResults.keySet() ) {
                        _connection.addAccessedNode( _node, RemoteConnectionHandle.fromConnectionHandle( _connection.getConnectionHandle() ) );
                    }

                    return statement.createBatchResultSet( _remoteBatchResults,
                            /* mergeResults */ ( __origins ) -> {
                                final Map<AbstractRemoteNode, Integer> __originUpdateCountIndexMap = new HashMap<>();

                                final long[] __updateCounts = new long[_mapToMergeTheUpdateCounts.keySet().size()];
                                for ( int updateCountIndex = 0; updateCountIndex < __updateCounts.length; ++updateCountIndex ) {
                                    final AbstractRemoteNode __origin = _mapToMergeTheUpdateCounts.get( updateCountIndex );
                                    final int __originUpdateCountIndex = __originUpdateCountIndexMap.getOrDefault( __origin, 0 );

                                    __updateCounts[updateCountIndex] = __origins.get( __origin ).toExecuteBatchResult().updateCounts[__originUpdateCountIndex];

                                    __originUpdateCountIndexMap.put( __origin, __originUpdateCountIndex + 1 );
                                }

                                return new ExecuteBatchResult( __updateCounts );
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

        final SqlIdentifier targetTable = SqlNodeUtils.DELETE_UTILS.getTargetTable( sql );

        // do now most of the work required for later execution
        final String catalogName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 2 );
        final String schemaName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 1 );
        final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 0 );
        final Map<String, Integer> primaryKeyColumnsNamesAndIndexes = CatalogUtils.lookupPrimaryKeyColumnNamesAndIndexes( connection, catalogName, schemaName, tableName );

        final SortedMap<Integer, Integer> primaryKeyColumnsIndexesToParametersIndexes = new TreeMap<>();
        final Map<String, Integer> columnNamesToParameterIndexes = SqlNodeUtils.getColumnsNameToDynamicParametersIndexMap( sql );

        boolean allPrimaryKeyColumnsAreInTheCondition = true;
        for ( Entry<String, Integer> primaryKeyColumnNameToIndex : primaryKeyColumnsNamesAndIndexes.entrySet() ) {
            // for every primary key and its index in the table, check if it is included in the condition
            final Integer parameterIndex = columnNamesToParameterIndexes.get( primaryKeyColumnNameToIndex.getKey() );
            if ( parameterIndex != null ) {
                // the primary key is present in the condition
                primaryKeyColumnsIndexesToParametersIndexes.put( primaryKeyColumnNameToIndex.getValue(), parameterIndex );
            } else {
                // the primary key is NOT in the condition
                // Thus, during execution, the statement has to be executed on all nodes
                allPrimaryKeyColumnsAreInTheCondition = false;
            }
        }

        final boolean executeOnAllNodes;
        if ( allPrimaryKeyColumnsAreInTheCondition && SqlNodeUtils.DELETE_UTILS.whereConditionContainsOnlyEquals( sql ) ) {
            executeOnAllNodes = false;
        } else {
            executeOnAllNodes = true;
        }

        // deletes have to be PREPARED on ALL nodes
        final RspList<RemoteStatementHandle> prepareResponseList = connection.getCluster().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount );
        // handle the responses of the PREPARE execution
        final Map<AbstractRemoteNode, RemoteStatementHandle> prepareRemoteResults = ClusterUtils.getRemoteResults( connection.getCluster(), prepareResponseList );

        return connection.createPreparedStatement( statement, prepareRemoteResults,
                //
                /* signatureMerge */ ( _remoteStatements ) -> {
                    // BEGIN HACK
                    return _remoteStatements.values().iterator().next().toStatementHandle().signature;
                    // END HACK
                },
                //
                /* execute */ ( _connection, _transaction, _statement, _parameterValues, _maxRowsInFirstFrame ) -> {
                    //
                    final Map<AbstractRemoteNode, RemoteExecuteResult> _executeRemoteResults;
                    try {
                        // EXECUTE the insert statement
                        final RspList<RemoteExecuteResult> _executeResponseList = _connection.getCluster().execute( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _parameterValues, -1/*_maxRowsInFirstFrame*/,
                                executeOnAllNodes ? _connection.getCluster().getMembers() :
                                        HorizontalHashFragmentation.this.getExecutionTargetsFunction.apply(
                                                _connection.getCluster().getMembers(),
                                                _parameterValues,
                                                primaryKeyColumnsIndexesToParametersIndexes
                                        )
                        );

                        // handle the EXECUTE responses
                        _executeRemoteResults = ClusterUtils.getRemoteResults( _connection.getCluster(), _executeResponseList );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    for ( AbstractRemoteNode _node : _executeRemoteResults.keySet() ) {
                        _connection.addAccessedNode( _node, RemoteConnectionHandle.fromConnectionHandle( _connection.getConnectionHandle() ) );
                    }

                    return _statement.createResultSet( _executeRemoteResults,
                            /* mergeResults */ ( __origins ) -> {
                                // todo: replace with a proper merge of the results
                                return new HorizontalExecuteResultMergeFunction().apply( _statement, __origins, _maxRowsInFirstFrame );
                            },
                            /* fetch */ ( __origins, __connection, __statement, __offset, __fetchMaxRowCount ) -> {
                                throw new UnsupportedOperationException( "Not implemented yet." );
                            } );
                },
                //
                /* executeBatch */ ( _connection, _transaction, _statement, _listOfUpdateBatches ) -> {
                    //
                    final List<AbstractRemoteNode> _availableRemotes = _connection.getCluster().getMembers();
                    final Map<Integer, AbstractRemoteNode> _mapToMergeTheUpdateCounts = new HashMap<>();
                    final Map<AbstractRemoteNode, RemoteExecuteBatchResult> _remoteBatchResults;

                    if ( executeOnAllNodes ) {
                        for ( ListIterator<UpdateBatch> _updateBatchIterator = _listOfUpdateBatches.listIterator(); _updateBatchIterator.hasNext(); _updateBatchIterator.next() ) {
                            _mapToMergeTheUpdateCounts.put( _updateBatchIterator.nextIndex(), _availableRemotes.get( 0 ) );
                        }

                        try {
                            // EXECUTE the delete statements
                            final RspList<RemoteExecuteBatchResult> _executeBatchResponseList = _connection.getCluster().executeBatch( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _listOfUpdateBatches, _connection.getCluster().getMembers() );

                            // handle the EXECUTE responses
                            _remoteBatchResults = ClusterUtils.getRemoteResults( _connection.getCluster(), _executeBatchResponseList );
                        } catch ( RemoteException ex ) {
                            throw Utils.wrapException( ex );
                        }
                    } else {
                        // we need to find out what to send where
                        final Map<AbstractRemoteNode, List<UpdateBatch>> _parameterValuesForRemoteNode = new HashMap<>();

                        for ( ListIterator<UpdateBatch> _updateBatchIterator = _listOfUpdateBatches.listIterator(); _updateBatchIterator.hasNext(); ) {
                            final UpdateBatch ub = _updateBatchIterator.next();
                            final List<TypedValue> _parameterValues = ub.getParameterValuesList();

                            for ( AbstractRemoteNode executionTarget :
                                    HorizontalHashFragmentation.this.getExecutionTargetsFunction.apply( _availableRemotes, _parameterValues, primaryKeyColumnsIndexesToParametersIndexes )
                            ) {
                                final List<UpdateBatch> pVs = _parameterValuesForRemoteNode.getOrDefault( executionTarget, new LinkedList<>() );
                                pVs.add( ub );
                                _parameterValuesForRemoteNode.put( executionTarget, pVs );
                                _mapToMergeTheUpdateCounts.put( _updateBatchIterator.previousIndex(), executionTarget ); // Here we have then the last of the list providing the updateCount. For now acceptable.
                            }
                        }

                        final RspList<RemoteExecuteBatchResult> _responseList = new RspList<>();
                        _parameterValuesForRemoteNode.entrySet().parallelStream().forEach( target -> {
                            try {
                                final RemoteExecuteBatchResult response = target.getKey().executeBatch( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), target.getValue() );
                                _responseList.addRsp( target.getKey().getNodeAddress(), response );
                            } catch ( RemoteException e ) {
                                _responseList.put( target.getKey().getNodeAddress(), new Rsp<>( e ) );
                            }
                        } );

                        try {
                            _remoteBatchResults = ClusterUtils.getRemoteResults( _connection.getCluster(), _responseList );
                        } catch ( RemoteException ex ) {
                            throw Utils.wrapException( ex );
                        }
                    }

                    for ( AbstractRemoteNode _node : _remoteBatchResults.keySet() ) {
                        _connection.addAccessedNode( _node, RemoteConnectionHandle.fromConnectionHandle( _connection.getConnectionHandle() ) );
                    }

                    return statement.createBatchResultSet( _remoteBatchResults,
                            /* mergeResults */ ( __origins ) -> {
                                final Map<AbstractRemoteNode, Integer> __originUpdateCountIndexMap = new HashMap<>();

                                final long[] __updateCounts = new long[_mapToMergeTheUpdateCounts.keySet().size()];
                                for ( int updateCountIndex = 0; updateCountIndex < __updateCounts.length; ++updateCountIndex ) {
                                    final AbstractRemoteNode __origin = _mapToMergeTheUpdateCounts.get( updateCountIndex );
                                    final int __originUpdateCountIndex = __originUpdateCountIndexMap.getOrDefault( __origin, 0 );

                                    __updateCounts[updateCountIndex] = __origins.get( __origin ).toExecuteBatchResult().updateCounts[__originUpdateCountIndex];

                                    __originUpdateCountIndexMap.put( __origin, __originUpdateCountIndex + 1 );
                                }

                                return new ExecuteBatchResult( __updateCounts );
                            } );
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

        // do now most of the work required for later execution
        final String catalogName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 2 );
        final String schemaName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 1 );
        final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 0 );
        final Map<String, Integer> primaryKeyColumnsNamesAndIndexes = CatalogUtils.lookupPrimaryKeyColumnNamesAndIndexes( connection, catalogName, schemaName, tableName );

        if ( SqlNodeUtils.UPDATE_UTILS.targetColumnsContainPrimaryKeyColumn( sql, primaryKeyColumnsNamesAndIndexes.keySet() ) ) {
            // At least one primary key column is in the targetColumns list and will be updated
            throw new UnsupportedOperationException( "Not implemented yet." );
        }

        final Map<String, Integer> columnNamesToParameterIndexes = SqlNodeUtils.getColumnsNameToDynamicParametersIndexMap( sql );
        final SortedMap<Integer, Integer> primaryKeyColumnsIndexesToParametersIndexes = new TreeMap<>();

        boolean allPrimaryKeyColumnsAreInTheCondition = true;
        for ( Entry<String, Integer> primaryKeyColumnNameAndIndex : primaryKeyColumnsNamesAndIndexes.entrySet() ) {
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

        final boolean executeOnAllNodes;
        if ( allPrimaryKeyColumnsAreInTheCondition && SqlNodeUtils.UPDATE_UTILS.whereConditionContainsOnlyEquals( sql ) ) {
            executeOnAllNodes = false;
        } else {
            executeOnAllNodes = true;
        }

        // updates have to be PREPARED on ALL nodes
        final RspList<RemoteStatementHandle> prepareResponseList = connection.getCluster().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount );
        // handle the responses of the PREPARE execution
        final Map<AbstractRemoteNode, RemoteStatementHandle> prepareRemoteResults = ClusterUtils.getRemoteResults( connection.getCluster(), prepareResponseList );

        return connection.createPreparedStatement( statement, prepareRemoteResults,
                //
                /* signatureMerge */ ( _remoteStatements ) -> {
                    // BEGIN HACK
                    return _remoteStatements.values().iterator().next().toStatementHandle().signature;
                    // END HACK
                },
                //
                /* execute */ ( _connection, _transaction, _statement, _parameterValues, _maxRowsInFirstFrame ) -> {
                    //
                    final Map<AbstractRemoteNode, RemoteExecuteResult> _executeRemoteResults;
                    try {
                        // EXECUTE the update statement
                        final RspList<RemoteExecuteResult> _executeResponseList = _connection.getCluster().execute( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _parameterValues, -1/*_maxRowsInFirstFrame*/,
                                executeOnAllNodes ? _connection.getCluster().getMembers() :
                                        HorizontalHashFragmentation.this.getExecutionTargetsFunction.apply(
                                                _connection.getCluster().getMembers(),
                                                _parameterValues,
                                                primaryKeyColumnsIndexesToParametersIndexes
                                        )
                        );

                        // handle the responses
                        _executeRemoteResults = ClusterUtils.getRemoteResults( _connection.getCluster(), _executeResponseList );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    for ( AbstractRemoteNode _node : _executeRemoteResults.keySet() ) {
                        _connection.addAccessedNode( _node, RemoteConnectionHandle.fromConnectionHandle( _connection.getConnectionHandle() ) );
                    }

                    return _statement.createResultSet( _executeRemoteResults,
                            /* mergeResults */ ( __origins ) -> {
                                //
                                // todo: replace with a proper merge of the results
                                return new HorizontalExecuteResultMergeFunction().apply( _statement, __origins, _maxRowsInFirstFrame );
                            },
                            /* fetch */ ( __origins, __connection, __statement, __offset, __fetchMaxRowCount ) -> {
                                throw new UnsupportedOperationException( "Not implemented yet." );
                            } );
                },
                //
                /* executeBatch */ ( _connection, _transaction, _statement, _listOfUpdateBatches ) -> {
                    //
                    final List<AbstractRemoteNode> _availableRemotes = _connection.getCluster().getMembers();
                    final Map<Integer, AbstractRemoteNode> _mapToMergeTheUpdateCounts = new HashMap<>();
                    final Map<AbstractRemoteNode, RemoteExecuteBatchResult> _remoteBatchResults;

                    if ( executeOnAllNodes ) {
                        for ( ListIterator<UpdateBatch> _updateBatchIterator = _listOfUpdateBatches.listIterator(); _updateBatchIterator.hasNext(); _updateBatchIterator.next() ) {
                            _mapToMergeTheUpdateCounts.put( _updateBatchIterator.nextIndex(), _availableRemotes.get( 0 ) );
                        }

                        try {
                            // EXECUTE the update statements
                            final RspList<RemoteExecuteBatchResult> _executeBatchResponseList = _connection.getCluster().executeBatch( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _listOfUpdateBatches, _connection.getCluster().getMembers() );

                            // handle the EXECUTE responses
                            _remoteBatchResults = ClusterUtils.getRemoteResults( _connection.getCluster(), _executeBatchResponseList );
                        } catch ( RemoteException ex ) {
                            throw Utils.wrapException( ex );
                        }
                    } else {
                        // we need to find out what to send where
                        final Map<AbstractRemoteNode, List<UpdateBatch>> _parameterValuesForRemoteNode = new HashMap<>();

                        for ( ListIterator<UpdateBatch> _updateBatchIterator = _listOfUpdateBatches.listIterator(); _updateBatchIterator.hasNext(); ) {
                            final UpdateBatch ub = _updateBatchIterator.next();
                            final List<TypedValue> _parameterValues = ub.getParameterValuesList();

                            for ( AbstractRemoteNode executionTarget :
                                    HorizontalHashFragmentation.this.getExecutionTargetsFunction.apply( _availableRemotes, _parameterValues, primaryKeyColumnsIndexesToParametersIndexes )
                            ) {
                                final List<UpdateBatch> pVs = _parameterValuesForRemoteNode.getOrDefault( executionTarget, new LinkedList<>() );
                                pVs.add( ub );
                                _parameterValuesForRemoteNode.put( executionTarget, pVs );
                                _mapToMergeTheUpdateCounts.put( _updateBatchIterator.previousIndex(), executionTarget ); // Here we have then the last of the list providing the updateCount. For now acceptable.
                            }
                        }

                        final RspList<RemoteExecuteBatchResult> _responseList = new RspList<>();
                        _parameterValuesForRemoteNode.entrySet().parallelStream().forEach( target -> {
                            try {
                                final RemoteExecuteBatchResult response = target.getKey().executeBatch( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), target.getValue() );
                                _responseList.addRsp( target.getKey().getNodeAddress(), response );
                            } catch ( RemoteException e ) {
                                _responseList.put( target.getKey().getNodeAddress(), new Rsp<>( e ) );
                            }
                        } );

                        try {
                            _remoteBatchResults = ClusterUtils.getRemoteResults( _connection.getCluster(), _responseList );
                        } catch ( RemoteException ex ) {
                            throw Utils.wrapException( ex );
                        }
                    }

                    for ( AbstractRemoteNode _node : _remoteBatchResults.keySet() ) {
                        _connection.addAccessedNode( _node, RemoteConnectionHandle.fromConnectionHandle( _connection.getConnectionHandle() ) );
                    }

                    return statement.createBatchResultSet( _remoteBatchResults,
                            /* mergeResults */ ( __origins ) -> {
                                final Map<AbstractRemoteNode, Integer> __originUpdateCountIndexMap = new HashMap<>();

                                final long[] __updateCounts = new long[_mapToMergeTheUpdateCounts.keySet().size()];
                                for ( int updateCountIndex = 0; updateCountIndex < __updateCounts.length; ++updateCountIndex ) {
                                    final AbstractRemoteNode __origin = _mapToMergeTheUpdateCounts.get( updateCountIndex );
                                    final int __originUpdateCountIndex = __originUpdateCountIndexMap.getOrDefault( __origin, 0 );

                                    __updateCounts[updateCountIndex] = __origins.get( __origin ).toExecuteBatchResult().updateCounts[__originUpdateCountIndex];

                                    __originUpdateCountIndexMap.put( __origin, __originUpdateCountIndex + 1 );
                                }

                                return new ExecuteBatchResult( __updateCounts );
                            } );
                }
        );
    }


    @Override
    public StatementInfos prepareDataManipulation( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount, int[] columnIndexes ) throws RemoteException {
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


    protected StatementInfos prepareDataQuerySelect( ConnectionInfos connection, StatementInfos statement, SqlSelect sql, long maxRowCount ) throws RemoteException {
        LOGGER.trace( "prepareDataQuerySelect( connection: {}, statement: {}, sql: {}, maxRowCount: {} )", connection, statement, sql, maxRowCount );

        if ( SqlNodeUtils.SELECT_UTILS.selectListContainsAggregate( sql ) ) {
            // SELECT MIN/MAX/AVG/ FROM ...
            return prepareDataQuerySelectAggregate( connection, statement, sql, maxRowCount );
        }

        final SqlIdentifier targetTable = SqlNodeUtils.SELECT_UTILS.getTargetTable( sql );

        // do now most of the work required for later execution
        final String catalogName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 2 );
        final String schemaName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 1 );
        final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 0 );
        final Map<String, Integer> primaryKeyColumnsNamesAndIndexes = CatalogUtils.lookupPrimaryKeyColumnNamesAndIndexes( connection, catalogName, schemaName, tableName );

        final Map<String, Integer> columnNamesToParameterIndexes = SqlNodeUtils.getColumnsNameToDynamicParametersIndexMap( sql );
        final SortedMap<Integer, Integer> primaryKeyColumnsIndexesToParametersIndexes = new TreeMap<>();

        boolean completePrimaryKey = true;
        for ( Entry<String, Integer> primaryKeyColumnNameAndIndex : primaryKeyColumnsNamesAndIndexes.entrySet() ) {
            // for every primary key and its index in the table
            final Integer parameterIndex = columnNamesToParameterIndexes.get( primaryKeyColumnNameAndIndex.getKey() );
            if ( parameterIndex != null ) {
                // the primary key is present in the condition
                primaryKeyColumnsIndexesToParametersIndexes.put( primaryKeyColumnNameAndIndex.getValue(), parameterIndex );
            } else {
                // the primary key is NOT in the condition
                completePrimaryKey = false;
            }
        }

        final boolean executeOnAllNodes;
        if ( completePrimaryKey && SqlNodeUtils.SELECT_UTILS.whereConditionContainsOnlyEquals( sql ) ) {
            executeOnAllNodes = false;
        } else {
            // we would need to send the SELECT to all nodes and then properly merge the result set
            executeOnAllNodes = true;
        }

        // selects might have to be PREPARED on ALL nodes
        final RspList<RemoteStatementHandle> prepareResponseList = connection.getCluster().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount );
        // handle the responses of the PREPARE execution
        final Map<AbstractRemoteNode, RemoteStatementHandle> prepareRemoteResults = ClusterUtils.getRemoteResults( connection.getCluster(), prepareResponseList );

        return connection.createPreparedStatement( statement, prepareRemoteResults,
                //
                /* signatureMerge */ ( _remoteStatements ) -> {
                    // BEGIN HACK
                    return _remoteStatements.values().iterator().next().toStatementHandle().signature;
                    // END HACK
                },
                //
                /* execute */ ( _connection, _transaction, _statement, _parameterValues, _maxRowsInFirstFrame ) -> {
                    //
                    final Map<AbstractRemoteNode, RemoteExecuteResult> _executeRemoteResults;
                    try {
                        // EXECUTE the select statement
                        final RspList<RemoteExecuteResult> _responseList = _connection.getCluster().execute( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _parameterValues, -1/*_maxRowsInFirstFrame*/,
                                executeOnAllNodes ? _connection.getCluster().getMembers() :
                                        HorizontalHashFragmentation.this.getExecutionTargetsFunction.apply(
                                                _connection.getCluster().getMembers(),
                                                _parameterValues,
                                                primaryKeyColumnsIndexesToParametersIndexes
                                        )
                        );

                        // handle the responses
                        _executeRemoteResults = ClusterUtils.getRemoteResults( _connection.getCluster(), _responseList );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    for ( AbstractRemoteNode _node : _executeRemoteResults.keySet() ) {
                        _connection.addAccessedNode( _node, RemoteConnectionHandle.fromConnectionHandle( _connection.getConnectionHandle() ) );
                    }

                    return _statement.createResultSet( _executeRemoteResults,
                            /* mergeResults */ ( __origins ) -> {
                                //
                                // todo: replace with a proper merge of the results
                                return new HorizontalExecuteResultMergeFunction().apply( _statement, __origins, _maxRowsInFirstFrame );
                            },
                            /* fetch */ ( origins, conn, stmt, offset, fetchMaxRowCount ) -> {
                                throw new UnsupportedOperationException( "Not implemented yet." );
                            } );
                },
                //
                /* executeBatch */ ( _connection, _transaction, _statement, _listOfUpdateBatches ) -> {
                    //
                    throw Utils.wrapException( new SQLException( "SELECT statements cannot be executed in a batch context." ) );
                } );
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

        final SqlNode asExpressionOrAggregateNode = selectList.get( 0 );
        final SqlNode aggregateNode;
        if ( asExpressionOrAggregateNode.getKind() == SqlKind.AS ) {
            aggregateNode = ((SqlBasicCall) asExpressionOrAggregateNode).operand( 0 );
        } else {
            aggregateNode = asExpressionOrAggregateNode;
        }

        final SqlIdentifier targetTable = SqlNodeUtils.SELECT_UTILS.getTargetTable( sql );

        // do now most of the work required for later execution
        final String catalogName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 2 );
        final String schemaName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 1 );
        final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 0 );
        final Map<String, Integer> primaryKeyColumnsNamesAndIndexes = CatalogUtils.lookupPrimaryKeyColumnNamesAndIndexes( connection, catalogName, schemaName, tableName );

        final Map<String, Integer> columnNamesToParameterIndexes = SqlNodeUtils.getColumnsNameToDynamicParametersIndexMap( sql );
        final SortedMap<Integer, Integer> primaryKeyColumnsIndexesToParametersIndexes = new TreeMap<>();

        boolean completePrimaryKey = true;
        for ( Entry<String, Integer> primaryKeyColumnNameAndIndex : primaryKeyColumnsNamesAndIndexes.entrySet() ) {
            // for every primary key and its index in the table
            final Integer parameterIndex = columnNamesToParameterIndexes.get( primaryKeyColumnNameAndIndex.getKey() );
            if ( parameterIndex != null ) {
                // the primary key is present in the condition
                primaryKeyColumnsIndexesToParametersIndexes.put( primaryKeyColumnNameAndIndex.getValue(), parameterIndex );
            } else {
                // the primary key is NOT in the condition
                completePrimaryKey = false;
            }
        }

        final boolean executeOnAllNodes;
        // If we know more about the condition we could limit the number of nodes being required to execute the query.
        if ( completePrimaryKey && SqlNodeUtils.SELECT_UTILS.whereConditionContainsOnlyEquals( sql ) ) {
            executeOnAllNodes = false;
        } else {
            // incomplete primary key or condition contains ranges
            executeOnAllNodes = true;
        }

        // selects (might) have to be PREPARED on ALL nodes
        final RspList<RemoteStatementHandle> prepareResponseList = connection.getCluster().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount );
        // handle the responses of the PREPARE execution
        final Map<AbstractRemoteNode, RemoteStatementHandle> prepareRemoteResults = ClusterUtils.getRemoteResults( connection.getCluster(), prepareResponseList );

        return connection.createPreparedStatement( statement, prepareRemoteResults,
                //
                /* signatureMerge */ ( _remoteStatements ) -> {
                    // BEGIN HACK
                    return _remoteStatements.values().iterator().next().toStatementHandle().signature;
                    // END HACK
                },
                //
                /* execute */ ( _connection, _transaction, _statement, _parameterValues, _maxRowsInFirstFrame ) -> {
                    //
                    final Map<AbstractRemoteNode, RemoteExecuteResult> _executeRemoteResults;
                    try {
                        // EXECUTE the select statement
                        final RspList<RemoteExecuteResult> _responseList = _connection.getCluster().execute( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _parameterValues, -1/*_maxRowsInFirstFrame*/,
                                executeOnAllNodes ? _connection.getCluster().getMembers() :
                                        HorizontalHashFragmentation.this.getExecutionTargetsFunction.apply(
                                                _connection.getCluster().getMembers(),
                                                _parameterValues,
                                                primaryKeyColumnsIndexesToParametersIndexes
                                        )
                        );

                        // handle the responses
                        _executeRemoteResults = ClusterUtils.getRemoteResults( _connection.getCluster(), _responseList );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    for ( AbstractRemoteNode _node : _executeRemoteResults.keySet() ) {
                        _connection.addAccessedNode( _node, RemoteConnectionHandle.fromConnectionHandle( _connection.getConnectionHandle() ) );
                    }

                    return _statement.createResultSet( _executeRemoteResults,
                            /* mergeResults */ ( __origins ) -> {
                                //
                                final Object aggregatedValue;
                                switch ( aggregateNode.getKind() ) {
                                    case MIN:
                                        int minValue = Integer.MAX_VALUE;
                                        for ( RemoteExecuteResult rex : __origins.values() ) {
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
                                        aggregatedValue = minValue;
                                        break;

                                    case SUM:
                                        BigDecimal sumValue = BigDecimal.ZERO;
                                        for ( RemoteExecuteResult rex : __origins.values() ) {
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
                                        aggregatedValue = sumValue;
                                        break;

                                    default:
                                        throw new UnsupportedOperationException( "Not implemented yet." );
                                }
                                Signature signature = __origins.values().iterator().next().toExecuteResult().resultSets.iterator().next().signature;
                                Frame frame = Frame.create( 0, true, Collections.singletonList( new Object[]{ aggregatedValue } ) );
                                return new Meta.ExecuteResult( Collections.singletonList( MetaResultSet.create( statement.getStatementHandle().connectionId, statement.getStatementHandle().id, false, signature, frame ) ) );
                            },
                            /* fetch */ ( origins, conn, stmt, offset, fetchMaxRowCount ) -> {
                                throw new UnsupportedOperationException( "Not implemented yet." );
                            } );
                },
                //
                /* executeBatch */ ( _connection, _transaction, _statement, _listOfUpdateBatches ) -> {
                    //
                    throw Utils.wrapException( new SQLException( "SELECT statements cannot be executed in a batch context." ) );
                } );
    }


    @Override
    public StatementInfos prepareDataQuery( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount, int[] columnIndexes ) throws RemoteException {
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


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
    public ResultSetInfos executeBatch( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<UpdateBatch> listOfParameterValues ) throws NoSuchStatementException, RemoteException {
        if ( !(statement instanceof PreparedStatementInfos) ) {
            throw new IllegalArgumentException( "The provided statement is not a PreparedStatement." );
        }

        // only DML

        try {
            return ((PreparedStatementInfos) statement).executeBatch( connection, transaction, statement, listOfParameterValues );
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
    public Iterable<Serializable> createIterable( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, QueryState state, Signature signature, List<TypedValue> parameterValues, Frame firstFrame ) throws RemoteException {
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    @Override
    public boolean syncResults( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, QueryState state, long offset ) throws RemoteException {
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    /*
     *
     */


    @Override
    public ReplicationProtocol setReplicationProtocol( ReplicationProtocol replicationProtocol ) {
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    public static class HorizontalExecuteResultMergeFunction implements Function3<StatementInfos, Map<AbstractRemoteNode, RemoteExecuteResult>, Integer, ExecuteResult> {

        @Override
        public ExecuteResult apply( StatementInfos statement, Map<AbstractRemoteNode, RemoteExecuteResult> origins, Integer maxRowsInFirstFrame ) {

            // results merge
            boolean done = true;
            List<Long> updateCounts = new LinkedList<>();
            List<Iterator<Object>> iterators = new LinkedList<>();
            for ( RemoteExecuteResult rex : origins.values() ) {
                if ( rex.toExecuteResult().resultSets.isEmpty() ) {
                    throw new InternalError( "The result set list of " + rex.getOrigin() + " is empty." );
                }
                MetaResultSet executionResult = rex.toExecuteResult().resultSets.get( 0 );
                {
                    if ( executionResult.updateCount > -1L ) {
                        updateCounts.add( executionResult.updateCount );
                    } else {
                        done &= executionResult.firstFrame.done;
                        iterators.add( executionResult.firstFrame.rows.iterator() );
                    }
                }
            }

            if ( !updateCounts.isEmpty() && !iterators.isEmpty() ) {
                throw new IllegalStateException( "Mixed update counts with actual results." );
            }

            if ( updateCounts.isEmpty() ) {
                // Merge frames
                List<Object> rows = new LinkedList<>();
                boolean _continue;
                do {
                    _continue = false;
                    for ( Iterator<Object> iterator : iterators ) {
                        if ( iterator.hasNext() ) {
                            rows.add( iterator.next() );
                            _continue = true;
                        }
                    }
                } while ( _continue );

                if ( !done ) {
                    LOGGER.trace( "The merge of the frames did not finish." );
                }

                return new Meta.ExecuteResult( Collections.singletonList(
                        MetaResultSet.create( statement.getStatementHandle().connectionId, statement.getStatementHandle().id, false, statement.getStatementHandle().signature, Frame.create( 0, done, rows ) )
                ) );
            } else {
                // Merge update counts
                long mergedUpdateCount = 0;
                for ( long updateCount : updateCounts ) {
                    mergedUpdateCount += updateCount;
                }
                return new Meta.ExecuteResult( Collections.singletonList(
                        MetaResultSet.count( statement.getStatementHandle().connectionId, statement.getStatementHandle().id, mergedUpdateCount )
                ) );
            }
        }
    }
}
