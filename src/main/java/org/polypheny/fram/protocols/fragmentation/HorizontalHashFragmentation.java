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
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.calcite.avatica.Meta.ConnectionHandle;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.avatica.Meta.StatementHandle;
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
import org.polypheny.fram.Node;
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
import org.polypheny.fram.standalone.parser.sql.SqlNodeUtils;


public class HorizontalHashFragmentation extends AbstractProtocol implements FragmentationProtocol {


    static final Function2<Object[], Integer, Integer> hashFunction =
            ( Object[] values, Integer modulo ) -> Objects.hash( values ) % modulo;


    static <NodeType extends Node> Set<NodeType> getExecutionTargetsFunction( Set<NodeType> nodes, List<TypedValue> parameterValues, SortedMap<Integer, Integer> sortedPrimaryKeyColumnsIndexesToParametersIndexes ) {
        final Object[] keyValues = new Object[sortedPrimaryKeyColumnsIndexesToParametersIndexes.size()];
        int keyValueIndex = 0;
        for ( Entry<Integer, Integer> primaryKeyColumnIndexToParameterIndex : sortedPrimaryKeyColumnsIndexesToParametersIndexes.entrySet() ) {
            final int parameterIndex = primaryKeyColumnIndexToParameterIndex.getValue();
            if ( parameterValues.size() > parameterIndex ) {
                keyValues[keyValueIndex++] = parameterValues.get( primaryKeyColumnIndexToParameterIndex.getValue() /* index of the parameter */ );
            } else {
                // incomplete key; return all!
                return nodes;
            }
        }

        final List<NodeType> candidates = new LinkedList<>( nodes );

        return Collections.singleton( candidates.get(
                HorizontalHashFragmentation.hashFunction.apply( keyValues, nodes.size() )
        ) );
    }


    public HorizontalHashFragmentation() {
    }


    /*
     *
     */


    @Override
    public ResultSetInfos prepareAndExecuteDataDefinition( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode catalogSql, SqlNode storeSql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        // execute on all nodes
        final RspList<RemoteExecuteResult> responseList = connection.getCluster().prepareAndExecuteDataDefinition( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), catalogSql, storeSql, maxRowCount, maxRowsInFirstFrame );

        final Map<AbstractRemoteNode, RemoteExecuteResult> remoteResults = new HashMap<>();

        for ( final Entry<Address, Rsp<RemoteExecuteResult>> responseEntry : responseList.entrySet() ) {
            final AbstractRemoteNode remoteNode = connection.getCluster().getRemoteNode( responseEntry.getKey() );
            final Rsp<RemoteExecuteResult> response = responseEntry.getValue();

            if ( response.hasException() ) {
                throw Utils.wrapException( response.getException() );
            }

            remoteResults.put( remoteNode, response.getValue() );
            response.getValue().toExecuteResult().resultSets.forEach( resultSet -> {
                connection.addAccessedNode( remoteNode, RemoteConnectionHandle.fromConnectionHandle( new ConnectionHandle( resultSet.connectionId ) ) );
                statement.addAccessedNode( remoteNode, RemoteStatementHandle.fromStatementHandle( new StatementHandle( resultSet.connectionId, resultSet.statementId, resultSet.signature ) ) );
            } );
        }

        return statement.createResultSet( remoteResults.keySet(),
                /* executeResultSupplier */ () -> MergeFunctions.mergeExecuteResults( statement, remoteResults, maxRowsInFirstFrame ),
                /* generatedKeysSupplier */ () -> MergeFunctions.mergeGeneratedKeys( statement, remoteResults )
        );
    }


    @Override
    public <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteDataDefinition( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode catalogSql, SqlNode storeSql, long maxRowCount, int maxRowsInFirstFrame, Set<NodeType> executionTargets ) throws RemoteException {
        throw new UnsupportedOperationException( "Not supported." );
    }


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


    @Override
    public <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteDataManipulation( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, Set<NodeType> executionTargets ) throws RemoteException {
        throw new UnsupportedOperationException( "Not supported." );
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


    @Override
    public <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteDataQuery( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, Set<NodeType> executionTargets ) throws RemoteException {
        throw new UnsupportedOperationException( "Not supported." );
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

        final Map<String, Integer> primaryKeysColumnIndexes = CatalogUtils.lookupPrimaryKeyColumnsNamesAndIndexes( connection, catalogName, schemaName, tableName );
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
            return statement.createResultSet( remoteResults.keySet(),
                    /* executeResultSupplier */ () -> MergeFunctions.mergeExecuteResults( statement, remoteResults, maxRowsInFirstFrame ),
                    /* generatedKeysSupplier */ () -> MergeFunctions.mergeGeneratedKeys( statement, remoteResults )
            );
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

            return statement.createResultSet( remoteResults.keySet(),
                    /* executeResultSupplier */ () -> MergeFunctions.mergeExecuteResults( statement, remoteResults, maxRowsInFirstFrame ),
                    /* generatedKeysSupplier */ () -> MergeFunctions.mergeGeneratedKeys( statement, remoteResults )
            );
        }

        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    private ResultSetInfos prepareAndExecuteDataQuerySelectAggregate( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlSelect sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        LOGGER.trace( "prepareAndExecuteDataQueryAggregate( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {} )", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame );

        if ( SqlNodeUtils.SELECT_UTILS.getGroupBy( sql ) != null && SqlNodeUtils.SELECT_UTILS.getGroupBy( sql ).size() > 0 ) {
            throw Utils.wrapException( new SQLFeatureNotSupportedException( "`GROUP BY` currently not supported." ) );
        }

        final SqlKind[] aggregateFunctions;
        if ( SqlNodeUtils.SELECT_UTILS.selectListContainsAggregate( sql ) ) {
            /*
                YCSB:
                    SELECT MAX(`USERTABLE`.`YCSB_KEY`)
                    FROM `PUBLIC`.`USERTABLE` AS `USERTABLE`
             */

            final SqlNodeList selectList = SqlNodeUtils.SELECT_UTILS.getSelectList( sql );
            aggregateFunctions = new SqlKind[selectList.size()];

            for ( int selectItemIndex = 0; selectItemIndex < selectList.size(); ++selectItemIndex ) {
                SqlNode selectItem = selectList.get( selectItemIndex );

                if ( selectItem.getKind() == SqlKind.AS ) {
                    // "undo" the "AS" operator
                    selectItem = ((SqlBasicCall) selectItem).operand( 0 ); // the original name, NOT the alias
                }
                if ( selectItem.isA( SqlKind.AGGREGATE ) ) {
                    aggregateFunctions[selectItemIndex] = selectItem.getKind();
                } else {
                    throw new UnsupportedOperationException( "Either non or all select items have to be aggregate functions." );
                }
            }
        } else {
            aggregateFunctions = new SqlKind[0];
        }

        // more than one table and especially the required join is currently not supported!
        final SqlIdentifier targetTable = SqlNodeUtils.SELECT_UTILS.getTargetTable( sql );

        // do now most of the work required for later execution
        final String catalogName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 2 );
        final String schemaName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 1 );
        final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 0 );
        final Map<String, Integer> primaryKeyColumnsNamesAndIndexes = CatalogUtils.lookupPrimaryKeyColumnsNamesAndIndexes( connection, catalogName, schemaName, tableName );

        boolean allPrimaryKeyColumnsAreInTheCondition = true;

        if ( sql.hasWhere() ) {
            // todo: check condition!
            SqlNode where = SqlNodeUtils.SELECT_UTILS.getWhere( sql );
            throw new UnsupportedOperationException( "Not implemented yet." );
        } else {
            allPrimaryKeyColumnsAreInTheCondition = false;
        }

        final Set<AbstractRemoteNode> executionTargets;
        if ( !primaryKeyColumnsNamesAndIndexes.isEmpty() && allPrimaryKeyColumnsAreInTheCondition && SqlNodeUtils.SELECT_UTILS.whereConditionContainsOnlyEquals( sql ) ) {
            throw new UnsupportedOperationException( "Not implemented yet." );
        } else {
            // no condition --> execute on all fragments
            executionTargets = connection.getCluster().getMembers();
        }

        // send to all
        // todo: if condition contains only equals -> get the responsible node; else -> send to all
        final RspList<RemoteExecuteResult> responseList = connection.getCluster().prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, maxRowsInFirstFrame );
        // handle the responses of the execution
        final Map<AbstractRemoteNode, RemoteExecuteResult> remoteResults = ClusterUtils.getRemoteResults( connection.getCluster(), responseList );

        return statement.createResultSet( remoteResults.keySet(),
                /* executeResultSupplier */ aggregateFunctions.length > 0 ?
                        () -> MergeFunctions.mergeAggregatedResults( statement, remoteResults, aggregateFunctions ) :
                        () -> MergeFunctions.mergeExecuteResults( statement, remoteResults, maxRowsInFirstFrame ),
                /* generatedKeysSupplier */ () -> MergeFunctions.mergeGeneratedKeys( statement, remoteResults )
        );
    }


    /*
     *
     */


    @Override
    public PreparedStatementInfos prepareDataManipulation( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {
        LOGGER.trace( "prepareDataManipulation( connection: {}, statement: {}, sql: {}, maxRowCount: {} )", connection, statement, sql, maxRowCount );

        final SqlIdentifier targetTable;
        switch ( sql.getKind() ) {
            // See org.apache.calcite.sql.SqlKind.DML
            case INSERT:
                targetTable = SqlNodeUtils.INSERT_UTILS.getTargetTable( (SqlInsert) sql );
                break;

            case UPDATE:
                // FAIL-FAST
                if ( SqlNodeUtils.UPDATE_UTILS.whereConditionContainsOnlyEquals( (SqlUpdate) sql ) == false ) {
                    // Currently only primary_key EQUALS('=') ? is supported
                    throw new UnsupportedOperationException( "Not implemented yet." );
                }

                targetTable = SqlNodeUtils.UPDATE_UTILS.getTargetTable( (SqlUpdate) sql );
                break;

            case DELETE:
                // FAIL-FAST
                if ( SqlNodeUtils.DELETE_UTILS.whereConditionContainsOnlyEquals( (SqlDelete) sql ) == false ) {
                    // Currently only primary_key EQUALS('=') ? is supported
                    throw new UnsupportedOperationException( "Not implemented yet." );
                }

                targetTable = SqlNodeUtils.DELETE_UTILS.getTargetTable( (SqlDelete) sql );
                break;

            case MERGE:
            case PROCEDURE_CALL:
            default:
                throw Utils.wrapException( new SQLFeatureNotSupportedException( sql.getKind() + " is not supported." ) );
        }

        // do now most of the work required for later execution
        final String catalogName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 2 );
        final String schemaName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 1 );
        final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 0 );
        final Map<String, Integer> primaryKeyColumnsNamesAndIndexes = CatalogUtils.lookupPrimaryKeyColumnsNamesAndIndexes( connection, catalogName, schemaName, tableName );

        final SortedMap<Integer, Integer> primaryKeyColumnsIndexesToParametersIndexes = new TreeMap<>();
        boolean allPrimaryKeyColumnsAreInTheCondition = true;
        switch ( sql.getKind() ) {
            case INSERT:
                primaryKeyColumnsIndexesToParametersIndexes.putAll( SqlNodeUtils.INSERT_UTILS.getPrimaryKeyColumnsIndexesToParametersIndexesMap( (SqlInsert) sql, primaryKeyColumnsNamesAndIndexes ) );
                break;

            case UPDATE:
                if ( SqlNodeUtils.UPDATE_UTILS.targetColumnsContainPrimaryKeyColumn( (SqlUpdate) sql, primaryKeyColumnsNamesAndIndexes.keySet() ) ) {
                    // At least one primary key column is in the targetColumns list and will be updated
                    throw new UnsupportedOperationException( "Not implemented yet." );
                }
                // intentional fall-though
            case DELETE:
                final Map<String, Integer> columnNamesToParameterIndexes = SqlNodeUtils.getColumnsNameToDynamicParametersIndexMap( sql );
                // Check the WHERE condition if the primary key is included
                for ( Entry<String, Integer> primaryKeyColumnNameAndIndex : primaryKeyColumnsNamesAndIndexes.entrySet() ) {
                    // for every primary key and its index in the table
                    final Integer parameterIndex = columnNamesToParameterIndexes.get( primaryKeyColumnNameAndIndex.getKey() );
                    if ( parameterIndex != null ) {
                        // the primary key is present in the condition
                        primaryKeyColumnsIndexesToParametersIndexes.put( primaryKeyColumnNameAndIndex.getValue(), parameterIndex );
                    } else {
                        // the primary key is NOT in the condition
                        allPrimaryKeyColumnsAreInTheCondition = false;
                    }
                }
                break;

            default:
                throw new UnsupportedOperationException( "Not implemented yet." );
        }

        final boolean executeOnAllNodes;
        switch ( sql.getKind() ) {
            case INSERT:
                if ( !primaryKeyColumnsNamesAndIndexes.isEmpty() && allPrimaryKeyColumnsAreInTheCondition ) {
                    executeOnAllNodes = false;
                } else {
                    executeOnAllNodes = true;
                }
                break;

            case UPDATE:
                if ( !primaryKeyColumnsNamesAndIndexes.isEmpty() && allPrimaryKeyColumnsAreInTheCondition && SqlNodeUtils.UPDATE_UTILS.whereConditionContainsOnlyEquals( (SqlUpdate) sql ) ) {
                    executeOnAllNodes = false;
                } else {
                    executeOnAllNodes = true;
                }
                break;

            case DELETE:
                if ( !primaryKeyColumnsNamesAndIndexes.isEmpty() && allPrimaryKeyColumnsAreInTheCondition && SqlNodeUtils.DELETE_UTILS.whereConditionContainsOnlyEquals( (SqlDelete) sql ) ) {
                    executeOnAllNodes = false;
                } else {
                    executeOnAllNodes = true;
                }
                break;

            default:
                throw new UnsupportedOperationException( "Not supported" );
        }

        // updates have to be PREPARED on ALL nodes
        final RspList<RemoteStatementHandle> prepareResponseList = connection.getCluster().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount );
        // handle the responses of the PREPARE execution
        final Map<AbstractRemoteNode, RemoteStatementHandle> preparedStatements = ClusterUtils.getRemoteResults( connection.getCluster(), prepareResponseList );

        return connection.createPreparedStatement( statement, preparedStatements,
                //
                /* signatureSupplier */ () -> MergeFunctions.mergeSignature( statement, preparedStatements, sql ),
                //
                /* execute */ ( executeConnection, executeTransaction, executeStatement, executeParameterValues, executeMaxRowsInFirstFrame ) -> {
                    //
                    final Map<AbstractRemoteNode, RemoteExecuteResult> executeResults;
                    try {
                        // EXECUTE the update statement
                        final RspList<RemoteExecuteResult> executeResponseList = executeConnection.getCluster().execute( RemoteTransactionHandle.fromTransactionHandle( executeTransaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( executeStatement.getStatementHandle() ), executeParameterValues, executeMaxRowsInFirstFrame,
                                executeOnAllNodes ? executeConnection.getCluster().getMembers() :
                                        HorizontalHashFragmentation.getExecutionTargetsFunction(
                                                executeConnection.getCluster().getMembers(),
                                                executeParameterValues,
                                                primaryKeyColumnsIndexesToParametersIndexes
                                        )
                        );

                        // handle the responses
                        executeResults = ClusterUtils.getRemoteResults( executeConnection.getCluster(), executeResponseList );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    for ( AbstractRemoteNode node : executeResults.keySet() ) {
                        executeConnection.addAccessedNode( node, RemoteConnectionHandle.fromConnectionHandle( executeConnection.getConnectionHandle() ) );
                    }

                    return executeStatement.createResultSet( executeResults.keySet(),
                            /* executeResultSupplier */ () -> MergeFunctions.mergeExecuteResults( executeStatement, executeResults, executeMaxRowsInFirstFrame ),
                            /* generatedKeysSupplier */ () -> MergeFunctions.mergeGeneratedKeys( executeStatement, executeResults )
                    );
                },
                //
                /* executeBatch */ ( executeBatchConnection, executeBatchTransaction, executeBatchStatement, executeBatchListOfUpdateBatches ) -> {
                    //
                    final Set<AbstractRemoteNode> availableRemotes = executeBatchConnection.getCluster().getMembers();
                    final Map<Integer, Set<AbstractRemoteNode>> mapToMergeTheUpdateCounts = new HashMap<>();
                    final Map<AbstractRemoteNode, RemoteExecuteBatchResult> executeBatchResults;

                    if ( executeOnAllNodes ) {
                        for ( ListIterator<UpdateBatch> updateBatchIterator = executeBatchListOfUpdateBatches.listIterator(); updateBatchIterator.hasNext(); updateBatchIterator.next() ) {
                            mapToMergeTheUpdateCounts.put( updateBatchIterator.nextIndex(), new LinkedHashSet<>( availableRemotes ) );
                        }

                        try {
                            // EXECUTE the update statements
                            final RspList<RemoteExecuteBatchResult> executeBatchResponseList = executeBatchConnection.getCluster().executeBatch( RemoteTransactionHandle.fromTransactionHandle( executeBatchTransaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( executeBatchStatement.getStatementHandle() ), executeBatchListOfUpdateBatches, executeBatchConnection.getCluster().getMembers() );

                            // handle the EXECUTE responses
                            executeBatchResults = ClusterUtils.getRemoteResults( executeBatchConnection.getCluster(), executeBatchResponseList );
                        } catch ( RemoteException ex ) {
                            throw Utils.wrapException( ex );
                        }
                    } else {
                        // we need to find out what to send where
                        final Map<AbstractRemoteNode, List<UpdateBatch>> parameterValuesForRemoteNode = new HashMap<>();

                        for ( ListIterator<UpdateBatch> updateBatchIterator = executeBatchListOfUpdateBatches.listIterator(); updateBatchIterator.hasNext(); ) {
                            final int batchLineNumber = updateBatchIterator.nextIndex();
                            final UpdateBatch ub = updateBatchIterator.next();
                            final List<TypedValue> parameterValues = ub.getParameterValuesList();

                            for ( AbstractRemoteNode executionTarget :
                                    HorizontalHashFragmentation.getExecutionTargetsFunction( availableRemotes, parameterValues, primaryKeyColumnsIndexesToParametersIndexes )
                            ) {
                                final List<UpdateBatch> pVs = parameterValuesForRemoteNode.getOrDefault( executionTarget, new LinkedList<>() );
                                pVs.add( ub );
                                parameterValuesForRemoteNode.put( executionTarget, pVs );

                                final Set<AbstractRemoteNode> nodesForBatchLineNumber = mapToMergeTheUpdateCounts.getOrDefault( batchLineNumber, new LinkedHashSet<>() );
                                nodesForBatchLineNumber.add( executionTarget );
                                mapToMergeTheUpdateCounts.put( batchLineNumber, nodesForBatchLineNumber );
                            }
                        }

                        final RspList<RemoteExecuteBatchResult> executeBatchResponseList = new RspList<>();
                        parameterValuesForRemoteNode.entrySet().parallelStream().forEach( target -> {
                            try {
                                final RemoteExecuteBatchResult response = target.getKey().executeBatch( RemoteTransactionHandle.fromTransactionHandle( executeBatchTransaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( executeBatchStatement.getStatementHandle() ), target.getValue() );
                                executeBatchResponseList.addRsp( target.getKey().getNodeAddress(), response );
                            } catch ( RemoteException e ) {
                                executeBatchResponseList.put( target.getKey().getNodeAddress(), new Rsp<>( e ) );
                            }
                        } );

                        try {
                            executeBatchResults = ClusterUtils.getRemoteResults( executeBatchConnection.getCluster(), executeBatchResponseList );
                        } catch ( RemoteException ex ) {
                            throw Utils.wrapException( ex );
                        }
                    }

                    for ( AbstractRemoteNode node : executeBatchResults.keySet() ) {
                        executeBatchConnection.addAccessedNode( node, RemoteConnectionHandle.fromConnectionHandle( executeBatchConnection.getConnectionHandle() ) );
                    }

                    return statement.createBatchResultSet( executeBatchResults.keySet(),
                            /* executeBatchResultSupplier */ () -> MergeFunctions.mergeExecuteBatchResults( executeBatchStatement, executeBatchResults, mapToMergeTheUpdateCounts ),
                            /* generatedKeysSupplier */ () -> MergeFunctions.mergeGeneratedKeys( executeBatchStatement, executeBatchResults )
                    );
                }
        );
    }


    @Override
    public <NodeType extends Node> Map<NodeType, PreparedStatementInfos> prepareDataManipulation( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount, Set<NodeType> executionTargets ) throws RemoteException {
        throw new UnsupportedOperationException( "Not supported." );
    }


    @Override
    public PreparedStatementInfos prepareDataQuery( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {
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
    public <NodeType extends Node> Map<NodeType, PreparedStatementInfos> prepareDataQuery( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount, Set<NodeType> executionTargets ) throws RemoteException {
        throw new UnsupportedOperationException( "Not supported." );
    }


    protected PreparedStatementInfos prepareDataQuerySelect( ConnectionInfos connection, StatementInfos statement, SqlSelect sql, long maxRowCount ) throws RemoteException {
        LOGGER.trace( "prepareDataQuerySelect( connection: {}, statement: {}, sql: {}, maxRowCount: {} )", connection, statement, sql, maxRowCount );

        if ( SqlNodeUtils.SELECT_UTILS.getGroupBy( sql ) != null && SqlNodeUtils.SELECT_UTILS.getGroupBy( sql ).size() > 0 ) {
            throw Utils.wrapException( new SQLFeatureNotSupportedException( "`GROUP BY` currently not supported." ) );
        }

        final SqlKind[] aggregateFunctions;
        if ( SqlNodeUtils.SELECT_UTILS.selectListContainsAggregate( sql ) ) {
            // SELECT MIN(`NEW_ORDER`.`NO_O_ID`)
            //FROM `PUBLIC`.`NEW_ORDER` AS `NEW_ORDER`
            //WHERE `NEW_ORDER`.`NO_D_ID` = ? AND `NEW_ORDER`.`NO_W_ID` = ?

            final SqlNodeList selectList = SqlNodeUtils.SELECT_UTILS.getSelectList( sql );
            aggregateFunctions = new SqlKind[selectList.size()];

            for ( int selectItemIndex = 0; selectItemIndex < selectList.size(); ++selectItemIndex ) {
                SqlNode selectItem = selectList.get( selectItemIndex );

                if ( selectItem.getKind() == SqlKind.AS ) {
                    // "undo" the "AS" operator
                    selectItem = ((SqlBasicCall) selectItem).operand( 0 ); // the original name, NOT the alias
                }
                if ( selectItem.isA( SqlKind.AGGREGATE ) ) {
                    aggregateFunctions[selectItemIndex] = selectItem.getKind();
                } else {
                    throw new UnsupportedOperationException( "Either non or all select items have to be aggregate functions." );
                }
            }
        } else {
            aggregateFunctions = new SqlKind[0];
        }

        final SqlIdentifier targetTable = SqlNodeUtils.SELECT_UTILS.getTargetTable( sql );

        // do now most of the work required for later execution
        final String catalogName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 2 );
        final String schemaName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 1 );
        final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 0 );
        final Map<String, Integer> primaryKeyColumnsNamesAndIndexes = CatalogUtils.lookupPrimaryKeyColumnsNamesAndIndexes( connection, catalogName, schemaName, tableName );

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
        if ( !primaryKeyColumnsNamesAndIndexes.isEmpty() && completePrimaryKey && SqlNodeUtils.SELECT_UTILS.whereConditionContainsOnlyEquals( sql ) ) {
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
                /* signatureSupplier */ () -> MergeFunctions.mergeSignature( statement, prepareRemoteResults, sql ),
                //
                /* execute */ ( executeConnection, executeTransaction, executeStatement, executeParameterValues, executeMaxRowsInFirstFrame ) -> {
                    //
                    final Map<AbstractRemoteNode, RemoteExecuteResult> executeResults;
                    try {
                        // EXECUTE the select statement
                        final RspList<RemoteExecuteResult> _responseList = executeConnection.getCluster().execute( RemoteTransactionHandle.fromTransactionHandle( executeTransaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( executeStatement.getStatementHandle() ), executeParameterValues, executeMaxRowsInFirstFrame,
                                executeOnAllNodes ? executeConnection.getCluster().getMembers() :
                                        HorizontalHashFragmentation.getExecutionTargetsFunction(
                                                executeConnection.getCluster().getMembers(),
                                                executeParameterValues,
                                                primaryKeyColumnsIndexesToParametersIndexes
                                        )
                        );

                        // handle the responses
                        executeResults = ClusterUtils.getRemoteResults( executeConnection.getCluster(), _responseList );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    for ( AbstractRemoteNode node : executeResults.keySet() ) {
                        executeConnection.addAccessedNode( node, RemoteConnectionHandle.fromConnectionHandle( executeConnection.getConnectionHandle() ) );
                    }

                    return executeStatement.createResultSet( executeResults.keySet(),
                            /* executeResultSupplier */ aggregateFunctions.length > 0 ?
                                    () -> MergeFunctions.mergeAggregatedResults( executeStatement, executeResults, aggregateFunctions ) :
                                    () -> MergeFunctions.mergeExecuteResults( executeStatement, executeResults, executeMaxRowsInFirstFrame ),
                            /* generatedKeysSupplier */ () -> MergeFunctions.mergeGeneratedKeys( executeStatement, executeResults ) // todo: replace with a proper merge of the results
                    );
                },
                //
                /* executeBatch */ ( executeBatchConnection, executeBatchTransaction, executeBatchStatement, executeBatchListOfUpdateBatches ) -> {
                    //
                    throw Utils.wrapException( new SQLException( "SELECT statements cannot be executed in a batch context." ) );
                } );
    }


    /*
     *
     */


    @Override
    public ReplicationProtocol setReplicationProtocol( ReplicationProtocol replicationProtocol ) {
        throw new UnsupportedOperationException( "Not supported." );
    }
}
