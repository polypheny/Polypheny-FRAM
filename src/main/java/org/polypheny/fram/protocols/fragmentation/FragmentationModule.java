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


import io.vavr.Function1;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.avatica.proto.Common.TypedValue;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlDdlNodes;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.polypheny.fram.Node;
import org.polypheny.fram.datadistribution.RecordIdentifier;
import org.polypheny.fram.protocols.AbstractProtocol;
import org.polypheny.fram.protocols.Protocol.FragmentationProtocol;
import org.polypheny.fram.protocols.replication.Replica;
import org.polypheny.fram.standalone.CatalogUtils;
import org.polypheny.fram.standalone.ConnectionInfos;
import org.polypheny.fram.standalone.ResultSetInfos;
import org.polypheny.fram.standalone.ResultSetInfos.BatchResultSet;
import org.polypheny.fram.standalone.ResultSetInfos.QueryResultSet;
import org.polypheny.fram.standalone.StatementInfos;
import org.polypheny.fram.standalone.StatementInfos.PreparedStatementInfos;
import org.polypheny.fram.standalone.TransactionInfos;
import org.polypheny.fram.standalone.Utils;
import org.polypheny.fram.standalone.Utils.WrappingException;
import org.polypheny.fram.standalone.parser.sql.SqlNodeUtils;
import org.polypheny.fram.standalone.parser.sql.ddl.SqlCreateIndex;
import org.polypheny.fram.standalone.parser.sql.ddl.SqlDdlIndexNodes;
import org.polypheny.fram.standalone.parser.sql.ddl.SqlDropIndex;


public class FragmentationModule extends AbstractProtocol implements FragmentationProtocol {

    public static final Fragment TEST_FRAGMENT = new Fragment( UUID.fromString( "88d6398e-6076-4b8b-afb2-4ad095306650" ) );

    private FragmentationSchema currentSchema = new FragmentationSchema();


    @Override
    public ResultSetInfos prepareAndExecuteDataDefinition( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, final SqlNode catalogSql, SqlNode storeSql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {

        // execute on ALL fragments
        final Map<Fragment, ResultSetInfos> remoteResults = this.prepareAndExecuteDataDefinition( connection, transaction, statement, catalogSql, storeSql, maxRowCount, maxRowsInFirstFrame,
                this.currentSchema.allFragments()
        );

        // merge the results
        return statement.createResultSet( remoteResults.keySet(),
                /* executeResultSupplier */ () -> MergeFunctions.mergeExecuteResults( statement, remoteResults, maxRowsInFirstFrame ),
                /* generatedKeysSupplier */ () -> MergeFunctions.mergeGeneratedKeys( statement, remoteResults ) );
    }


    @Override
    public <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteDataDefinition( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode catalogSql, SqlNode storeSql, long maxRowCount, int maxRowsInFirstFrame, Set<NodeType> executionTargets ) throws RemoteException {

        final Function1<Fragment, SqlNode> alteredSqlGenerator;
        switch ( storeSql.getKind() ) {
            case CREATE_TABLE:
                alteredSqlGenerator = ( fragment ) -> {
                    final SqlCreateTable original = (SqlCreateTable) storeSql;
                    final SqlIdentifier originalTableIdentifier = original.operand( 0 );
                    final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( originalTableIdentifier, /* table name */ 0 )
                            + "_" + fragment.id.toString();

                    return SqlDdlNodes.createTable( original.getParserPosition(), original.getReplace(), original.toSqlString( AnsiSqlDialect.DEFAULT ).getSql().contains( "IF NOT EXISTS" ),
                            SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( originalTableIdentifier, /* table name */ 0, tableName ), original.operand( 1 ), original.operand( 2 ) );
                };
                break;

            case DROP_TABLE:
                alteredSqlGenerator = ( fragment ) -> {
                    final SqlDropTable original = (SqlDropTable) storeSql;
                    final SqlIdentifier originalTableIdentifier = original.operand( 0 );
                    final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( originalTableIdentifier, 0 )
                            + "_" + fragment.id.toString();

                    return SqlDdlNodes.dropTable( original.getParserPosition(), original.toSqlString( AnsiSqlDialect.DEFAULT ).getSql().contains( "IF EXISTS" ),
                            SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( originalTableIdentifier, 0, tableName ) );
                };
                break;

            case CREATE_INDEX:
                alteredSqlGenerator = ( fragment ) -> {
                    final SqlCreateIndex original = (SqlCreateIndex) storeSql;
                    final SqlIdentifier originalIndexIdentifier = original.operand( 0 );
                    final String indexName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( originalIndexIdentifier, 0 )
                            + "_" + fragment.id.toString();
                    final SqlIdentifier originalTableIdentifier = original.operand( 1 );
                    final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( originalTableIdentifier, 0 )
                            + "_" + fragment.id.toString();

                    return SqlDdlIndexNodes.createIndex( original.getParserPosition(), original.getReplace(), original.toSqlString( AnsiSqlDialect.DEFAULT ).getSql().contains( "IF NOT EXISTS" ),
                            SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( originalIndexIdentifier, 0, indexName ),
                            SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( originalTableIdentifier, 0, tableName ),
                            original.operand( 2 ) );
                };
                break;

            case DROP_INDEX:
                alteredSqlGenerator = ( fragment ) -> {
                    final SqlDropIndex original = (SqlDropIndex) storeSql;
                    final SqlIdentifier originalIndexIdentifier = original.operand( 0 );
                    final String indexName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( originalIndexIdentifier, 0 )
                            + "_" + fragment.id.toString();

                    return SqlDdlIndexNodes.dropIndex( original.getParserPosition(), original.toSqlString( AnsiSqlDialect.DEFAULT ).getSql().contains( "IF EXISTS" ),
                            SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( originalIndexIdentifier, 0, indexName ) );
                };
                break;

            default:
                throw new UnsupportedOperationException( "Case `" + storeSql.getKind() + "` not implemented yet." );
        }

        final Map<NodeType, ResultSetInfos> executeOnTargetResults = new HashMap<>();
        try {
            executionTargets.parallelStream().forEach( executionTarget -> {
                final Map<Fragment, ResultSetInfos> executeOnFragmentsResults = new HashMap<>();
                final Set<Fragment> fragments;
                if ( executionTarget instanceof Fragment ) {
                    fragments = Collections.singleton( (Fragment) executionTarget );
                } else if ( executionTarget instanceof Replica ) {
                    fragments = this.currentSchema.lookupFragments( (Replica) executionTarget );
                } else {
                    throw new IllegalArgumentException( "Type of `executionTarget` is not supported." );
                }

                fragments.parallelStream().forEach( fragment -> {
                    final SqlNode alteredStoreSql = alteredSqlGenerator.apply( fragment );
                    try {
                        executeOnFragmentsResults.putAll( down.prepareAndExecuteDataDefinition( connection, transaction, statement, catalogSql, alteredStoreSql, maxRowCount, maxRowsInFirstFrame, Collections.singleton( fragment ) ) );
                    } catch ( RemoteException e ) {
                        throw Utils.wrapException( e );
                    }
                } );

                executeOnTargetResults.put( executionTarget, statement.createResultSet( executeOnFragmentsResults.keySet(),
                        /* executeResultSupplier */ () -> MergeFunctions.mergeExecuteResults( statement, executeOnFragmentsResults, maxRowsInFirstFrame ),
                        /* generatedKeysSupplier */ () -> MergeFunctions.mergeGeneratedKeys( statement, executeOnFragmentsResults )
                ) );
            } );
        } catch ( WrappingException we ) {
            final Throwable t = Utils.xtractException( we );
            if ( t instanceof RemoteException ) {
                throw (RemoteException) t;
            } else {
                throw we;
            }
        }

        return executeOnTargetResults;
    }


    @Override
    public ResultSetInfos prepareAndExecuteDataManipulation( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    @Override
    public <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteDataManipulation( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, Set<NodeType> executionTargets ) throws RemoteException {
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


    public <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteDataQuery( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, Set<NodeType> executionTargets ) throws RemoteException {

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

        final Set<Fragment> executionTargets;
        if ( !primaryKeyColumnsNamesAndIndexes.isEmpty() && allPrimaryKeyColumnsAreInTheCondition && SqlNodeUtils.SELECT_UTILS.whereConditionContainsOnlyEquals( sql ) ) {
            // todo: know where to send it
            throw new UnsupportedOperationException( "Not implemented yet." );
        } else {
            // no condition --> execute on all fragments
            executionTargets = this.currentSchema.allFragments();
        }

        // execute
        final Map<Fragment, ResultSetInfos> prepareAndExecuteResults = this.prepareAndExecuteDataQuerySelect( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, executionTargets );

        // merge
        return statement.createResultSet( prepareAndExecuteResults.keySet(),
                /* executeResultSupplier */  aggregateFunctions.length > 0 ?
                        () -> MergeFunctions.mergeAggregatedResults( statement, prepareAndExecuteResults, aggregateFunctions ) :
                        () -> MergeFunctions.mergeExecuteResults( statement, prepareAndExecuteResults, maxRowsInFirstFrame ),
                /* generatedKeysSupplier */ () -> MergeFunctions.mergeGeneratedKeys( statement, prepareAndExecuteResults ) );
    }


    protected <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteDataQuerySelect( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlSelect sql, long maxRowCount, int maxRowsInFirstFrame, Set<NodeType> executionTargets ) throws RemoteException {

        // FAIL-FAST
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

        final Function1<Fragment, SqlNode> alteredSqlSupplier;
        switch ( sql.getKind() ) {
            case SELECT:
                alteredSqlSupplier = ( fragment ) -> {
                    final SqlSelect original = (SqlSelect) sql;

                    final SqlNode alteredFrom;
                    switch ( original.getFrom().getKind() ) {
                        case AS: {
                            final SqlBasicCall as = (SqlBasicCall) original.getFrom();
                            final String fragmentTableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( as.operand( 0 ), /* table name */ 0 )
                                    + "_" + fragment.id.toString();
                            alteredFrom = SqlStdOperatorTable.AS.createCall( as.getParserPosition(), SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( as.operand( 0 ), 0, fragmentTableName ), as.operand( 1 ) );
                            break;
                        }

                        case IDENTIFIER: {
                            final SqlIdentifier identifier = (SqlIdentifier) original.getFrom();
                            final String fragmentTableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( identifier, /* table name */ 0 )
                                    + "_" + fragment.id.toString();
                            alteredFrom = SqlStdOperatorTable.AS.createCall( identifier.getParserPosition(), SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( identifier, 0, fragmentTableName ), identifier.getComponent( 0 ) );
                            break;
                        }

                        default:
                            throw new UnsupportedOperationException( "Case `" + original.getFrom().getKind() + "` not implemented yet." );
                    }

                    return new SqlSelect( original.getParserPosition(), /* keywordList */ original.operand( 0 ), /* selectList */ original.operand( 1 ),
                            alteredFrom,
                            /* where */ original.operand( 3 ), /* groupBy */ original.operand( 4 ), /* having */ original.operand( 5 ), /* windowDecls */ original.operand( 6 ), /* orderBy */ original.operand( 7 ), /* offset */ original.operand( 8 ), /* fetch */ original.operand( 9 ), /* hints */ original.operand( 10 ) );
                };
                break;

            default:
                throw new UnsupportedOperationException( "Case `" + sql.getKind() + "` not implemented yet." );
        }

        final Map<NodeType, ResultSetInfos> executeOnTargetResults = new HashMap<>();
        try {
            executionTargets.parallelStream().forEach( ( executionTarget ) -> {
                final Set<Fragment> fragments;
                if ( executionTarget instanceof Fragment ) {
                    fragments = Collections.singleton( (Fragment) executionTarget );
                } else if ( executionTarget instanceof Replica ) {
                    fragments = this.currentSchema.lookupFragments( (Replica) executionTarget );
                } else {
                    throw new IllegalArgumentException( "Type of `executionTarget` is not supported." );
                }

                if ( sql.hasWhere() ) {
                    // todo: remove unnecessary fragments
                }

                final Map<Fragment, ResultSetInfos> executeOnFragmentResults = new HashMap<>();
                fragments.parallelStream().forEach( ( fragment ) -> {
                    final SqlNode alteredSql = alteredSqlSupplier.apply( fragment );
                    try {
                        final Map<Fragment, ResultSetInfos> executeResult = down.prepareAndExecuteDataQuery( connection, transaction, statement, alteredSql, maxRowCount, maxRowsInFirstFrame, Collections.singleton( fragment ) );
                        executeOnFragmentResults.put( fragment, executeResult.get( fragment ) );
                    } catch ( RemoteException e ) {
                        throw Utils.wrapException( e );
                    }
                } );

                executeOnTargetResults.put( executionTarget, statement.createResultSet( executeOnFragmentResults.keySet(),
                        /* executeResultSupplier */ aggregateFunctions.length > 0 ?
                                () -> MergeFunctions.mergeAggregatedResults( statement, executeOnFragmentResults, aggregateFunctions ) :
                                () -> MergeFunctions.mergeExecuteResults( statement, executeOnFragmentResults, maxRowsInFirstFrame ),
                        /* generatedKeysSupplier */ () -> MergeFunctions.mergeGeneratedKeys( statement, executeOnFragmentResults )
                ) );
            } );
        } catch ( WrappingException we ) {
            final Throwable t = Utils.xtractException( we );
            if ( t instanceof RemoteException ) {
                throw (RemoteException) t;
            } else {
                throw we;
            }
        }

        return executeOnTargetResults;
    }


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

        final boolean executeOnAllFragments;
        switch ( sql.getKind() ) {
            case INSERT:
                if ( !primaryKeyColumnsNamesAndIndexes.isEmpty() && allPrimaryKeyColumnsAreInTheCondition ) {
                    executeOnAllFragments = false;
                } else {
                    executeOnAllFragments = true;
                }
                break;

            case UPDATE:
                if ( !primaryKeyColumnsNamesAndIndexes.isEmpty() && allPrimaryKeyColumnsAreInTheCondition && SqlNodeUtils.UPDATE_UTILS.whereConditionContainsOnlyEquals( (SqlUpdate) sql ) ) {
                    executeOnAllFragments = false;
                } else {
                    executeOnAllFragments = true;
                }
                break;

            case DELETE:
                if ( !primaryKeyColumnsNamesAndIndexes.isEmpty() && allPrimaryKeyColumnsAreInTheCondition && SqlNodeUtils.DELETE_UTILS.whereConditionContainsOnlyEquals( (SqlDelete) sql ) ) {
                    executeOnAllFragments = false;
                } else {
                    executeOnAllFragments = true;
                }
                break;

            default:
                throw new UnsupportedOperationException( "Not supported" );
        }

        // execute the PREPARE on ALL fragments
        final Map<Fragment, PreparedStatementInfos> preparedStatements = this.prepareDataManipulation( connection, statement, sql, maxRowCount,
                this.currentSchema.allFragments()
        );

        // construct the prepared statement together with the execution functions
        return connection.createPreparedStatement( statement, preparedStatements,
                //
                /* signatureSupplier */ () -> MergeFunctions.mergeSignature( statement, preparedStatements, sql ),
                /* execute */ ( executeConnection, executeTransaction, executeStatement, executeParameterValues, executeMaxRowsInFirstFrame ) -> {
                    //
                    final Set<Fragment> executeFragments;
                    if ( executeOnAllFragments ) {
                        executeFragments = this.currentSchema.allFragments();
                    } else {
                        final Serializable[] keyValues = new Serializable[primaryKeyColumnsIndexesToParametersIndexes.size()];
                        int keyValueIndex = 0;
                        for ( Entry<Integer, Integer> primaryKeyColumnIndexToParameterIndex : primaryKeyColumnsIndexesToParametersIndexes.entrySet() ) {
                            final int parameterIndex = primaryKeyColumnIndexToParameterIndex.getValue();
                            if ( executeParameterValues.size() > parameterIndex ) {
                                keyValues[keyValueIndex++] = executeParameterValues.get( primaryKeyColumnIndexToParameterIndex.getValue() /* index of the parameter */ );
                            } else {
                                // incomplete key!
                                throw Utils.wrapException( new SQLFeatureNotSupportedException( "Incomplete primary key in `" + sql.getKind() + "` statement is not supported." ) );
                            }
                        }

                        // Fragment on which the dql is EXECUTED
                        final Fragment executeFragment = this.currentSchema.lookupFragment( new RecordIdentifier( catalogName, schemaName, tableName, keyValues ) );
                        executeFragments = Collections.singleton( executeFragment );
                    }

                    final Map<Fragment, QueryResultSet> executeResults = new HashMap<>();
                    executeFragments.parallelStream().forEach( executeFragment -> {
                        final PreparedStatementInfos preparedStatement;
                        synchronized ( preparedStatements ) {
                            if ( preparedStatements.get( executeFragment ) == null ) {
                                // if not prepared on that new? fragment --> prepare and then use for execution
                                try {
                                    preparedStatements.putAll( FragmentationModule.this.prepareDataManipulation( connection, statement, sql, maxRowCount, Collections.singleton( executeFragment ) ) );
                                } catch ( RemoteException e ) {
                                    throw Utils.wrapException( e );
                                }
                            }
                            preparedStatement = preparedStatements.get( executeFragment );
                        }
                        executeResults.put( executeFragment, executeStatement.createResultSet(
                                executeFragment, preparedStatement.execute( executeConnection, executeTransaction, preparedStatement, executeParameterValues, executeMaxRowsInFirstFrame ) ) );
                    } );

                    return executeStatement.createResultSet( executeResults.keySet(),
                            /* executeResultSupplier */ () -> MergeFunctions.mergeExecuteResults( executeStatement, executeResults, executeMaxRowsInFirstFrame ),
                            /* generatedKeysSupplier */ () -> MergeFunctions.mergeGeneratedKeys( executeStatement, executeResults )
                    );
                },
                /* executeBatch */ ( executeBatchConnection, executeBatchTransaction, executeBatchStatement, executeBatchListOfUpdateBatches ) -> {
                    //
                    final Map<Fragment, List<UpdateBatch>> fragmentToParameterValues = new HashMap<>();
                    final Map<Integer, Set<Fragment>> mapToMergeTheUpdateCounts = new HashMap<>();
                    if ( executeOnAllFragments ) {
                        final Set<Fragment> executeFragments = this.currentSchema.allFragments();
                        for ( Fragment executeFragment : executeFragments ) {
                            fragmentToParameterValues.put( executeFragment, executeBatchListOfUpdateBatches );
                        }
                        for ( ListIterator<UpdateBatch> updateBatchIterator = executeBatchListOfUpdateBatches.listIterator(); updateBatchIterator.hasNext(); updateBatchIterator.next() ) {
                            mapToMergeTheUpdateCounts.put( updateBatchIterator.nextIndex(), executeFragments );
                        }
                    } else {
                        for ( ListIterator<UpdateBatch> updateBatchIterator = executeBatchListOfUpdateBatches.listIterator(); updateBatchIterator.hasNext(); ) {
                            final int batchLineNumber = updateBatchIterator.nextIndex();
                            final UpdateBatch ub = updateBatchIterator.next(); // this is ONE dml line / value line
                            final List<TypedValue> parameterValues = ub.getParameterValuesList();

                            final Serializable[] keyValues = new Serializable[primaryKeyColumnsIndexesToParametersIndexes.size()];
                            int keyValueIndex = 0;
                            for ( Entry<Integer, Integer> primaryKeyColumnIndexToParameterIndex : primaryKeyColumnsIndexesToParametersIndexes.entrySet() ) {
                                final int parameterIndex = primaryKeyColumnIndexToParameterIndex.getValue();
                                if ( parameterValues.size() > parameterIndex ) {
                                    keyValues[keyValueIndex++] = parameterValues.get( primaryKeyColumnIndexToParameterIndex.getValue() /* index of the parameter */ );
                                } else {
                                    // incomplete key!
                                    throw Utils.wrapException( new SQLFeatureNotSupportedException( "Incomplete primary key in `INSERT` statement is not supported." ) );
                                }
                            }

                            // Fragment on which the insert is EXECUTED
                            final Fragment executeFragment = this.currentSchema.lookupFragment( new RecordIdentifier( catalogName, schemaName, tableName, keyValues ) );

                            final List<UpdateBatch> parameterValuesOfExecuteFragment = fragmentToParameterValues.getOrDefault( executeFragment, new LinkedList<>() );
                            parameterValuesOfExecuteFragment.add( ub );
                            fragmentToParameterValues.put( executeFragment, parameterValuesOfExecuteFragment );

                            final Set<Fragment> fragmentsForBatchLineNumber = mapToMergeTheUpdateCounts.getOrDefault( batchLineNumber, new LinkedHashSet<>() );
                            fragmentsForBatchLineNumber.add( executeFragment );
                            mapToMergeTheUpdateCounts.put( batchLineNumber, fragmentsForBatchLineNumber );
                        }
                    }

                    final Map<Fragment, BatchResultSet> executeBatchResults = new HashMap<>();
                    fragmentToParameterValues.entrySet().parallelStream().forEach( fragmentValuesEntry -> {
                        final PreparedStatementInfos preparedStatement;
                        synchronized ( preparedStatements ) {
                            if ( preparedStatements.get( fragmentValuesEntry.getKey() ) == null ) {
                                // if not prepared on that new? fragment --> prepare and then use for execution
                                try {
                                    preparedStatements.putAll( FragmentationModule.this.prepareDataManipulation( connection, statement, sql, maxRowCount, Collections.singleton( fragmentValuesEntry.getKey() ) ) );
                                } catch ( RemoteException e ) {
                                    throw Utils.wrapException( e );
                                }
                            }
                            preparedStatement = preparedStatements.get( fragmentValuesEntry.getKey() );
                        }
                        executeBatchResults.put( fragmentValuesEntry.getKey(), preparedStatement.executeBatch( executeBatchConnection, executeBatchTransaction, preparedStatement, fragmentValuesEntry.getValue() ) );
                    } );

                    return executeBatchStatement.createBatchResultSet( executeBatchResults.keySet(),
                            /* executeBatchResultSupplier */ () -> MergeFunctions.mergeExecuteBatchResults( executeBatchStatement, executeBatchResults, mapToMergeTheUpdateCounts ),
                            /* generatedKeysSupplier */ () -> MergeFunctions.mergeGeneratedKeys( executeBatchStatement, executeBatchResults ) );
                }
        );
    }


    @Override
    public <NodeType extends Node> Map<NodeType, PreparedStatementInfos> prepareDataManipulation( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount, Set<NodeType> executionTargets ) throws RemoteException {
        // PREPARE on ALL targets

        final Function1<Fragment, SqlNode> alteredSqlFunction;
        switch ( sql.getKind() ) {
            // See org.apache.calcite.sql.SqlKind.DML
            case INSERT:
                alteredSqlFunction = ( fragment ) -> {
                    final SqlInsert original = (SqlInsert) sql;
                    final SqlIdentifier originalTableIdentifier = SqlNodeUtils.INSERT_UTILS.getTargetTable( original );
                    final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( originalTableIdentifier, /* table name */ 0 )
                            + "_" + fragment.id.toString();

                    return new SqlInsert( original.getParserPosition(), original.operand( 0 ),
                            SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( originalTableIdentifier, 0, tableName ),
                            original.operand( 2 ), original.operand( 3 ) );
                };
                break;

            case UPDATE:
                alteredSqlFunction = ( fragment ) -> {
                    final SqlUpdate original = (SqlUpdate) sql;
                    final SqlIdentifier originalTableIdentifier = SqlNodeUtils.UPDATE_UTILS.getTargetTable( original );
                    final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( originalTableIdentifier, 0 )
                            + "_" + fragment.id.toString();

                    return new SqlUpdate( original.getParserPosition(),
                            SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( originalTableIdentifier, 0, tableName ),
                            original.operand( 1 ), original.operand( 2 ), original.operand( 3 ), original.getSourceSelect(), original.operand( 4 ) );
                };
                break;

            case DELETE:
                alteredSqlFunction = ( fragment ) -> {
                    final SqlDelete original = (SqlDelete) sql;
                    final SqlIdentifier originalTableIdentifier = SqlNodeUtils.DELETE_UTILS.getTargetTable( original );
                    final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( originalTableIdentifier, 0 )
                            + "_" + fragment.id.toString();

                    return new SqlDelete( original.getParserPosition(), SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( originalTableIdentifier, 0, tableName ),
                            original.operand( 1 ), original.getSourceSelect(), original.operand( 2 ) );
                };
                break;

            case MERGE:
            case PROCEDURE_CALL:
            default:
                throw new UnsupportedOperationException( "Case `" + sql.getKind() + "` not supported yet." );
        }

        final Map<NodeType, PreparedStatementInfos> preparedStatements = new ConcurrentHashMap<>();
        try {
            executionTargets.parallelStream().forEach( ( executionTarget ) -> {
                //
                final Map<Fragment, PreparedStatementInfos> prepareOnTargetResult = new HashMap<>();

                final Set<Fragment> fragments;
                if ( executionTarget instanceof Fragment ) {
                    fragments = Collections.singleton( (Fragment) executionTarget );
                } else if ( executionTarget instanceof Replica ) {
                    fragments = this.currentSchema.lookupFragments( (Replica) executionTarget );
                } else {
                    throw new IllegalArgumentException( "Type of `executionTarget` is not supported." );
                }

                fragments.parallelStream().forEach( ( fragment ) -> {
                    //
                    final SqlNode alteredSql = alteredSqlFunction.apply( fragment ); // get the new table name
                    try {
                        // PREPARE
                        prepareOnTargetResult.putAll( down.prepareDataManipulation( connection, statement, alteredSql, maxRowCount, Collections.singleton( fragment ) ) );
                    } catch ( RemoteException e ) {
                        throw Utils.wrapException( e );
                    }
                } );

                preparedStatements.put( executionTarget, connection.createPreparedStatement( statement, prepareOnTargetResult,
                        //
                        /* signatureSupplier */ () -> MergeFunctions.mergeSignature( statement, prepareOnTargetResult, sql ),
                        //
                        /* execute */ ( executeConnection, executeTransaction, executeStatement, executeParameterValues, executeMaxRowsInFirstFrame ) -> {
                            //
                            final Set<Fragment> executeFragments;

                            if ( executionTarget instanceof Fragment ) {
                                executeFragments = Collections.singleton( (Fragment) executionTarget );
                            } else if ( executionTarget instanceof Replica ) {
                                executeFragments = this.currentSchema.lookupFragments( (Replica) executionTarget );
                                // we need to check using the parameterValues on which of the fragments of the replica this statement has to be executed!
                                throw new UnsupportedOperationException( "Not implemented yet." );
                            } else {
                                throw new IllegalArgumentException( "Type of `executionTarget` is not supported." );
                            }

                            final Map<Fragment, QueryResultSet> executeResults = new HashMap<>();

                            // EXECUTE the statements
                            executeFragments.parallelStream().forEach( executeFragment -> {
                                final PreparedStatementInfos preparedStatement;
                                synchronized ( prepareOnTargetResult ) {
                                    if ( prepareOnTargetResult.get( executeFragment ) == null ) {
                                        // if not prepared on that new? fragment --> prepare and then use for execution
                                        try {
                                            prepareOnTargetResult.putAll( FragmentationModule.this.down.prepareDataManipulation( connection, statement, alteredSqlFunction.apply( executeFragment ), maxRowCount, Collections.singleton( executeFragment ) ) );
                                        } catch ( RemoteException e ) {
                                            throw Utils.wrapException( e );
                                        }
                                    }
                                    preparedStatement = prepareOnTargetResult.get( executeFragment );
                                }
                                executeResults.put( executeFragment,
                                        preparedStatement.execute( executeConnection, executeTransaction, preparedStatement, executeParameterValues, executeMaxRowsInFirstFrame )
                                );
                            } );

                            return executeStatement.createResultSet( executeResults.keySet(),
                                    /* executeResultSupplier */ () -> MergeFunctions.mergeExecuteResults( executeStatement, executeResults, executeMaxRowsInFirstFrame ),
                                    /* generatedKeysSupplier */ () -> MergeFunctions.mergeGeneratedKeys( executeStatement, executeResults )  // todo: this might not be correct!
                            );
                        },
                        //
                        /* executeBatch */ ( executeBatchConnection, executeBatchTransaction, executeBatchStatement, executeBatchListOfUpdateBatches ) -> {
                            //
                            final Set<Fragment> executeBatchFragments;
                            final Map<Integer, Set<Fragment>> mapToMergeTheUpdateCounts = new HashMap<>();

                            if ( executionTarget instanceof Fragment ) {
                                executeBatchFragments = Collections.singleton( (Fragment) executionTarget );
                                for ( ListIterator<UpdateBatch> updateBatchIterator = executeBatchListOfUpdateBatches.listIterator(); updateBatchIterator.hasNext(); updateBatchIterator.next() ) {
                                    mapToMergeTheUpdateCounts.put( updateBatchIterator.nextIndex(), executeBatchFragments );
                                }
                            } else if ( executionTarget instanceof Replica ) {
                                executeBatchFragments = this.currentSchema.lookupFragments( (Replica) executionTarget );
                                // we need to check using the parameterValues on which of the fragments of the replica this statement has to be executed!
                                throw new UnsupportedOperationException( "Not implemented yet." );
                            } else {
                                throw new IllegalArgumentException( "Type of `executionTarget` is not supported." );
                            }

                            final Map<Fragment, BatchResultSet> executeBatchResults = new HashMap<>();

                            // EXECUTE the statements
                            executeBatchFragments.parallelStream().forEach( executeBatchFragment -> {
                                final PreparedStatementInfos preparedStatement;
                                synchronized ( prepareOnTargetResult ) {
                                    if ( prepareOnTargetResult.get( executeBatchFragment ) == null ) {
                                        // if not prepared on that new? fragment --> prepare and then use for execution
                                        try {
                                            prepareOnTargetResult.putAll( FragmentationModule.this.down.prepareDataManipulation( connection, statement, alteredSqlFunction.apply( executeBatchFragment ), maxRowCount, Collections.singleton( executeBatchFragment ) ) );
                                        } catch ( RemoteException e ) {
                                            throw Utils.wrapException( e );
                                        }
                                    }
                                    preparedStatement = prepareOnTargetResult.get( executeBatchFragment );
                                }
                                executeBatchResults.put( executeBatchFragment,
                                        preparedStatement.executeBatch( executeBatchConnection, executeBatchTransaction, preparedStatement, executeBatchListOfUpdateBatches )
                                );
                            } );

                            return executeBatchStatement.createBatchResultSet( executeBatchResults.keySet(),
                                    /* executeBatchResultSupplier */ () -> MergeFunctions.mergeExecuteBatchResults( executeBatchStatement, executeBatchResults, mapToMergeTheUpdateCounts ),
                                    /* generatedKeysSupplier */ () -> MergeFunctions.mergeGeneratedKeys( executeBatchStatement, executeBatchResults )  // todo: this might not be correct!
                            );
                        } )
                );
            } );
        } catch ( WrappingException we ) {
            final Throwable t = Utils.xtractException( we );
            if ( t instanceof RemoteException ) {
                throw (RemoteException) t;
            } else {
                throw we;
            }
        }

        return preparedStatements;
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

        switch ( sql.getKind() ) {
            // See org.apache.calcite.sql.SqlKind.QUERY
            case SELECT:
                return prepareDataQuerySelect( connection, statement, (SqlSelect) sql, maxRowCount, executionTargets );

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

        // more than one table and especially the required join is currently not supported!
        final SqlIdentifier targetTable = SqlNodeUtils.SELECT_UTILS.getTargetTable( sql );

        // do now most of the work required for later execution
        final String catalogName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 2 );
        final String schemaName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 1 );
        final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 0 );
        final Map<String, Integer> primaryKeyColumnsNamesAndIndexes = CatalogUtils.lookupPrimaryKeyColumnsNamesAndIndexes( connection, catalogName, schemaName, tableName );

        final SortedMap<Integer, Integer> primaryKeyColumnsIndexesToParametersIndexes = new TreeMap<>();
        boolean allPrimaryKeyColumnsAreInTheCondition = true;

        final Map<String, Integer> columnNamesToParameterIndexes = SqlNodeUtils.getColumnsNameToDynamicParametersIndexMap( sql );
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

        final boolean executeOnAllFragments;
        if ( !primaryKeyColumnsNamesAndIndexes.isEmpty() && allPrimaryKeyColumnsAreInTheCondition && SqlNodeUtils.SELECT_UTILS.whereConditionContainsOnlyEquals( sql ) ) {
            executeOnAllFragments = false;
        } else {
            executeOnAllFragments = true;
        }

        // execute the PREPARE on ALL fragments
        final Map<Fragment, PreparedStatementInfos> preparedStatements = this.prepareDataQuery( connection, statement, sql, maxRowCount,
                this.currentSchema.allFragments()
        );

        // construct the prepared statement together with the execution functions
        return connection.createPreparedStatement( statement, preparedStatements,
                //
                /* signatureSupplier */ () -> MergeFunctions.mergeSignature( statement, preparedStatements, sql ),
                //
                /* execute */ ( executeConnection, executeTransaction, executeStatement, executeParameterValues, executeMaxRowsInFirstFrame ) -> {
                    //
                    final Set<Fragment> executeFragments;
                    if ( executeOnAllFragments ) {
                        executeFragments = this.currentSchema.allFragments();
                    } else {
                        final Serializable[] keyValues = new Serializable[primaryKeyColumnsIndexesToParametersIndexes.size()];
                        int keyValueIndex = 0;
                        for ( Entry<Integer, Integer> primaryKeyColumnIndexToParameterIndex : primaryKeyColumnsIndexesToParametersIndexes.entrySet() ) {
                            final int parameterIndex = primaryKeyColumnIndexToParameterIndex.getValue();
                            if ( executeParameterValues.size() > parameterIndex ) {
                                keyValues[keyValueIndex++] = executeParameterValues.get( primaryKeyColumnIndexToParameterIndex.getValue() /* index of the parameter */ );
                            } else {
                                // incomplete key!
                                throw Utils.wrapException( new SQLFeatureNotSupportedException( "Incomplete primary key in `" + sql.getKind() + "` statement is not supported." ) );
                            }
                        }

                        // Fragment on which the dql is EXECUTED
                        final Fragment executeFragment = this.currentSchema.lookupFragment( new RecordIdentifier( catalogName, schemaName, tableName, keyValues ) );
                        executeFragments = Collections.singleton( executeFragment );
                    }

                    final Map<Fragment, QueryResultSet> executeResults = new HashMap<>();
                    executeFragments.parallelStream().forEach( executeFragment -> {
                        final PreparedStatementInfos preparedStatement;
                        synchronized ( preparedStatements ) {
                            if ( preparedStatements.get( executeFragment ) == null ) {
                                // if not prepared on that new? fragment --> prepare and then use for execution
                                try {
                                    preparedStatements.putAll( FragmentationModule.this.prepareDataQuery( connection, statement, sql, maxRowCount, Collections.singleton( executeFragment ) ) );
                                } catch ( RemoteException e ) {
                                    throw Utils.wrapException( e );
                                }
                            }
                            preparedStatement = preparedStatements.get( executeFragment );
                        }
                        executeResults.put( executeFragment, executeStatement.createResultSet(
                                executeFragment, preparedStatement.execute( executeConnection, executeTransaction, preparedStatement, executeParameterValues, executeMaxRowsInFirstFrame ) ) );
                    } );

                    return executeStatement.createResultSet( executeResults.keySet(),
                            /* executeResultSupplier */ aggregateFunctions.length > 0 ?
                                    () -> MergeFunctions.mergeAggregatedResults( executeStatement, executeResults, aggregateFunctions ) :
                                    () -> MergeFunctions.mergeExecuteResults( executeStatement, executeResults, executeMaxRowsInFirstFrame ),
                            /* generatedKeysSupplier */ () -> MergeFunctions.mergeGeneratedKeys( executeStatement, executeResults )
                    );
                },
                //
                /* executeBatch */ ( executeBatchConnection, executeBatchTransaction, executeBatchStatement, executeBatchListOfUpdateBatches ) -> {
                    throw Utils.wrapException( new SQLFeatureNotSupportedException( "`SELECT` statements cannot be executed in a batch context." ) );
                }
        );
    }


    protected <NodeType extends Node> Map<NodeType, PreparedStatementInfos> prepareDataQuerySelect( ConnectionInfos connection, StatementInfos statement, SqlSelect sql, long maxRowCount, Set<NodeType> executionTargets ) throws RemoteException {
        // prepare the given sql on the given targets
        // if the targets are replicas --> lookup the fragments, know where to send to, alter the sql, and merge the results
        // if the targets are fragments --> know where to send to???, alter the sql, and merge the results

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
                    throw new UnsupportedOperationException( "Either all or non select items have to be aggregate functions." );
                }
            }
        } else {
            aggregateFunctions = new SqlKind[0];
        }

        final Function1<Fragment, SqlNode> alteredSqlFunction;
        switch ( sql.getKind() ) {
            case SELECT:
                alteredSqlFunction = ( fragment ) -> {
                    final SqlSelect original = (SqlSelect) sql;

                    final SqlNode alteredFrom;
                    switch ( original.getFrom().getKind() ) {
                        case AS: {
                            final SqlBasicCall as = (SqlBasicCall) original.getFrom();
                            final String replicaTableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( as.operand( 0 ), /* table name */ 0 )
                                    + "_" + fragment.id.toString();
                            alteredFrom = SqlStdOperatorTable.AS.createCall( as.getParserPosition(), SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( as.operand( 0 ), 0, replicaTableName ), as.operand( 1 ) );
                            break;
                        }

                        case IDENTIFIER: {
                            final SqlIdentifier identifier = (SqlIdentifier) original.getFrom();
                            final String replicaTableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( identifier, /* table name */ 0 )
                                    + "_" + fragment.id.toString();
                            alteredFrom = SqlStdOperatorTable.AS.createCall( identifier.getParserPosition(), SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( identifier, 0, replicaTableName ), identifier.getComponent( 0 ) );
                            break;
                        }

                        default:
                            throw new UnsupportedOperationException( "Case `" + original.getFrom().getKind() + "` not implemented yet." );
                    }

                    return new SqlSelect( original.getParserPosition(), /* keywordList */ original.operand( 0 ), /* selectList */ original.operand( 1 ),
                            alteredFrom,
                            /* where */ original.operand( 3 ), /* groupBy */ original.operand( 4 ), /* having */ original.operand( 5 ), /* windowDecls */ original.operand( 6 ), /* orderBy */ original.operand( 7 ), /* offset */ original.operand( 8 ), /* fetch */ original.operand( 9 ), /* hints */ original.operand( 10 ) );
                };
                break;

            default:
                throw new UnsupportedOperationException( "Case `" + sql.getKind() + "` not implemented yet." );
        }

        final Map<NodeType, PreparedStatementInfos> preparedStatements = new ConcurrentHashMap<>();
        try {
            executionTargets.parallelStream().forEach( ( executionTarget ) -> {
                //

                // the result of the prepare call to executionTarget
                final Map<Fragment, PreparedStatementInfos> prepareOnTargetResult = new HashMap<>();

                final Set<Fragment> fragments;
                if ( executionTarget instanceof Fragment ) {
                    fragments = Collections.singleton( (Fragment) executionTarget );
                } else if ( executionTarget instanceof Replica ) {
                    // SCHEMA LOOKUP
                    fragments = this.currentSchema.lookupFragments( (Replica) executionTarget );
                } else {
                    throw new IllegalArgumentException( "Type of `executionTarget` is not supported." );
                }

                fragments.parallelStream().forEach( ( fragment ) -> {
                    //
                    final SqlNode alteredSql = alteredSqlFunction.apply( fragment ); // get the new table name
                    try {
                        // PREPARE
                        prepareOnTargetResult.putAll( down.prepareDataQuery( connection, statement, alteredSql, maxRowCount, Collections.singleton( fragment ) ) );
                    } catch ( RemoteException e ) {
                        throw Utils.wrapException( e );
                    }
                } );

                preparedStatements.put( executionTarget, connection.createPreparedStatement( statement, prepareOnTargetResult,
                        //
                        /* signatureSupplier */ () -> MergeFunctions.mergeSignature( statement, prepareOnTargetResult, sql ),
                        //
                        /* execute */ ( executeConnection, executeTransaction, executeStatement, executeParameterValues, executeMaxRowsInFirstFrame ) -> {
                            //
                            final Set<Fragment> executeFragments;

                            if ( executionTarget instanceof Fragment ) {
                                executeFragments = Collections.singleton( (Fragment) executionTarget );
                            } else if ( executionTarget instanceof Replica ) {
                                executeFragments = this.currentSchema.lookupFragments( (Replica) executionTarget );
                                // we need to check using the parameterValues on which of the fragments of the replica this statement has to be executed!
                                throw new UnsupportedOperationException( "Not implemented yet." );
                            } else {
                                throw new IllegalArgumentException( "Type of `executionTarget` is not supported." );
                            }

                            // todo: remove all unnecessary fragments (to do so, check executeParameterValues)

                            final Map<Fragment, QueryResultSet> executeResults = new HashMap<>();

                            // EXECUTE the statements
                            executeFragments.parallelStream().forEach( executeFragment -> {
                                final PreparedStatementInfos preparedStatement;
                                synchronized ( prepareOnTargetResult ) {
                                    if ( prepareOnTargetResult.get( executeFragment ) == null ) {
                                        // if not prepared on that new? fragment --> prepare and then use for execution
                                        try {
                                            prepareOnTargetResult.putAll( FragmentationModule.this.down.prepareDataQuery( connection, statement, alteredSqlFunction.apply( executeFragment ), maxRowCount, Collections.singleton( executeFragment ) ) );
                                        } catch ( RemoteException e ) {
                                            throw Utils.wrapException( e );
                                        }
                                    }
                                    preparedStatement = prepareOnTargetResult.get( executeFragment );
                                }
                                executeResults.put( executeFragment,
                                        preparedStatement.execute( executeConnection, executeTransaction, preparedStatement, executeParameterValues, executeMaxRowsInFirstFrame )
                                );
                            } );

                            return executeStatement.createResultSet( executeResults.keySet(),
                                    /* executeResultSupplier */ aggregateFunctions.length > 0 ?
                                            () -> MergeFunctions.mergeAggregatedResults( executeStatement, executeResults, aggregateFunctions ) :
                                            () -> MergeFunctions.mergeExecuteResults( executeStatement, executeResults, executeMaxRowsInFirstFrame ),
                                    /* generatedKeysSupplier */ () -> MergeFunctions.mergeGeneratedKeys( executeStatement, executeResults )  // todo: this might not be correct!
                            );
                        },
                        //
                        /* executeBatch */ ( executeBatchConnection, executeBatchTransaction, executeBatchStatement, executeBatchListOfUpdateBatches ) -> {
                            throw Utils.wrapException( new SQLFeatureNotSupportedException( "`SELECT` statements cannot be executed in a batch context." ) );
                        } )
                );
            } );
        } catch ( WrappingException we ) {
            final Throwable t = Utils.xtractException( we );
            if ( t instanceof RemoteException ) {
                throw (RemoteException) t;
            } else {
                throw we;
            }
        }

        return preparedStatements;
    }


    @Override
    public ReplicationProtocol setReplicationProtocol( ReplicationProtocol replicationProtocol ) {
        return null;
    }
}
