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
import io.vavr.Function4;
import io.vavr.Function5;
import java.io.Serializable;
import java.math.BigDecimal;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
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
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.jgroups.Address;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.polypheny.fram.protocols.AbstractProtocol;
import org.polypheny.fram.protocols.Protocol.FragmentationProtocol;
import org.polypheny.fram.remote.AbstractRemoteNode;
import org.polypheny.fram.remote.types.RemoteConnectionHandle;
import org.polypheny.fram.remote.types.RemoteExecuteBatchResult;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.remote.types.RemoteStatementHandle;
import org.polypheny.fram.remote.types.RemoteTransactionHandle;
import org.polypheny.fram.standalone.ConnectionInfos;
import org.polypheny.fram.standalone.ResultSetInfos;
import org.polypheny.fram.standalone.ResultSetInfos.BatchResultSetInfos;
import org.polypheny.fram.standalone.ResultSetInfos.QueryResultSet;
import org.polypheny.fram.standalone.StatementInfos;
import org.polypheny.fram.standalone.StatementInfos.PreparedStatementInfos;
import org.polypheny.fram.standalone.TransactionInfos;
import org.polypheny.fram.standalone.Utils;
import org.polypheny.fram.standalone.Utils.WrappingException;
import org.polypheny.fram.standalone.parser.sql.SqlNodeUtils;


public class HorizontalHashFragmentation extends AbstractProtocol implements FragmentationProtocol {

    private final Map<PreparedStatementInfos, Map<SqlIdentifier, Integer>> primaryKeyOrdinals = new HashMap<>();
    private final Map<
            PreparedStatementInfos,
            Function2<List<AbstractRemoteNode>, List<TypedValue>, List<AbstractRemoteNode>>
            > executionTargetsFunctions = new HashMap<>();
    private final Map<
            PreparedStatementInfos,
            Function5<ConnectionInfos, TransactionInfos, StatementInfos, List<TypedValue>, Integer, QueryResultSet>
            > executePreparedStatementFunctions = new HashMap<>();
    private final Map<
            PreparedStatementInfos,
            Function4<ConnectionInfos, TransactionInfos, StatementInfos, List<UpdateBatch>, BatchResultSetInfos>
            > executeBatchPreparedStatementFunctions = new HashMap<>();


    public HorizontalHashFragmentation() {
    }


    protected Map<String, Integer> lookupPrimaryKeyColumnNamesAndIndexes( final ConnectionInfos connection, final SqlIdentifier table ) {
        final String catalogName;
        final String schemaName;
        final String tableName;
        switch ( table.names.size() ) {
            case 3:
                catalogName = table.names.get( 0 );
                schemaName = table.names.get( 1 );
                tableName = table.names.get( 2 );
                break;
            case 2:
                catalogName = null;
                schemaName = table.names.get( 0 );
                tableName = table.names.get( 1 );
                break;
            case 1:
                catalogName = null;
                schemaName = null;
                tableName = table.names.get( 0 );
                break;
            default:
                throw new RuntimeException( "Something went terrible wrong here..." );
        }

        final List<String> primaryKeysColumnNames = lookupPrimaryKeyColumnsNames( connection, catalogName, schemaName, tableName );

        final Map<String, Integer> primaryKeyNamesAndIndexes = new HashMap<>();
        for ( String primaryKeyColumnName : primaryKeysColumnNames ) {
            MetaResultSet columnInfo = connection.getCatalog().getColumns( connection, catalogName, Meta.Pat.of( schemaName ), Meta.Pat.of( tableName ), Meta.Pat.of( primaryKeyColumnName ) );
            for ( Object row : columnInfo.firstFrame.rows ) {
                Object[] cells = (Object[]) row;
                final int columnIndex = (int) cells[16] - 1; // convert ordinal to index
                if ( primaryKeyNamesAndIndexes.put( primaryKeyColumnName, columnIndex ) != null ) {
                    throw new RuntimeException( "Search for the ordinals of the primary key columns was not specific enough!" );
                }
            }
        }

        return primaryKeyNamesAndIndexes;
    }


    protected List<Integer> lookupPrimaryKeyColumnsIndexes( final ConnectionInfos connection, final SqlIdentifier table ) {
        final Map<String, Integer> primaryKeyColumnsNamesAndIndexes = lookupPrimaryKeyColumnNamesAndIndexes( connection, table );
        List<Integer> primaryKeyColumnsIndexes = new ArrayList<>( primaryKeyColumnsNamesAndIndexes.values().size() );
        primaryKeyColumnsIndexes.addAll( primaryKeyColumnsNamesAndIndexes.values() );
        Collections.sort( primaryKeyColumnsIndexes );
        return primaryKeyColumnsIndexes;
    }


    private List<String> lookupPrimaryKeyColumnsNames( final ConnectionInfos connection, final SqlIdentifier table ) {
        final String catalogName;
        final String schemaName;
        final String tableName;
        switch ( table.names.size() ) {
            case 3:
                catalogName = table.names.get( 0 );
                schemaName = table.names.get( 1 );
                tableName = table.names.get( 2 );
                break;
            case 2:
                catalogName = null;
                schemaName = table.names.get( 0 );
                tableName = table.names.get( 1 );
                break;
            case 1:
                catalogName = null;
                schemaName = null;
                tableName = table.names.get( 0 );
                break;
            default:
                throw new RuntimeException( "Something went terrible wrong here..." );
        }
        return lookupPrimaryKeyColumnsNames( connection, catalogName, schemaName, tableName );
    }


    private List<String> lookupPrimaryKeyColumnsNames( final ConnectionInfos connection, final String catalogName, final String schemaName, final String tableName ) {
        final List<String> primaryKeysColumnNames = new LinkedList<>();
        MetaResultSet mrs = connection.getCatalog().getPrimaryKeys( connection, catalogName, schemaName, tableName );
        for ( Object row : mrs.firstFrame.rows ) {
            Object[] cells = (Object[]) row;
            primaryKeysColumnNames.add( (String) cells[3] );
        }
        return primaryKeysColumnNames;
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

        if ( sql.getSelectList().accept( new SqlBasicVisitor<Boolean>() {
            @Override
            public Boolean visit( SqlNodeList nodeList ) {
                boolean isAggregate = false;
                for ( SqlNode n : nodeList.getList() ) {
                    isAggregate |= n.accept( this );
                }
                return isAggregate;
            }


            @Override
            public Boolean visit( SqlCall call ) {
                return call.isA( SqlKind.AGGREGATE );
            }


            @Override
            public Boolean visit( SqlIdentifier id ) {
                return Boolean.FALSE;
            }
        } ) ) {
            // SELECT MIN/MAX/AVG/ FROM ...
            return prepareAndExecuteDataQuerySelectAggregate( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
        }

        final SqlIdentifier table = (SqlIdentifier) sql.getFrom().accept( new SqlBasicVisitor<SqlIdentifier>() {
            @Override
            public SqlIdentifier visit( SqlIdentifier id ) {
                return id;
            }


            @Override
            public SqlIdentifier visit( SqlCall call ) {
                switch ( call.getKind() ) {
                    case AS:
                        return call.operand( 0 );

                    default:
                        return super.visit( call );
                }
            }
        } );

        final Map<String, Integer> primaryKeysColumnIndexes = lookupPrimaryKeyColumnNamesAndIndexes( connection, table );
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

        // send to all
        final RspList<RemoteExecuteResult> responseList = connection.getCluster().prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, maxRowsInFirstFrame );

        final Map<AbstractRemoteNode, RemoteExecuteResult> remoteResults = new HashMap<>();
        responseList.forEach( ( address, remoteExecuteResultRsp ) -> {
            if ( remoteExecuteResultRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteExecuteResultRsp.getException() );
            }
            remoteResults.put( connection.getCluster().getRemoteNode( address ), remoteExecuteResultRsp.getValue() );
        } );

        final SqlNodeList selectList = sql.getSelectList();
        if ( selectList.size() > 1 ) {
            throw new UnsupportedOperationException( "Not implemented yet." );
        }

        final SqlNode aggregateNode = selectList.get( 0 );
        switch ( aggregateNode.getKind() ) {
            case MAX:
                return statement.createResultSet( remoteResults, origins -> {
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
                }, ( v1, v2, v3, v4, v5 ) -> Frame.EMPTY );

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

        final SqlIdentifier table = (SqlIdentifier) sql.getTargetTable();
        final SqlNodeList targetColumns = sql.getTargetColumnList();

        final Map<String, Integer> primaryKeyNameAndIndex = this.lookupPrimaryKeyColumnNamesAndIndexes( connection, table );
        final Set<Integer> primaryKeyColumnsIndexes = new TreeSet<>( primaryKeyNameAndIndex.values() ); // naturally ordered and thus the indexes of the primary key columns are in the correct order
        final Map<Integer, Integer> primaryKeyColumnsIndexesToParametersIndexes = new HashMap<>();

        if ( targetColumns == null || targetColumns.size() == 0 ) {
            // The insert statement has to provide values for every column in the order of the columns
            // Thus, we can use the indexes of the primary key columns to find their corresponding values
            for ( int primaryKeyColumnIndex : primaryKeyColumnsIndexes ) {
                // The primary key FOO with the column index 2
                // maps to the 3rd parameter value
                primaryKeyColumnsIndexesToParametersIndexes.put( primaryKeyColumnIndex, primaryKeyColumnIndex );
            }
        } else {
            // search in targetColumns for the index of the primary keys in this PreparedStatement
            for ( int columnIndex = 0; columnIndex < targetColumns.size(); ++columnIndex ) {
                SqlNode node = targetColumns.get( columnIndex );
                if ( node.isA( EnumSet.of( SqlKind.IDENTIFIER ) ) ) {
                    final SqlIdentifier targetColumn = (SqlIdentifier) node;
                    final Integer primaryKeyIndex = primaryKeyNameAndIndex.remove( targetColumn.names.reverse().get( 0 ) );
                    if ( primaryKeyIndex != null ) {
                        // The name was in the map
                        primaryKeyColumnsIndexesToParametersIndexes.put( primaryKeyIndex, columnIndex );
                    }
                } else {
                    throw new UnsupportedOperationException( "Not implemented yet." );
                }
            }
        }

        // inserts have to be prepared on all nodes
        final RspList<RemoteStatementHandle> responseList = connection.getCluster().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount );

        final Map<AbstractRemoteNode, RemoteStatementHandle> remoteResults = new HashMap<>();
        final Map<AbstractRemoteNode, Throwable> throwables = new HashMap<>();
        for ( Entry<Address, Rsp<RemoteStatementHandle>> responseEntry : responseList.entrySet() ) {
            final AbstractRemoteNode node = connection.getCluster().getRemoteNode( responseEntry.getKey() );
            final Rsp<RemoteStatementHandle> response = responseEntry.getValue();

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

        final PreparedStatementInfos preparedStatement = connection.createPreparedStatement( statement, remoteResults, remoteStatements -> {
            // BEGIN HACK - get the first signature
            return remoteStatements.values().iterator().next().toStatementHandle().signature;
            // END HACK
        } );

        this.executionTargetsFunctions.put( preparedStatement, ( clusterMembers, parameterValues ) -> {
            final AbstractRemoteNode[] executionTargets = clusterMembers.toArray( new AbstractRemoteNode[clusterMembers.size()] );
            final Object[] primaryKeyValues = new Object[primaryKeyColumnsIndexes.size()];
            int primaryKeyValueIndex = 0;
            for ( Iterator<Integer> primaryKeyColumnsIndexIterator = primaryKeyColumnsIndexes.iterator(); primaryKeyColumnsIndexIterator.hasNext(); ) {
                primaryKeyValues[primaryKeyValueIndex++] = parameterValues.get( primaryKeyColumnsIndexesToParametersIndexes.get( primaryKeyColumnsIndexIterator.next() ) );
            }
            final int executionTargetIndex = Math.abs( Objects.hash( primaryKeyValues ) % executionTargets.length );
            return Arrays.asList( executionTargets[executionTargetIndex] );
        } );

        return preparedStatement;
    }


    protected StatementInfos prepareDataManipulationDelete( ConnectionInfos connection, StatementInfos statement, SqlDelete sql, long maxRowCount ) throws RemoteException {
        LOGGER.trace( "prepareDataManipulationDelete( connection: {}, statement: {}, sql: {}, maxRowCount: {} )", connection, statement, sql, maxRowCount );

        if ( SqlNodeUtils.DELETE_UTILS.whereConditionContainsOnlyEquals( sql ) ) {
            // Currently only primary_key EQUALS ? is supported
            throw new UnsupportedOperationException( "Not supported yet." );
        }

        final SqlIdentifier table = (SqlIdentifier) sql.getTargetTable();

        final Map<String, Integer> primaryKeyColumnsNamesAndIndexes = this.lookupPrimaryKeyColumnNamesAndIndexes( connection, table );
        final Set<Integer> primaryKeyColumnsIndexes = new TreeSet<>( primaryKeyColumnsNamesAndIndexes.values() );
        final Map<Integer, Integer> primaryKeyColumnsIndexesToParametersIndexes = new HashMap<>();

        final Map<String, Integer> columnNamesToParameterIndexes = SqlNodeUtils.getColumnsNameToDynamicParametersIndexMap( sql );

        for ( Entry<String, Integer> primaryKeyColumnNameAndIndex : primaryKeyColumnsNamesAndIndexes.entrySet() ) {
            // for every primary key and its index in the table
            final Integer parameterIndex = columnNamesToParameterIndexes.get( primaryKeyColumnNameAndIndex.getKey() );
            if ( parameterIndex != null ) {
                // the primary key is present in the condition
                primaryKeyColumnsIndexesToParametersIndexes.put( primaryKeyColumnNameAndIndex.getValue(), parameterIndex );
            } else {
                // the primary key is NOT in the condition
                throw new UnsupportedOperationException( "Not implemented yet." );
            }
        }

        final RspList<RemoteStatementHandle> responseList = connection.getCluster().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount );

        final Map<AbstractRemoteNode, RemoteStatementHandle> remoteResults = new HashMap<>();
        responseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            remoteResults.put( connection.getCluster().getRemoteNode( address ), remoteStatementHandleRsp.getValue() );
        } );

        final PreparedStatementInfos preparedStatement = connection.createPreparedStatement( statement, remoteResults, remoteStatements -> {
            // BEGIN HACK
            return remoteStatements.values().iterator().next().toStatementHandle().signature;
            // END HACK
        } );

        this.executionTargetsFunctions.put( preparedStatement, ( clusterMembers, parameterValues ) -> {
            final AbstractRemoteNode[] executionTargets = clusterMembers.toArray( new AbstractRemoteNode[clusterMembers.size()] );
            final Object[] primaryKeyValues = new Object[primaryKeyColumnsIndexes.size()];
            int primaryKeyValueIndex = 0;
            for ( Iterator<Integer> primaryKeyColumnsIndexIterator = primaryKeyColumnsIndexes.iterator(); primaryKeyColumnsIndexIterator.hasNext(); ) {
                primaryKeyValues[primaryKeyValueIndex++] = parameterValues.get( primaryKeyColumnsIndexesToParametersIndexes.get( primaryKeyColumnsIndexIterator.next() ) );
            }
            final int executionTargetIndex = Math.abs( Objects.hash( primaryKeyValues ) % executionTargets.length );
            return Arrays.asList( executionTargets[executionTargetIndex] );
        } );

        return preparedStatement;
    }


    protected StatementInfos prepareDataManipulationUpdate( ConnectionInfos connection, StatementInfos statement, SqlUpdate sql, long maxRowCount ) throws RemoteException {
        LOGGER.trace( "prepareDataManipulationUpdate( connection: {}, statement: {}, sql: {}, maxRowCount: {} )", connection, statement, sql, maxRowCount );

        final SqlIdentifier table = (SqlIdentifier) sql.getTargetTable();
        final SqlNodeList targetColumns = sql.getTargetColumnList();
        final SqlNode condition = sql.getCondition();

        final Map<String, Integer> primaryKeyColumnsNamesAndIndexes = this.lookupPrimaryKeyColumnNamesAndIndexes( connection, table );
        final Set<Integer> primaryKeyColumnsIndexes = new TreeSet<>( primaryKeyColumnsNamesAndIndexes.values() );
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
                return primaryKeyColumnsNamesAndIndexes.containsKey( id.names.reverse().get( 0 ) );
            }
        } ) ) {
            // At least one primary key column is in the targetColumns list and will be updated
            throw new UnsupportedOperationException( "Not implemented yet." );
        }

        final Map<String, Integer> columnNamesToParameterIndexes = SqlNodeUtils.getColumnsNameToDynamicParametersIndexMap( sql );

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

        // updates need to go everywhere
        final RspList<RemoteStatementHandle> prepareResponseList = connection.getCluster().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount );

        final Map<AbstractRemoteNode, RemoteStatementHandle> prepareRemoteResults = new HashMap<>();
        prepareResponseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            prepareRemoteResults.put( connection.getCluster().getRemoteNode( address ), remoteStatementHandleRsp.getValue() );
        } );

        final PreparedStatementInfos preparedStatement = connection.createPreparedStatement( statement, prepareRemoteResults, remoteStatements -> {
            // BEGIN HACK
            return remoteStatements.values().iterator().next().toStatementHandle().signature;
            // END HACK
        } );

        if ( sql.getCondition().accept( new SqlBasicVisitor<Boolean>() {
            @Override
            public Boolean visit( SqlCall call ) {
                boolean containsNotOnlyEquals = call.isA( EnumSet.of( SqlKind.NOT_EQUALS, SqlKind.GREATER_THAN, SqlKind.GREATER_THAN_OR_EQUAL, SqlKind.LESS_THAN, SqlKind.LESS_THAN_OR_EQUAL, SqlKind.IS_DISTINCT_FROM, SqlKind.IS_NOT_DISTINCT_FROM ) );
                for ( SqlNode n : call.getOperandList() ) {
                    if ( n != null ) {
                        containsNotOnlyEquals |= n.accept( this );
                    }
                }
                return containsNotOnlyEquals;
            }


            @Override
            public Boolean visit( SqlNodeList nodeList ) {
                boolean containsNotOnlyEquals = false;
                for ( SqlNode n : nodeList ) {
                    if ( n != null ) {
                        containsNotOnlyEquals |= n.accept( this );
                    }
                }
                return containsNotOnlyEquals;
            }


            @Override
            public Boolean visit( SqlIdentifier id ) {
                return false;
            }


            @Override
            public Boolean visit( SqlLiteral literal ) {
                return false;
            }


            @Override
            public Boolean visit( SqlIntervalQualifier intervalQualifier ) {
                throw new UnsupportedOperationException( "Not implemented yet." );
            }


            @Override
            public Boolean visit( SqlDataTypeSpec type ) {
                return false;
            }


            @Override
            public Boolean visit( SqlDynamicParam param ) {
                return false;
            }
        } ) ) {
            // Currently only primary_key EQUALS ? is supported
            throw new UnsupportedOperationException( "Not supported yet." );
        }

        this.executionTargetsFunctions.put( preparedStatement, executionTargetsFunction );

        return preparedStatement;
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

        final SqlIdentifier table = SqlNodeUtils.SELECT_UTILS.getTargetTable( sql );

        final Map<String, Integer> primaryKeyColumnsNamesAndIndexes = this.lookupPrimaryKeyColumnNamesAndIndexes( connection, table );
        final Set<Integer> primaryKeyColumnsIndexes = new TreeSet<>( primaryKeyColumnsNamesAndIndexes.values() );
        final Map<Integer, Integer> primaryKeyColumnsIndexesToParametersIndexes = new HashMap<>();

        final Map<String, Integer> columnNamesToParameterIndexes = SqlNodeUtils.getColumnsNameToDynamicParametersIndexMap( sql );

        boolean incompletePrimaryKey = false;
        for ( Entry<String, Integer> primaryKeyColumnNameAndIndex : primaryKeyColumnsNamesAndIndexes.entrySet() ) {
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

        final RspList<RemoteStatementHandle> prepareResponseList = connection.getCluster().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount );

        final Map<AbstractRemoteNode, RemoteStatementHandle> prepareRemoteResults = new HashMap<>();
        prepareResponseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            prepareRemoteResults.put( connection.getCluster().getRemoteNode( address ), remoteStatementHandleRsp.getValue() );
        } );

        final PreparedStatementInfos preparedStatement = connection.createPreparedStatement( statement, prepareRemoteResults, remoteStatements -> {
            // BEGIN HACK
            return remoteStatements.values().iterator().next().toStatementHandle().signature;
            // END HACK
        } );

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

        this.executionTargetsFunctions.put( preparedStatement, executionTargetsFunction );

        return preparedStatement;
    }


    protected StatementInfos prepareDataQuerySelectAggregate( ConnectionInfos connection, StatementInfos statement, SqlSelect sql, long maxRowCount ) throws RemoteException {
        LOGGER.trace( "prepareDataQuerySelect( connection: {}, statement: {}, sql: {}, maxRowCount: {} )", connection, statement, sql, maxRowCount );

        // todo: MIN(pk_column) SUM(foreign_column) - both have incomplete primary keys
        // SELECT MIN(`NEW_ORDER`.`NO_O_ID`)
        //FROM `PUBLIC`.`NEW_ORDER` AS `NEW_ORDER`
        //WHERE `NEW_ORDER`.`NO_D_ID` = ? AND `NEW_ORDER`.`NO_W_ID` = ?

        final SqlIdentifier table = SqlNodeUtils.SELECT_UTILS.getTargetTable( sql );

        final Map<String, Integer> primaryKeyColumnsNamesAndIndexes = this.lookupPrimaryKeyColumnNamesAndIndexes( connection, table );
        final Set<Integer> primaryKeyColumnsIndexes = new TreeSet<>( primaryKeyColumnsNamesAndIndexes.values() );
        final Map<Integer, Integer> primaryKeyColumnsIndexesToParametersIndexes = new HashMap<>();

        final Map<String, Integer> columnNamesToParameterIndexes = SqlNodeUtils.getColumnsNameToDynamicParametersIndexMap( sql );

        boolean incompletePrimaryKey = false;
        for ( Entry<String, Integer> primaryKeyColumnNameAndIndex : primaryKeyColumnsNamesAndIndexes.entrySet() ) {
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

        final RspList<RemoteStatementHandle> prepareResponseList = connection.getCluster().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount );

        final Map<AbstractRemoteNode, RemoteStatementHandle> prepareRemoteResults = new HashMap<>();
        prepareResponseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            prepareRemoteResults.put( connection.getCluster().getRemoteNode( address ), remoteStatementHandleRsp.getValue() );
        } );

        final PreparedStatementInfos preparedStatement = connection.createPreparedStatement( statement, prepareRemoteResults, remoteStatements -> {
            // BEGIN HACK
            return remoteStatements.values().iterator().next().toStatementHandle().signature;
            // END HACK
        } );

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
        switch ( aggregateNode.getKind() ) {
            case MIN:
                this.executePreparedStatementFunctions.put( preparedStatement, ( exConnection, exTransaction, exStatement, exParameterValues, exMaxRowsInFirstFrame ) -> {

                    final List<AbstractRemoteNode> executionTargets = executionTargetsFunction.apply( exConnection.getCluster().getMembers(), exParameterValues );
                    LOGGER.trace( "execute on {}", executionTargets );

                    final RspList<RemoteExecuteResult> executeResponseList;
                    try {
                        executeResponseList = exConnection.getCluster().execute( RemoteTransactionHandle.fromTransactionHandle( exTransaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( exStatement.getStatementHandle() ), exParameterValues, -1/*maxRowsInFirstFrame*/, executionTargets );
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

                        final AbstractRemoteNode currentRemote = exConnection.getCluster().getRemoteNode( address );

                        executeRemoteResults.put( currentRemote, remoteExecuteResultRsp.getValue() );

                        exConnection.addAccessedNode( currentRemote, RemoteConnectionHandle.fromConnectionHandle( exConnection.getConnectionHandle() ) );
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
                } );
                break;

            case SUM:
                this.executePreparedStatementFunctions.put( preparedStatement, ( exConnection, exTransaction, exStatement, exParameterValues, exMaxRowsInFirstFrame ) -> {

                    final List<AbstractRemoteNode> executionTargets = executionTargetsFunction.apply( exConnection.getCluster().getMembers(), exParameterValues );
                    LOGGER.trace( "execute on {}", executionTargets );

                    final RspList<RemoteExecuteResult> executeResponseList;
                    try {
                        executeResponseList = exConnection.getCluster().execute( RemoteTransactionHandle.fromTransactionHandle( exTransaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( exStatement.getStatementHandle() ), exParameterValues, -1/*maxRowsInFirstFrame*/, executionTargets );
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

                        final AbstractRemoteNode currentRemote = exConnection.getCluster().getRemoteNode( address );

                        executeRemoteResults.put( currentRemote, remoteExecuteResultRsp.getValue() );

                        exConnection.addAccessedNode( currentRemote, RemoteConnectionHandle.fromConnectionHandle( exConnection.getConnectionHandle() ) );
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
                } );
                break;

            default:
                throw new UnsupportedOperationException( "Not implemented yet." );
        }

        this.executionTargetsFunctions.put( preparedStatement, executionTargetsFunction );

        return preparedStatement;
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

        if ( this.executePreparedStatementFunctions.containsKey( statement ) ) {
            // customized execution
            try {
                return this.executePreparedStatementFunctions.get( statement ).apply( connection, transaction, statement, parameterValues, maxRowsInFirstFrame );
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

        List<AbstractRemoteNode> executionTargets = this.executionTargetsFunctions.get( statement ).apply( connection.getCluster().getMembers(), parameterValues );

        LOGGER.trace( "execute on {}", executionTargets );

        final RspList<RemoteExecuteResult> responseList = connection.getCluster().execute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), parameterValues, -1/*maxRowsInFirstFrame*/, executionTargets );

        final Map<AbstractRemoteNode, RemoteExecuteResult> remoteResults = new HashMap<>();
        for ( Entry<Address, Rsp<RemoteExecuteResult>> e : responseList.entrySet() ) {
            final Address address = e.getKey();
            final Rsp<RemoteExecuteResult> remoteExecuteResultRsp = e.getValue();

            if ( remoteExecuteResultRsp.hasException() ) {
                final Throwable t = remoteExecuteResultRsp.getException();
                if ( t instanceof NoSuchStatementException ) {
                    throw (NoSuchStatementException) t;
                }
                if ( t instanceof RemoteException ) {
                    throw (RemoteException) t;
                }
                throw Utils.wrapException( t );
            }

            final AbstractRemoteNode currentRemote = connection.getCluster().getRemoteNode( address );

            remoteResults.put( currentRemote, remoteExecuteResultRsp.getValue() );

            connection.addAccessedNode( currentRemote, RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ) );
        }

        return statement.createResultSet( remoteResults, origins -> new HorizontalExecuteResultMergeFunction().apply( statement, origins, maxRowsInFirstFrame ), ( origins, conn, stmt, offset, fetchMaxRowCount ) -> {
            // fetch
            throw new UnsupportedOperationException( "Not implemented yet." );
        } );
    }


    @Override
    public ResultSetInfos executeBatch( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<UpdateBatch> listOfParameterValues ) throws NoSuchStatementException, RemoteException {
        if ( !(statement instanceof PreparedStatementInfos) ) {
            throw new IllegalArgumentException( "The provided statement is not a PreparedStatement." );
        }

        // only INSERTS (or updates)

        if ( this.executeBatchPreparedStatementFunctions.containsKey( statement ) ) {
            try {
                return this.executeBatchPreparedStatementFunctions.get( statement ).apply( connection, transaction, statement, listOfParameterValues );
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

        final List<AbstractRemoteNode> availableRemotes = connection.getCluster().getMembers();
        final Map<AbstractRemoteNode, List<UpdateBatch>> parameterValuesForRemoteNode = new HashMap<>();
        final Map<Integer, AbstractRemoteNode> mapToMergeTheUpdateCounts = new HashMap<>();

        for ( ListIterator<UpdateBatch> it = listOfParameterValues.listIterator(); it.hasNext(); ) {
            UpdateBatch ub = it.next();
            final List<TypedValue> parameterValues = ub.getParameterValuesList();

            final List<AbstractRemoteNode> executionTargets = this.executionTargetsFunctions.get( statement ).apply( availableRemotes, parameterValues );

            for ( AbstractRemoteNode executionTarget : executionTargets ) {
                final List<UpdateBatch> pVs = parameterValuesForRemoteNode.getOrDefault( executionTarget, new LinkedList<>() );
                pVs.add( ub );
                parameterValuesForRemoteNode.put( executionTarget, pVs );
                mapToMergeTheUpdateCounts.put( it.previousIndex(), executionTarget ); // Here we have then the last of the list providing the updateCount. For now acceptable.
            }
        }

        LOGGER.trace( "executeBatch on {}", parameterValuesForRemoteNode.keySet() );

        final RspList<RemoteExecuteBatchResult> responseList = new RspList<>();
        parameterValuesForRemoteNode.entrySet().parallelStream().forEach( target -> {
            try {
                final RemoteExecuteBatchResult response = target.getKey().executeBatch( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), target.getValue() );
                responseList.addRsp( target.getKey().getNodeAddress(), response );
            } catch ( RemoteException e ) {
                responseList.put( target.getKey().getNodeAddress(), new Rsp<>( e ) );
            }
        } );

        final Map<AbstractRemoteNode, RemoteExecuteBatchResult> remoteResults = new HashMap<>();
        for ( Entry<Address, Rsp<RemoteExecuteBatchResult>> e : responseList.entrySet() ) {
            final Address address = e.getKey();
            final Rsp<RemoteExecuteBatchResult> remoteExecuteBatchResultRsp = e.getValue();

            if ( remoteExecuteBatchResultRsp.hasException() ) {
                final Throwable t = remoteExecuteBatchResultRsp.getException();
                if ( t instanceof NoSuchStatementException ) {
                    throw (NoSuchStatementException) t;
                }
                if ( t instanceof RemoteException ) {
                    throw (RemoteException) t;
                }
                throw Utils.wrapException( t );
            }

            final AbstractRemoteNode currentRemote = connection.getCluster().getRemoteNode( address );

            remoteResults.put( currentRemote, remoteExecuteBatchResultRsp.getValue() );

            connection.addAccessedNode( currentRemote, RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ) );
        }

        return statement.createBatchResultSet( remoteResults, origins -> {
            final Map<AbstractRemoteNode, Integer> originUpdateCountIndexMap = new HashMap<>();

            final long[] updateCounts = new long[mapToMergeTheUpdateCounts.keySet().size()];
            for ( int updateCountIndex = 0; updateCountIndex < updateCounts.length; ++updateCountIndex ) {
                final AbstractRemoteNode origin = mapToMergeTheUpdateCounts.get( updateCountIndex );
                final int originUpdateCountIndex = originUpdateCountIndexMap.getOrDefault( origin, 0 );

                updateCounts[updateCountIndex] = origins.get( origin ).toExecuteBatchResult().updateCounts[originUpdateCountIndex];

                originUpdateCountIndexMap.put( origin, originUpdateCountIndex + 1 );
            }

            return new ExecuteBatchResult( updateCounts );
        } );
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
                for ( MetaResultSet rs : rex.toExecuteResult().resultSets ) {
                    if ( rs.updateCount > -1L ) {
                        updateCounts.add( rs.updateCount );
                    } else {
                        done &= rs.firstFrame.done;
                        iterators.add( rs.firstFrame.rows.iterator() );
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
