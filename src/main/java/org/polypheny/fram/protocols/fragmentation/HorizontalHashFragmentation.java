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


import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.ConnectionHandle;
import org.apache.calcite.avatica.Meta.ExecuteBatchResult;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.proto.Common.TypedValue;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.jgroups.Address;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.polypheny.fram.protocols.AbstractProtocol;
import org.polypheny.fram.protocols.Protocol.FragmentationProtocol;
import org.polypheny.fram.remote.AbstractRemoteNode;
import org.polypheny.fram.remote.Cluster;
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


public class HorizontalHashFragmentation extends AbstractProtocol implements FragmentationProtocol {

    private final Cluster cluster;

    private final Map<PreparedStatementInfos, Map<String, Integer>> primaryKeyOrdinals = new HashMap<>();


    public HorizontalHashFragmentation() {
        this.cluster = Cluster.getDefaultCluster();
    }


    @Override
    public ExecuteResult prepareAndExecuteDataManipulation( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        LOGGER.trace( "prepareAndExecuteDataManipulation( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, callback: {} )", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );

        switch ( sql.getKind() ) {
            case INSERT:
                return prepareAndExecuteDataManipulationInsert( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
        }

        final RspList<RemoteExecuteResult> responseList = cluster.prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, maxRowsInFirstFrame );

        final Map<AbstractRemoteNode, RemoteExecuteResult> remoteResults = new HashMap<>();
        responseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            remoteResults.put( cluster.getRemoteNode( address ), remoteStatementHandleRsp.getValue() );
        } );

        final ResultSetInfos resultSetInfos = statement.createResultSet( remoteResults, origins -> origins.entrySet().iterator().next().getValue().toExecuteResult(), ( origins, conn, stmt, offset, fetchMaxRowCount ) -> {
            try {
                return origins.entrySet().iterator().next().getKey().fetch( RemoteStatementHandle.fromStatementHandle( stmt.getStatementHandle() ), offset, fetchMaxRowCount ).toFrame();
            } catch ( RemoteException e ) {
                throw Utils.wrapException( e );
            }
        } );

        return resultSetInfos.getExecuteResult();
    }


    protected ExecuteResult prepareAndExecuteDataManipulationInsert( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        LOGGER.trace( "prepareAndExecuteDataManipulationInsert( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, callback: {} )", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );

        final SqlInsert insertSql = (SqlInsert) sql;
        final SqlIdentifier targetTable = (SqlIdentifier) insertSql.getTargetTable();
        final SqlNodeList targetTableColumnList = insertSql.getTargetColumnList();
        final SqlBasicCall source = (SqlBasicCall) insertSql.getSource();

        switch ( source.getKind() ) {
            case VALUES:
                final List<SqlNode> rows = source.getOperandList();
                for ( SqlNode row : rows ) {
                    final List<SqlNode> columns = ((SqlBasicCall) row).getOperandList();
                    for ( SqlNode column : columns ) {
                        LOGGER.error( "Found column value of type: " + column.getKind() + " with value: " + ((SqlLiteral) column).toValue() );
                    }
                }
                break;

            default:
                throw new UnsupportedOperationException( "source.getKind() = " + source.getKind() + " - Not supported yet." );
        }

        final String catalog;
        final String schema;
        final String table;
        switch ( targetTable.names.size() ) {
            case 3:
                catalog = targetTable.names.get( 0 );
                schema = targetTable.names.get( 1 );
                table = targetTable.names.get( 2 );
                break;
            case 2:
                catalog = null;
                schema = targetTable.names.get( 0 );
                table = targetTable.names.get( 1 );
                break;
            case 1:
                catalog = null;
                schema = null;
                table = targetTable.names.get( 0 );
                break;
            default:
                throw new RuntimeException( "Something went terrible wrong here..." );
        }

        final Map<String, Integer> primaryKeysColumnOrdinals;
        if ( targetTableColumnList == null ) {
            primaryKeysColumnOrdinals = lookupPrimaryKeyOrdinals( connection, catalog, schema, table );
        } else {
            // search in targetTableColumnList for the ordinal of the primary keys in this Statement
            throw new UnsupportedOperationException( "Not implemented yet." );
        }

        final RspList<RemoteExecuteResult> responseList = cluster.prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, maxRowsInFirstFrame );

        final Map<AbstractRemoteNode, RemoteExecuteResult> remoteResults = new HashMap<>();
        responseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            remoteResults.put( cluster.getRemoteNode( address ), remoteStatementHandleRsp.getValue() );
        } );

        final ResultSetInfos resultSetInfos = statement.createResultSet( remoteResults, origins -> origins.entrySet().iterator().next().getValue().toExecuteResult(), ( origins, conn, stmt, offset, fetchMaxRowCount ) -> {
            try {
                return origins.entrySet().iterator().next().getKey().fetch( RemoteStatementHandle.fromStatementHandle( stmt.getStatementHandle() ), offset, fetchMaxRowCount ).toFrame();
            } catch ( RemoteException e ) {
                throw Utils.wrapException( e );
            }
        } );
        return resultSetInfos.getExecuteResult();
    }


    @Override
    public ExecuteResult prepareAndExecuteDataQuery( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) {
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    @Override
    public StatementInfos prepareDataManipulation( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {
        LOGGER.trace( "prepareDataManipulation( connection: {}, statement: {}, sql: {}, maxRowCount: {} )", connection, statement, sql, maxRowCount );

        switch ( sql.getKind() ) {
            case INSERT:
                return prepareDataManipulationInsert( connection, statement, sql, maxRowCount );
        }

        final RspList<RemoteStatementHandle> responseList = cluster.prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount );

        final List<Entry<AbstractRemoteNode, RemoteStatementHandle>> remoteResults = new LinkedList<>();
        responseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            remoteResults.add( new SimpleImmutableEntry<>( cluster.getRemoteNode( address ), remoteStatementHandleRsp.getValue() ) );
        } );

        return connection.createPreparedStatement( statement, remoteResults );
    }


    protected StatementInfos prepareDataManipulationInsert( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {
        LOGGER.trace( "prepareDataManipulationInsert( connection: {}, statement: {}, sql: {}, maxRowCount: {} )", connection, statement, sql, maxRowCount );

        final SqlInsert insertSql = (SqlInsert) sql;
        final SqlIdentifier targetTable = (SqlIdentifier) insertSql.getTargetTable();
        final SqlNodeList targetTableColumnList = insertSql.getTargetColumnList();
        final SqlBasicCall source = (SqlBasicCall) insertSql.getSource();

        switch ( source.getKind() ) {
            case VALUES:
                final List<SqlNode> rows = source.getOperandList();
                for ( SqlNode row : rows ) {
                    final List<SqlNode> columns = ((SqlBasicCall) row).getOperandList();
                    for ( SqlNode column : columns ) {
                        if ( column.getKind() != SqlKind.DYNAMIC_PARAM ) {
                            throw new UnsupportedOperationException( "Found column value of type: " + column.getKind() + " with value: " + ((SqlLiteral) column).toValue() );
                        }
                    }
                }
                break;

            default:
                throw new UnsupportedOperationException( "source.getKind() = " + source.getKind() + " - Not supported yet." );
        }

        final String catalog;
        final String schema;
        final String table;
        switch ( targetTable.names.size() ) {
            case 3:
                catalog = targetTable.names.get( 0 );
                schema = targetTable.names.get( 1 );
                table = targetTable.names.get( 2 );
                break;
            case 2:
                catalog = null;
                schema = targetTable.names.get( 0 );
                table = targetTable.names.get( 1 );
                break;
            case 1:
                catalog = null;
                schema = null;
                table = targetTable.names.get( 0 );
                break;
            default:
                throw new RuntimeException( "Something went terrible wrong here..." );
        }

        final Map<String, Integer> primaryKeysColumnOrdinals;
        if ( targetTableColumnList == null ) {
            primaryKeysColumnOrdinals = lookupPrimaryKeyOrdinals( connection, catalog, schema, table );
        } else {
            // search in targetTableColumnList for the ordinal of the primary keys in this PreparedStatement
            throw new UnsupportedOperationException( "Not implemented yet." );
        }

        final RspList<RemoteStatementHandle> responseList = cluster.prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount );

        final List<Entry<AbstractRemoteNode, RemoteStatementHandle>> remoteResults = new LinkedList<>();
        responseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            remoteResults.add( new SimpleImmutableEntry<>( cluster.getRemoteNode( address ), remoteStatementHandleRsp.getValue() ) );
        } );

        final PreparedStatementInfos preparedStatement = connection.createPreparedStatement( statement, remoteResults );
        this.primaryKeyOrdinals.put( preparedStatement, primaryKeysColumnOrdinals );
        return preparedStatement;
    }


    protected Map<String, Integer> lookupPrimaryKeyOrdinals( final ConnectionInfos connection, final String catalog, final String schema, final String table ) {
        final List<String> primaryKeysColumnNames = new LinkedList<>();
        MetaResultSet mrs = connection.getCatalog().getPrimaryKeys( connection, catalog, schema, table );
        for ( Object row : mrs.firstFrame.rows ) {
            Object[] cells = (Object[]) row;
            primaryKeysColumnNames.add( (String) cells[3] );
        }

        final Map<String, Integer> primaryKeyOrdinals = new HashMap<>();
        for ( String primaryKeyColumnName : primaryKeysColumnNames ) {
            MetaResultSet columnInfo = connection.getCatalog().getColumns( connection, catalog, Meta.Pat.of( schema ), Meta.Pat.of( table ), Meta.Pat.of( primaryKeyColumnName ) );
            for ( Object row : columnInfo.firstFrame.rows ) {
                Object[] cells = (Object[]) row;
                final int ordinal = (int) cells[16];
                if ( primaryKeyOrdinals.put( primaryKeyColumnName, ordinal ) != null ) {
                    throw new RuntimeException( "Search for the ordinals of the primary key columns was not specific enough!" );
                }
            }
        }

        return primaryKeyOrdinals;
    }


    @Override
    public StatementInfos prepareDataQuery( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) {
        // Map<Key, Collection<RemoteNodes>> Schema.find( key, Attributes...)
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    @Override
    public ExecuteResult execute( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<TypedValue> parameterValues, int maxRowsInFirstFrame ) throws NoSuchStatementException, RemoteException {
        if ( !(statement instanceof PreparedStatementInfos) ) {
            throw new IllegalArgumentException( "The provided statement is not a PreparedStatement." );
        }

        final AbstractRemoteNode[] remotes = connection.getCluster().getAllMembers().toArray( new AbstractRemoteNode[0] );
        final Map<String, Integer> primaryKeyOrdinals = this.primaryKeyOrdinals.get( statement );
        Objects.requireNonNull( primaryKeyOrdinals );

        final List<TypedValue> primaryKeyValues = new LinkedList<>();
        for ( int ordinal : primaryKeyOrdinals.values() ) {
            primaryKeyValues.add( parameterValues.get( ordinal - 1 ) );
        }

        final int hash = Objects.hash( primaryKeyValues.toArray() );
        final int winner = Math.abs( hash % remotes.length );
        final AbstractRemoteNode executionTarget = remotes[winner];

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "execute on {}", executionTarget );
        }

        final RemoteExecuteResult response = executionTarget.execute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), parameterValues, maxRowsInFirstFrame );
        response.toExecuteResult().resultSets.forEach( resultSet -> {
            connection.addAccessedNode( executionTarget, RemoteConnectionHandle.fromConnectionHandle( new ConnectionHandle( resultSet.connectionId ) ) );
            statement.addAccessedNode( executionTarget, RemoteStatementHandle.fromStatementHandle( new StatementHandle( resultSet.connectionId, resultSet.statementId, resultSet.signature ) ) );
        } );

        final ResultSetInfos resultSet = statement.createResultSet( Collections.singletonMap( executionTarget, response ), origins -> origins.entrySet().iterator().next().getValue().toExecuteResult(), ( origins, conn, stmt, offset, fetchMaxRowCount ) -> {
            try {
                return origins.entrySet().iterator().next().getKey().fetch( RemoteStatementHandle.fromStatementHandle( stmt.getStatementHandle() ), offset, fetchMaxRowCount ).toFrame();
            } catch ( RemoteException e ) {
                throw Utils.wrapException( e );
            }
        } );
        return resultSet.getExecuteResult();
    }


    @Override
    public ResultSetInfos executeBatch( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<UpdateBatch> listOfParameterValues ) throws NoSuchStatementException, RemoteException {
        if ( !(statement instanceof PreparedStatementInfos) ) {
            throw new IllegalArgumentException( "The provided statement is not a PreparedStatement." );
        }

        final AbstractRemoteNode[] remotes = connection.getCluster().getAllMembers().toArray( new AbstractRemoteNode[0] );
        final Map<AbstractRemoteNode, List<UpdateBatch>> parameterValuesForRemoteNode = new HashMap<>();

        final Map<String, Integer> primaryKeyOrdinals = this.primaryKeyOrdinals.get( statement );
        Objects.requireNonNull( primaryKeyOrdinals );

        final Map<Integer, AbstractRemoteNode> resultsMap = new HashMap<>();

        for ( ListIterator<UpdateBatch> it = listOfParameterValues.listIterator(); it.hasNext(); ) {
            UpdateBatch ub = it.next();
            final List<TypedValue> parameterValues = ub.getParameterValuesList();

            final List<TypedValue> primaryKeyValues = new LinkedList<>();
            for ( int ordinal : primaryKeyOrdinals.values() ) {
                primaryKeyValues.add( parameterValues.get( ordinal - 1 ) );
            }

            final int hash = Objects.hash( primaryKeyValues.toArray() );
            final int winner = Math.abs( hash % remotes.length );
            final AbstractRemoteNode executionTarget = remotes[winner];

            final List<UpdateBatch> pVs = parameterValuesForRemoteNode.getOrDefault( executionTarget, new LinkedList<>() );
            pVs.add( ub );
            parameterValuesForRemoteNode.put( executionTarget, pVs );
            resultsMap.put( it.previousIndex(), executionTarget );
        }

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "executeBatch on {}", parameterValuesForRemoteNode.keySet() );
        }

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

            final long[] updateCounts = new long[resultsMap.keySet().size()];
            for ( int updateCountIndex = 0; updateCountIndex < updateCounts.length; ++updateCountIndex ) {
                final AbstractRemoteNode origin = resultsMap.get( updateCountIndex );
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
}
