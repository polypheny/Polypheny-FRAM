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
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import org.apache.calcite.avatica.Meta.ExecuteBatchResult;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.proto.Common.TypedValue;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.jgroups.util.RspList;
import org.polypheny.fram.protocols.AbstractProtocol;
import org.polypheny.fram.protocols.Protocol.FragmentationProtocol;
import org.polypheny.fram.remote.AbstractRemoteNode;
import org.polypheny.fram.remote.Cluster;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.remote.types.RemoteStatementHandle;
import org.polypheny.fram.remote.types.RemoteTransactionHandle;
import org.polypheny.fram.standalone.ConnectionInfos;
import org.polypheny.fram.standalone.ResultSetInfos;
import org.polypheny.fram.standalone.StatementInfos;
import org.polypheny.fram.standalone.TransactionInfos;


public class HorizontalHashFragmentation extends AbstractProtocol implements FragmentationProtocol {

    private final Cluster cluster;


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


    protected ExecuteResult prepareAndExecuteDataManipulationInsert( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        LOGGER.trace( "prepareAndExecuteDataManipulationInsert( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, callback: {} )", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );

        final SqlInsert insertSql = (SqlInsert) sql;
        final SqlNode targetTable = insertSql.getTargetTable();
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

        final RspList<RemoteExecuteResult> responseList = cluster.prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, maxRowsInFirstFrame );

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
    public ExecuteResult prepareAndExecuteDataQuery( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) {
        return null;
    }


    @Override
    public StatementInfos prepareDataManipulation( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) {
        return null;
    }


    @Override
    public StatementInfos prepareDataQuery( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) {
        // Map<Key, Collection<RemoteNodes>> Schema.find( key, Attributes...)
        return null;
    }


    @Override
    public ExecuteResult execute( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<TypedValue> parameterValues, int maxRowsInFirstFrame ) throws NoSuchStatementException, RemoteException {
        return null;
    }


    @Override
    public ExecuteBatchResult executeBatch( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<UpdateBatch> parameterValues ) throws NoSuchStatementException, RemoteException {
        return null;
    }


    @Override
    public Iterable<Serializable> createIterable( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, QueryState state, Signature signature, List<TypedValue> parameterValues, Frame firstFrame ) throws RemoteException {
        return null;
    }


    @Override
    public boolean syncResults( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, QueryState state, long offset ) throws RemoteException {
        return false;
    }


    /*
     *
     */


    @Override
    public ReplicationProtocol setReplicationProtocol( ReplicationProtocol replicationProtocol ) {
        return null;
    }
}
