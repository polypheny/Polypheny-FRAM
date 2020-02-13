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


import org.polypheny.fram.protocols.AbstractProtocol;
import org.polypheny.fram.protocols.Protocol.FragmentationProtocol;
import org.polypheny.fram.remote.Cluster;
import org.polypheny.fram.standalone.ConnectionInfos;
import org.polypheny.fram.standalone.StatementInfos;
import org.polypheny.fram.standalone.TransactionInfos;
import java.rmi.RemoteException;
import java.util.List;
import org.apache.calcite.avatica.Meta.ConnectionProperties;
import org.apache.calcite.avatica.Meta.ExecuteBatchResult;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.proto.Common.TypedValue;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.apache.calcite.sql.SqlNode;


public class HorizontalHashFragmentation extends AbstractProtocol implements FragmentationProtocol {

    private final Cluster cluster;


    public HorizontalHashFragmentation() {
        this.cluster = Cluster.getDefaultCluster();
    }


    @Override
    public ConnectionProperties connectionSync( Cluster cluster, ConnectionInfos connection, ConnectionProperties newConnectionProperties ) {
        return null;
    }


    @Override
    public ExecuteResult prepareAndExecuteDataDefinition( Cluster cluster, ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) {
        return null;
    }


    @Override
    public ExecuteResult prepareAndExecuteDataManipulation( Cluster cluster, ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) {
        return null;
    }


    @Override
    public ExecuteResult prepareAndExecuteDataQuery( Cluster cluster, ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) {
        return null;
    }


    @Override
    public ExecuteResult prepareAndExecuteTransactionCommit( Cluster cluster, ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) {
        return null;
    }


    @Override
    public ExecuteResult prepareAndExecuteTransactionRollback( Cluster cluster, ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) {
        return null;
    }


    @Override
    public StatementInfos prepareDataManipulation( Cluster cluster, ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) {
        return null;
    }


    @Override
    public StatementInfos prepareDataQuery( Cluster cluster, ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) {
        // Map<Key, Collection<RemoteNodes>> Schema.find( key, Attributes...)
        return null;
    }


    @Override
    public ExecuteResult execute( final Cluster cluster, final ConnectionInfos connection, final TransactionInfos transaction, final StatementInfos statement, final List<TypedValue> parameterValues, final int maxRowsInFirstFrame ) throws NoSuchStatementException, RemoteException {
        return null;
    }


    @Override
    public ExecuteBatchResult executeBatch( final Cluster cluster, final ConnectionInfos connection, final TransactionInfos transaction, final StatementInfos statement, final List<UpdateBatch> parameterValues ) throws NoSuchStatementException, RemoteException {
        return null;
    }


    @Override
    public Frame fetch( Cluster cluster, StatementHandle statementHandle, long offset, int fetchMaxRowCount ) throws NoSuchStatementException, MissingResultsException {
        return null;
    }


    @Override
    public void commit( Cluster cluster, ConnectionInfos connection, TransactionInfos transaction ) {

    }


    @Override
    public void rollback( Cluster cluster, ConnectionInfos connection, TransactionInfos transaction ) {

    }


    @Override
    public void closeStatement( Cluster cluster, StatementInfos statement ) {

    }


    @Override
    public void closeConnection( Cluster cluster, ConnectionInfos connection ) {

    }


    @Override
    public ReplicationProtocol setReplicationProtocol( ReplicationProtocol replicationProtocol ) {
        return null;
    }
}
