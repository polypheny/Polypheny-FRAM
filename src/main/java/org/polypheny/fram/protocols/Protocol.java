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


public interface Protocol {


    Protocol setUp( final Protocol protocol );

    Protocol setDown( final Protocol protocol );

    ConnectionProperties connectionSync( final Cluster cluster, final ConnectionInfos connection, final ConnectionProperties newConnectionProperties ) throws RemoteException;

    ExecuteResult prepareAndExecuteDataDefinition( final Cluster cluster, final ConnectionInfos connection, final TransactionInfos transaction, final StatementInfos statement, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final PrepareCallback callback ) throws RemoteException;

    ExecuteResult prepareAndExecuteDataManipulation( final Cluster cluster, final ConnectionInfos connection, final TransactionInfos transaction, final StatementInfos statement, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final PrepareCallback callback ) throws RemoteException;

    ExecuteResult prepareAndExecuteDataQuery( final Cluster cluster, final ConnectionInfos connection, final TransactionInfos transaction, final StatementInfos statement, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final PrepareCallback callback ) throws RemoteException;

    ExecuteResult prepareAndExecuteTransactionCommit( final Cluster cluster, final ConnectionInfos connection, final TransactionInfos transaction, final StatementInfos statement, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final PrepareCallback callback ) throws RemoteException;

    ExecuteResult prepareAndExecuteTransactionRollback( final Cluster cluster, final ConnectionInfos connection, final TransactionInfos transaction, final StatementInfos statement, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final PrepareCallback callback ) throws RemoteException;

    StatementInfos prepareDataManipulation( final Cluster cluster, final ConnectionInfos connection, final StatementInfos statement, final SqlNode sql, final long maxRowCount ) throws RemoteException;

    StatementInfos prepareDataQuery( final Cluster cluster, final ConnectionInfos connection, final StatementInfos statement, final SqlNode sql, final long maxRowCount ) throws RemoteException;

    ExecuteResult execute( final Cluster cluster, final ConnectionInfos connection, final TransactionInfos transaction, final StatementInfos statement, final List<TypedValue> parameterValues, final int maxRowsInFirstFrame ) throws NoSuchStatementException, RemoteException;

    ExecuteBatchResult executeBatch( final Cluster cluster, final ConnectionInfos connection, final TransactionInfos transaction, final StatementInfos statement, final List<UpdateBatch> parameterValues ) throws NoSuchStatementException, RemoteException;

    Frame fetch( final Cluster cluster, final StatementHandle statementHandle, final long offset, final int fetchMaxRowCount ) throws NoSuchStatementException, MissingResultsException;

    void commit( final Cluster cluster, final ConnectionInfos connection, final TransactionInfos transaction ) throws RemoteException;

    void rollback( final Cluster cluster, final ConnectionInfos connection, final TransactionInfos transaction ) throws RemoteException;

    void closeStatement( final Cluster cluster, final StatementInfos statement ) throws RemoteException;

    void closeConnection( final Cluster cluster, final ConnectionInfos connection ) throws RemoteException;


    interface FragmentationProtocol extends Protocol {

        ReplicationProtocol setReplicationProtocol( ReplicationProtocol replicationProtocol );
    }


    interface ReplicationProtocol extends Protocol {

        FragmentationProtocol setFragmentationProtocol( FragmentationProtocol fragmentationProtocol );

        PlacementProtocol setAllocationProtocol( PlacementProtocol placementProtocol );
    }


    interface PlacementProtocol extends Protocol {

        ReplicationProtocol setReplicationProtocol( ReplicationProtocol replicationProtocol );

        MigrationProtocol setMigrationProtocol( MigrationProtocol migrationProtocol );
    }


    interface MigrationProtocol extends Protocol {

        PlacementProtocol setAllocationProtocol( PlacementProtocol placementProtocol );
    }
}
