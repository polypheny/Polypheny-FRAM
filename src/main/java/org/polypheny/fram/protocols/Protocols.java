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
import java.util.List;
import java.util.Objects;
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
import org.polypheny.fram.protocols.fragmentation.HorizontalHashFragmentation;
import org.polypheny.fram.protocols.replication.QuorumReplication;
import org.polypheny.fram.standalone.ConnectionInfos;
import org.polypheny.fram.standalone.StatementInfos;
import org.polypheny.fram.standalone.TransactionInfos;


/**
 * Collection of Well-known Data Management Protocols
 */
public enum Protocols implements Protocol {
    PASS_THROUGH( new Passthrough() ),
    ROWA( QuorumReplication.ROWA ),
    HASH_FRAGMENTATION( new HorizontalHashFragmentation() ),
    ;


    Protocols( Protocol... protocolChain ) {
        if ( Objects.requireNonNull( protocolChain ).length == 0 ) {
            throw new IllegalArgumentException( "Empty array protocolChain." );
        }

        if ( protocolChain.length > 1 ) {
            // we need to link them together
            for ( int index = 0; (index + 1) < protocolChain.length; /* incremented in the loop */ ) {
                final Protocol current = protocolChain[index];
                final Protocol next = protocolChain[++index]; // index increment

                current.setDown( next );
                next.setUp( current );
            }
        }

        this.delegate = protocolChain[0];
    }


    protected final Protocol delegate;


    @Override
    public Protocol setUp( Protocol protocol ) {
        return delegate.setUp( protocol );
    }


    @Override
    public Protocol setDown( Protocol protocol ) {
        return delegate.setDown( protocol );
    }


    @Override
    public ConnectionProperties connectionSync( ConnectionInfos connection, ConnectionProperties newConnectionProperties ) throws RemoteException {
        return delegate.connectionSync( connection, newConnectionProperties );
    }


    @Override
    public ExecuteResult prepareAndExecuteDataDefinition( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        return delegate.prepareAndExecuteDataDefinition( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
    }


    @Override
    public ExecuteResult prepareAndExecuteDataManipulation( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        return delegate.prepareAndExecuteDataManipulation( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
    }


    @Override
    public ExecuteResult prepareAndExecuteDataQuery( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        return delegate.prepareAndExecuteDataQuery( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
    }


    @Override
    public ExecuteResult prepareAndExecuteTransactionCommit( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        return delegate.prepareAndExecuteTransactionCommit( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
    }


    @Override
    public ExecuteResult prepareAndExecuteTransactionRollback( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        return delegate.prepareAndExecuteTransactionRollback( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
    }


    @Override
    public StatementInfos prepareDataManipulation( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {
        return delegate.prepareDataManipulation( connection, statement, sql, maxRowCount );
    }


    @Override
    public StatementInfos prepareDataQuery( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {
        return delegate.prepareDataQuery( connection, statement, sql, maxRowCount );
    }


    @Override
    public ExecuteResult execute( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<TypedValue> parameterValues, int maxRowsInFirstFrame ) throws NoSuchStatementException, RemoteException {
        return delegate.execute( connection, transaction, statement, parameterValues, maxRowsInFirstFrame );
    }


    @Override
    public ExecuteBatchResult executeBatch( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<UpdateBatch> parameterValues ) throws NoSuchStatementException, RemoteException {
        return delegate.executeBatch( connection, transaction, statement, parameterValues );
    }


    @Override
    public Frame fetch( StatementHandle statementHandle, long offset, int fetchMaxRowCount ) throws NoSuchStatementException, MissingResultsException {
        return delegate.fetch( statementHandle, offset, fetchMaxRowCount );
    }


    @Override
    public void commit( ConnectionInfos connection, TransactionInfos transaction ) throws RemoteException {
        delegate.commit( connection, transaction );
    }


    @Override
    public void rollback( ConnectionInfos connection, TransactionInfos transaction ) throws RemoteException {
        delegate.rollback( connection, transaction );
    }


    @Override
    public void closeStatement( ConnectionInfos connection, StatementInfos statement ) throws RemoteException {
        delegate.closeStatement( connection, statement );
    }


    @Override
    public void closeConnection( ConnectionInfos connection ) throws RemoteException {
        delegate.closeConnection( connection );
    }
}
