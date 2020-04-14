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


import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.EnumSet;
import java.util.List;
import org.apache.calcite.avatica.Meta.ConnectionProperties;
import org.apache.calcite.avatica.Meta.ExecuteBatchResult;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.proto.Common.TypedValue;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.polypheny.fram.remote.types.RemoteConnectionHandle;
import org.polypheny.fram.remote.types.RemoteStatementHandle;
import org.polypheny.fram.remote.types.RemoteTransactionHandle;
import org.polypheny.fram.standalone.ConnectionInfos;
import org.polypheny.fram.standalone.StatementInfos;
import org.polypheny.fram.standalone.TransactionInfos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Passthrough extends AbstractProtocol implements Protocol {

    private static final Logger LOGGER = LoggerFactory.getLogger( Protocol.class );


    @Override
    public Protocol setUp( Protocol protocol ) {
        return null;
    }


    @Override
    public Protocol setDown( Protocol protocol ) {
        return null;
    }


    @Override
    public ConnectionProperties connectionSync( ConnectionInfos connection, ConnectionProperties newConnectionProperties ) throws RemoteException {
        /*return ConnectionPropertiesImpl.fromProto(
                connection.getCluster().getLocalNode().connectionSync(
                        RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ),
                        newConnectionProperties.toProto()
                )
        );*/
        return newConnectionProperties;
    }


    @Override
    public ExecuteResult prepareAndExecuteDataDefinition( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        String serializedSql = sql.toSqlString( connection.getCluster().getLocalNode().getSqlDialect() ).getSql();
        if ( sql.isA( EnumSet.of( SqlKind.CREATE_TABLE, SqlKind.ALTER_TABLE ) ) ) {
            // HSQLDB does not accept an expression as DEFAULT value. The toSqlString method, however, creates an expression in parentheses. Thus, we have to "extract" the value.
            serializedSql = serializedSql.replaceAll( "DEFAULT \\(([^)]*)\\)", "DEFAULT $1" );  // search for everything between '(' and ')' which does not include a ')'. For now, this should cover most cases.
        }

        return connection.getCluster().getLocalNode().prepareAndExecuteDataDefinition( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), serializedSql, serializedSql, maxRowCount, maxRowsInFirstFrame )
                .toExecuteResult();
    }


    @Override
    public ExecuteResult prepareAndExecuteDataManipulation( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        String serializedSql = sql.toSqlString( connection.getCluster().getLocalNode().getSqlDialect() ).getSql();

        return connection.getCluster().getLocalNode().prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), serializedSql, maxRowCount, maxRowsInFirstFrame )
                .toExecuteResult();
    }


    @Override
    public ExecuteResult prepareAndExecuteDataQuery( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        String serializedSql = sql.toSqlString( connection.getCluster().getLocalNode().getSqlDialect() ).getSql();

        return connection.getCluster().getLocalNode().prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), serializedSql, maxRowCount, maxRowsInFirstFrame )
                .toExecuteResult();
    }


    @Override
    public ExecuteResult prepareAndExecuteTransactionCommit( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        String serializedSql = sql.toSqlString( connection.getCluster().getLocalNode().getSqlDialect() ).getSql();

        return connection.getCluster().getLocalNode().prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), serializedSql, maxRowCount, maxRowsInFirstFrame )
                .toExecuteResult();
    }


    @Override
    public ExecuteResult prepareAndExecuteTransactionRollback( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        String serializedSql = sql.toSqlString( connection.getCluster().getLocalNode().getSqlDialect() ).getSql();

        return connection.getCluster().getLocalNode().prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), serializedSql, maxRowCount, maxRowsInFirstFrame )
                .toExecuteResult();
    }


    @Override
    public StatementInfos prepareDataManipulation( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {
        String serializedSql = sql.toSqlString( connection.getCluster().getLocalNode().getSqlDialect() ).getSql();

        return connection.createPreparedStatement( statement, connection.getCluster().getLocalNode().asRemoteNode(), connection.getCluster().getLocalNode().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), serializedSql, maxRowCount ) );
    }


    @Override
    public StatementInfos prepareDataQuery( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {
        String serializedSql = sql.toSqlString( connection.getCluster().getLocalNode().getSqlDialect() ).getSql();

        return connection.createPreparedStatement( statement, connection.getCluster().getLocalNode().asRemoteNode(), connection.getCluster().getLocalNode().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), serializedSql, maxRowCount ) );
    }


    @Override
    public ExecuteResult execute( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<TypedValue> parameterValues, int maxRowsInFirstFrame ) throws NoSuchStatementException, RemoteException {
        return connection.getCluster().getLocalNode().execute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), parameterValues, maxRowsInFirstFrame )
                .toExecuteResult();
    }


    @Override
    public ExecuteBatchResult executeBatch( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<UpdateBatch> parameterValues ) throws NoSuchStatementException, RemoteException {
        return connection.getCluster().getLocalNode().executeBatch( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), parameterValues )
                .toExecuteBatchResult();
    }


    @Override
    public Frame fetch( final ConnectionInfos connection, StatementHandle statementHandle, long offset, int fetchMaxRowCount ) throws NoSuchStatementException, MissingResultsException, RemoteException {
        return connection.getCluster().getLocalNode().fetch( RemoteStatementHandle.fromStatementHandle( statementHandle ), offset, fetchMaxRowCount ).toFrame();
    }


    @Override
    public void commit( ConnectionInfos connection, TransactionInfos transaction ) throws RemoteException {
        connection.getCluster().getLocalNode().commit( RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ), RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ) );
    }


    @Override
    public void rollback( ConnectionInfos connection, TransactionInfos transaction ) throws RemoteException {
        connection.getCluster().getLocalNode().rollback( RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ), RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ) );
    }


    @Override
    public void closeStatement( ConnectionInfos connection, StatementInfos statement ) throws RemoteException {
        connection.getCluster().getLocalNode().closeStatement( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ) );
    }


    @Override
    public void closeConnection( ConnectionInfos connection ) throws RemoteException {
        connection.getCluster().getLocalNode().closeConnection( RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ) );
    }


    @Override
    public Iterable<Serializable> createIterable( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, QueryState state, Signature signature, List<TypedValue> parameterValues, Frame firstFrame ) throws RemoteException {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean syncResults( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, QueryState state, long offset ) throws RemoteException {
        throw new UnsupportedOperationException();
    }
}
