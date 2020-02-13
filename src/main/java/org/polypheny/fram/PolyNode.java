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

package org.polypheny.fram;


import org.polypheny.fram.remote.AbstractLocalNode;
import org.polypheny.fram.remote.types.RemoteConnectionHandle;
import org.polypheny.fram.remote.types.RemoteExecuteBatchResult;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.remote.types.RemoteFrame;
import org.polypheny.fram.remote.types.RemoteStatementHandle;
import org.polypheny.fram.remote.types.RemoteTransactionHandle;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.avatica.proto.Common.ConnectionProperties;
import org.apache.calcite.avatica.proto.Common.DatabaseProperty;
import org.apache.calcite.avatica.proto.Common.TypedValue;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParser.Config;


/**
 *
 */
class PolyNode extends AbstractLocalNode {


    @Override
    public Map<DatabaseProperty, Serializable> getDatabaseProperties( RemoteConnectionHandle remoteConnectionHandle ) {
        return null;
    }


    @Override
    public RemoteStatementHandle prepare( RemoteStatementHandle remoteStatementHandle, String sql, long maxRowCount ) throws RemoteException {
        return null;
    }


    @Override
    public RemoteExecuteResult prepareAndExecute( RemoteTransactionHandle remoteTransactionHandle, RemoteStatementHandle remoteStatementHandle, String sql, long maxRowCount, int maxRowsInFirstFrame ) throws RemoteException {
        return null;
    }


    @Override
    public RemoteExecuteBatchResult prepareAndExecuteBatch( RemoteTransactionHandle remoteTransactionHandle, RemoteStatementHandle remoteStatementHandle, List<String> sqlCommands ) throws RemoteException {
        return null;
    }


    @Override
    public RemoteExecuteBatchResult executeBatch( RemoteTransactionHandle remoteTransactionHandle, RemoteStatementHandle remoteStatementHandle, List<UpdateBatch> parameterValues ) throws RemoteException {
        return null;
    }


    @Override
    public RemoteFrame fetch( RemoteStatementHandle remoteStatementHandle, long offset, int fetchMaxRowCount ) throws RemoteException {
        return null;
    }


    @Override
    public RemoteExecuteResult execute( RemoteTransactionHandle remoteTransactionHandle, RemoteStatementHandle remoteStatementHandle, List<TypedValue> parameterValues, int maxRowsInFirstFrame ) throws RemoteException {
        return null;
    }


    @Override
    public Void closeStatement( RemoteStatementHandle remoteStatementHandle ) throws RemoteException {
        return null;
    }


    @Override
    public Void closeConnection( RemoteConnectionHandle remoteConnectionHandle ) throws RemoteException {
        return null;
    }


    @Override
    public Void abortConnection( RemoteConnectionHandle remoteConnectionHandle, RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        return null;
    }


    @Override
    public ConnectionProperties connectionSync( RemoteConnectionHandle ch, ConnectionProperties connProps ) throws RemoteException {
        return null;
    }


    @Override
    public Void onePhaseCommit( RemoteConnectionHandle remoteConnectionHandle, RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        return null;
    }


    @Override
    public boolean prepareCommit( RemoteConnectionHandle remoteConnectionHandle, RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        return false;
    }


    @Override
    public Void commit( RemoteConnectionHandle remoteConnectionHandle, RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        return null;
    }


    @Override
    public Void rollback( RemoteConnectionHandle remoteConnectionHandle, RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        return null;
    }


    @Override
    public Config getSqlParserConfig() {
        return null;
    }


    @Override
    public JdbcImplementor getRelToSqlConverter() {
        return null;
    }


    @Override
    public SqlDialect getSqlDialect() {
        return null;
    }


    @Override
    public DataSource getDataSource() {
        return null;
    }


    @Override
    public AbstractCatalog getCatalog() {
        return PolyCatalog.getInstance();
    }


    private static class SingletonHolder {

        private static PolyNode INSTANCE = new PolyNode();


        private SingletonHolder() {
        }
    }


    static PolyNode getInstance() {
        return SingletonHolder.INSTANCE;
    }
}
