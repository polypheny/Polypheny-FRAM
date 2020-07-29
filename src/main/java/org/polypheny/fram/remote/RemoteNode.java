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

package org.polypheny.fram.remote;


import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.proto.Common.DatabaseProperty;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.jgroups.Address;
import org.polypheny.fram.remote.types.RemoteConnectionHandle;
import org.polypheny.fram.remote.types.RemoteExecuteBatchResult;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.remote.types.RemoteFrame;
import org.polypheny.fram.remote.types.RemoteStatementHandle;
import org.polypheny.fram.remote.types.RemoteTransactionHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A remote node in a cluster.
 */
@EqualsAndHashCode(callSuper = true)
public class RemoteNode extends AbstractRemoteNode implements Serializable {

    private static final long serialVersionUID = 2020_03_26__17_20L;

    protected static final Logger LOGGER = LoggerFactory.getLogger( RemoteNode.class );


    RemoteNode( final Address address ) {
        super( address, Cluster.getDefaultCluster() );
    }


    RemoteNode( final Address address, final Cluster cluster ) {
        super( address, cluster );
    }


    @Override
    public Map<DatabaseProperty, Serializable> getDatabaseProperties( RemoteConnectionHandle remoteConnectionHandle ) throws RemoteException {
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    @Override
    public RemoteStatementHandle prepare( final RemoteStatementHandle remoteStatementHandle, final String sql, final long maxRowCount ) throws RemoteException {
        LOGGER.trace( "{}: prepare( remoteStatementHandle: {}, sql: {}, maxRowCount: {} )", this.address, remoteStatementHandle, sql, maxRowCount );

        final RemoteStatementHandle result = this.callRemoteMethod( Method.prepare( remoteStatementHandle, sql, maxRowCount ) );

        LOGGER.trace( "{}: prepare( remoteStatementHandle: {}, sql: {}, maxRowCount: {} ) = {}", this.address, remoteStatementHandle, sql, maxRowCount, result );
        return result;
    }


    @Override
    public RemoteStatementHandle prepare( final RemoteStatementHandle remoteStatementHandle, final String sql, final long maxRowCount, final int[] columnIndexes ) throws RemoteException {
        LOGGER.trace( "{}: prepare( remoteStatementHandle: {}, sql: {}, maxRowCount: {}, columnIndexes: {} )", this.address, remoteStatementHandle, sql, maxRowCount, columnIndexes );

        final RemoteStatementHandle result = this.callRemoteMethod( Method.prepare( remoteStatementHandle, sql, maxRowCount, columnIndexes ) );

        LOGGER.trace( "{}: prepare( remoteStatementHandle: {}, sql: {}, maxRowCount: {}, columnIndexes: {} ) = {}", this.address, remoteStatementHandle, sql, maxRowCount, columnIndexes, result );
        return result;
    }


    @Override
    public RemoteExecuteResult prepareAndExecute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final String sql, final long maxRowCount, final int maxRowsInFirstFrame ) throws RemoteException {
        LOGGER.trace( "{}: prepareAndExecute( remoteTransactionHandle: {}, remoteStatementHandle: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {} )", this.address, remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame );

        final RemoteExecuteResult result = this.callRemoteMethod( Method.prepareAndExecute( remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame ) );

        LOGGER.trace( "{}: prepareAndExecute( remoteTransactionHandle: {}, remoteStatementHandle: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {} ) = {}", this.address, remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame, result );
        return result;
    }


    @Override
    public RemoteExecuteResult prepareAndExecute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final String sql, final long maxRowCount, final int maxRowsInFirstFrame, final int[] columnIndexes ) throws RemoteException {
        LOGGER.trace( "{}: prepareAndExecute( remoteTransactionHandle: {}, remoteStatementHandle: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, columnIndexes: {} )", this.address, remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame, columnIndexes );

        final RemoteExecuteResult result = this.callRemoteMethod( Method.prepareAndExecute( remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame, columnIndexes ) );

        LOGGER.trace( "{}: prepareAndExecute( remoteTransactionHandle: {}, remoteStatementHandle: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, columnIndexes: {} ) = {}", this.address, remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame, columnIndexes, result );
        return result;
    }


    @Override
    public RemoteExecuteResult prepareAndExecuteDataDefinition( RemoteTransactionHandle remoteTransactionHandle, RemoteStatementHandle remoteStatementHandle, String globalCatalogSql, String localStoreSql, long maxRowCount, int maxRowsInFirstFrame ) throws RemoteException {
        LOGGER.trace( "{}: prepareAndExecuteDataDefinition( remoteTransactionHandle: {}, remoteStatementHandle: {}, globalCatalogSql: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {} )", this.address, remoteTransactionHandle, remoteStatementHandle, globalCatalogSql, localStoreSql, maxRowCount, maxRowsInFirstFrame );

        final RemoteExecuteResult result = this.callRemoteMethod( Method.prepareAndExecuteDataDefinition( remoteTransactionHandle, remoteStatementHandle, globalCatalogSql, localStoreSql, maxRowCount, maxRowsInFirstFrame ) );

        LOGGER.trace( "{}: prepareAndExecuteDataDefinition( remoteTransactionHandle: {}, remoteStatementHandle: {}, globalCatalogSql: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {} ) = {}", this.address, remoteTransactionHandle, remoteStatementHandle, globalCatalogSql, localStoreSql, maxRowCount, maxRowsInFirstFrame, result );
        return result;
    }


    @Override
    public RemoteExecuteBatchResult prepareAndExecuteBatch( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<String> sqlCommands ) throws RemoteException {
        LOGGER.trace( "{}: prepareAndExecuteBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, sqlCommands: {} )", this.address, remoteTransactionHandle, remoteStatementHandle, sqlCommands );

        final RemoteExecuteBatchResult result = this.callRemoteMethod( Method.prepareAndExecuteBatch( remoteTransactionHandle, remoteStatementHandle, sqlCommands ) );

        LOGGER.trace( "{}: prepareAndExecuteBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, sqlCommands: {} ) = {}", this.address, remoteTransactionHandle, remoteStatementHandle, sqlCommands, result );
        return result;
    }


    @Override
    public RemoteExecuteBatchResult executeBatch( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<UpdateBatch> parameterValues ) throws RemoteException {
        LOGGER.trace( "{}: executeBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {} )", this.address, remoteTransactionHandle, remoteStatementHandle, parameterValues );

        final RemoteExecuteBatchResult result = this.callRemoteMethod( Method.executeBatch( remoteTransactionHandle, remoteStatementHandle, parameterValues ) );

        LOGGER.trace( "{}: executeBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {} ) = {}", this.address, remoteTransactionHandle, remoteStatementHandle, parameterValues, result );
        return result;
    }


    @Override
    public RemoteFrame fetch( final RemoteStatementHandle remoteStatementHandle, final long offset, final int fetchMaxRowCount ) throws RemoteException {
        LOGGER.trace( "{}: fetch( remoteStatementHandle: {}, offset: {}, fetchMaxRowCount: {} )", this.address, remoteStatementHandle, offset, fetchMaxRowCount );

        final RemoteFrame result = this.callRemoteMethod( Method.fetch( remoteStatementHandle, offset, fetchMaxRowCount ) );

        LOGGER.trace( "{}: fetch( remoteStatementHandle: {}, offset: {}, fetchMaxRowCount: {} ) = {}", this.address, remoteStatementHandle, offset, fetchMaxRowCount, result );
        return result;
    }


    @Override
    public RemoteExecuteResult execute( RemoteTransactionHandle remoteTransactionHandle, RemoteStatementHandle remoteStatementHandle, List<Common.TypedValue> parameterValues, int maxRowsInFirstFrame ) throws RemoteException {
        LOGGER.trace( "{}: execute( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {}, maxRowsInFirstFrame: {} )", this.address, remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame );

        final RemoteExecuteResult result = this.callRemoteMethod( Method.execute( remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame ) );

        LOGGER.trace( "{}: execute( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {}, maxRowsInFirstFrame: {} ) = {}", this.address, remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame, result );
        return result;
    }


    @Override
    public Void closeStatement( RemoteStatementHandle remoteStatementHandle ) throws RemoteException {
        LOGGER.trace( "{}: closeStatement( remoteStatementHandle: {} )", this.address, remoteStatementHandle );

        /*final Void result =*/
        this.callRemoteMethod( Method.closeStatement( remoteStatementHandle ) );

        LOGGER.trace( "{}: execute( remoteStatementHandle: {} ) = {VOID}", this.address, remoteStatementHandle );
        return null; //
    }


    @Override
    public Void closeConnection( RemoteConnectionHandle remoteConnectionHandle ) throws RemoteException {
        LOGGER.trace( "{}: closeConnection( remoteConnectionHandle: {} )", this.address, remoteConnectionHandle );

        /*final Void result =*/
        this.callRemoteMethod( Method.closeConnection( remoteConnectionHandle ) );

        LOGGER.trace( "{}: closeConnection( remoteConnectionHandle: {} ) = {VOID}", this.address, remoteConnectionHandle );
        return null; //
    }


    @Override
    public Void abortConnection( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        LOGGER.trace( "{}: abortConnection( remoteConnectionHandle: {}, remoteTransactionHandle: {} )", this.address, remoteConnectionHandle, remoteTransactionHandle );

        /*final Void result =*/
        this.callRemoteMethod( Method.abort( remoteConnectionHandle, remoteTransactionHandle ) );

        LOGGER.trace( "{}: abortConnection( remoteConnectionHandle: {}, remoteTransactionHandle: {} ) = {VOID}", this.address, remoteConnectionHandle, remoteTransactionHandle );
        return null; //
    }


    @Override
    public Common.ConnectionProperties connectionSync( RemoteConnectionHandle remoteConnectionHandle, Common.ConnectionProperties properties ) throws RemoteException {
        LOGGER.trace( "{}: abortConnection( remoteConnectionHandle: {}, properties: {} )", this.address, remoteConnectionHandle, properties );

        final Common.ConnectionProperties result = this.callRemoteMethod( Method.connectionSync( remoteConnectionHandle, properties ) );

        LOGGER.trace( "{}: abortConnection( remoteConnectionHandle: {}, properties: {} ) = {VOID}", this.address, remoteConnectionHandle, properties );
        return result;
    }


    @Override
    public Void onePhaseCommit( RemoteConnectionHandle remoteConnectionHandle, RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        LOGGER.trace( "{}: onePhaseCommit( remoteConnectionHandle: {}, remoteTransactionHandle: {} )", this.address, remoteConnectionHandle, remoteTransactionHandle );

        /*final Void result =*/
        this.callRemoteMethod( Method.onePhaseCommit( remoteConnectionHandle, remoteTransactionHandle ) );

        LOGGER.trace( "{}: onePhaseCommit( remoteConnectionHandle: {}, remoteTransactionHandle: {} ) = {VOID}", this.address, remoteConnectionHandle, remoteTransactionHandle );
        return null; //
    }


    @Override
    public boolean prepareCommit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        LOGGER.trace( "{}: prepareCommit( remoteConnectionHandle: {}, remoteTransactionHandle: {} )", this.address, remoteConnectionHandle, remoteTransactionHandle );

        final boolean result = this.callRemoteMethod( Method.prepareCommit( remoteConnectionHandle, remoteTransactionHandle ) );

        LOGGER.trace( "{}: prepareCommit( remoteConnectionHandle: {}, remoteTransactionHandle: {} ) = {}", this.address, remoteConnectionHandle, remoteTransactionHandle, result );
        return result;
    }


    @Override
    public Void commit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        LOGGER.trace( "{}: commit( remoteConnectionHandle: {}, remoteTransactionHandle: {} )", this.address, remoteConnectionHandle, remoteTransactionHandle );

        /*final Void result =*/
        this.callRemoteMethod( Method.commit( remoteConnectionHandle, remoteTransactionHandle ) );

        LOGGER.trace( "{}: commit( remoteConnectionHandle: {}, remoteTransactionHandle: {} ) = {VOID}", this.address, remoteConnectionHandle, remoteTransactionHandle );
        return null; //
    }


    @Override
    public Void rollback( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        LOGGER.trace( "{}: rollback( remoteConnectionHandle: {}, remoteTransactionHandle: {} )", this.address, remoteConnectionHandle, remoteTransactionHandle );

        /*final Void result =*/
        this.callRemoteMethod( Method.rollback( remoteConnectionHandle, remoteTransactionHandle ) );

        LOGGER.trace( "{}: rollback( remoteConnectionHandle: {}, remoteTransactionHandle: {} ) = {VOID}", this.address, remoteConnectionHandle, remoteTransactionHandle );
        return null; //
    }
}
