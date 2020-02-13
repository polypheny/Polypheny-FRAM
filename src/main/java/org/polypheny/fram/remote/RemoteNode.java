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


import org.polypheny.fram.remote.types.RemoteConnectionHandle;
import org.polypheny.fram.remote.types.RemoteExecuteBatchResult;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.remote.types.RemoteFrame;
import org.polypheny.fram.remote.types.RemoteStatementHandle;
import org.polypheny.fram.remote.types.RemoteTransactionHandle;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.EqualsAndHashCode;
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.jgroups.Address;
import org.jgroups.Message.Flag;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
@EqualsAndHashCode(callSuper = false)
public class RemoteNode implements RemoteMeta, Serializable {

    private static final Field FIELD_cluster;


    static {
        try {
            FIELD_cluster = RemoteNode.class.getDeclaredField( "cluster" );
        } catch ( NoSuchFieldException e ) {
            e.printStackTrace();
            throw new Error( e );
        }
    }


    private static final Logger LOGGER = LoggerFactory.getLogger( RemoteNode.class );

    private static final RequestOptions DEFAULT_REQUEST_OPTIONS = new RequestOptions()
            .setMode( ResponseMode.GET_FIRST )
            .setTimeout( TimeUnit.SECONDS.toMillis( 60 ) )
            .setFlags( Flag.OOB );

    private final Address address;
    private final transient Cluster cluster;


    RemoteNode( final Address address ) {
        this( address, Cluster.getDefaultCluster() );
    }


    RemoteNode( final Address address, final Cluster cluster ) {
        this.address = address;
        this.cluster = cluster;
    }


    private void writeObject( final java.io.ObjectOutputStream out ) throws IOException {
        out.defaultWriteObject();

        out.writeObject( cluster.getClusterId() );
    }


    private void readObject( final java.io.ObjectInputStream in ) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        final UUID clusterId = (UUID) in.readObject();
        final Cluster cluster = Cluster.getCluster( clusterId );
        try {
            FIELD_cluster.setAccessible( true );
            FIELD_cluster.set( this, cluster );
        } catch ( IllegalAccessException e ) {
            e.printStackTrace();
            throw new Error( e );
        } finally {
            FIELD_cluster.setAccessible( false );
        }
    }


    public Address getNodeAddress() {
        return this.address;
    }


    public Cluster getCluster() {
        return this.cluster;
    }


    protected <ReturnType> ReturnType callRemoteMethod( final MethodCall methodCall ) throws RemoteException {
        return callRemoteMethod( DEFAULT_REQUEST_OPTIONS, methodCall );
    }


    protected <ReturnType> ReturnType callRemoteMethod( final RequestOptions requestOptions, final MethodCall methodCall ) throws RemoteException {
        return this.cluster.callMethod( requestOptions, methodCall, this );
    }


    @Override
    public Map<Common.DatabaseProperty, Serializable> getDatabaseProperties( RemoteConnectionHandle remoteConnectionHandle ) throws RemoteException {
        return null;
    }


    @Override
    public RemoteStatementHandle prepare( final RemoteStatementHandle remoteStatementHandle, final String sql, final long maxRowCount ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: prepare( remoteStatementHandle: {}, sql: {}, maxRowCount: {} )", this.address, remoteStatementHandle, sql, maxRowCount );
        }

        final RemoteStatementHandle result = this.callRemoteMethod( Method.prepare( remoteStatementHandle, sql, maxRowCount ) );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: prepare( remoteStatementHandle: {}, sql: {}, maxRowCount: {} ) = {}", this.address, remoteStatementHandle, sql, maxRowCount, result );
        }
        return result;
    }


    @Override
    public RemoteExecuteResult prepareAndExecute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final String sql, final long maxRowCount, final int maxRowsInFirstFrame ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: prepareAndExecute( remoteTransactionHandle: {}, remoteStatementHandle: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {} )", this.address, remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame );
        }

        final RemoteExecuteResult result = this.callRemoteMethod( Method.prepareAndExecute( remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame ) );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: prepareAndExecute( remoteTransactionHandle: {}, remoteStatementHandle: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {} ) = {}", this.address, remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame, result );
        }
        return result;
    }


    @Override
    public RemoteExecuteBatchResult prepareAndExecuteBatch( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<String> sqlCommands ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: prepareAndExecuteBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, sqlCommands: {} )", this.address, remoteTransactionHandle, remoteStatementHandle, sqlCommands );
        }

        final RemoteExecuteBatchResult result = this.callRemoteMethod( Method.prepareAndExecuteBatch( remoteTransactionHandle, remoteStatementHandle, sqlCommands ) );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: prepareAndExecuteBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, sqlCommands: {} ) = {}", this.address, remoteTransactionHandle, remoteStatementHandle, sqlCommands, result );
        }
        return result;
    }


    @Override
    public RemoteExecuteBatchResult executeBatch( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<UpdateBatch> parameterValues ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: executeBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {} )", this.address, remoteTransactionHandle, remoteStatementHandle, parameterValues );
        }

        final RemoteExecuteBatchResult result = this.callRemoteMethod( Method.executeBatch( remoteTransactionHandle, remoteStatementHandle, parameterValues ) );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: executeBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {} ) = {}", this.address, remoteTransactionHandle, remoteStatementHandle, parameterValues, result );
        }
        return result;
    }


    @Override
    public RemoteFrame fetch( final RemoteStatementHandle remoteStatementHandle, final long offset, final int fetchMaxRowCount ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: fetch( remoteStatementHandle: {}, offset: {}, fetchMaxRowCount: {} )", this.address, remoteStatementHandle, offset, fetchMaxRowCount );
        }

        final RemoteFrame result = this.callRemoteMethod( Method.fetch( remoteStatementHandle, offset, fetchMaxRowCount ) );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: fetch( remoteStatementHandle: {}, offset: {}, fetchMaxRowCount: {} ) = {}", this.address, remoteStatementHandle, offset, fetchMaxRowCount, result );
        }
        return result;
    }


    @Override
    public RemoteExecuteResult execute( RemoteTransactionHandle remoteTransactionHandle, RemoteStatementHandle remoteStatementHandle, List<Common.TypedValue> parameterValues, int maxRowsInFirstFrame ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: execute( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {}, maxRowsInFirstFrame: {} )", this.address, remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame );
        }

        final RemoteExecuteResult result = this.callRemoteMethod( Method.execute( remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame ) );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: execute( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {}, maxRowsInFirstFrame: {} ) = {}", this.address, remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame, result );
        }
        return result;
    }


    @Override
    public Void closeStatement( RemoteStatementHandle remoteStatementHandle ) throws RemoteException {

        // TODO

        return null; //
    }


    @Override
    public Void closeConnection( RemoteConnectionHandle remoteConnectionHandle ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: closeConnection( remoteConnectionHandle: {} )", this.address, remoteConnectionHandle );
        }

        /*final Void result =*/
        this.callRemoteMethod( Method.closeConnection( remoteConnectionHandle ) );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: closeConnection( remoteConnectionHandle: {} ) = {}", this.address, remoteConnectionHandle, "{VOID}" );
        }

        return null; //
    }


    @Override
    public Void abortConnection( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: abortConnection( remoteConnectionHandle: {}, remoteTransactionHandle: {} )", this.address, remoteConnectionHandle, remoteTransactionHandle );
        }

        /*final Void result =*/
        this.callRemoteMethod( Method.abort( remoteConnectionHandle, remoteTransactionHandle ) );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: abortConnection( remoteConnectionHandle: {}, remoteTransactionHandle: {} ) = {}", this.address, remoteConnectionHandle, remoteTransactionHandle, "{VOID}" );
        }

        return null; //
    }


    @Override
    public Common.ConnectionProperties connectionSync( RemoteConnectionHandle remoteConnectionHandle, Common.ConnectionProperties properties ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: abortConnection( remoteConnectionHandle: {}, properties: {} )", this.address, remoteConnectionHandle, properties );
        }

        final Common.ConnectionProperties result = this.callRemoteMethod( Method.connectionSync( remoteConnectionHandle, properties ) );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: abortConnection( remoteConnectionHandle: {}, properties: {} ) = {}", this.address, remoteConnectionHandle, properties, "{VOID}" );
        }
        return result;
    }


    @Override
    public Void onePhaseCommit( RemoteConnectionHandle remoteConnectionHandle, RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: onePhaseCommit( remoteConnectionHandle: {}, remoteTransactionHandle: {} )", this.address, remoteConnectionHandle, remoteTransactionHandle );
        }

        /*final Void result =*/
        this.callRemoteMethod( Method.onePhaseCommit( remoteConnectionHandle, remoteTransactionHandle ) );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: onePhaseCommit( remoteConnectionHandle: {}, remoteTransactionHandle: {} ) = {}", this.address, remoteConnectionHandle, remoteTransactionHandle, "{VOID}" );
        }

        return null; //
    }


    @Override
    public boolean prepareCommit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: prepareCommit( remoteConnectionHandle: {}, remoteTransactionHandle: {} )", this.address, remoteConnectionHandle, remoteTransactionHandle );
        }

        final boolean result = this.callRemoteMethod( Method.prepareCommit( remoteConnectionHandle, remoteTransactionHandle ) );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: prepareCommit( remoteConnectionHandle: {}, remoteTransactionHandle: {} ) = {}", this.address, remoteConnectionHandle, remoteTransactionHandle, result );
        }
        return result;
    }


    @Override
    public Void commit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: commit( remoteConnectionHandle: {}, remoteTransactionHandle: {} )", this.address, remoteConnectionHandle, remoteTransactionHandle );
        }

        /*final Void result =*/
        this.callRemoteMethod( Method.commit( remoteConnectionHandle, remoteTransactionHandle ) );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: commit( remoteConnectionHandle: {}, remoteTransactionHandle: {} ) = {}", this.address, remoteConnectionHandle, remoteTransactionHandle, "{VOID}" );
        }

        return null; //
    }


    @Override
    public Void rollback( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: rollback( remoteConnectionHandle: {}, remoteTransactionHandle: {} )", this.address, remoteConnectionHandle, remoteTransactionHandle );
        }

        /*final Void result =*/
        this.callRemoteMethod( Method.rollback( remoteConnectionHandle, remoteTransactionHandle ) );

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "{}: rollback( remoteConnectionHandle: {}, remoteTransactionHandle: {} ) = {}", this.address, remoteConnectionHandle, remoteTransactionHandle, "{VOID}" );
        }

        return null; //
    }


    @Override
    public String toString() {
        return this.address.toString();
    }
}
