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


import com.typesafe.config.ConfigFactory;
import io.micrometer.core.instrument.Metrics;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.proto.Common.ConnectionProperties;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.apache.calcite.sql.SqlNode;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.Message.Flag;
import org.jgroups.View;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.protocols.BARRIER;
import org.jgroups.protocols.BPING;
import org.jgroups.protocols.FD_ALL;
import org.jgroups.protocols.FD_SOCK;
import org.jgroups.protocols.FRAG2;
import org.jgroups.protocols.MERGE3;
import org.jgroups.protocols.MFC;
import org.jgroups.protocols.MULTI_PING;
import org.jgroups.protocols.RSVP;
import org.jgroups.protocols.UDP;
import org.jgroups.protocols.UFC;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.VERIFY_SUSPECT;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.pbcast.STATE_TRANSFER;
import org.jgroups.stack.Protocol;
import org.jgroups.util.RspList;
import org.polypheny.fram.remote.RemoteMeta.Method;
import org.polypheny.fram.remote.types.RemoteConnectionHandle;
import org.polypheny.fram.remote.types.RemoteExecuteBatchResult;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.remote.types.RemoteFrame;
import org.polypheny.fram.remote.types.RemoteStatementHandle;
import org.polypheny.fram.remote.types.RemoteTransactionHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Cluster implements MembershipListener {

    static {
        // IPv6 likely breaks things...
        System.setProperty( "java.net.preferIPv4Stack", "true" );
    }


    private static final Collection<RemoteNode> ALL_NODES_IN_THE_CLUSTER = null;

    private static final Logger LOGGER = LoggerFactory.getLogger( Cluster.class );

    private static final Map<UUID, Cluster> CLUSTER_REGISTRY = new HashMap<>();


    public static Cluster getDefaultCluster() {
        Cluster _default = DefaultCluster.DEFAULT_CLUSTER_INSTANCE;
        return CLUSTER_REGISTRY.getOrDefault( UUID.nameUUIDFromBytes( _default.clusterName.getBytes( StandardCharsets.UTF_8 ) ), _default );
    }


    public static Cluster getCluster( final UUID clusterId ) {
        if ( clusterId == null ) {
            return getDefaultCluster();
        }
        return CLUSTER_REGISTRY.get( clusterId );
    }


    public static Cluster getCluster( final String clusterName ) {
        if ( clusterName == null || clusterName.isEmpty() ) {
            return getDefaultCluster();
        }
        return getCluster( UUID.nameUUIDFromBytes( clusterName.getBytes( StandardCharsets.UTF_8 ) ) );
    }


    @Getter
    @EqualsAndHashCode.Include
    private final UUID clusterId;
    @EqualsAndHashCode.Include
    private final String clusterName;
    @EqualsAndHashCode.Include
    private final int clusterPort;

    private final JChannel channel;
    private final Thread channelShutdownHook;

    private final RpcDispatcher rpc;
    private static final RequestOptions DEFAULT_REQUEST_OPTIONS = RequestOptions.SYNC()
            .anycasting( true )
            .setTimeout( TimeUnit.SECONDS.toMillis( 0 /* == infinite */ ) )
            .setFlags( Flag.OOB )
            .setFlags( Flag.DONT_BUNDLE )
            .setFlags( Flag.RSVP );

    private final ConcurrentMap<Address, RemoteNode> currentNodes = new ConcurrentHashMap<>();
    private View currentView = null;

    private RemoteNode thisRemoteNode;
    private RemoteMeta thisMeta;
    private AbstractLocalNode thisLocalNode;


    private Cluster( final String clusterName, final int clusterPort ) throws Exception {
        this.clusterId = UUID.nameUUIDFromBytes( clusterName.getBytes( StandardCharsets.UTF_8 ) );
        this.clusterName = clusterName;
        this.clusterPort = clusterPort;
        this.channel = createChannel( null /* == all interfaces */, clusterPort );
        this.rpc = createRpcDispatcher( this.channel );

        this.channel.setDiscardOwnMessages( false );

        this.rpc.setMethodLookup( RemoteMeta.Method::findMethod );
        this.rpc.setMembershipListener( this );

        this.channelShutdownHook = new Thread( channel::close, "Shutdown Hook for Cluster \"" + clusterName + "\" [" + clusterId + "]" );
        Runtime.getRuntime().addShutdownHook( channelShutdownHook );

        CLUSTER_REGISTRY.put( this.clusterId, this );
    }


    @Override
    protected void finalize() throws Throwable {
        try {
            CLUSTER_REGISTRY.remove( this.clusterId );
        } finally {
            super.finalize();
        }
    }


    private JChannel createChannel( final InetAddress bindAddress, final int bindPort ) throws Exception {
        final List<Protocol> protocols = new ArrayList<Protocol>();

        // Programmatically setup of the protocol stack
        // If replaced with file (XML); change connect()
        //
        final UDP udp = new UDP();
        if ( bindAddress == null ) {
            udp.setProperties( Collections.singletonMap( "bind_addr", "GLOBAL" ) );
            //udp.setBindToAllInterfaces( true );
        } else {
            udp.setBindAddress( bindAddress );
        }
        udp.setBindPort( bindPort );
        udp.setMulticasting( true );
        //udp.setMulticasting( false );
        udp.bundler( "no-bundler" );

        protocols.add( udp );
        protocols.add( new BPING() );
        protocols.add( new MULTI_PING() );
        protocols.add( new MERGE3() );
        protocols.add( new FD_SOCK() );
        protocols.add( new FD_ALL() );
        protocols.add( new VERIFY_SUSPECT() );
        protocols.add( new BARRIER() );
        protocols.add( new NAKACK2() );
        protocols.add( new UNICAST3() );
        protocols.add( new RSVP() );
        protocols.add( new STABLE() );
        protocols.add( new GMS() );
        protocols.add( new UFC() );
        protocols.add( new MFC() );
        protocols.add( new FRAG2() );
        protocols.add( new STATE_TRANSFER() );

        return new JChannel( protocols );
    }


    private RpcDispatcher createRpcDispatcher( final JChannel channel ) {
        return new RpcDispatcher( channel, null );
    }


    public synchronized Cluster connect( final RemoteMeta serverObject ) {
        if ( LOGGER.isInfoEnabled() ) {
            LOGGER.info( "Connecting to cluster" );
        }

        if ( !(serverObject instanceof AbstractLocalNode) ) {
            throw new IllegalArgumentException( "serverObject is required to be an instance of AbstractLocalNode" );
        }

        try {
            if ( this.channel.isOpen() ) {
                this.rpc.setServerObject( serverObject );
                this.thisMeta = serverObject;
                this.thisLocalNode = (AbstractLocalNode) serverObject;
            }

            this.channel.connect( this.clusterName );
            assert this.channel.isConnected();

            this.thisRemoteNode = new RemoteNode( this.channel.getAddress(), this );

            return this;
        } catch ( Exception e ) {
            // Problems with the protocol stack
            // Change if the stack is not done programmatically
            throw new RuntimeException( e );
        }
    }


    public void disconnect() {
        this.channel.disconnect();
    }


    public RemoteNode thisNode() {
        if ( this.channel.isConnected() ) {
            return thisRemoteNode;
        }
        throw new IllegalStateException( "This cluster is not connected." );
    }


    private final AtomicInteger numberOfViewMembers = Metrics.gauge( Cluster.class.getSimpleName() + "." + "currentView.size", new AtomicInteger( 0 ) );


    @Override
    public void viewAccepted( final View newView ) {
        if ( LOGGER.isInfoEnabled() ) {
            LOGGER.info( "Cluster has changed. New view: {}", newView );
        }

        synchronized ( this ) {
            if ( this.currentView == null ) {
                addNodesToTheCluster( newView.getMembers() );
            } else {
                removeNodesFromTheCluster( View.leftMembers( this.currentView, newView ) );
                addNodesToTheCluster( View.newMembers( this.currentView, newView ) );
            }
            this.currentView = newView;
            this.numberOfViewMembers.set( this.currentView.size() );
            notifyNewView( newView );
        }
    }


    private void notifyNewView( final View newView ) {
        // TODO: DataDistributionUnitFrontend.getInstance().viewAccepted( this, newView );
    }


    private void addNodesToTheCluster( final List<Address> newMembers ) {
        if ( newMembers == null || newMembers.isEmpty() ) {
            return;
        }

        synchronized ( this.currentNodes ) {
            for ( Address newMember : newMembers ) {
                if ( LOGGER.isInfoEnabled() ) {
                    LOGGER.info( "Node {} has joined.", newMember );
                }
                this.currentNodes.put( newMember, new RemoteNode( newMember, this ) );
            }
        }
    }


    private void removeNodesFromTheCluster( final List<Address> leftMembers ) {
        if ( leftMembers == null || leftMembers.isEmpty() ) {
            return;
        }

        synchronized ( this.currentNodes ) {
            for ( Address leftMember : leftMembers ) {
                if ( LOGGER.isInfoEnabled() ) {
                    LOGGER.info( "Node {} has left.", leftMember );
                }
                this.currentNodes.remove( leftMember );
            }
        }
    }


    /*
     *
     *
     *
     *
     */


    protected <ReturnType> ReturnType callMethod( final MethodCall method, final RemoteNode remoteNode ) throws RemoteException {
        return this.callMethod( DEFAULT_REQUEST_OPTIONS, method, remoteNode );
    }


    protected <ReturnType> ReturnType callMethod( final RequestOptions requestOptions, final MethodCall method, final RemoteNode remoteNode ) throws RemoteException {
        return this.doCallMethod( requestOptions, method, remoteNode == null ? null : remoteNode.getNodeAddress() );
    }


    private <ReturnType> ReturnType doCallMethod( final RequestOptions requestOptions, final MethodCall method, final Address remoteNodeAddress ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "doCallMethod( requestOptions: {}, method: {}, remoteNodeAddress: {} )", requestOptions, method, remoteNodeAddress );
        }

        final ReturnType result;
        try {
            result = this.rpc.callRemoteMethod( remoteNodeAddress, method, requestOptions );
        } catch ( RemoteException | RuntimeException ex ) {
            if ( LOGGER.isDebugEnabled() ) {
                LOGGER.debug( "re-throwing {}", ex );
            }
            throw ex;
        } catch ( Exception ex ) {
            if ( LOGGER.isDebugEnabled() ) {
                LOGGER.debug( "Wrapping Exception {}", ex );
            }
            throw new RemoteException( ex.getMessage(), ex );
        }

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "doCallMethod( requestOptions: {}, method: {}, remoteNodeAddress: {} ) = {}", requestOptions, method, remoteNodeAddress, result );
        }
        return result;
    }


    /*
     *
     */


    protected <ReturnType> CompletableFuture<ReturnType> asyncCallMethod( final MethodCall method, final RemoteNode remoteNode ) throws RemoteException {
        return this.asyncCallMethod( DEFAULT_REQUEST_OPTIONS, method, remoteNode );
    }


    protected <ReturnType> CompletableFuture<ReturnType> asyncCallMethod( final RequestOptions requestOptions, final MethodCall method, final RemoteNode remoteNode ) throws RemoteException {
        return this.doAsyncCallMethod( requestOptions, method, remoteNode == null ? null : remoteNode.getNodeAddress() );
    }


    private <ReturnType> CompletableFuture<ReturnType> doAsyncCallMethod( final RequestOptions requestOptions, final MethodCall method, final Address remoteNodeAddress ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "doAsyncCallMethod( requestOptions: {}, method: {}, remoteNodeAddress: {} )", requestOptions, method, remoteNodeAddress );
        }

        final CompletableFuture<ReturnType> result;
        try {
            result = this.rpc.callRemoteMethodWithFuture( remoteNodeAddress, method, requestOptions );
        } catch ( RemoteException | RuntimeException ex ) {
            if ( LOGGER.isDebugEnabled() ) {
                LOGGER.debug( "re-throwing {}", ex );
            }
            throw ex;
        } catch ( Exception ex ) {
            if ( LOGGER.isDebugEnabled() ) {
                LOGGER.debug( "Wrapping Exception {}", ex );
            }
            throw new RemoteException( ex.getMessage(), ex );
        }

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "doAsyncCallMethod( requestOptions: {}, method: {}, remoteNodeAddress: {} ) = {}", requestOptions, method, remoteNodeAddress, result );
        }
        return result;
    }


    /*
     *
     */


    protected <ReturnType> RspList<ReturnType> callMethods( final MethodCall method, final Collection<RemoteNode> remoteNodes ) throws RemoteException {
        return this.callMethods( DEFAULT_REQUEST_OPTIONS, method, remoteNodes );
    }


    protected <ReturnType> RspList<ReturnType> callMethods( final RequestOptions requestOptions, final MethodCall method, final Collection<RemoteNode> remoteNodes ) throws RemoteException {
        return this.doCallMethods( requestOptions, method, remoteNodes == null ? null : remoteNodes.stream().map( RemoteNode::getNodeAddress ).collect( Collectors.toList() ) );
    }


    private <ReturnType> RspList<ReturnType> doCallMethods( final RequestOptions requestOptions, final MethodCall method, final Collection<Address> remoteNodeAddresses ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "doCallMethods( requestOptions: {}, method: {}, remoteNodeAddresses: {} )", requestOptions, method, remoteNodeAddresses == null ? "<CLUSTER>" : remoteNodeAddresses.stream().map( Object::toString ).collect( Collectors.joining( "," ) ) );
        }

        final RspList<ReturnType> result;
        try {
            if ( method.getMethodId() == 30 /*COMMIT*/ ) {
                LOGGER.error( "SENDING COMMIT ..." );
            }
            result = this.rpc.callRemoteMethods( remoteNodeAddresses, method, requestOptions );
            if ( method.getMethodId() == 30 /*COMMIT*/ ) {
                LOGGER.error( "SENDING COMMIT ... DONE" );
            }
        } catch ( RemoteException | RuntimeException ex ) {
            if ( LOGGER.isDebugEnabled() ) {
                LOGGER.debug( "re-throwing {}", ex );
            }
            throw ex;
        } catch ( Exception ex ) {
            if ( LOGGER.isDebugEnabled() ) {
                LOGGER.debug( "Wrapping Exception {}", ex );
            }
            throw new RemoteException( ex.getMessage(), ex );
        }

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "doCallMethods( requestOptions: {}, method: {}, remoteNodeAddresses: {} ) = {}", requestOptions, method, remoteNodeAddresses == null ? "<CLUSTER>" : remoteNodeAddresses.stream().map( Object::toString ).collect( Collectors.joining( "," ) ), result );
        }
        return result;
    }


    /*
     *
     */


    protected <ReturnType> CompletableFuture<RspList<ReturnType>> asyncCallMethods( final MethodCall method, final Collection<RemoteNode> remoteNodes ) throws RemoteException {
        return this.asyncCallMethods( DEFAULT_REQUEST_OPTIONS, method, remoteNodes );
    }


    protected <ReturnType> CompletableFuture<RspList<ReturnType>> asyncCallMethods( final RequestOptions requestOptions, final MethodCall method, final Collection<RemoteNode> remoteNodes ) throws RemoteException {
        return this.doAsyncCallMethods( requestOptions, method, remoteNodes == null ? null : remoteNodes.stream().map( RemoteNode::getNodeAddress ).collect( Collectors.toList() ) );
    }


    protected <ReturnType> CompletableFuture<RspList<ReturnType>> doAsyncCallMethods( final RequestOptions requestOptions, final MethodCall method, final Collection<Address> remoteNodeAddresses ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "doAsyncCallMethods( requestOptions: {}, method: {}, remoteNodeAddresses: {} )", requestOptions, method, remoteNodeAddresses );
        }

        final CompletableFuture<RspList<ReturnType>> result;
        try {
            result = this.rpc.callRemoteMethodsWithFuture( remoteNodeAddresses, method, requestOptions );
        } catch ( RemoteException | RuntimeException ex ) {
            if ( LOGGER.isDebugEnabled() ) {
                LOGGER.debug( "re-throwing {}", ex );
            }
            throw ex;
        } catch ( Exception ex ) {
            if ( LOGGER.isDebugEnabled() ) {
                LOGGER.debug( "Wrapping Exception {}", ex );
            }
            throw new RemoteException( ex.getMessage(), ex );
        }

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "doAsyncCallMethods( requestOptions: {}, method: {}, remoteNodeAddresses: {} ) = {}", requestOptions, method, remoteNodeAddresses, result );
        }
        return result;
    }


    /*
     *
     *
     *
     *
     */


    public RspList<RemoteStatementHandle> prepare( final RemoteStatementHandle remoteStatementHandle, final SqlNode sql, final long maxRowCount ) throws RemoteException {
        return this.prepare( remoteStatementHandle, sql, maxRowCount, ALL_NODES_IN_THE_CLUSTER );
    }


    public RspList<RemoteStatementHandle> prepare( final RemoteStatementHandle remoteStatementHandle, final SqlNode sql, final long maxRowCount, final RemoteNode... remoteNodes ) throws RemoteException {
        return this.prepare( remoteStatementHandle, sql, maxRowCount, remoteNodes == null ? null : Arrays.asList( remoteNodes ) );
    }


    public RspList<RemoteStatementHandle> prepare( final RemoteStatementHandle remoteStatementHandle, final SqlNode sql, final long maxRowCount, final Collection<RemoteNode> remoteNodes ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepare( remoteStatementHandle: {}, sql: {}, maxRowCount: {}, remoteNodes: {} )", remoteStatementHandle, sql, maxRowCount, remoteNodes );
        }

        final String serializedSql = sql.toSqlString( getLocalNode().getSqlDialect() ).getSql();
        final RspList<RemoteStatementHandle> result;

        if ( remoteNodes != null && remoteNodes.size() == 1 && remoteNodes.contains( thisRemoteNode ) ) {
            if ( LOGGER.isTraceEnabled() ) {
                LOGGER.trace( "prepare( remoteStatementHandle: {}, sql: {}, maxRowCount: {}, remoteNodes: {} ) -- Bypassing network stack for local call.", remoteStatementHandle, sql, maxRowCount, remoteNodes );
            }
            result = new RspList<>( 1 );
            result.addRsp( thisRemoteNode.getNodeAddress(), thisMeta.prepare( remoteStatementHandle, serializedSql, maxRowCount ) );
        } else {
            result = this.callMethods( Method.prepare( remoteStatementHandle, serializedSql, maxRowCount ), remoteNodes );
        }

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepare( remoteStatementHandle: {}, sql: {}, maxRowCount: {}, remoteNodes: {} ) = {}", remoteStatementHandle, sql, maxRowCount, remoteNodes, result );
        }
        return result;
    }

    //


    public RspList<Void> closeStatement( final RemoteStatementHandle remoteStatementHandle ) throws RemoteException {
        return this.closeStatement( remoteStatementHandle, ALL_NODES_IN_THE_CLUSTER );
    }


    public RspList<Void> closeStatement( final RemoteStatementHandle remoteStatementHandle, final RemoteNode... remoteNodes ) throws RemoteException {
        return this.closeStatement( remoteStatementHandle, remoteNodes == null ? null : Arrays.asList( remoteNodes ) );
    }


    public RspList<Void> closeStatement( final RemoteStatementHandle remoteStatementHandle, final Collection<RemoteNode> remoteNodes ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "closeStatement( remoteStatementHandle: {}, remoteNodes: {} )", remoteStatementHandle, remoteNodes );
        }

        final RspList<Void> result;

        if ( remoteNodes != null && remoteNodes.size() == 1 && remoteNodes.contains( thisRemoteNode ) ) {
            if ( LOGGER.isTraceEnabled() ) {
                LOGGER.trace( "closeStatement( remoteStatementHandle: {}, remoteNodes: {} ) -- Bypassing network stack for local call.", remoteStatementHandle, remoteNodes );
            }
            result = new RspList<>( 1 );
            result.addRsp( thisRemoteNode.getNodeAddress(), thisMeta.closeStatement( remoteStatementHandle ) );
        } else {
            result = this.callMethods( Method.closeStatement( remoteStatementHandle ), remoteNodes );
        }

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "closeStatement( remoteStatementHandle: {}, remoteNodes: {} ) = {}", remoteStatementHandle, remoteNodes, result );
        }
        return result;
    }


    /*
     *
     */


    public RspList<RemoteExecuteResult> prepareAndExecute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame ) throws RemoteException {
        return this.prepareAndExecute( remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame, ALL_NODES_IN_THE_CLUSTER );
    }


    public RspList<RemoteExecuteResult> prepareAndExecute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final RemoteNode... remoteNodes ) throws RemoteException {
        return this.prepareAndExecute( remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame, remoteNodes == null ? null : Arrays.asList( remoteNodes ) );
    }


    public RspList<RemoteExecuteResult> prepareAndExecute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final Collection<RemoteNode> remoteNodes ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepareAndExecute( remoteTransactionHandle: {}, remoteStatementHandle: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, remoteNodes: {} )", remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame, remoteNodes );
        }

        final String serializedSql = sql.toSqlString( getLocalNode().getSqlDialect() ).getSql()
                // HSQLDB does not accept an expression as DEFAULT value. The toSqlString method, however, creates an expression in parentheses. Thus, we have to "extract" the value.
                .replaceAll( "DEFAULT \\(([^)]*)\\)", "DEFAULT $1" ); // search for everything between '(' and ')' which does not include a ')'. For now, this should cover most cases.
        final RspList<RemoteExecuteResult> result;

        if ( remoteNodes != null && remoteNodes.size() == 1 && remoteNodes.contains( thisRemoteNode ) ) {
            if ( LOGGER.isTraceEnabled() ) {
                LOGGER.trace( "prepareAndExecute( remoteTransactionHandle: {}, remoteStatementHandle: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, remoteNodes: {} ) -- Bypassing network stack for local call.", remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame, remoteNodes );
            }
            result = new RspList<>( 1 );
            result.addRsp( thisRemoteNode.getNodeAddress(), thisMeta.prepareAndExecute( remoteTransactionHandle, remoteStatementHandle, serializedSql, maxRowCount, maxRowsInFirstFrame ) );
        } else {
            result = this.callMethods( Method.prepareAndExecute( remoteTransactionHandle, remoteStatementHandle, serializedSql, maxRowCount, maxRowsInFirstFrame ), remoteNodes );
        }

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepareAndExecute( remoteTransactionHandle: {}, remoteStatementHandle: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, remoteNodes: {} ) = {}", remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame, remoteNodes, result );
        }
        return result;
    }


    /*
     *
     */


    public RspList<RemoteExecuteBatchResult> prepareAndExecuteBatch( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<SqlNode> sqlCommands ) throws RemoteException {
        return this.prepareAndExecuteBatch( remoteTransactionHandle, remoteStatementHandle, sqlCommands, ALL_NODES_IN_THE_CLUSTER );
    }


    public RspList<RemoteExecuteBatchResult> prepareAndExecuteBatch( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<SqlNode> sqlCommands, final RemoteNode... remoteNodes ) throws RemoteException {
        return this.prepareAndExecuteBatch( remoteTransactionHandle, remoteStatementHandle, sqlCommands, remoteNodes == null ? null : Arrays.asList( remoteNodes ) );
    }


    public RspList<RemoteExecuteBatchResult> prepareAndExecuteBatch( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<SqlNode> sqlCommands, final Collection<RemoteNode> remoteNodes ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepareAndExecuteBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, sqlCommands: {}, remoteNodes: {} )", remoteTransactionHandle, remoteStatementHandle, sqlCommands, remoteNodes );
        }

        List<String> serializedSqlCommands = sqlCommands.stream().map( sqlNode -> sqlNode.toSqlString( getLocalNode().getSqlDialect() ).getSql() ).collect( Collectors.toList() );
        final RspList<RemoteExecuteBatchResult> result;

        if ( remoteNodes != null && remoteNodes.size() == 1 && remoteNodes.contains( thisRemoteNode ) ) {
            if ( LOGGER.isTraceEnabled() ) {
                LOGGER.trace( "prepareAndExecuteBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, sqlCommands: {}, remoteNodes: {} ) -- Bypassing network stack for local call.", remoteTransactionHandle, remoteStatementHandle, sqlCommands, remoteNodes );
            }
            result = new RspList<>( 1 );
            result.addRsp( thisRemoteNode.getNodeAddress(), thisMeta.prepareAndExecuteBatch( remoteTransactionHandle, remoteStatementHandle, serializedSqlCommands ) );
        } else {
            result = this.callMethods( Method.prepareAndExecuteBatch( remoteTransactionHandle, remoteStatementHandle, serializedSqlCommands ), remoteNodes );
        }

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepareAndExecuteBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, sqlCommands: {}, remoteNodes: {} ) = {}", remoteTransactionHandle, remoteStatementHandle, sqlCommands, remoteNodes, result );
        }

        return result;
    }


    /*
     *
     */


    public RspList<RemoteExecuteBatchResult> executeBatch( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<UpdateBatch> parameterValues ) throws RemoteException {
        return this.executeBatch( remoteTransactionHandle, remoteStatementHandle, parameterValues, ALL_NODES_IN_THE_CLUSTER );
    }


    public RspList<RemoteExecuteBatchResult> executeBatch( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<UpdateBatch> parameterValues, final RemoteNode... remoteNodes ) throws RemoteException {
        return this.executeBatch( remoteTransactionHandle, remoteStatementHandle, parameterValues, remoteNodes == null ? null : Arrays.asList( remoteNodes ) );
    }


    public RspList<RemoteExecuteBatchResult> executeBatch( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<UpdateBatch> parameterValues, final Collection<RemoteNode> remoteNodes ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "executeBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {}, remoteNodes: {} )", remoteTransactionHandle, remoteStatementHandle, parameterValues, remoteNodes );
        }

        final RspList<RemoteExecuteBatchResult> result;

        if ( remoteNodes != null && remoteNodes.size() == 1 && remoteNodes.contains( thisRemoteNode ) ) {
            if ( LOGGER.isTraceEnabled() ) {
                LOGGER.trace( "executeBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {}, remoteNodes: {} ) -- Bypassing network stack for local call.", remoteTransactionHandle, remoteStatementHandle, parameterValues, remoteNodes );
            }
            result = new RspList<>( 1 );
            result.addRsp( thisRemoteNode.getNodeAddress(), thisMeta.executeBatch( remoteTransactionHandle, remoteStatementHandle, parameterValues ) );
        } else {
            result = this.callMethods( Method.executeBatch( remoteTransactionHandle, remoteStatementHandle, parameterValues ), remoteNodes );
        }

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "executeBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {}, remoteNodes: {} ) = {}", remoteTransactionHandle, remoteStatementHandle, parameterValues, remoteNodes, result );
        }

        return result;
    }


    /*
     *
     */


    public RspList<RemoteFrame> fetch( final RemoteStatementHandle remoteStatementHandle, final long offset, final int fetchMaxRowCount ) throws RemoteException {
        return this.fetch( remoteStatementHandle, offset, fetchMaxRowCount, ALL_NODES_IN_THE_CLUSTER );
    }


    public RspList<RemoteFrame> fetch( final RemoteStatementHandle remoteStatementHandle, final long offset, final int fetchMaxRowCount, final RemoteNode... remoteNodes ) throws RemoteException {
        return this.fetch( remoteStatementHandle, offset, fetchMaxRowCount, remoteNodes == null ? null : Arrays.asList( remoteNodes ) );
    }


    public RspList<RemoteFrame> fetch( final RemoteStatementHandle remoteStatementHandle, final long offset, final int fetchMaxRowCount, final Collection<RemoteNode> remoteNodes ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "fetch( remoteStatementHandle: {}, offset: {}, fetchMaxRowCount: {}, remoteNodes: {} )", remoteStatementHandle, offset, fetchMaxRowCount, remoteNodes );
        }

        final RspList<RemoteFrame> result;

        if ( remoteNodes != null && remoteNodes.size() == 1 && remoteNodes.contains( thisRemoteNode ) ) {
            if ( LOGGER.isTraceEnabled() ) {
                LOGGER.trace( "fetch( remoteStatementHandle: {}, offset: {}, fetchMaxRowCount: {}, remoteNodes: {} ) -- Bypassing network stack for local call.", remoteStatementHandle, offset, fetchMaxRowCount, remoteNodes );
            }
            result = new RspList<>( 1 );
            result.addRsp( thisRemoteNode.getNodeAddress(), thisMeta.fetch( remoteStatementHandle, offset, fetchMaxRowCount ) );
        } else {
            result = this.callMethods( Method.fetch( remoteStatementHandle, offset, fetchMaxRowCount ), remoteNodes );
        }

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "fetch( remoteStatementHandle: {}, offset: {}, fetchMaxRowCount: {}, remoteNodes: {} ) = {}", remoteStatementHandle, offset, fetchMaxRowCount, remoteNodes, result );
        }

        return result;
    }


    /*
     *
     */


    public RspList<RemoteExecuteResult> execute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<Common.TypedValue> parameterValues, final int maxRowsInFirstFrame ) throws RemoteException {
        return this.execute( remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame, ALL_NODES_IN_THE_CLUSTER );
    }


    public RspList<RemoteExecuteResult> execute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<Common.TypedValue> parameterValues, final int maxRowsInFirstFrame, final RemoteNode... remoteNodes ) throws RemoteException {
        return this.execute( remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame, remoteNodes == null ? null : Arrays.asList( remoteNodes ) );
    }


    public RspList<RemoteExecuteResult> execute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<Common.TypedValue> parameterValues, final int maxRowsInFirstFrame, final Collection<RemoteNode> remoteNodes ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "execute( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {}, maxRowsInFirstFrame: {}, remoteNodes: {} )", remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame, remoteNodes );
        }

        final RspList<RemoteExecuteResult> result;

        if ( remoteNodes != null && remoteNodes.size() == 1 && remoteNodes.contains( thisRemoteNode ) ) {
            if ( LOGGER.isTraceEnabled() ) {
                LOGGER.trace( "execute( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {}, maxRowsInFirstFrame: {}, remoteNodes: {} ) -- Bypassing network stack for local call.", remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame, remoteNodes );
            }
            result = new RspList<>( 1 );
            result.addRsp( thisRemoteNode.getNodeAddress(), thisMeta.execute( remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame ) );
        } else {
            result = this.callMethods( Method.execute( remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame ), remoteNodes );
        }

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "execute( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {}, maxRowsInFirstFrame: {}, remoteNodes: {} ) = {}", remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame, remoteNodes, result );
        }

        return result;
    }


    /*
     *
     */


    public RspList<Void> onePhaseCommit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        return this.commit( remoteConnectionHandle, remoteTransactionHandle, ALL_NODES_IN_THE_CLUSTER );
    }


    public RspList<Void> onePhaseCommit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle, final RemoteNode... remoteNodes ) throws RemoteException {
        return this.commit( remoteConnectionHandle, remoteTransactionHandle, remoteNodes == null ? null : Arrays.asList( remoteNodes ) );
    }


    public RspList<Void> onePhaseCommit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle, final Collection<RemoteNode> remoteNodes ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "commit( remoteConnectionHandle: {}, remoteTransactionHandle: {}, remoteNodes: {} )", remoteConnectionHandle, remoteTransactionHandle, remoteNodes );
        }

        final RspList<Void> result;

        if ( remoteNodes != null && remoteNodes.size() == 1 && remoteNodes.contains( thisRemoteNode ) ) {
            if ( LOGGER.isTraceEnabled() ) {
                LOGGER.trace( "commit( remoteConnectionHandle: {}, remoteTransactionHandle: {}, remoteNodes: {} ) -- Bypassing network stack for local call.", remoteConnectionHandle, remoteTransactionHandle, remoteNodes );
            }
            result = new RspList<>( 1 );
            result.addRsp( thisRemoteNode.getNodeAddress(), thisMeta.onePhaseCommit( remoteConnectionHandle, remoteTransactionHandle ) );
        } else if ( remoteNodes != null && remoteNodes.size() == 1 ) {
            final RemoteNode theNode = remoteNodes.iterator().next();
            if ( LOGGER.isTraceEnabled() ) {
                LOGGER.trace( "Calling commit( remoteConnectionHandle: {}, remoteTransactionHandle: {} ) on {}", remoteConnectionHandle, remoteTransactionHandle, theNode );
            }
            result = new RspList<>( 1 );
            result.addRsp( theNode.getNodeAddress(), theNode.onePhaseCommit( remoteConnectionHandle, remoteTransactionHandle ) );
        } else {
            LOGGER.warn( "Executing a One Phase Commit on multiple nodes." );
            result = this.callMethods( Method.onePhaseCommit( remoteConnectionHandle, remoteTransactionHandle ), remoteNodes );
        }

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "commit( remoteConnectionHandle: {}, remoteTransactionHandle: {}, remoteNodes: {} ) = {}", remoteConnectionHandle, remoteTransactionHandle, remoteNodes, result );
        }
        return result;
    }


    /*
     *
     */


    public RspList<Boolean> prepareCommit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        return this.prepareCommit( remoteConnectionHandle, remoteTransactionHandle, ALL_NODES_IN_THE_CLUSTER );
    }


    public RspList<Boolean> prepareCommit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle, final RemoteNode... remoteNodes ) throws RemoteException {
        return this.prepareCommit( remoteConnectionHandle, remoteTransactionHandle, remoteNodes == null ? null : Arrays.asList( remoteNodes ) );
    }


    public RspList<Boolean> prepareCommit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle, final Collection<RemoteNode> remoteNodes ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepareCommit( remoteConnectionHandle: {}, remoteTransactionHandle: {}, remoteNodes: {} )", remoteConnectionHandle, remoteTransactionHandle, remoteNodes );
        }

        final RspList<Boolean> result;

        if ( remoteNodes != null && remoteNodes.size() == 1 && remoteNodes.contains( thisRemoteNode ) ) {
            if ( LOGGER.isTraceEnabled() ) {
                LOGGER.trace( "prepareCommit( remoteConnectionHandle: {}, remoteTransactionHandle: {}, remoteNodes: {} ) -- Bypassing network stack for local call.", remoteConnectionHandle, remoteTransactionHandle, remoteNodes );
            }
            result = new RspList<>( 1 );
            result.addRsp( thisRemoteNode.getNodeAddress(), thisMeta.prepareCommit( remoteConnectionHandle, remoteTransactionHandle ) );
        } else {
            result = this.callMethods( Method.prepareCommit( remoteConnectionHandle, remoteTransactionHandle ), remoteNodes );
        }

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepareCommit( remoteConnectionHandle: {}, remoteTransactionHandle: {}, remoteNodes: {} ) = {}", remoteConnectionHandle, remoteTransactionHandle, remoteNodes, result );
        }
        return result;
    }


    /*
     *
     */


    public RspList<Void> commit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        return this.commit( remoteConnectionHandle, remoteTransactionHandle, ALL_NODES_IN_THE_CLUSTER );
    }


    public RspList<Void> commit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle, final RemoteNode... remoteNodes ) throws RemoteException {
        return this.commit( remoteConnectionHandle, remoteTransactionHandle, remoteNodes == null ? null : Arrays.asList( remoteNodes ) );
    }


    public RspList<Void> commit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle, final Collection<RemoteNode> remoteNodes ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "commit( remoteConnectionHandle: {}, remoteTransactionHandle: {}, remoteNodes: {} )", remoteConnectionHandle, remoteTransactionHandle, remoteNodes );
        }

        final RspList<Void> result;

        if ( remoteNodes != null && remoteNodes.size() == 1 && remoteNodes.contains( thisRemoteNode ) ) {
            if ( LOGGER.isTraceEnabled() ) {
                LOGGER.trace( "commit( remoteConnectionHandle: {}, remoteTransactionHandle: {}, remoteNodes: {} ) -- Bypassing network stack for local call.", remoteConnectionHandle, remoteTransactionHandle, remoteNodes );
            }
            result = new RspList<>( 1 );
            result.addRsp( thisRemoteNode.getNodeAddress(), thisMeta.commit( remoteConnectionHandle, remoteTransactionHandle ) );
        } else {
            result = this.callMethods( Method.commit( remoteConnectionHandle, remoteTransactionHandle ), remoteNodes );
        }

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "commit( remoteConnectionHandle: {}, remoteTransactionHandle: {}, remoteNodes: {} ) = {}", remoteConnectionHandle, remoteTransactionHandle, remoteNodes, result );
        }
        return result;
    }


    /*
     *
     */


    public RspList<Void> rollback( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        return this.rollback( remoteConnectionHandle, remoteTransactionHandle, ALL_NODES_IN_THE_CLUSTER );
    }


    public RspList<Void> rollback( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle, final RemoteNode... remoteNodes ) throws RemoteException {
        return this.rollback( remoteConnectionHandle, remoteTransactionHandle, remoteNodes == null ? null : Arrays.asList( remoteNodes ) );
    }


    public RspList<Void> rollback( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle, Collection<RemoteNode> remoteNodes ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "rollback( remoteConnectionHandle: {}, remoteTransactionHandle: {}, remoteNodes: {} )", remoteConnectionHandle, remoteTransactionHandle, remoteNodes );
        }

        final RspList<Void> result;

        if ( remoteNodes != null && remoteNodes.size() == 1 && remoteNodes.contains( thisRemoteNode ) ) {
            if ( LOGGER.isTraceEnabled() ) {
                LOGGER.trace( "rollback( remoteConnectionHandle: {}, remoteTransactionHandle: {}, remoteNodes: {} ) -- Bypassing network stack for local call.", remoteConnectionHandle, remoteTransactionHandle, remoteNodes );
            }
            result = new RspList<>( 1 );
            result.addRsp( thisRemoteNode.getNodeAddress(), thisMeta.rollback( remoteConnectionHandle, remoteTransactionHandle ) );
        } else {
            result = this.callMethods( Method.rollback( remoteConnectionHandle, remoteTransactionHandle ), remoteNodes );
        }

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "rollback( remoteConnectionHandle: {}, remoteTransactionHandle: {}, remoteNodes: {} ) = {}", remoteConnectionHandle, remoteTransactionHandle, remoteNodes, result );
        }
        return result;
    }


    /*
     *
     */


    public RspList<ConnectionProperties> connectionSync( final RemoteConnectionHandle remoteConnectionHandle, final ConnectionProperties properties ) throws RemoteException {
        return this.connectionSync( remoteConnectionHandle, properties, ALL_NODES_IN_THE_CLUSTER );
    }


    public RspList<ConnectionProperties> connectionSync( final RemoteConnectionHandle remoteConnectionHandle, final ConnectionProperties properties, final RemoteNode... remoteNodes ) throws RemoteException {
        return this.connectionSync( remoteConnectionHandle, properties, remoteNodes == null ? null : Arrays.asList( remoteNodes ) );
    }


    public RspList<ConnectionProperties> connectionSync( final RemoteConnectionHandle remoteConnectionHandle, final ConnectionProperties properties, final Collection<RemoteNode> remoteNodes ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "connectionSync( remoteConnectionHandle: {}, properties: {}, remoteNodes: {} )", remoteConnectionHandle, properties, remoteNodes );
        }

        final RspList<ConnectionProperties> result;

        if ( remoteNodes != null && remoteNodes.size() == 1 && remoteNodes.contains( thisRemoteNode ) ) {
            if ( LOGGER.isTraceEnabled() ) {
                LOGGER.trace( "connectionSync( remoteConnectionHandle: {}, properties: {}, remoteNodes: {} ) -- Bypassing network stack for local call.", remoteConnectionHandle, properties, remoteNodes );
            }
            result = new RspList<>( 1 );
            result.addRsp( thisRemoteNode.getNodeAddress(), thisMeta.connectionSync( remoteConnectionHandle, properties ) );
        } else {
            result = this.callMethods( Method.connectionSync( remoteConnectionHandle, properties ), remoteNodes );
        }

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "connectionSync( remoteConnectionHandle: {}, properties: {}, remoteNodes: {} ) = {}", remoteConnectionHandle, properties, remoteNodes, result );
        }

        return result;
    }

    //


    public RspList<Void> closeConnection( final RemoteConnectionHandle remoteConnectionHandle ) throws RemoteException {
        return this.closeConnection( remoteConnectionHandle, ALL_NODES_IN_THE_CLUSTER );
    }


    public RspList<Void> closeConnection( final RemoteConnectionHandle remoteConnectionHandle, final RemoteNode... remoteNodes ) throws RemoteException {
        return this.closeConnection( remoteConnectionHandle, remoteNodes == null ? null : Arrays.asList( remoteNodes ) );
    }


    public RspList<Void> closeConnection( final RemoteConnectionHandle remoteConnectionHandle, final Collection<RemoteNode> remoteNodes ) throws RemoteException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "closeConnection( remoteConnectionHandle: {}, remoteNodes: {} )", remoteConnectionHandle, remoteNodes );
        }

        final RspList<Void> result;

        if ( remoteNodes != null && remoteNodes.size() == 1 && remoteNodes.contains( thisRemoteNode ) ) {
            if ( LOGGER.isTraceEnabled() ) {
                LOGGER.trace( "closeConnection( remoteConnectionHandle: {}, remoteNodes: {} ) -- Bypassing network stack for local call.", remoteConnectionHandle, remoteNodes );
            }
            result = new RspList<>( 1 );
            result.addRsp( thisRemoteNode.getNodeAddress(), thisMeta.closeConnection( remoteConnectionHandle ) );
        } else {
            result = this.callMethods( Method.closeConnection( remoteConnectionHandle ), remoteNodes );
        }

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "closeConnection( remoteConnectionHandle: {}, remoteNodes: {} ) = {}", remoteConnectionHandle, remoteNodes, result );
        }
        return result;
    }


    /*
     *
     *
     *
     *
     */


    public RemoteNode getRemoteNode( Address address ) {
        if ( address == null ) {
            return null;
        }

        synchronized ( this ) {
            return this.currentNodes.get( address );
        }
    }


    public List<RemoteNode> getRemoteNodes( List<Address> addresses ) {
        if ( addresses == null ) {
            return null;
        }

        List<RemoteNode> remoteNodes = new LinkedList<>();
        synchronized ( this ) {
            for ( Address address : addresses ) {
                remoteNodes.add( this.getRemoteNode( address ) );
            }
        }
        return remoteNodes;
    }


    public Collection<RemoteNode> getAllMembers() {
        synchronized ( this ) {
            return this.currentNodes.values();
        }
    }


    public View getView() {
        return this.currentView;
    }


    public AbstractLocalNode getLocalNode() {
        return thisLocalNode;
    }


    public RemoteNode getLocalNodeAsRemoteNode() {
        return thisRemoteNode;
    }


    /**
     *
     */
    private static class DefaultCluster {

        private static final Cluster DEFAULT_CLUSTER_INSTANCE;


        static {
            try {
                DEFAULT_CLUSTER_INSTANCE = new Cluster( ConfigFactory.load().getString( "cluster.name" ), ConfigFactory.load().getInt( "cluster.port" ) );
            } catch ( Exception e ) {
                throw new RuntimeException( e );
            }
        }
    }
}
