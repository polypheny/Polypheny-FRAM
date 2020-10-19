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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.calcite.sql.SqlKind;
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
import org.polypheny.fram.protocols.allocation.AllocationSchema;
import org.polypheny.fram.protocols.replication.Replica;
import org.polypheny.fram.protocols.replication.ReplicationSchema;
import org.polypheny.fram.remote.RemoteMeta.Method;
import org.polypheny.fram.remote.types.RemoteConnectionHandle;
import org.polypheny.fram.remote.types.RemoteExecuteBatchResult;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.remote.types.RemoteFrame;
import org.polypheny.fram.remote.types.RemoteStatementHandle;
import org.polypheny.fram.remote.types.RemoteTransactionHandle;
import org.polypheny.fram.standalone.Main;
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


    private static final Collection<PhysicalNode> ALL_NODES_IN_THE_CLUSTER = null;

    private static final Logger LOGGER = LoggerFactory.getLogger( Cluster.class );

    private static final Map<UUID, Cluster> CLUSTER_REGISTRY = new HashMap<>();


    public static UUID getDefaultClusterId() {
        return DefaultCluster.DEFAULT_CLUSTER_ID;
    }


    public static Cluster getDefaultCluster() {
        return CLUSTER_REGISTRY.getOrDefault( getDefaultClusterId(), DefaultCluster.DEFAULT_CLUSTER_INSTANCE );
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
            .flags( Flag.OOB, Flag.DONT_BUNDLE, Flag.RSVP );
    private final long requestTimeoutMillis;

    private final ConcurrentMap<Address, RemoteNode> currentNodes = new ConcurrentHashMap<>();
    private View currentView = null;

    private AbstractLocalNode thisNode;


    private Cluster( final String clusterName, final int clusterPort ) throws Exception {
        this.clusterId = UUID.nameUUIDFromBytes( clusterName.getBytes( StandardCharsets.UTF_8 ) );
        this.clusterName = clusterName;
        this.clusterPort = clusterPort;
        this.requestTimeoutMillis = TimeUnit.SECONDS.toMillis( Math.min( 0L, Main.configuration().getInt( "cluster.requestTimeout_seconds" ) ) );
        this.channel = createChannel( null /* == all interfaces */, clusterPort );
        this.rpc = createRpcDispatcher( this.channel );

        this.channel.setDiscardOwnMessages( false );

        this.rpc.setMethodLookup( RemoteMeta.Method::findMethod );
        this.rpc.setMembershipListener( this );

        this.channelShutdownHook = new Thread( channel::close, "Shutdown Hook for Cluster \"" + clusterName + "\" [" + clusterId + "]" );
        Runtime.getRuntime().addShutdownHook( channelShutdownHook );

        CLUSTER_REGISTRY.put( this.clusterId, this );
    }


    public void removeClusterFromRegistry() {
        CLUSTER_REGISTRY.remove( this.clusterId );
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


    public synchronized Cluster connect( final AbstractLocalNode serverObject ) {
        LOGGER.info( "Connecting to cluster" );

        try {
            if ( this.channel.isOpen() ) {
                this.rpc.setServerObject( serverObject.asRemoteMeta() );
            }

            this.channel.connect( this.clusterName );
            assert this.channel.isConnected();

            serverObject.setNodeAddress( this.clusterId, this.channel.getAddress() );
            this.thisNode = serverObject;

            AllocationSchema.LOCAL_NODE = thisNode;

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


    public AbstractLocalNode thisNode() {
        if ( this.channel.isConnected() ) {
            return thisNode;
        }
        throw new IllegalStateException( "This cluster is not connected." );
    }


    private final AtomicInteger numberOfViewMembers = Metrics.gauge( Cluster.class.getSimpleName() + "." + "currentView.size", new AtomicInteger( 0 ) );


    @Override
    public void viewAccepted( final View newView ) {
        LOGGER.info( "Cluster has changed. New view: {}", newView );

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
        // todo: DataDistributionUnitFrontend.getInstance().viewAccepted( this, newView );
    }


    private void addNodesToTheCluster( final List<Address> newMembers ) {
        if ( newMembers == null || newMembers.isEmpty() ) {
            return;
        }

        synchronized ( this.currentNodes ) {
            for ( Address newMember : newMembers ) {
                LOGGER.info( "Node {} has joined.", newMember );
                final RemoteNode node = new RemoteNode( newMember, this );
                this.currentNodes.put( newMember, node );

                final Replica replica;
                if ( newMember instanceof org.jgroups.util.UUID ) {
                    org.jgroups.util.UUID newMemberAsUUID = (org.jgroups.util.UUID) newMember;
                    replica = new Replica( new UUID( newMemberAsUUID.getMostSignificantBits(), newMemberAsUUID.getLeastSignificantBits() ) );
                } else {
                    throw new UnsupportedOperationException( "Not implemented yet." );
                }

                ReplicationSchema.ALL_REPLICAS.add( replica );
                AllocationSchema.ALL_NODES.add( node );
            }
        }
    }


    private void removeNodesFromTheCluster( final List<Address> leftMembers ) {
        if ( leftMembers == null || leftMembers.isEmpty() ) {
            return;
        }

        synchronized ( this.currentNodes ) {
            for ( Address leftMember : leftMembers ) {
                LOGGER.info( "Node {} has left.", leftMember );
                final RemoteNode node = this.currentNodes.remove( leftMember );

                final Replica replica;
                if ( leftMember instanceof org.jgroups.util.UUID ) {
                    org.jgroups.util.UUID newMemberAsUUID = (org.jgroups.util.UUID) leftMember;
                    replica = new Replica( new UUID( newMemberAsUUID.getMostSignificantBits(), newMemberAsUUID.getLeastSignificantBits() ) );
                } else {
                    throw new UnsupportedOperationException( "Not implemented yet." );
                }

                AllocationSchema.ALL_NODES.remove( node );
                ReplicationSchema.ALL_REPLICAS.remove( replica );
            }
        }
    }


    public String serializeSql( SqlNode sql ) {
        String serializedSql = sql.toSqlString( this.getLocalNode().getSqlDialect() ).getSql();
        if ( sql.isA( EnumSet.of( SqlKind.CREATE_TABLE, SqlKind.ALTER_TABLE ) ) ) {
            // HSQLDB does not accept an expression as DEFAULT value. The toSqlString method, however, creates an expression in parentheses. Thus, we have to "extract" the value.
            serializedSql = serializedSql.replaceAll( "DEFAULT \\(([^)]*)\\)", "DEFAULT $1" );  // search for everything between '(' and ')' which does not include a ')'. For now, this should cover most cases.
        }
        return serializedSql;
    }


    /*
     *
     *
     *
     *
     */


    protected <PhysicalNodeType extends PhysicalNode, ReturnType> ReturnType callMethod( final MethodCall method, final PhysicalNodeType physicalNode ) throws RemoteException {
        return this.callMethod( DEFAULT_REQUEST_OPTIONS, method, physicalNode );
    }


    protected <PhysicalNodeType extends PhysicalNode, ReturnType> ReturnType callMethod( final RequestOptions requestOptions, final MethodCall method, final PhysicalNodeType physicalNode ) throws RemoteException {
        return this.doCallMethod( requestOptions, method, physicalNode == null ? null : physicalNode.getNodeAddress() );
    }


    private <ReturnType> ReturnType doCallMethod( final RequestOptions requestOptions, final MethodCall method, final Address physicalNodeAddress ) throws RemoteException {
        LOGGER.trace( "doCallMethod( requestOptions: {}, method: {}, physicalNodeAddress: {} )", requestOptions, method, physicalNodeAddress );

        final ReturnType result;
        try {
            result = this.rpc.callRemoteMethod( physicalNodeAddress, method, requestOptions.setTimeout( requestTimeoutMillis ) );
        } catch ( RemoteException | RuntimeException ex ) {
            LOGGER.debug( "re-throwing {}", ex );
            throw ex;
        } catch ( Exception ex ) {
            LOGGER.debug( "Wrapping Exception {}", ex );
            throw new RemoteException( ex.getMessage(), ex );
        }

        LOGGER.trace( "doCallMethod( requestOptions: {}, method: {}, physicalNodeAddress: {} ) = {}", requestOptions, method, physicalNodeAddress, result );
        return result;
    }


    /*
     *
     */


    protected <PhysicalNodeType extends PhysicalNode, ReturnType> CompletableFuture<ReturnType> asyncCallMethod( final MethodCall method, final PhysicalNodeType physicalNode ) throws RemoteException {
        return this.asyncCallMethod( DEFAULT_REQUEST_OPTIONS, method, physicalNode );
    }


    protected <PhysicalNodeType extends PhysicalNode, ReturnType> CompletableFuture<ReturnType> asyncCallMethod( final RequestOptions requestOptions, final MethodCall method, final PhysicalNodeType physicalNode ) throws RemoteException {
        return this.doAsyncCallMethod( requestOptions, method, physicalNode == null ? null : physicalNode.getNodeAddress() );
    }


    private <ReturnType> CompletableFuture<ReturnType> doAsyncCallMethod( final RequestOptions requestOptions, final MethodCall method, final Address remoteNodeAddress ) throws RemoteException {
        LOGGER.trace( "doAsyncCallMethod( requestOptions: {}, method: {}, remoteNodeAddress: {} )", requestOptions, method, remoteNodeAddress );

        final CompletableFuture<ReturnType> result;
        try {
            result = this.rpc.callRemoteMethodWithFuture( remoteNodeAddress, method, requestOptions.setTimeout( requestTimeoutMillis ) );
        } catch ( RemoteException | RuntimeException ex ) {
            LOGGER.debug( "re-throwing {}", ex );
            throw ex;
        } catch ( Exception ex ) {
            LOGGER.debug( "Wrapping Exception {}", ex );
            throw new RemoteException( ex.getMessage(), ex );
        }

        LOGGER.trace( "doAsyncCallMethod( requestOptions: {}, method: {}, remoteNodeAddress: {} ) = {}", requestOptions, method, remoteNodeAddress, result );
        return result;
    }


    /*
     *
     */


    protected <PhysicalNodeType extends PhysicalNode, ReturnType> RspList<ReturnType> callMethods( final MethodCall method, final Collection<PhysicalNodeType> physicalNodes ) throws RemoteException {
        return this.callMethods( DEFAULT_REQUEST_OPTIONS, method, physicalNodes );
    }


    protected <PhysicalNodeType extends PhysicalNode, ReturnType> RspList<ReturnType> callMethods( final RequestOptions requestOptions, final MethodCall method, final Collection<PhysicalNodeType> physicalNodes ) throws RemoteException {
        return this.doCallMethods( requestOptions, method, physicalNodes == null ? null : physicalNodes.stream().map( PhysicalNode::getNodeAddress ).collect( Collectors.toList() ) );
    }


    protected <ReturnType> RspList<ReturnType> doCallMethods( final RequestOptions requestOptions, final MethodCall method, final Collection<Address> physicalNodeAddresses ) throws RemoteException {
        LOGGER.trace( "doCallMethods( requestOptions: {}, method: {}, physicalNodeAddresses: {} )", requestOptions, method, physicalNodeAddresses == null ? "<CLUSTER>" : physicalNodeAddresses.stream().map( Object::toString ).collect( Collectors.joining( "," ) ) );

        final RspList<ReturnType> result;
        try {
            result = this.rpc.callRemoteMethods( physicalNodeAddresses, method, requestOptions.setTimeout( requestTimeoutMillis ) );
        } catch ( RemoteException | RuntimeException ex ) {
            LOGGER.debug( "re-throwing {}", ex );
            throw ex;
        } catch ( Exception ex ) {
            LOGGER.debug( "Wrapping Exception {}", ex );
            throw new RemoteException( ex.getMessage(), ex );
        }

        LOGGER.trace( "doCallMethods( requestOptions: {}, method: {}, physicalNodeAddresses: {} ) = {}", requestOptions, method, physicalNodeAddresses == null ? "<CLUSTER>" : physicalNodeAddresses.stream().map( Object::toString ).collect( Collectors.joining( "," ) ), result );
        return result;
    }


    /*
     *
     */


    protected <PhysicalNodeType extends PhysicalNode, ReturnType> CompletableFuture<RspList<ReturnType>> asyncCallMethods( final MethodCall method, final Collection<PhysicalNodeType> physicalNodes ) throws RemoteException {
        return this.asyncCallMethods( DEFAULT_REQUEST_OPTIONS, method, physicalNodes );
    }


    protected <PhysicalNodeType extends PhysicalNode, ReturnType> CompletableFuture<RspList<ReturnType>> asyncCallMethods( final RequestOptions requestOptions, final MethodCall method, final Collection<PhysicalNodeType> physicalNodes ) throws RemoteException {
        return this.doAsyncCallMethods( requestOptions, method, physicalNodes == null ? null : physicalNodes.stream().map( PhysicalNode::getNodeAddress ).collect( Collectors.toList() ) );
    }


    protected <ReturnType> CompletableFuture<RspList<ReturnType>> doAsyncCallMethods( final RequestOptions requestOptions, final MethodCall method, final Collection<Address> remoteNodeAddresses ) throws RemoteException {
        LOGGER.trace( "doAsyncCallMethods( requestOptions: {}, method: {}, remoteNodeAddresses: {} )", requestOptions, method, remoteNodeAddresses );

        final CompletableFuture<RspList<ReturnType>> result;
        try {
            result = this.rpc.callRemoteMethodsWithFuture( remoteNodeAddresses, method, requestOptions.setTimeout( requestTimeoutMillis ) );
        } catch ( RemoteException | RuntimeException ex ) {
            LOGGER.debug( "re-throwing {}", ex );
            throw ex;
        } catch ( Exception ex ) {
            LOGGER.debug( "Wrapping Exception {}", ex );
            throw new RemoteException( ex.getMessage(), ex );
        }

        LOGGER.trace( "doAsyncCallMethods( requestOptions: {}, method: {}, remoteNodeAddresses: {} ) = {}", requestOptions, method, remoteNodeAddresses, result );
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


    public <PhysicalNodeType extends PhysicalNode> RspList<RemoteStatementHandle> prepare( final RemoteStatementHandle remoteStatementHandle, final SqlNode sql, final long maxRowCount, final PhysicalNodeType... physicalNodes ) throws RemoteException {
        return this.prepare( remoteStatementHandle, sql, maxRowCount, physicalNodes == null ? null : Arrays.asList( physicalNodes ) );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<RemoteStatementHandle> prepare( final RemoteStatementHandle remoteStatementHandle, final SqlNode sql, final long maxRowCount, final Collection<PhysicalNodeType> physicalNodes ) throws RemoteException {
        LOGGER.trace( "prepare( remoteStatementHandle: {}, sql: {}, maxRowCount: {}, physicalNodes: {} )", remoteStatementHandle, sql, maxRowCount, physicalNodes );

        final String serializedSql = serializeSql( sql );
        final RspList<RemoteStatementHandle> result;

        if ( physicalNodes != null && physicalNodes.size() == 1 ) {
            final PhysicalNode singleNode = physicalNodes.iterator().next();
            result = new RspList<>( 1 );
            if ( singleNode.equals( thisNode ) ) {
                LOGGER.trace( "prepare( remoteStatementHandle: {}, sql: {}, maxRowCount: {}, physicalNodes: {} ) -- Bypassing network stack for local call.", remoteStatementHandle, sql, maxRowCount, physicalNodes );
                result.addRsp( thisNode.getNodeAddress( this ), thisNode.prepare( remoteStatementHandle, serializedSql, maxRowCount ) );
            } else {
                result.addRsp( singleNode.getNodeAddress(), singleNode.prepare( remoteStatementHandle, serializedSql, maxRowCount ) );
            }
        } else {
            result = this.callMethods( Method.prepare( remoteStatementHandle, serializedSql, maxRowCount ), physicalNodes );
        }

        LOGGER.trace( "prepare( remoteStatementHandle: {}, sql: {}, maxRowCount: {}, physicalNodes: {} ) = {}", remoteStatementHandle, sql, maxRowCount, physicalNodes, result );
        return result;
    }


    public RspList<RemoteStatementHandle> prepare( final RemoteStatementHandle remoteStatementHandle, final SqlNode sql, final long maxRowCount, final int[] columnIndexes ) throws RemoteException {
        return this.prepare( remoteStatementHandle, sql, maxRowCount, columnIndexes, ALL_NODES_IN_THE_CLUSTER );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<RemoteStatementHandle> prepare( final RemoteStatementHandle remoteStatementHandle, final SqlNode sql, final long maxRowCount, final int[] columnIndexes, final PhysicalNodeType... physicalNodes ) throws RemoteException {
        return this.prepare( remoteStatementHandle, sql, maxRowCount, columnIndexes, physicalNodes == null ? null : Arrays.asList( physicalNodes ) );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<RemoteStatementHandle> prepare( final RemoteStatementHandle remoteStatementHandle, final SqlNode sql, final long maxRowCount, final int[] columnIndexes, final Collection<PhysicalNodeType> physicalNodes ) throws RemoteException {
        LOGGER.trace( "prepare( remoteStatementHandle: {}, sql: {}, maxRowCount: {}, columnIndexes: {}, physicalNodes: {} )", remoteStatementHandle, sql, maxRowCount, columnIndexes, physicalNodes );

        final String serializedSql = serializeSql( sql );
        final RspList<RemoteStatementHandle> result;

        if ( physicalNodes != null && physicalNodes.size() == 1 ) {
            final PhysicalNode singleNode = physicalNodes.iterator().next();
            result = new RspList<>( 1 );
            if ( singleNode.equals( thisNode ) ) {
                LOGGER.trace( "prepare( remoteStatementHandle: {}, sql: {}, maxRowCount: {}, columnIndexes: {}, physicalNodes: {} ) -- Bypassing network stack for local call.", remoteStatementHandle, sql, maxRowCount, columnIndexes, physicalNodes );
                result.addRsp( thisNode.getNodeAddress( this ), thisNode.prepare( remoteStatementHandle, serializedSql, maxRowCount, columnIndexes ) );
            } else {
                result.addRsp( singleNode.getNodeAddress(), singleNode.prepare( remoteStatementHandle, serializedSql, maxRowCount, columnIndexes ) );
            }
        } else {
            result = this.callMethods( Method.prepare( remoteStatementHandle, serializedSql, maxRowCount, columnIndexes ), physicalNodes );
        }

        LOGGER.trace( "prepare( remoteStatementHandle: {}, sql: {}, maxRowCount: {}, columnIndexes: {}, physicalNodes: {} ) = {}", remoteStatementHandle, sql, maxRowCount, columnIndexes, physicalNodes, result );
        return result;
    }
    //


    public RspList<Void> closeStatement( final RemoteStatementHandle remoteStatementHandle ) throws RemoteException {
        return this.closeStatement( remoteStatementHandle, ALL_NODES_IN_THE_CLUSTER );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<Void> closeStatement( final RemoteStatementHandle remoteStatementHandle, final PhysicalNodeType... physicalNodes ) throws RemoteException {
        return this.closeStatement( remoteStatementHandle, physicalNodes == null ? null : Arrays.asList( physicalNodes ) );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<Void> closeStatement( final RemoteStatementHandle remoteStatementHandle, final Collection<PhysicalNodeType> physicalNodes ) throws RemoteException {
        LOGGER.trace( "closeStatement( remoteStatementHandle: {}, physicalNodes: {} )", remoteStatementHandle, physicalNodes );

        final RspList<Void> result;

        if ( physicalNodes != null && physicalNodes.size() == 1 ) {
            final PhysicalNode singleNode = physicalNodes.iterator().next();
            result = new RspList<>( 1 );
            if ( singleNode.equals( thisNode ) ) {
                LOGGER.trace( "closeStatement( remoteStatementHandle: {}, physicalNodes: {} ) -- Bypassing network stack for local call.", remoteStatementHandle, physicalNodes );
                result.addRsp( thisNode.getNodeAddress( this ), thisNode.closeStatement( remoteStatementHandle ) );
            } else {
                result.addRsp( singleNode.getNodeAddress(), singleNode.closeStatement( remoteStatementHandle ) );
            }
        } else {
            result = this.callMethods( Method.closeStatement( remoteStatementHandle ), physicalNodes );
        }

        LOGGER.trace( "closeStatement( remoteStatementHandle: {}, physicalNodes: {} ) = {}", remoteStatementHandle, physicalNodes, result );
        return result;
    }


    /*
     *
     */


    public RspList<RemoteExecuteResult> prepareAndExecute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame ) throws RemoteException {
        return this.prepareAndExecute( remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame, ALL_NODES_IN_THE_CLUSTER );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<RemoteExecuteResult> prepareAndExecute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final PhysicalNodeType... physicalNodes ) throws RemoteException {
        return this.prepareAndExecute( remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame, physicalNodes == null ? null : Arrays.asList( physicalNodes ) );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<RemoteExecuteResult> prepareAndExecute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final Collection<PhysicalNodeType> physicalNodes ) throws RemoteException {
        LOGGER.trace( "prepareAndExecute( remoteTransactionHandle: {}, remoteStatementHandle: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, physicalNodes: {} )", remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame, physicalNodes );

        String serializedSql = serializeSql( sql );
        if ( sql.isA( EnumSet.of( SqlKind.CREATE_TABLE, SqlKind.ALTER_TABLE ) ) ) {
            // HSQLDB does not accept an expression as DEFAULT value. The toSqlString method, however, creates an expression in parentheses. Thus, we have to "extract" the value.
            serializedSql = serializedSql.replaceAll( "DEFAULT \\(([^)]*)\\)", "DEFAULT $1" );  // search for everything between '(' and ')' which does not include a ')'. For now, this should cover most cases.
        }

        final RspList<RemoteExecuteResult> result;

        if ( physicalNodes != null && physicalNodes.size() == 1 ) {
            final PhysicalNode singleNode = physicalNodes.iterator().next();
            result = new RspList<>( 1 );
            if ( singleNode.equals( thisNode ) ) {
                LOGGER.trace( "prepareAndExecute( remoteTransactionHandle: {}, remoteStatementHandle: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, physicalNodes: {} ) -- Bypassing network stack for local call.", remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame, physicalNodes );
                result.addRsp( thisNode.getNodeAddress( this ), thisNode.prepareAndExecute( remoteTransactionHandle, remoteStatementHandle, serializedSql, maxRowCount, maxRowsInFirstFrame ) );
            } else {
                result.addRsp( singleNode.getNodeAddress(), singleNode.prepareAndExecute( remoteTransactionHandle, remoteStatementHandle, serializedSql, maxRowCount, maxRowsInFirstFrame ) );
            }
        } else {
            result = this.callMethods( Method.prepareAndExecute( remoteTransactionHandle, remoteStatementHandle, serializedSql, maxRowCount, maxRowsInFirstFrame ), physicalNodes );
        }

        LOGGER.trace( "prepareAndExecute( remoteTransactionHandle: {}, remoteStatementHandle: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, physicalNodes: {} ) = {}", remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame, physicalNodes, result );
        return result;
    }


    public RspList<RemoteExecuteResult> prepareAndExecute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final int[] columnIndexes ) throws RemoteException {
        return this.prepareAndExecute( remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame, columnIndexes, ALL_NODES_IN_THE_CLUSTER );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<RemoteExecuteResult> prepareAndExecute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final int[] columnIndexes, final PhysicalNodeType... physicalNodes ) throws RemoteException {
        return this.prepareAndExecute( remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame, columnIndexes, physicalNodes == null ? null : Arrays.asList( physicalNodes ) );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<RemoteExecuteResult> prepareAndExecute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final int[] columnIndexes, final Collection<PhysicalNodeType> physicalNodes ) throws RemoteException {
        LOGGER.trace( "prepareAndExecute( remoteTransactionHandle: {}, remoteStatementHandle: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, columnIndexes: {}, physicalNodes: {} )", remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame, columnIndexes, physicalNodes );

        String serializedSql = serializeSql( sql );
        if ( sql.isA( EnumSet.of( SqlKind.CREATE_TABLE, SqlKind.ALTER_TABLE ) ) ) {
            // HSQLDB does not accept an expression as DEFAULT value. The toSqlString method, however, creates an expression in parentheses. Thus, we have to "extract" the value.
            serializedSql = serializedSql.replaceAll( "DEFAULT \\(([^)]*)\\)", "DEFAULT $1" );  // search for everything between '(' and ')' which does not include a ')'. For now, this should cover most cases.
        }

        final RspList<RemoteExecuteResult> result;

        if ( physicalNodes != null && physicalNodes.size() == 1 ) {
            final PhysicalNode singleNode = physicalNodes.iterator().next();
            result = new RspList<>( 1 );
            if ( singleNode.equals( thisNode ) ) {
                LOGGER.trace( "prepareAndExecute( remoteTransactionHandle: {}, remoteStatementHandle: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, columnIndexes: {}, physicalNodes: {} ) -- Bypassing network stack for local call.", remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame, columnIndexes, physicalNodes );
                result.addRsp( thisNode.getNodeAddress( this ), thisNode.prepareAndExecute( remoteTransactionHandle, remoteStatementHandle, serializedSql, maxRowCount, maxRowsInFirstFrame, columnIndexes ) );
            } else {
                result.addRsp( singleNode.getNodeAddress(), singleNode.prepareAndExecute( remoteTransactionHandle, remoteStatementHandle, serializedSql, maxRowCount, maxRowsInFirstFrame, columnIndexes ) );
            }
        } else {
            result = this.callMethods( Method.prepareAndExecute( remoteTransactionHandle, remoteStatementHandle, serializedSql, maxRowCount, maxRowsInFirstFrame, columnIndexes ), physicalNodes );
        }

        LOGGER.trace( "prepareAndExecute( remoteTransactionHandle: {}, remoteStatementHandle: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, columnIndexes: {}, physicalNodes: {} ) = {}", remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame, columnIndexes, physicalNodes, result );
        return result;
    }


    /*
     *
     */


    public RspList<RemoteExecuteResult> prepareAndExecuteDataDefinition( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final SqlNode catalogSql, final SqlNode storeSql, final long maxRowCount, final int maxRowsInFirstFrame ) throws RemoteException {
        return this.prepareAndExecuteDataDefinition( remoteTransactionHandle, remoteStatementHandle, catalogSql, storeSql, maxRowCount, maxRowsInFirstFrame, ALL_NODES_IN_THE_CLUSTER );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<RemoteExecuteResult> prepareAndExecuteDataDefinition( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final SqlNode catalogSql, final SqlNode storeSql, final long maxRowCount, final int maxRowsInFirstFrame, final PhysicalNodeType... physicalNodes ) throws RemoteException {
        return this.prepareAndExecuteDataDefinition( remoteTransactionHandle, remoteStatementHandle, catalogSql, storeSql, maxRowCount, maxRowsInFirstFrame, physicalNodes == null ? null : Arrays.asList( physicalNodes ) );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<RemoteExecuteResult> prepareAndExecuteDataDefinition( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final SqlNode catalogSql, final SqlNode storeSql, final long maxRowCount, final int maxRowsInFirstFrame, final Collection<PhysicalNodeType> physicalNodes ) throws RemoteException {
        LOGGER.trace( "prepareAndExecuteDataDefinition( remoteTransactionHandle: {}, remoteStatementHandle: {}, catalogSql: {}, storeSql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, physicalNodes: {} )", remoteTransactionHandle, remoteStatementHandle, catalogSql, storeSql, maxRowCount, maxRowsInFirstFrame, physicalNodes );

        String serializedCatalogSql = serializeSql( catalogSql );
        if ( catalogSql.isA( EnumSet.of( SqlKind.CREATE_TABLE, SqlKind.ALTER_TABLE ) ) ) {
            // HSQLDB does not accept an expression as DEFAULT value. The toSqlString method, however, creates an expression in parentheses. Thus, we have to "extract" the value.
            serializedCatalogSql = serializedCatalogSql.replaceAll( "DEFAULT \\(([^)]*)\\)", "DEFAULT $1" );  // search for everything between '(' and ')' which does not include a ')'. For now, this should cover most cases.
        }
        String serializedStoreSql = serializeSql( storeSql );
        if ( storeSql.isA( EnumSet.of( SqlKind.CREATE_TABLE, SqlKind.ALTER_TABLE ) ) ) {
            // HSQLDB does not accept an expression as DEFAULT value. The toSqlString method, however, creates an expression in parentheses. Thus, we have to "extract" the value.
            serializedStoreSql = serializedStoreSql.replaceAll( "DEFAULT \\(([^)]*)\\)", "DEFAULT $1" );  // search for everything between '(' and ')' which does not include a ')'. For now, this should cover most cases.
        }

        final RspList<RemoteExecuteResult> result;

        if ( physicalNodes != null && physicalNodes.size() == 1 ) {
            final PhysicalNode singleNode = physicalNodes.iterator().next();
            result = new RspList<>( 1 );
            if ( singleNode.equals( thisNode ) ) {
                LOGGER.trace( "prepareAndExecuteDataDefinition( remoteTransactionHandle: {}, remoteStatementHandle: {}, catalogSql: {}, storeSql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, physicalNodes: {} ) -- Bypassing network stack for local call.", remoteTransactionHandle, remoteStatementHandle, catalogSql, storeSql, maxRowCount, maxRowsInFirstFrame, physicalNodes );
                result.addRsp( thisNode.getNodeAddress( this ), thisNode.prepareAndExecuteDataDefinition( remoteTransactionHandle, remoteStatementHandle, serializedCatalogSql, serializedStoreSql, maxRowCount, maxRowsInFirstFrame ) );
            } else {
                result.addRsp( singleNode.getNodeAddress(), singleNode.prepareAndExecuteDataDefinition( remoteTransactionHandle, remoteStatementHandle, serializedCatalogSql, serializedStoreSql, maxRowCount, maxRowsInFirstFrame ) );
            }
        } else {
            result = this.callMethods( Method.prepareAndExecuteDataDefinition( remoteTransactionHandle, remoteStatementHandle, serializedCatalogSql, serializedStoreSql, maxRowCount, maxRowsInFirstFrame ), physicalNodes );
        }

        LOGGER.trace( "prepareAndExecuteDataDefinition( remoteTransactionHandle: {}, remoteStatementHandle: {}, catalogSql: {}, storeSql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, physicalNodes: {} ) = {}", remoteTransactionHandle, remoteStatementHandle, catalogSql, storeSql, maxRowCount, maxRowsInFirstFrame, physicalNodes, result );
        return result;
    }


    /*
     *
     */


    public RspList<RemoteExecuteBatchResult> prepareAndExecuteBatch( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<SqlNode> sqlCommands ) throws RemoteException {
        return this.prepareAndExecuteBatch( remoteTransactionHandle, remoteStatementHandle, sqlCommands, ALL_NODES_IN_THE_CLUSTER );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<RemoteExecuteBatchResult> prepareAndExecuteBatch( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<SqlNode> sqlCommands, final PhysicalNodeType... physicalNodes ) throws RemoteException {
        return this.prepareAndExecuteBatch( remoteTransactionHandle, remoteStatementHandle, sqlCommands, physicalNodes == null ? null : Arrays.asList( physicalNodes ) );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<RemoteExecuteBatchResult> prepareAndExecuteBatch( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<SqlNode> sqlCommands, final Collection<PhysicalNodeType> physicalNodes ) throws RemoteException {
        LOGGER.trace( "prepareAndExecuteBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, sqlCommands: {}, physicalNodes: {} )", remoteTransactionHandle, remoteStatementHandle, sqlCommands, physicalNodes );

        List<String> serializedSqlCommands = sqlCommands.stream().map( this::serializeSql ).collect( Collectors.toList() );
        final RspList<RemoteExecuteBatchResult> result;

        if ( physicalNodes != null && physicalNodes.size() == 1 ) {
            final PhysicalNode singleNode = physicalNodes.iterator().next();
            result = new RspList<>( 1 );
            if ( singleNode.equals( thisNode ) ) {
                LOGGER.trace( "prepareAndExecuteBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, sqlCommands: {}, physicalNodes: {} ) -- Bypassing network stack for local call.", remoteTransactionHandle, remoteStatementHandle, sqlCommands, physicalNodes );
                result.addRsp( thisNode.getNodeAddress( this ), thisNode.prepareAndExecuteBatch( remoteTransactionHandle, remoteStatementHandle, serializedSqlCommands ) );
            } else {
                result.addRsp( singleNode.getNodeAddress(), singleNode.prepareAndExecuteBatch( remoteTransactionHandle, remoteStatementHandle, serializedSqlCommands ) );
            }
        } else {
            result = this.callMethods( Method.prepareAndExecuteBatch( remoteTransactionHandle, remoteStatementHandle, serializedSqlCommands ), physicalNodes );
        }

        LOGGER.trace( "prepareAndExecuteBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, sqlCommands: {}, physicalNodes: {} ) = {}", remoteTransactionHandle, remoteStatementHandle, sqlCommands, physicalNodes, result );
        return result;
    }


    /*
     *
     */


    public RspList<RemoteExecuteBatchResult> executeBatch( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<UpdateBatch> parameterValues ) throws RemoteException {
        return this.executeBatch( remoteTransactionHandle, remoteStatementHandle, parameterValues, ALL_NODES_IN_THE_CLUSTER );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<RemoteExecuteBatchResult> executeBatch( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<UpdateBatch> parameterValues, final PhysicalNodeType... physicalNodes ) throws RemoteException {
        return this.executeBatch( remoteTransactionHandle, remoteStatementHandle, parameterValues, physicalNodes == null ? null : Arrays.asList( physicalNodes ) );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<RemoteExecuteBatchResult> executeBatch( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<UpdateBatch> parameterValues, final Collection<PhysicalNodeType> physicalNodes ) throws RemoteException {
        LOGGER.trace( "executeBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {}, physicalNodes: {} )", remoteTransactionHandle, remoteStatementHandle, parameterValues, physicalNodes );

        final RspList<RemoteExecuteBatchResult> result;

        if ( physicalNodes != null && physicalNodes.size() == 1 ) {
            final PhysicalNode singleNode = physicalNodes.iterator().next();
            result = new RspList<>( 1 );
            if ( singleNode.equals( thisNode ) ) {
                LOGGER.trace( "executeBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {}, physicalNodes: {} ) -- Bypassing network stack for local call.", remoteTransactionHandle, remoteStatementHandle, parameterValues, physicalNodes );
                result.addRsp( thisNode.getNodeAddress( this ), thisNode.executeBatch( remoteTransactionHandle, remoteStatementHandle, parameterValues ) );
            } else {
                result.addRsp( singleNode.getNodeAddress(), singleNode.executeBatch( remoteTransactionHandle, remoteStatementHandle, parameterValues ) );
            }
        } else {
            result = this.callMethods( Method.executeBatch( remoteTransactionHandle, remoteStatementHandle, parameterValues ), physicalNodes );
        }

        LOGGER.trace( "executeBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {}, physicalNodes: {} ) = {}", remoteTransactionHandle, remoteStatementHandle, parameterValues, physicalNodes, result );
        return result;
    }


    /*
     *
     */


    public RspList<RemoteFrame> fetch( final RemoteStatementHandle remoteStatementHandle, final long offset, final int fetchMaxRowCount ) throws RemoteException {
        return this.fetch( remoteStatementHandle, offset, fetchMaxRowCount, ALL_NODES_IN_THE_CLUSTER );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<RemoteFrame> fetch( final RemoteStatementHandle remoteStatementHandle, final long offset, final int fetchMaxRowCount, final PhysicalNodeType... physicalNodes ) throws RemoteException {
        return this.fetch( remoteStatementHandle, offset, fetchMaxRowCount, physicalNodes == null ? null : Arrays.asList( physicalNodes ) );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<RemoteFrame> fetch( final RemoteStatementHandle remoteStatementHandle, final long offset, final int fetchMaxRowCount, final Collection<PhysicalNodeType> physicalNodes ) throws RemoteException {
        LOGGER.trace( "fetch( remoteStatementHandle: {}, offset: {}, fetchMaxRowCount: {}, physicalNodes: {} )", remoteStatementHandle, offset, fetchMaxRowCount, physicalNodes );

        final RspList<RemoteFrame> result;

        if ( physicalNodes != null && physicalNodes.size() == 1 ) {
            final PhysicalNode singleNode = physicalNodes.iterator().next();
            result = new RspList<>( 1 );
            if ( singleNode.equals( thisNode ) ) {
                LOGGER.trace( "fetch( remoteStatementHandle: {}, offset: {}, fetchMaxRowCount: {}, physicalNodes: {} ) -- Bypassing network stack for local call.", remoteStatementHandle, offset, fetchMaxRowCount, physicalNodes );
                result.addRsp( thisNode.getNodeAddress( this ), thisNode.fetch( remoteStatementHandle, offset, fetchMaxRowCount ) );
            } else {
                result.addRsp( singleNode.getNodeAddress(), singleNode.fetch( remoteStatementHandle, offset, fetchMaxRowCount ) );
            }
        } else {
            result = this.callMethods( Method.fetch( remoteStatementHandle, offset, fetchMaxRowCount ), physicalNodes );
        }

        LOGGER.trace( "fetch( remoteStatementHandle: {}, offset: {}, fetchMaxRowCount: {}, physicalNodes: {} ) = {}", remoteStatementHandle, offset, fetchMaxRowCount, physicalNodes, result );
        return result;
    }


    /*
     *
     */


    public RspList<RemoteExecuteResult> execute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<Common.TypedValue> parameterValues, final int maxRowsInFirstFrame ) throws RemoteException {
        return this.execute( remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame, ALL_NODES_IN_THE_CLUSTER );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<RemoteExecuteResult> execute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<Common.TypedValue> parameterValues, final int maxRowsInFirstFrame, final PhysicalNodeType... physicalNodes ) throws RemoteException {
        return this.execute( remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame, physicalNodes == null ? null : Arrays.asList( physicalNodes ) );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<RemoteExecuteResult> execute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<Common.TypedValue> parameterValues, final int maxRowsInFirstFrame, final Collection<PhysicalNodeType> physicalNodes ) throws RemoteException {
        LOGGER.trace( "execute( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {}, maxRowsInFirstFrame: {}, physicalNodes: {} )", remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame, physicalNodes );

        final RspList<RemoteExecuteResult> result;

        if ( physicalNodes != null && physicalNodes.size() == 1 ) {
            final PhysicalNode singleNode = physicalNodes.iterator().next();
            result = new RspList<>( 1 );
            if ( singleNode.equals( thisNode ) ) {
                LOGGER.trace( "execute( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {}, maxRowsInFirstFrame: {}, physicalNodes: {} ) -- Bypassing network stack for local call.", remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame, physicalNodes );
                result.addRsp( thisNode.getNodeAddress( this ), thisNode.execute( remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame ) );
            } else {
                result.addRsp( singleNode.getNodeAddress(), singleNode.execute( remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame ) );
            }
        } else {
            result = this.callMethods( Method.execute( remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame ), physicalNodes );
        }

        LOGGER.trace( "execute( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {}, maxRowsInFirstFrame: {}, physicalNodes: {} ) = {}", remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame, physicalNodes, result );
        return result;
    }


    /*
     *
     */


    public RspList<Void> onePhaseCommit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        return this.onePhaseCommit( remoteConnectionHandle, remoteTransactionHandle, ALL_NODES_IN_THE_CLUSTER );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<Void> onePhaseCommit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle, final PhysicalNodeType... physicalNodes ) throws RemoteException {
        return this.onePhaseCommit( remoteConnectionHandle, remoteTransactionHandle, physicalNodes == null ? null : Arrays.asList( physicalNodes ) );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<Void> onePhaseCommit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle, final Collection<PhysicalNodeType> physicalNodes ) throws RemoteException {
        LOGGER.trace( "commit( remoteConnectionHandle: {}, remoteTransactionHandle: {}, physicalNodes: {} )", remoteConnectionHandle, remoteTransactionHandle, physicalNodes );

        final RspList<Void> result;

        if ( physicalNodes != null && physicalNodes.size() == 1 ) {
            final PhysicalNode singleNode = physicalNodes.iterator().next();
            result = new RspList<>( 1 );
            if ( singleNode.equals( thisNode ) ) {
                LOGGER.trace( "commit( remoteConnectionHandle: {}, remoteTransactionHandle: {}, physicalNodes: {} ) -- Bypassing network stack for local call.", remoteConnectionHandle, remoteTransactionHandle, physicalNodes );
                result.addRsp( thisNode.getNodeAddress( this ), thisNode.onePhaseCommit( remoteConnectionHandle, remoteTransactionHandle ) );
            } else {
                result.addRsp( singleNode.getNodeAddress(), singleNode.onePhaseCommit( remoteConnectionHandle, remoteTransactionHandle ) );
            }
        } else {
            LOGGER.warn( "Executing a One Phase Commit on multiple nodes." );
            result = this.callMethods( Method.onePhaseCommit( remoteConnectionHandle, remoteTransactionHandle ), physicalNodes );
        }

        LOGGER.trace( "commit( remoteConnectionHandle: {}, remoteTransactionHandle: {}, physicalNodes: {} ) = {}", remoteConnectionHandle, remoteTransactionHandle, physicalNodes, result );
        return result;
    }


    /*
     *
     */


    public RspList<Boolean> prepareCommit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        return this.prepareCommit( remoteConnectionHandle, remoteTransactionHandle, ALL_NODES_IN_THE_CLUSTER );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<Boolean> prepareCommit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle, final PhysicalNodeType... physicalNodes ) throws RemoteException {
        return this.prepareCommit( remoteConnectionHandle, remoteTransactionHandle, physicalNodes == null ? null : Arrays.asList( physicalNodes ) );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<Boolean> prepareCommit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle, final Collection<PhysicalNodeType> physicalNodes ) throws RemoteException {
        LOGGER.trace( "prepareCommit( remoteConnectionHandle: {}, remoteTransactionHandle: {}, physicalNodes: {} )", remoteConnectionHandle, remoteTransactionHandle, physicalNodes );

        final RspList<Boolean> result;

        if ( physicalNodes != null && physicalNodes.size() == 1 ) {
            final PhysicalNode singleNode = physicalNodes.iterator().next();
            result = new RspList<>( 1 );
            if ( singleNode.equals( thisNode ) ) {
                LOGGER.trace( "prepareCommit( remoteConnectionHandle: {}, remoteTransactionHandle: {}, physicalNodes: {} ) -- Bypassing network stack for local call.", remoteConnectionHandle, remoteTransactionHandle, physicalNodes );
                result.addRsp( thisNode.getNodeAddress( this ), thisNode.prepareCommit( remoteConnectionHandle, remoteTransactionHandle ) );
            } else {
                result.addRsp( singleNode.getNodeAddress(), singleNode.prepareCommit( remoteConnectionHandle, remoteTransactionHandle ) );
            }
        } else {
            result = this.callMethods( Method.prepareCommit( remoteConnectionHandle, remoteTransactionHandle ), physicalNodes );
        }

        LOGGER.trace( "prepareCommit( remoteConnectionHandle: {}, remoteTransactionHandle: {}, physicalNodes: {} ) = {}", remoteConnectionHandle, remoteTransactionHandle, physicalNodes, result );
        return result;
    }


    /*
     *
     */


    public RspList<Void> commit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        return this.commit( remoteConnectionHandle, remoteTransactionHandle, ALL_NODES_IN_THE_CLUSTER );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<Void> commit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle, final PhysicalNodeType... physicalNodes ) throws RemoteException {
        return this.commit( remoteConnectionHandle, remoteTransactionHandle, physicalNodes == null ? null : Arrays.asList( physicalNodes ) );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<Void> commit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle, final Collection<PhysicalNodeType> physicalNodes ) throws RemoteException {
        LOGGER.trace( "commit( remoteConnectionHandle: {}, remoteTransactionHandle: {}, physicalNodes: {} )", remoteConnectionHandle, remoteTransactionHandle, physicalNodes );

        final RspList<Void> result;

        if ( physicalNodes != null && physicalNodes.size() == 1 ) {
            final PhysicalNode singleNode = physicalNodes.iterator().next();
            result = new RspList<>( 1 );
            if ( singleNode.equals( thisNode ) ) {
                LOGGER.trace( "commit( remoteConnectionHandle: {}, remoteTransactionHandle: {}, physicalNodes: {} ) -- Bypassing network stack for local call.", remoteConnectionHandle, remoteTransactionHandle, physicalNodes );
                result.addRsp( thisNode.getNodeAddress( this ), thisNode.commit( remoteConnectionHandle, remoteTransactionHandle ) );
            } else {
                result.addRsp( singleNode.getNodeAddress(), singleNode.commit( remoteConnectionHandle, remoteTransactionHandle ) );
            }
        } else {
            result = this.callMethods( Method.commit( remoteConnectionHandle, remoteTransactionHandle ), physicalNodes );
        }

        LOGGER.trace( "commit( remoteConnectionHandle: {}, remoteTransactionHandle: {}, physicalNodes: {} ) = {}", remoteConnectionHandle, remoteTransactionHandle, physicalNodes, result );
        return result;
    }


    /*
     *
     */


    public RspList<Void> rollback( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        return this.rollback( remoteConnectionHandle, remoteTransactionHandle, ALL_NODES_IN_THE_CLUSTER );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<Void> rollback( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle, final PhysicalNodeType... physicalNodes ) throws RemoteException {
        return this.rollback( remoteConnectionHandle, remoteTransactionHandle, physicalNodes == null ? null : Arrays.asList( physicalNodes ) );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<Void> rollback( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle, Collection<PhysicalNodeType> physicalNodes ) throws RemoteException {
        LOGGER.trace( "rollback( remoteConnectionHandle: {}, remoteTransactionHandle: {}, physicalNodes: {} )", remoteConnectionHandle, remoteTransactionHandle, physicalNodes );

        final RspList<Void> result;

        if ( physicalNodes != null && physicalNodes.size() == 1 ) {
            final PhysicalNode singleNode = physicalNodes.iterator().next();
            result = new RspList<>( 1 );
            if ( singleNode.equals( thisNode ) ) {
                LOGGER.trace( "rollback( remoteConnectionHandle: {}, remoteTransactionHandle: {}, physicalNodes: {} ) -- Bypassing network stack for local call.", remoteConnectionHandle, remoteTransactionHandle, physicalNodes );
                result.addRsp( thisNode.getNodeAddress( this ), thisNode.rollback( remoteConnectionHandle, remoteTransactionHandle ) );
            } else {
                result.addRsp( singleNode.getNodeAddress(), singleNode.rollback( remoteConnectionHandle, remoteTransactionHandle ) );
            }
        } else {
            result = this.callMethods( Method.rollback( remoteConnectionHandle, remoteTransactionHandle ), physicalNodes );
        }

        LOGGER.trace( "rollback( remoteConnectionHandle: {}, remoteTransactionHandle: {}, physicalNodes: {} ) = {}", remoteConnectionHandle, remoteTransactionHandle, physicalNodes, result );
        return result;
    }


    /*
     *
     */


    public RspList<ConnectionProperties> connectionSync( final RemoteConnectionHandle remoteConnectionHandle, final ConnectionProperties properties ) throws RemoteException {
        return this.connectionSync( remoteConnectionHandle, properties, ALL_NODES_IN_THE_CLUSTER );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<ConnectionProperties> connectionSync( final RemoteConnectionHandle remoteConnectionHandle, final ConnectionProperties properties, final PhysicalNodeType... physicalNodes ) throws RemoteException {
        return this.connectionSync( remoteConnectionHandle, properties, physicalNodes == null ? null : Arrays.asList( physicalNodes ) );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<ConnectionProperties> connectionSync( final RemoteConnectionHandle remoteConnectionHandle, final ConnectionProperties properties, final Collection<PhysicalNodeType> physicalNodes ) throws RemoteException {
        LOGGER.trace( "connectionSync( remoteConnectionHandle: {}, properties: {}, physicalNodes: {} )", remoteConnectionHandle, properties, physicalNodes );

        final RspList<ConnectionProperties> result;

        if ( physicalNodes != null && physicalNodes.size() == 1 ) {
            final PhysicalNode singleNode = physicalNodes.iterator().next();
            result = new RspList<>( 1 );
            if ( singleNode.equals( thisNode ) ) {
                LOGGER.trace( "connectionSync( remoteConnectionHandle: {}, properties: {}, physicalNodes: {} ) -- Bypassing network stack for local call.", remoteConnectionHandle, properties, physicalNodes );
                result.addRsp( thisNode.getNodeAddress( this ), thisNode.connectionSync( remoteConnectionHandle, properties ) );
            } else {
                result.addRsp( singleNode.getNodeAddress(), singleNode.connectionSync( remoteConnectionHandle, properties ) );
            }
        } else {
            result = this.callMethods( Method.connectionSync( remoteConnectionHandle, properties ), physicalNodes );
        }

        LOGGER.trace( "connectionSync( remoteConnectionHandle: {}, properties: {}, physicalNodes: {} ) = {}", remoteConnectionHandle, properties, physicalNodes, result );
        return result;
    }

    //


    public RspList<Void> closeConnection( final RemoteConnectionHandle remoteConnectionHandle ) throws RemoteException {
        return this.closeConnection( remoteConnectionHandle, ALL_NODES_IN_THE_CLUSTER );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<Void> closeConnection( final RemoteConnectionHandle remoteConnectionHandle, final PhysicalNodeType... physicalNodes ) throws RemoteException {
        return this.closeConnection( remoteConnectionHandle, physicalNodes == null ? null : Arrays.asList( physicalNodes ) );
    }


    public <PhysicalNodeType extends PhysicalNode> RspList<Void> closeConnection( final RemoteConnectionHandle remoteConnectionHandle, final Collection<PhysicalNodeType> physicalNodes ) throws RemoteException {
        LOGGER.trace( "closeConnection( remoteConnectionHandle: {}, physicalNodes: {} )", remoteConnectionHandle, physicalNodes );

        final RspList<Void> result;

        if ( physicalNodes != null && physicalNodes.size() == 1 ) {
            final PhysicalNode singleNode = physicalNodes.iterator().next();
            result = new RspList<>( 1 );
            if ( singleNode.equals( thisNode ) ) {
                LOGGER.trace( "closeConnection( remoteConnectionHandle: {}, physicalNodes: {} ) -- Bypassing network stack for local call.", remoteConnectionHandle, physicalNodes );
                result.addRsp( thisNode.getNodeAddress( this ), thisNode.closeConnection( remoteConnectionHandle ) );
            } else {
                result.addRsp( singleNode.getNodeAddress(), singleNode.closeConnection( remoteConnectionHandle ) );
            }
        } else {
            result = this.callMethods( Method.closeConnection( remoteConnectionHandle ), physicalNodes );
        }

        LOGGER.trace( "closeConnection( remoteConnectionHandle: {}, physicalNodes: {} ) = {}", remoteConnectionHandle, physicalNodes, result );
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


    public <PhysicalNodeType extends PhysicalNode> Set<PhysicalNodeType> getMembers() {
        return new LinkedHashSet<>( this.currentView.getMembers().stream().map( address -> (PhysicalNodeType) new RemoteNode( address, Cluster.this ) ).collect( Collectors.toList() ) );
    }


    public <PhysicalNodeType extends PhysicalNode> PhysicalNodeType[] getMembersAsArray() {
        return (PhysicalNodeType[]) this.currentView.getMembers().stream().map( address -> (PhysicalNodeType) new RemoteNode( address, Cluster.this ) ).toArray( PhysicalNode[]::new );
    }


    public View getView() {
        return this.currentView;
    }


    public AbstractLocalNode getLocalNode() {
        return thisNode;
    }


    public AbstractRemoteNode getLocalNodeAsRemoteNode() {
        return thisNode.asRemoteNode();
    }


    /**
     *
     */
    private static class DefaultCluster {

        private static final Cluster DEFAULT_CLUSTER_INSTANCE;
        private static final UUID DEFAULT_CLUSTER_ID;


        static {
            try {
                DEFAULT_CLUSTER_INSTANCE = new Cluster( ConfigFactory.load().getString( "cluster.name" ), ConfigFactory.load().getInt( "cluster.port" ) );
                DEFAULT_CLUSTER_ID = DEFAULT_CLUSTER_INSTANCE.clusterId;
            } catch ( Exception e ) {
                throw new RuntimeException( e );
            }
        }
    }
}
