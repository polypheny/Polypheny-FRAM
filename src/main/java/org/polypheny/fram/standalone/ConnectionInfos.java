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

package org.polypheny.fram.standalone;


import io.vavr.Function1;
import java.sql.Connection;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.EqualsAndHashCode;
import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.avatica.Meta.ConnectionHandle;
import org.apache.calcite.avatica.Meta.ConnectionProperties;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.tools.Planner;
import org.polypheny.fram.Catalog;
import org.polypheny.fram.remote.AbstractRemoteNode;
import org.polypheny.fram.remote.Cluster;
import org.polypheny.fram.remote.types.RemoteConnectionHandle;
import org.polypheny.fram.remote.types.RemoteStatementHandle;
import org.polypheny.fram.standalone.StatementInfos.PreparedStatementInfos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Represents a Connection
 */
@EqualsAndHashCode(doNotUseGetters = true, onlyExplicitlyIncluded = true /* use only connectionId */)
public class ConnectionInfos {

    private static final Logger LOGGER = LoggerFactory.getLogger( ConnectionInfos.class );

    private static final Boolean DEFAULT_AUTO_COMMIT = true;
    private static final Boolean DEFAULT_READ_ONLY = false;
    private static final Integer DEFAULT_TRANSACTION_ISOLATION = Connection.TRANSACTION_SERIALIZABLE;
    private static final String DEFAULT_CATALOG = "";
    private static final String DEFAULT_SCHEMA = "";

    final UUID nodeId;
    final UUID userId;
    private final ConnectionHandle connectionHandle;

    private final ConnectionPropertiesImpl connectionProperties;
    private volatile boolean isDirty = false;

    private final AtomicInteger statementIdGenerator = new AtomicInteger();

    @EqualsAndHashCode.Include
    final UUID connectionId;

    private final Map<AbstractRemoteNode, RemoteConnectionHandle> remoteConnections = new LinkedHashMap<>();
    private final Map<RemoteConnectionHandle, Set<AbstractRemoteNode>> remoteNodes = new LinkedHashMap<>();

    private Catalog catalog;
    private Cluster cluster;
    private TransactionInfos currentTransaction;
    private Planner planner;


    public ConnectionInfos( final ConnectionHandle ch ) {
        this( null, null, ch );
    }


    public ConnectionInfos( final UUID nodeId, final UUID userId, final ConnectionHandle ch ) {
        this.nodeId = nodeId;
        this.userId = userId;
        this.connectionHandle = ch;

        this.connectionProperties = new ConnectionPropertiesImpl();
        this.statementIdGenerator.set( 0 );

        UUID connectionId = null;
        try {
            connectionId = UUID.fromString( ch.id );
        } catch ( IllegalArgumentException ex ) {
            connectionId = UUID.randomUUID();
        } finally {
            this.connectionId = connectionId;
        }

        this.catalog = GlobalCatalog.getInstance();
        this.cluster = Cluster.getDefaultCluster();
    }


    public ConnectionProperties getConnectionProperties() {
        return this.connectionProperties;
    }


    public ConnectionInfos merge( final ConnectionProperties that ) {
        this.connectionProperties.merge( that );
        this.isDirty = this.connectionProperties.isDirty();
        return this;
    }


    public boolean isDirty() {
        return this.isDirty;
    }


    public void clearDirty() {
        this.isDirty = false;
    }


    public ConnectionHandle getConnectionHandle() {
        return this.connectionHandle;
    }


    protected int getNextStatementId() {
        return this.statementIdGenerator.decrementAndGet();
    }


    public StatementInfos createStatement() {
        return new StatementInfos( this, new StatementHandle( this.connectionHandle.id, getNextStatementId(), null ) );
    }


    public PreparedStatementInfos createPreparedStatement( final StatementInfos statement, final AbstractRemoteNode remoteNode, final RemoteStatementHandle remoteStatement ) {
        return statement.new PreparedStatementInfos( remoteNode, remoteStatement );
    }


    public PreparedStatementInfos createPreparedStatement( final StatementInfos statement, final Map<AbstractRemoteNode, RemoteStatementHandle> remoteStatements, final Function1<Map<AbstractRemoteNode, RemoteStatementHandle>, Signature> signatureMergeFunction ) {
        return statement.new PreparedStatementInfos( remoteStatements, signatureMergeFunction );
    }


    /*
     *
     */


    public Catalog getCatalog() {
        return this.catalog;
    }


    public Cluster getCluster() {
        return this.cluster;
    }


    public synchronized TransactionInfos getOrStartTransaction() {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getOrStartTransaction()" );
        }

        if ( this.currentTransaction == null ) {
            this.currentTransaction = new TransactionInfos( this );
            if ( LOGGER.isTraceEnabled() ) {
                LOGGER.trace( "getOrStartTransaction() - START - {}", this.currentTransaction.getTransactionHandle() );
            }
        } else {
            if ( LOGGER.isTraceEnabled() ) {
                LOGGER.trace( "getOrStartTransaction() - GET - {}", this.currentTransaction.getTransactionHandle() );
            }
        }

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getOrStartTransaction() = {}", this.currentTransaction );
        }
        return this.currentTransaction;
    }


    public TransactionInfos getTransaction() {
        return this.currentTransaction;
    }


    public synchronized void endTransaction() {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "endTransaction() - Transaction {}", this.currentTransaction.getTransactionHandle() );
        }
        this.currentTransaction = null;
    }


    public void addAccessedNode( final AbstractRemoteNode node, RemoteConnectionHandle remoteConnection ) {
        this.addAccessedNodes( Collections.singleton( new SimpleImmutableEntry<>( node, remoteConnection ) ) );
    }


    public void addAccessedNode( final Entry<AbstractRemoteNode, RemoteConnectionHandle> node ) {
        this.addAccessedNodes( Collections.singleton( node ) );
    }


    public void addAccessedNodes( final Collection<Entry<AbstractRemoteNode, RemoteConnectionHandle>> nodes ) {
        nodes.forEach( entry -> {
            final AbstractRemoteNode node = entry.getKey();
            final RemoteConnectionHandle rch = entry.getValue();

            this.remoteConnections.put( node, rch );
            this.remoteNodes.compute( rch, ( handle, set ) -> {
                if ( set == null ) {
                    set = new HashSet<>();
                }
                set.add( node );
                return set;
            } );

            if ( this.currentTransaction != null ) {
                this.currentTransaction.addAccessedNode( node );
            }
        } );
    }


    public Collection<AbstractRemoteNode> getAccessedNodes() {
        return Collections.unmodifiableCollection( remoteConnections.keySet() );
    }


    /**
     * Return the planner of this connection
     */
    public Planner getPlanner() {
        return this.getPlanner( false );
    }


    public synchronized Planner getPlanner( final boolean forceNew ) {
        if ( !forceNew && this.planner != null ) {
            this.planner.close();
            this.planner.reset();
            return this.planner;
        }

        return this.planner = this.catalog.getPlanner();
    }
}
