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


import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import javax.transaction.xa.Xid;
import org.polypheny.fram.remote.PhysicalNode;
import org.polypheny.fram.standalone.transaction.TransactionHandle;


/**
 *
 */
public class TransactionInfos {

    private final ConnectionInfos connection;
    private final TransactionHandle transactionHandle;

    private final Set<PhysicalNode> accessedNodes = new HashSet<>();

    private State state = State.STARTED;


    TransactionInfos( final ConnectionInfos connection ) {
        this( connection, TransactionHandle.generateGlobalTransactionIdentifier( connection.nodeId, connection.userId, connection.connectionId, UUID.randomUUID() ) );
    }


    TransactionInfos( final ConnectionInfos connection, final TransactionHandle transactionHandle ) {
        this.connection = connection;
        this.transactionHandle = transactionHandle;
    }


    public TransactionHandle getTransactionHandle() {
        return this.transactionHandle;
    }


    public Xid getTransactionId() {
        return this.transactionHandle;
    }


    public boolean requires2pc() {
        synchronized ( this.accessedNodes ) {
            return this.accessedNodes.size() > 1;
        }
    }


    public boolean isStarted() {
        return state == State.STARTED;
    }


    public boolean isAborted() {
        return state == State.ABORTED;
    }


    public boolean isPrepared() {
        return state == State.PREPARED;
    }


    public boolean isCommitted() {
        return state == State.COMMITTED;
    }


    public boolean isRolledback() {
        return state == State.ROLLEDBACK;
    }


    void addAccessedNode( final PhysicalNode node ) {
        this.addAccessedNodes( Collections.singleton( node ) );
    }


    void addAccessedNodes( final Collection<PhysicalNode> nodes ) {
        synchronized ( this.accessedNodes ) {
            this.accessedNodes.addAll( nodes );
        }
    }


    public Set<PhysicalNode> getAccessedNodes() {
        synchronized ( this.accessedNodes ) {
            return Collections.unmodifiableSet( new HashSet<>( this.accessedNodes ) );
        }
    }


    /**
     *
     */
    private enum State {
        STARTED,
        ABORTED,
        PREPARED,
        COMMITTED,
        ROLLEDBACK,
    }
}
