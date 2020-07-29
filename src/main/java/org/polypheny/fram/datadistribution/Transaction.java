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

package org.polypheny.fram.datadistribution;


import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import javax.transaction.xa.Xid;
import lombok.EqualsAndHashCode;
import org.polypheny.fram.datadistribution.Transaction.Action;


@EqualsAndHashCode(doNotUseGetters = true, onlyExplicitlyIncluded = true)
public class Transaction implements Iterable<Action>, Serializable {

    private static final Map<ByteBuffer, Transaction> TRANSACTIONS = new HashMap<>();


    public static Transaction getTransaction( final Xid xid ) {
        synchronized ( TRANSACTIONS ) {
            return TRANSACTIONS.computeIfAbsent( ByteBuffer.wrap( xid.getGlobalTransactionId() ), ( __ ) -> new Transaction( xid ) );
        }
    }


    public static Transaction removeTransaction( final Transaction transaction ) {
        synchronized ( TRANSACTIONS ) {
            return TRANSACTIONS.remove( ByteBuffer.wrap( transaction.id ) );
        }
    }


    private final byte[] id = new byte[javax.transaction.xa.Xid.MAXBQUALSIZE];

    @EqualsAndHashCode.Include
    private final List<Action> actions = new LinkedList<>();

    private volatile boolean containsWriteOperation = false;


    private Transaction( final Xid xid ) {
        final byte[] globalTransactionId = xid.getGlobalTransactionId();
        System.arraycopy( globalTransactionId, 0, this.id, 0, Math.min( globalTransactionId.length, this.id.length ) );
    }


    public void addAction( final Operation operation, final RecordIdentifier record ) {
        if ( operation == Operation.WRITE ) {
            containsWriteOperation = true;
        }
        actions.add( new Action( operation, record ) );
    }


    public boolean isEmpty() {
        return actions.isEmpty();
    }


    public boolean isUpdate() {
        return containsWriteOperation;
    }


    public boolean isReadOnly() {
        return !containsWriteOperation;
    }


    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }
        Transaction that = (Transaction) o;
        return Objects.equals( actions, that.actions );
    }


    @Override
    public int hashCode() {
        return Objects.hash( actions );
    }


    @Override
    public Iterator<Action> iterator() {
        return actions.iterator();
    }


    @Override
    public void forEach( Consumer<? super Action> action ) {
        actions.forEach( action );
    }


    @Override
    public Spliterator<Action> spliterator() {
        return actions.spliterator();
    }


    public static enum Operation {
        READ,
        WRITE
    }


    @EqualsAndHashCode(doNotUseGetters = true)
    public static class Action implements Serializable {

        public final Operation operation;
        public final RecordIdentifier record;


        public Action( final Operation operation, final RecordIdentifier record ) {
            this.operation = operation;
            this.record = record;
        }
    }
}
