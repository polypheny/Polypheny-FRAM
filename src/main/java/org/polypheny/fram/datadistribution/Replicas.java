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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import org.polypheny.fram.remote.AbstractRemoteNode;


public class Replicas implements Iterable<AbstractRemoteNode>, Serializable {

    private static final long serialVersionUID = 2020_05_27__16_00L;

    private final Set<AbstractRemoteNode> replicas;
    private final ReplicationStrategy strategy;


    public Replicas( final AbstractRemoteNode replica ) {
        this.replicas = new HashSet<>();
        this.replicas.add( replica );
        this.strategy = ReplicationStrategy.UNDEFINED;
    }


    public Replicas( final Collection<AbstractRemoteNode> replicas, final ReplicationStrategy strategy ) {
        this.replicas = new HashSet<>( replicas );
        this.strategy = strategy;

        // check replicas and strategy if they are logical
        switch ( strategy ) {
            case UNDEFINED:
            case NONE:
                if ( replicas.size() < 1 ) {
                    throw new IllegalArgumentException( "At least one replica is required!" );
                }
                break;
            case ROWA:
                if ( replicas.size() < 2 ) {
                    throw new IllegalArgumentException( "ROWA-Replication needs at least two replicas!" );
                }
                break;
            case MAJORITY:
                if ( replicas.size() < 2 ) {
                    throw new IllegalArgumentException( "Majority-Replication needs at least two replicas!" );
                }
                break;
            case TWO:
                if ( replicas.size() < 2 ) {
                    throw new IllegalArgumentException( "2-Way-Replication needs at least two replicas!" );
                }
                break;
            case THREE:
                if ( replicas.size() < 3 ) {
                    throw new IllegalArgumentException( "3-Way-Replication needs at least three replicas!" );
                }
                break;
            default:
                throw new IllegalStateException( "Missing check for " + strategy );
        }
    }


    public void add( AbstractRemoteNode replica ) {
        this.replicas.add( replica );
    }


    public int size() {
        return this.replicas.size();
    }


    public Set<AbstractRemoteNode> getReplicas() {
        return Collections.unmodifiableSet( this.replicas );
    }


    public ReplicationStrategy getStrategy() {
        return this.strategy;
    }


    @Override
    public Iterator<AbstractRemoteNode> iterator() {
        return this.replicas.iterator();
    }


    @Override
    public void forEach( final Consumer<? super AbstractRemoteNode> action ) {
        this.replicas.forEach( action );
    }


    @Override
    public Spliterator<AbstractRemoteNode> spliterator() {
        return this.replicas.spliterator();
    }


    public enum ReplicationStrategy {
        UNDEFINED,
        NONE,
        ROWA,
        MAJORITY,
        TWO,
        THREE,
    }
}
