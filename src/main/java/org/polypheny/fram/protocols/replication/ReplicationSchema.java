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

package org.polypheny.fram.protocols.replication;


import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.polypheny.fram.Node;
import org.polypheny.fram.datadistribution.RecordIdentifier;
import org.polypheny.fram.protocols.fragmentation.Fragment;


public class ReplicationSchema {

    public static final Replica TEST_REPLICA = new Replica( UUID.fromString( "13588b9d-986f-45f0-b1cf-d0f282f99c10" ) );

    public static final Set<Replica> ALL_REPLICAS = new HashSet<>();

    private final Map<Fragment, Set<Replica>> fragmentReplicasMap = new ConcurrentHashMap<>();


    public Replica lookupReplica( RecordIdentifier record ) {
        return TEST_REPLICA;
    }


    public Replica lookupReplica( Fragment fragment ) {
        return TEST_REPLICA;
    }


    public Replica lookupReplica( Node node ) {
        return TEST_REPLICA;
    }


    public Set<Replica> lookupReplicas( Fragment fragment ) {
        return fragmentReplicasMap.computeIfAbsent( fragment, __ -> {
            if ( ALL_REPLICAS.isEmpty() ) {
                return Collections.singleton( TEST_REPLICA );
            } else {
                return ALL_REPLICAS;
            }
        } );
    }


    public Set<Replica> allReplicas() {
        return Collections.unmodifiableSet( new HashSet<>( ALL_REPLICAS ) );
    }
}
