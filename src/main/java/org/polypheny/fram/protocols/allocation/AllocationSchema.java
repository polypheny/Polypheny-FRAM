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

package org.polypheny.fram.protocols.allocation;


import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.polypheny.fram.Node;
import org.polypheny.fram.datadistribution.VirtualNode;
import org.polypheny.fram.remote.Cluster;
import org.polypheny.fram.remote.PhysicalNode;


public class AllocationSchema {

    public static PhysicalNode LOCAL_NODE;

    public static final Set<PhysicalNode> ALL_NODES = new HashSet<>();

    private final Map<VirtualNode, PhysicalNode> allocationSchema = new ConcurrentHashMap<>();


    public AllocationSchema() {

    }


    public AllocationSchema( final Map<VirtualNode, PhysicalNode> allocationSchema ) {
        this.allocationSchema.putAll( allocationSchema );
    }


    public PhysicalNode lookupNode( Node node ) {
        if ( node instanceof PhysicalNode ) {
            return (PhysicalNode) node;
        }
        if ( node instanceof VirtualNode ) {
            return allocationSchema.computeIfAbsent( (VirtualNode) node, virtualNode -> {
                PhysicalNode physicalNode = Cluster.getDefaultCluster().getRemoteNode( new org.jgroups.util.UUID( virtualNode.id.getMostSignificantBits(), virtualNode.id.getLeastSignificantBits() ) );
                if ( physicalNode == null ) {
                    physicalNode = Cluster.getDefaultCluster().getLocalNode();
                }
                return physicalNode;
            } );
        }
        throw new IllegalArgumentException( "Unknown type of node" );
    }


    public Set<PhysicalNode> allNodes() {
        return Collections.unmodifiableSet( new HashSet<>( allocationSchema.values() ) );
    }
}
