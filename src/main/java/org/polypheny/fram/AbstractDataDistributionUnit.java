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

package org.polypheny.fram;


import org.polypheny.fram.remote.AbstractLocalNode;
import org.polypheny.fram.remote.Cluster;
import org.polypheny.fram.remote.RemoteNode;
import java.util.UUID;


/**
 *
 */
public abstract class AbstractDataDistributionUnit {


    public final UUID nodeId;

    protected final AbstractLocalNode localNode;

    protected final Cluster defaultCluster;
    protected final RemoteNode thisNode;


    public AbstractDataDistributionUnit( final AbstractLocalNode localNode ) {
        this.localNode = localNode;

        this.defaultCluster = Cluster.getDefaultCluster().connect( this.localNode );
        this.thisNode = this.defaultCluster.thisNode();

        this.nodeId = localNode.getCatalog().getNodeId();
    }
}
