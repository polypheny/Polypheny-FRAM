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


import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.sql.DataSource;
import lombok.EqualsAndHashCode;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.jgroups.Address;
import org.jgroups.blocks.MethodLookup;
import org.polypheny.fram.Catalog;


/**
 *
 */
@EqualsAndHashCode(callSuper = false, onlyExplicitlyIncluded = true)
public abstract class AbstractLocalNode extends AbstractNode implements MethodLookup {

    protected static final Map<UUID, Address> CLUSTER_ID_TO_LOCAL_NODE_ADDRESS = new HashMap<>();


    protected AbstractLocalNode() {

    }


    @Override
    public final java.lang.reflect.Method findMethod( final short id ) {
        return RemoteMeta.Method.findMethod( id );
    }


    public final RemoteMeta asRemoteMeta() {
        return this;
    }


    public final AbstractRemoteNode asRemoteNode() {
        return this.asRemoteNode( Cluster.getDefaultClusterId() );
    }


    public final AbstractRemoteNode asRemoteNode( final UUID clusterId ) {
        return new RemoteNode( CLUSTER_ID_TO_LOCAL_NODE_ADDRESS.get( clusterId ), Cluster.getCluster( clusterId ) );
    }


    @Override
    @EqualsAndHashCode.Include
    public Address getNodeAddress() {
        return CLUSTER_ID_TO_LOCAL_NODE_ADDRESS.get( Cluster.getDefaultClusterId() );
    }


    public Address getNodeAddress( final Cluster cluster ) {
        return this.getNodeAddress( cluster.getClusterId() );
    }


    public Address getNodeAddress( final UUID clusterId ) {
        return CLUSTER_ID_TO_LOCAL_NODE_ADDRESS.get( clusterId );
    }


    public void setNodeAddress( final Cluster cluster, final Address addressOfThisLocalNodeInTheCluster ) {
        setNodeAddress( cluster.getClusterId(), addressOfThisLocalNodeInTheCluster );
    }


    public void setNodeAddress( final UUID clusterId, final Address addressOfThisLocalNodeInTheCluster ) {
        CLUSTER_ID_TO_LOCAL_NODE_ADDRESS.put( clusterId, addressOfThisLocalNodeInTheCluster );
    }


    public abstract Config getSqlParserConfig();

    public abstract JdbcImplementor getRelToSqlConverter();

    public abstract SqlDialect getSqlDialect();

    public abstract DataSource getDataSource();

    public abstract Catalog getCatalog();

}
