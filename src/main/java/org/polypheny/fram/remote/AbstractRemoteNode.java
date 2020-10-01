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


import java.io.IOException;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.EqualsAndHashCode;
import org.jgroups.Address;
import org.jgroups.Message.Flag;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;


@EqualsAndHashCode(callSuper = false)
public abstract class AbstractRemoteNode extends PhysicalNode implements RemoteMeta, Serializable {

    private static final long serialVersionUID = 2020_03_26__17_20L;

    private static final RequestOptions DEFAULT_REQUEST_OPTIONS = new RequestOptions()
            .setMode( ResponseMode.GET_FIRST )
            .setTimeout( TimeUnit.SECONDS.toMillis( 60 ) )
            .setFlags( Flag.OOB );


    protected final Address address;
    protected transient Cluster cluster;


    AbstractRemoteNode( final Address address, final Cluster cluster ) {
        super();
        this.address = address;
        this.cluster = cluster;
    }


    private void writeObject( final java.io.ObjectOutputStream out ) throws IOException {
        out.defaultWriteObject();

        out.writeObject( (UUID) cluster.getClusterId() );
    }


    private void readObject( final java.io.ObjectInputStream in ) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        this.cluster = Cluster.getCluster( (UUID) in.readObject() );
    }


    public Cluster getCluster() {
        return this.cluster;
    }


    @Override
    public Address getNodeAddress() {
        return this.address;
    }


    public final RemoteMeta asRemoteMeta() {
        return this;
    }


    public final boolean isLocalNode() {
        return this.address.equals( this.cluster.getLocalNode().getNodeAddress( this.cluster ) );
    }


    public final AbstractLocalNode asLocalNode() {
        if ( isLocalNode() ) {
            return this.cluster.getLocalNode();
        } else {
            throw new IllegalStateException( "This is not the remote representation of the local node." );
        }
    }


    protected <ReturnType> ReturnType callRemoteMethod( final MethodCall methodCall ) throws RemoteException {
        return callRemoteMethod( DEFAULT_REQUEST_OPTIONS, methodCall );
    }


    protected <ReturnType> ReturnType callRemoteMethod( final RequestOptions requestOptions, final MethodCall methodCall ) throws RemoteException {
        return this.cluster.callMethod( requestOptions, methodCall, this );
    }


    @Override
    public String toString() {
        return this.address.toString();
    }
}
