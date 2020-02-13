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

package org.polypheny.fram.remote.types;


import java.io.IOException;
import java.io.Serializable;
import lombok.EqualsAndHashCode;
import org.apache.calcite.avatica.Meta.ConnectionHandle;


/**
 *
 */
@EqualsAndHashCode(doNotUseGetters = true, onlyExplicitlyIncluded = true)
public class RemoteConnectionHandle implements Serializable {

    private static final long serialVersionUID = 1L;

    @EqualsAndHashCode.Include
    private transient ConnectionHandle theConnectionHandle;


    private RemoteConnectionHandle( final ConnectionHandle connectionHandle ) {
        this.theConnectionHandle = connectionHandle;
    }


    public static RemoteConnectionHandle fromConnectionHandle( final ConnectionHandle connectionHandle ) {
        return new RemoteConnectionHandle( connectionHandle );
    }


    public ConnectionHandle toConnectionHandle() {
        return theConnectionHandle;
    }


    private void writeObject( final java.io.ObjectOutputStream out ) throws IOException {
        out.defaultWriteObject();

        out.writeUTF( this.theConnectionHandle.id );
    }


    private void readObject( final java.io.ObjectInputStream in ) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        this.theConnectionHandle = new ConnectionHandle( in.readUTF() );
    }


    @Override
    public String toString() {
        return theConnectionHandle.toString();
    }
}
