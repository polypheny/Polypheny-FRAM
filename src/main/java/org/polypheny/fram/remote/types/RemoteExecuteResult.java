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
import java.util.LinkedList;
import java.util.List;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.proto.Common;
import org.polypheny.fram.remote.RemoteNode;


/**
 *
 */
public class RemoteExecuteResult implements RemoteResult, Serializable {

    private static final long serialVersionUID = 1L;
    private transient ExecuteResult theResult;
    private transient RemoteNode origin;


    private RemoteExecuteResult( ExecuteResult theResult ) {
        this.theResult = theResult;
    }


    public static RemoteExecuteResult fromExecuteResult( final ExecuteResult result ) {
        return new RemoteExecuteResult( result );
    }


    public ExecuteResult toExecuteResult() {
        return theResult;
    }


    private void writeObject( final java.io.ObjectOutputStream out ) throws IOException {
        out.defaultWriteObject();

        out.writeInt( this.theResult.resultSets.size() );
        for ( MetaResultSet resultSet : this.theResult.resultSets ) {
            out.writeUTF( resultSet.connectionId );
            out.writeInt( resultSet.statementId );
            out.writeLong( resultSet.updateCount );
            if ( resultSet.updateCount == -1 ) {
                out.writeBoolean( resultSet.ownStatement );
                resultSet.firstFrame.toProto().writeDelimitedTo( out );
                resultSet.signature.toProto().writeDelimitedTo( out );
            } else {
                // Update count result set
                // NO-OP: Everything is done
            }
        }
    }


    private void readObject( final java.io.ObjectInputStream in ) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        final int numberOfMetaResultSets = in.readInt();
        final List<MetaResultSet> resultSets = new LinkedList<>();
        for ( int i = 0; i < numberOfMetaResultSets; ++i ) {
            final String connectionId = in.readUTF();
            final int statementId = in.readInt();
            final long updateCount = in.readLong();
            if ( updateCount == -1 ) {
                final boolean ownStatement = in.readBoolean();
                final Frame firstFrame = Frame.fromProto( Common.Frame.parseDelimitedFrom( in ) );
                final Signature signature = Signature.fromProto( Common.Signature.parseDelimitedFrom( in ) );

                resultSets.add( MetaResultSet.create( connectionId, statementId, ownStatement, signature, firstFrame ) );
            } else {
                resultSets.add( MetaResultSet.count( connectionId, statementId, updateCount ) );
            }
        }
        this.theResult = new ExecuteResult( resultSets );
    }


    public void setOrigin( final RemoteNode origin ) {
        withOrigin( origin );
    }


    public RemoteExecuteResult withOrigin( final RemoteNode origin ) {
        this.origin = origin;
        return this;
    }


    public RemoteNode getOrigin() {
        return this.origin;
    }
}
