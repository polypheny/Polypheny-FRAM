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


import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.jgroups.Address;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;


public class ClusterUtils {

    private ClusterUtils() {
    }


    public static <ResponseType> Map<AbstractRemoteNode, ResponseType> getRemoteResults( final Cluster cluster, final RspList<ResponseType> responses ) throws RemoteException {
        final Map<AbstractRemoteNode, ResponseType> remoteResults = new HashMap<>();

        final Map<AbstractRemoteNode, Throwable> throwables = new HashMap<>();

        for ( Entry<Address, Rsp<ResponseType>> responseEntry : responses.entrySet() ) {
            final RemoteNode node = cluster.getRemoteNode( responseEntry.getKey() );
            final Rsp<ResponseType> response = responseEntry.getValue();

            if ( response.hasException() ) {
                throwables.put( node, response.getException() );
            } else {
                remoteResults.put( node, response.getValue() );
            }
        }

        if ( !throwables.isEmpty() ) {
            final RemoteException ex = new RemoteException( "Exception at " + throwables.keySet().toString() + " occurred." );
            for ( Throwable suppressed : throwables.values() ) {
                ex.addSuppressed( suppressed );
            }
            throw ex;
        }

        return remoteResults;
    }
}
