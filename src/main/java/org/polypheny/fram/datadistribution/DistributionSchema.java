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
import java.util.HashMap;
import java.util.Map;
import org.polypheny.fram.remote.AbstractRemoteNode;


public class DistributionSchema implements Serializable {

    private static final long serialVersionUID = 2020_05_27__15_00L;


    private final long id;
    private final Map<RecordIdentifier, Replicas> schema;


    public DistributionSchema() {
        this.id = System.currentTimeMillis();
        this.schema = new HashMap<>();
    }


    public void add( final RecordIdentifier record, final AbstractRemoteNode location ) {
        if ( schema.containsKey( record ) ) {
            Replicas locations = schema.get( record );
            locations.add( location );
        } else {
            Replicas locations = new Replicas( location );
            schema.put( record, locations );
        }
    }
}
