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
import java.util.Arrays;
import lombok.EqualsAndHashCode;
import lombok.Getter;


@Getter
@EqualsAndHashCode(doNotUseGetters = true)
public class RecordIdentifier implements Serializable {

    private static final long serialVersionUID = 2020_05_27__15_00L;

    private final String catalog;
    private final String schema;
    private final String table;
    private final Serializable[] key;


    public RecordIdentifier( final String catalog, final String schema, final String table, final Serializable[] key ) {
        if ( schema == null || schema.isEmpty() ) {
            throw new IllegalArgumentException( "schema == null || schema.isEmpty()" );
        }
        if ( table == null || table.isEmpty() ) {
            throw new IllegalArgumentException( "table == null || table.isEmpty()" );
        }
        if ( key == null || key.length == 0 || key[0] == null ) {
            throw new IllegalArgumentException( "key == null || key.length == 0 || key[0] == null" );
        }

        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
        this.key = key;
    }


    public RecordIdentifier( final String schema, final String table, final Serializable[] key ) {
        this( null, schema, table, key );
    }


    @Override
    public String toString() {
        return "RecordIdentifier{" +
                "catalog='" + catalog + '\'' +
                ", schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                ", key=" + Arrays.toString( key ) +
                '}';
    }
}
