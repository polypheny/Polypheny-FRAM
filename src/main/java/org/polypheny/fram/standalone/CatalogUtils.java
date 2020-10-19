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

package org.polypheny.fram.standalone;


import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.polypheny.fram.Catalog;


public class CatalogUtils {

    private CatalogUtils() {
    }


    public static Map<String, Integer> lookupPrimaryKeyColumnsNamesAndIndexes( final ConnectionInfos connection, final String catalogName, final String schemaName, final String tableName ) {
        final Catalog catalog = connection.getCatalog();

        final List<String> primaryKeyColumnsNames = lookupPrimaryKeyColumnsNames( connection, catalogName, schemaName, tableName );

        final Map<String, Integer> primaryKeyNamesAndIndexes = new LinkedHashMap<>();
        for ( String primaryKeyColumnName : primaryKeyColumnsNames ) {
            MetaResultSet columnInfo = catalog.getColumns( connection, catalogName, Meta.Pat.of( schemaName ), Meta.Pat.of( tableName ), Meta.Pat.of( primaryKeyColumnName ) );
            for ( Object row : columnInfo.firstFrame.rows ) {
                Object[] cells = (Object[]) row;
                final int columnIndex = (int) cells[16] - 1; // convert ordinal to index
                if ( primaryKeyNamesAndIndexes.put( primaryKeyColumnName, columnIndex ) != null ) {
                    throw new IllegalStateException( "Search for the ordinals of the primary key columns was not specific enough!" );
                }
            }
        }

        return primaryKeyNamesAndIndexes;
    }


    public static List<String> lookupPrimaryKeyColumnsNames( final ConnectionInfos connection, final String catalogName, final String schemaName, final String tableName ) {
        final Catalog catalog = connection.getCatalog();

        final List<String> primaryKeyColumnsNames = new LinkedList<>();
        MetaResultSet mrs = catalog.getPrimaryKeys( connection, catalogName, schemaName, tableName );
        for ( Object row : mrs.firstFrame.rows ) {
            Object[] cells = (Object[]) row;
            primaryKeyColumnsNames.add( (String) cells[3] );
        }
        return primaryKeyColumnsNames;
    }


    public static List<Integer> lookupPrimaryKeyColumnsIndexes( final ConnectionInfos connection, final String catalogName, final String schemaName, final String tableName ) {
        final Catalog catalog = connection.getCatalog();

        final Map<String, Integer> primaryKeyColumnsNamesAndIndexes = lookupPrimaryKeyColumnsNamesAndIndexes( connection, catalogName, schemaName, tableName );
        List<Integer> primaryKeyColumnsIndexes = new ArrayList<>( primaryKeyColumnsNamesAndIndexes.values().size() );
        primaryKeyColumnsIndexes.addAll( primaryKeyColumnsNamesAndIndexes.values() );
        Collections.sort( primaryKeyColumnsIndexes );
        return primaryKeyColumnsIndexes;
    }


    public static List<String> lookupColumnsNames( final ConnectionInfos connection, final String catalogName, final String schemaName, final String tableName ) {
        final MetaResultSet columnInfos = connection.getCatalog().getColumns( connection, catalogName, Meta.Pat.of( schemaName ), Meta.Pat.of( tableName ), Meta.Pat.of( null ) );
        final List<String> columnsNames = new LinkedList<>();
        for ( Object row : columnInfos.firstFrame.rows ) {
            Object[] cells = (Object[]) row;
            final int columnIndex = (int) cells[16] - 1; // convert ordinal to index
            final String columnName = (String) cells[3];
            columnsNames.add( columnName );
        }
        return columnsNames;
    }
}
