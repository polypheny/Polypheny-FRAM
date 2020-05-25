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


import javax.sql.DataSource;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParser.Config;


public class DataStore {

    private DataStore() {
    }


    private static Store STORE;


    public static Store setStore( Store store ) {
        Store old = DataStore.STORE;
        DataStore.STORE = store;
        return old;
    }


    public static XAMeta getStorage() {
        return STORE.getStorageAsXAMeta();
    }


    public static DataSource getStorageDataSource() {
        return STORE.getStorageDataSource();
    }


    public static Config getStorageParserConfig() {
        return STORE.getSqlParserConfig();
    }


    public static JdbcImplementor getRelToSqlConverter() {
        return STORE.getRelToSqlConverter();
    }


    public static SqlDialect getStorageDialect() {
        return STORE.getStoreDialect();
    }


    public static String getStorageJdbcConnectionUrl() {
        return STORE.getStorageJdbcConnectionUrl();
    }


    public static XAMeta getCatalog() {
        return STORE.getCatalogAsXAMeta();
    }


    public static DataSource getCatalogDataSource() {
        return STORE.getCatalogDataSource();
    }


    public static Config getCatalogParserConfig() {
        return STORE.getSqlParserConfig();
    }


    public static SqlDialect getCatalogDialect() {
        return STORE.getStoreDialect();
    }
}
