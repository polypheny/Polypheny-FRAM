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


import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import javax.sql.XADataSource;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.avatica.jdbc.JdbcMeta.ConnectionCacheSettings;
import org.apache.calcite.avatica.jdbc.JdbcMeta.StatementCacheSettings;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialect.DatabaseProduct;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.polypheny.fram.standalone.parser.SqlParserImpl;


public final class HSQLDB implements Store {

    private static final HSQLDB INSTANCE;


    static {
        INSTANCE = new HSQLDB();
    }


    private final JdbcXAMeta catalog;
    private final DataSource catalogDataSource;
    private final XADataSource catalogXaDataSource;

    private final JdbcXAMeta storage;
    private final DataSource storageDataSource;
    private final XADataSource storageXaDataSource;

    private final SqlDialect storeDialect;
    private final Config sqlParserConfig;
    private final JdbcImplementor rel2sqlConverter;

    private final String storageJdbcConnectionUrl;
    private final String catalogJdbcConnectionUrl;


    private HSQLDB() {
        final com.typesafe.config.Config configuration = Main.configuration();

        org.hsqldb.Server hsqldbServer = new org.hsqldb.Server();
        hsqldbServer.setAddress( configuration.getString( "standalone.datastore.jdbc.listens" ) );
        hsqldbServer.setPort( configuration.getInt( "standalone.datastore.jdbc.port" ) );
        hsqldbServer.setDaemon( true );

        hsqldbServer.setDatabaseName( 0, configuration.getString( "standalone.datastore.catalog.name" ) );
        hsqldbServer.setDatabasePath( 0, "mem:" + configuration.getString( "standalone.datastore.catalog.name" ) );

        hsqldbServer.setDatabaseName( 1, configuration.getString( "standalone.datastore.database.name" ) );
        hsqldbServer.setDatabasePath( 1, "mem:" + configuration.getString( "standalone.datastore.database.name" ) );

        hsqldbServer.setLogWriter( null );
        hsqldbServer.setErrWriter( null );
        hsqldbServer.setSilent( true );
        hsqldbServer.setNoSystemExit( true );

        Runtime.getRuntime().addShutdownHook( new Thread( () -> {
            if ( hsqldbServer != null ) {
                hsqldbServer.shutdown();
            }
        }, "HSQLDB ShutdownHook" ) );

        hsqldbServer.start();

        try {
            Properties cacheSettings = new Properties();
            cacheSettings.put( ConnectionCacheSettings.EXPIRY_UNIT.key(), TimeUnit.HOURS.name() );
            cacheSettings.put( StatementCacheSettings.EXPIRY_UNIT.key(), TimeUnit.HOURS.name() );

            catalogJdbcConnectionUrl = "jdbc:hsqldb:hsql://" + "127.0.0.1" + ":" + configuration.getInt( "standalone.datastore.jdbc.port" ) + "/" + configuration.getString( "standalone.datastore.catalog.name" )
                    + ";hsqldb.tx=" + configuration.getString( "standalone.datastore.connection.hsqldb.tx" )
                    + ";hsqldb.tx_level=" + configuration.getString( "standalone.datastore.connection.hsqldb.tx_level" )
                    + ";close_result=true"
                    + ";allow_empty_batch=true"
            ;

            if ( configuration.hasPath( "standalone.datastore.connection.url" ) ) {
                storageJdbcConnectionUrl = configuration.getString( "standalone.datastore.connection.url" );
            } else {
                storageJdbcConnectionUrl = "jdbc:hsqldb:hsql://" + "127.0.0.1" + ":" + configuration.getInt( "standalone.datastore.jdbc.port" ) + "/" + configuration.getString( "standalone.datastore.database.name" )
                        + ";hsqldb.tx=" + configuration.getString( "standalone.datastore.connection.hsqldb.tx" )
                        + ";hsqldb.tx_level=" + configuration.getString( "standalone.datastore.connection.hsqldb.tx_level" )
                        + ";close_result=true"
                        + ";allow_empty_batch=true"
                ;
            }

            org.hsqldb.jdbc.pool.JDBCXADataSource catalogJdbcXaDataSource = new org.hsqldb.jdbc.pool.JDBCXADataSource();
            catalogJdbcXaDataSource.setDatabase( catalogJdbcConnectionUrl );
            catalogJdbcXaDataSource.setUser( configuration.getString( "standalone.datastore.connection.user" ) );
            catalogJdbcXaDataSource.setPassword( configuration.getString( "standalone.datastore.connection.password" ) );
            catalog = new JdbcXAMeta( catalogJdbcXaDataSource, new Properties( cacheSettings ) );
            catalogDataSource = catalog.getDataSource();
            catalogXaDataSource = catalogJdbcXaDataSource;

            org.hsqldb.jdbc.pool.JDBCXADataSource storageJdbcXaDataSource = new org.hsqldb.jdbc.pool.JDBCXADataSource();
            storageJdbcXaDataSource.setDatabase( storageJdbcConnectionUrl );
            storageJdbcXaDataSource.setUser( configuration.getString( "standalone.datastore.connection.user" ) );
            storageJdbcXaDataSource.setPassword( configuration.getString( "standalone.datastore.connection.password" ) );
            storage = new JdbcXAMeta( storageJdbcXaDataSource, new Properties( cacheSettings ) );
            storageDataSource = storage.getDataSource();
            storageXaDataSource = storageJdbcXaDataSource;

            storeDialect = new SqlDialect( SqlDialect.EMPTY_CONTEXT.withDatabaseProduct( DatabaseProduct.HSQLDB ).withIdentifierQuoteString( "\"" ) );
            sqlParserConfig = SqlParser.configBuilder().setParserFactory( SqlParserImpl.FACTORY ).setLex( Lex.MYSQL ).setCaseSensitive( false ).build();
            rel2sqlConverter = new JdbcImplementor( storeDialect, new JavaTypeFactoryImpl() );

        } catch ( SQLException ex ) {
            throw new RuntimeException( ex );
        }

        DataStore.setStore( this );
    }


    @Override
    public XAMeta getCatalogAsXAMeta() {
        return catalog;
    }


    @Override
    public DataSource getCatalogDataSource() {
        return catalogDataSource;
    }


    @Override
    public XAMeta getStorageAsXAMeta() {
        return storage;
    }


    @Override
    public DataSource getStorageDataSource() {
        return storageDataSource;
    }


    @Override
    public SqlDialect getStoreDialect() {
        return storeDialect;
    }


    @Override
    public Config getSqlParserConfig() {
        return sqlParserConfig;
    }


    @Override
    public JdbcImplementor getRelToSqlConverter() {
        return rel2sqlConverter;
    }


    @Override
    public String getStorageJdbcConnectionUrl() {
        return storageJdbcConnectionUrl;
    }
}
