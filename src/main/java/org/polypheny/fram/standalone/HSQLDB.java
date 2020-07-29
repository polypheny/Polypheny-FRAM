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


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
import org.hsqldb.Server;
import org.hsqldb.jdbc.JDBCStatement;
import org.hsqldb.jdbc.pool.JDBCXADataSource;
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

        Server hsqldbServer = new Server();
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

            JDBCXADataSource catalogJdbcXaDataSource = new JDBCXADataSource();
            catalogJdbcXaDataSource.setDatabase( catalogJdbcConnectionUrl );
            catalogJdbcXaDataSource.setUser( configuration.getString( "standalone.datastore.connection.user" ) );
            catalogJdbcXaDataSource.setPassword( configuration.getString( "standalone.datastore.connection.password" ) );
            catalog = new JdbcXAMeta( catalogJdbcXaDataSource, new Properties( cacheSettings ) );
            catalogDataSource = catalog.getDataSource();
            catalogXaDataSource = catalogJdbcXaDataSource;

            JDBCXADataSource storageJdbcXaDataSource = new JDBCXADataSource();
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

        //test();
    }


    private void test() {

        try ( Connection c = storageDataSource.getConnection() ) {
            try ( Statement s = c.createStatement() ) {
                System.out.println( "CREATE: " + s.execute( "CREATE TABLE USERTABLE ( YCSB_KEY INT, FIELD1 VARCHAR(100), PRIMARY KEY (YCSB_KEY) )" ) );
                System.out.println( "UC = " + s.getUpdateCount() );

                System.out.println( "INSERT: " + s.execute( "INSERT INTO USERTABLE ( YCSB_KEY, FIELD1 ) VALUES ( 1, 'one' )" ) );
                System.out.println( "UC = " + s.getUpdateCount() );

                s.execute( "INSERT INTO USERTABLE ( YCSB_KEY, FIELD1 ) VALUES ( 10, 'This' )" );
                s.execute( "INSERT INTO USERTABLE ( YCSB_KEY, FIELD1 ) VALUES ( 11, 'might' )" );
                s.execute( "INSERT INTO USERTABLE ( YCSB_KEY, FIELD1 ) VALUES ( 12, 'actually' )" );
                s.execute( "INSERT INTO USERTABLE ( YCSB_KEY, FIELD1 ) VALUES ( 13, 'work' )" );
                s.execute( "INSERT INTO USERTABLE ( YCSB_KEY, FIELD1 ) VALUES ( 14, 'and' )" );
                s.execute( "INSERT INTO USERTABLE ( YCSB_KEY, FIELD1 ) VALUES ( 15, 'if' )" );
                s.execute( "INSERT INTO USERTABLE ( YCSB_KEY, FIELD1 ) VALUES ( 16, 'it' )" );
                s.execute( "INSERT INTO USERTABLE ( YCSB_KEY, FIELD1 ) VALUES ( 17, 'does' )" );
                s.execute( "INSERT INTO USERTABLE ( YCSB_KEY, FIELD1 ) VALUES ( 18, 'it' )" );
                s.execute( "INSERT INTO USERTABLE ( YCSB_KEY, FIELD1 ) VALUES ( 19, 'is' )" );
                s.execute( "INSERT INTO USERTABLE ( YCSB_KEY, FIELD1 ) VALUES ( 20, 'awesome' )" );

                s.execute( "CREATE TABLE USERTABLE2 ( YCSB_KEY INT, YCSB_KEY2 INT, FIELD1 INT, PRIMARY KEY (YCSB_KEY, YCSB_KEY2) )" );
                s.execute( "INSERT INTO USERTABLE2 ( YCSB_KEY, YCSB_KEY2, FIELD1 ) VALUES ( 98,  5, 10 )" );
                s.execute( "INSERT INTO USERTABLE2 ( YCSB_KEY, YCSB_KEY2, FIELD1 ) VALUES ( 98,  7, 5 )" );
                s.execute( "INSERT INTO USERTABLE2 ( YCSB_KEY, YCSB_KEY2, FIELD1 ) VALUES ( 99, 10, 90 )" );

                System.out.println();
                System.out.println();

                System.out.println( "SELECT: " + s.execute( "SELECT count(YCSB_KEY) FROM USERTABLE", JDBCStatement.RETURN_PRIMARY_KEYS ) );
                System.out.println( "UC = " + s.getUpdateCount() );
                try ( ResultSet rs = s.getResultSet() ) {
                    while ( rs.next() ) {
                        System.out.println( "count = " + rs.getInt( 1 ) );
                    }
                }
                try ( ResultSet gk = s.getGeneratedKeys() ) {
                    if ( gk.next() ) {
                        do {
                            System.out.println( "PK = " + gk.getInt( 1 ) );
                        } while ( gk.next() );
                    } else {
                        System.out.println( "ERROR: getGeneratedKeys() for SELECT is not supported!" );
                    }
                }
                System.out.println();

                System.out.println( "SELECT: " + s.execute( "SELECT count(YCSB_KEY) FROM USERTABLE WHERE FIELD1 LIKE 'it'", JDBCStatement.RETURN_PRIMARY_KEYS ) );
                System.out.println( "UC = " + s.getUpdateCount() );
                try ( ResultSet rs = s.getResultSet() ) {
                    while ( rs.next() ) {
                        System.out.println( "count = " + rs.getInt( 1 ) );
                    }
                }
                try ( ResultSet gk = s.getGeneratedKeys() ) {
                    if ( gk.next() ) {
                        do {
                            System.out.println( "PK = " + gk.getInt( 1 ) );
                        } while ( gk.next() );
                    } else {
                        System.out.println( "ERROR: getGeneratedKeys() for SELECT is not supported!" );
                    }
                }
                System.out.println();

                System.out.println( "SELECT: " + s.execute( "SELECT * FROM USERTABLE", JDBCStatement.RETURN_PRIMARY_KEYS ) );
                System.out.println( "UC = " + s.getUpdateCount() );
                try ( ResultSet rs = s.getResultSet() ) {
                    while ( rs.next() ) {
                        System.out.println( "YCSB_KEY = " + rs.getInt( 1 ) + ", FIELD1 = " + rs.getString( 2 ) );
                    }
                }
                try ( ResultSet gk = s.getGeneratedKeys() ) {
                    if ( gk.next() ) {
                        do {
                            System.out.println( "PK = " + gk.getInt( 1 ) );
                        } while ( gk.next() );
                    } else {
                        System.out.println( "ERROR: getGeneratedKeys() for SELECT is not supported!" );
                    }
                }
                System.out.println();

                System.out.println( "SELECT: " + s.execute( "SELECT * FROM USERTABLE2", JDBCStatement.RETURN_PRIMARY_KEYS ) );
                System.out.println( "UC = " + s.getUpdateCount() );
                try ( ResultSet rs = s.getResultSet() ) {
                    while ( rs.next() ) {
                        System.out.println( "YCSB_KEY = " + rs.getInt( 1 ) + ", YCSB_KEY2 = " + rs.getInt( 2 ) + ", FIELD1 = " + rs.getString( 3 ) );
                    }
                }
                try ( ResultSet gk = s.getGeneratedKeys() ) {
                    if ( gk.next() ) {
                        do {
                            System.out.println( "PK1 = " + gk.getInt( 1 ) + ", PK2 = " + gk.getInt( 2 ) );
                        } while ( gk.next() );
                    } else {
                        System.out.println( "ERROR: getGeneratedKeys() for SELECT is not supported!" );
                    }
                }
                System.out.println();

                System.out.println( "SELECT: " + s.execute( "SELECT avg(FIELD1) FROM USERTABLE2 WHERE YCSB_KEY2 < 10", JDBCStatement.RETURN_PRIMARY_KEYS ) );
                System.out.println( "UC = " + s.getUpdateCount() );
                try ( ResultSet rs = s.getResultSet() ) {
                    while ( rs.next() ) {
                        System.out.println( "AVG = " + rs.getDouble( 1 ) );
                    }
                }
                try ( ResultSet gk = s.getGeneratedKeys() ) {
                    if ( gk.next() ) {
                        do {
                            System.out.println( "PK1 = " + gk.getInt( 1 ) + ", PK2 = " + gk.getInt( 2 ) );
                        } while ( gk.next() );
                    } else {
                        System.out.println( "ERROR: getGeneratedKeys() for SELECT is not supported!" );
                    }
                }
                System.out.println();

                /*
                System.out.println( "SELECT: " + s.execute( "SELECT YCSB_KEY, USERTABLE.FIELD1, JM_KEY, JOIN_ME.FIELD1 FROM USERTABLE, JOIN_ME WHERE YCSB_KEY = JM_KEY AND USERTABLE.FIELD1 like 'it'", JDBCStatement.RETURN_PRIMARY_KEYS ) );
                System.out.println( "UC = " + s.getUpdateCount() );
                try ( ResultSet rs = s.getResultSet() ) {
                    while ( rs.next() ) {
                        System.out.println( "YCSB_KEY = " + rs.getInt( 1 ) + ", USERTABLE.FIELD1 = " + rs.getString( 2 ) + ", JM_KEY = " + rs.getInt( 3 ) + ", JOIN_ME.FIELD1 = " + rs.getString( 4 ) );
                    }
                }
                try ( ResultSet gk = s.getGeneratedKeys() ) {
                    if ( gk.next() ) {
                        do {
                            System.out.println( "YCSB_KEY = " + gk.getInt( 1 ) + ", JM_KEY = " + gk.getInt( 2 ) );
                        } while ( gk.next() );
                    } else {
                        System.out.println("ERROR: getGeneratedKeys() for SELECT is not supported!");
                    }
                }
                System.out.println();
                */

                System.out.println( "INSERT: " + s.execute( "INSERT INTO USERTABLE ( YCSB_KEY, FIELD1 ) VALUES ( 2, 'two' )", JDBCStatement.RETURN_PRIMARY_KEYS ) );
                System.out.println( "UC = " + s.getUpdateCount() );
                try ( ResultSet gk = s.getGeneratedKeys() ) {
                    if ( gk.next() ) {
                        do {
                            System.out.println( "YCSB_KEY = " + gk.getInt( 1 ) );
                        } while ( gk.next() );
                    } else {
                        System.out.println( "ERROR: getGeneratedKeys() for INSERT is not supported!" );
                    }
                }
                System.out.println();

                System.out.println( "UPDATE: " + s.execute( "UPDATE USERTABLE SET FIELD1='two-new' WHERE YCSB_KEY=2", JDBCStatement.RETURN_PRIMARY_KEYS ) );
                System.out.println( "UC = " + s.getUpdateCount() );
                try ( ResultSet gk = s.getGeneratedKeys() ) {
                    if ( gk.next() ) {
                        do {
                            System.out.println( "PK = " + gk.getInt( 1 ) );
                        } while ( gk.next() );
                    } else {
                        System.out.println( "ERROR: getGeneratedKeys() for UPDATE is not supported!" );
                    }
                }
                System.out.println();

                System.out.println( "DELETE: " + s.execute( "DELETE FROM USERTABLE WHERE YCSB_KEY=2", JDBCStatement.RETURN_PRIMARY_KEYS ) );
                System.out.println( "UC = " + s.getUpdateCount() );
                try ( ResultSet gk = s.getGeneratedKeys() ) {
                    if ( gk.next() ) {
                        do {
                            System.out.println( "PK = " + gk.getInt( 1 ) );
                        } while ( gk.next() );
                    } else {
                        System.out.println( "ERROR: getGeneratedKeys() for DELETE is not supported!" );
                    }
                }
                System.out.println();
            }

            System.out.println();

            PreparedStatement psI = c.prepareStatement( "INSERT INTO USERTABLE ( YCSB_KEY, FIELD1 ) VALUES ( ?, ? )", JDBCStatement.RETURN_PRIMARY_KEYS );
            PreparedStatement psU = c.prepareStatement( "UPDATE USERTABLE SET FIELD1=? WHERE YCSB_KEY=?", JDBCStatement.RETURN_PRIMARY_KEYS );
            PreparedStatement psD = c.prepareStatement( "DELETE FROM USERTABLE WHERE YCSB_KEY=?", JDBCStatement.RETURN_PRIMARY_KEYS );

            psI.setInt( 1, 3 );
            psI.setString( 2, "three" );
            System.out.println( "INSERT: " + psI.execute() );
            System.out.println( "UC = " + psI.getUpdateCount() );
            try ( ResultSet gk = psI.getGeneratedKeys() ) {
                while ( gk.next() ) {
                    System.out.println( "PK = " + gk.getInt( 1 ) );
                }
            }

            psU.setInt( 2, 3 );
            psU.setString( 1, "three-new" );
            System.out.println( "UPDATE: " + psU.execute() );
            System.out.println( "UC = " + psU.getUpdateCount() );
            try ( ResultSet gk = psU.getGeneratedKeys() ) {
                while ( gk.next() ) {
                    System.out.println( "PK = " + gk.getInt( 1 ) );
                }
            }

            psD.setInt( 1, 3 );
            System.out.println( "DELETE: " + psD.execute() );
            System.out.println( "UC = " + psD.getUpdateCount() );
            try ( ResultSet gk = psD.getGeneratedKeys() ) {
                while ( gk.next() ) {
                    System.out.println( "PK = " + gk.getInt( 1 ) );
                }
            }

            Thread.sleep( 1000 );
            System.out.println( "FINISHED - TERMINATING" );
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
        System.exit( 0 );
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
