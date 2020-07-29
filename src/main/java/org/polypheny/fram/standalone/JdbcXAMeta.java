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


import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchConnectionException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.jdbc.JdbcResultSet;
import org.apache.calcite.avatica.jdbc.StatementInfo;
import org.apache.calcite.avatica.remote.TypedValue;
import org.polypheny.fram.standalone.transaction.TransactionHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class JdbcXAMeta extends JdbcMeta implements XAMeta {

    private static final Logger LOG = LoggerFactory.getLogger( JdbcXAMeta.class );
    private static final Executor ABORT_CONNECTION_EXECUTOR = Executors.newCachedThreadPool();

    private final UUID storeId;

    private final XADataSource xaDataSource;
    private final DataSource dataSource;
    private final Properties settings;

    private final Cache<String, XAConnection> xaConnectionCache;
    private final Map<TransactionHandle, TransactionInfos> transactionMap;


    public JdbcXAMeta( final XADataSource xaDataSource, final Properties settings ) throws SQLException {
        super( null, settings );
        this.storeId = UUID.randomUUID(); // this might not be a good way to identify this store, especially if this object is constructed more than once for the given datasource!

        this.xaDataSource = xaDataSource;
        this.dataSource = new XADataSourceAdapter( this.xaDataSource );
        this.settings = settings;

        int concurrencyLevel = Integer.parseInt( settings.getProperty( ConnectionCacheSettings.CONCURRENCY_LEVEL.key(), ConnectionCacheSettings.CONCURRENCY_LEVEL.defaultValue() ) );
        int initialCapacity = Integer.parseInt( settings.getProperty( ConnectionCacheSettings.INITIAL_CAPACITY.key(), ConnectionCacheSettings.INITIAL_CAPACITY.defaultValue() ) );
        long maxCapacity = Long.parseLong( settings.getProperty( ConnectionCacheSettings.MAX_CAPACITY.key(), ConnectionCacheSettings.MAX_CAPACITY.defaultValue() ) );
        long connectionExpiryDuration = Long.parseLong( settings.getProperty( ConnectionCacheSettings.EXPIRY_DURATION.key(), ConnectionCacheSettings.EXPIRY_DURATION.defaultValue() ) );
        TimeUnit connectionExpiryUnit = TimeUnit.valueOf( settings.getProperty( ConnectionCacheSettings.EXPIRY_UNIT.key(), ConnectionCacheSettings.EXPIRY_UNIT.defaultValue() ) );
        this.xaConnectionCache = CacheBuilder.newBuilder()
                .concurrencyLevel( concurrencyLevel )
                .initialCapacity( initialCapacity )
                .maximumSize( maxCapacity )
                .expireAfterAccess( connectionExpiryDuration, connectionExpiryUnit )
                .removalListener( (RemovalListener<String, XAConnection>) notification -> {
                    String connectionId = notification.getKey();
                    XAConnection doomed = notification.getValue();
                    LOG.debug( "Expiring connection {} because {}", connectionId, notification.getCause() );
                    try {
                        if ( doomed != null ) {
                            doomed.close();
                        }
                    } catch ( Throwable t ) {
                        LOG.info( "Exception thrown while expiring connection {}", connectionId, t );
                    }
                } )
                .build();

        this.transactionMap = new ConcurrentHashMap<>();
    }


    public XADataSource getXaDataSource() {
        return xaDataSource;
    }


    public DataSource getDataSource() {
        return dataSource;
    }


    @Override
    public void openConnection( final ConnectionHandle connectionHandle, final Map<String, String> info ) {
        LOG.trace( "openConnection( connectionHandle: {}, info: {} )", connectionHandle, info );

        final Properties fullInfo = new Properties();
        fullInfo.putAll( this.settings );
        if ( info != null ) {
            fullInfo.putAll( info );
        }

        final ConcurrentMap<String, XAConnection> xaConnectionCacheAsMap = xaConnectionCache.asMap();
        if ( xaConnectionCacheAsMap.containsKey( connectionHandle.id ) ) {
            throw new RuntimeException( "Connection already exists: " + connectionHandle.id );
        }

        // Avoid global synchronization of connection opening
        try {
            LOG.trace( "opening new XAConnection" );
            // todo: check if info.user = settings.user --- if not, then use getXAConnection( String, String )
            XAConnection xaConnection = xaDataSource.getXAConnection( /*fullInfo.getProperty( "user", "SA" ), fullInfo.getProperty( "password", "" )*/ );
            XAConnection loadedXaConnection = xaConnectionCacheAsMap.putIfAbsent( connectionHandle.id, xaConnection );
            // Race condition: someone beat us to storing the connection in the cache.
            if ( loadedXaConnection != null ) {
                xaConnection.close();
                throw new RuntimeException( "Connection already exists: " + connectionHandle.id );
            }

            // Store the actual, physical connection (used by getConnection( String ) )
            super.getConnectionCache().asMap().putIfAbsent( connectionHandle.id, xaConnection.getConnection() );
            //this.transactionMap.putIfAbsent( connectionHandle.id, XidWrapper. )
        } catch ( SQLException ex ) {
            throw new RuntimeException( ex );
        }
    }


    @Override
    public void closeConnection( final ConnectionHandle connectionHandle ) {
        LOG.trace( "closeConnection( connectionHandle: {} )", connectionHandle );

        final XAConnection physicalXAConnection = xaConnectionCache.getIfPresent( connectionHandle.id );
        if ( physicalXAConnection == null ) {
            LOG.debug( "client requested close unknown connection {}", connectionHandle );
            return;
        }

        final Connection physicalConnection = super.getConnectionCache().getIfPresent( connectionHandle.id );

        if ( physicalConnection != null ) {
            try {
                physicalConnection.close();
            } catch ( SQLException ignored ) {
            } finally {
                super.getConnectionCache().invalidate( connectionHandle.id );
            }
        }

        try {
            physicalXAConnection.close();
        } catch ( SQLException ex ) {
            throw new RuntimeException( ex );
        } finally {
            xaConnectionCache.invalidate( connectionHandle.id );
        }
    }


    public void abortConnection( final ConnectionHandle connectionHandle ) {
        LOG.trace( "abortConnection( connectionHandle: {} )", connectionHandle );

        final XAConnection physicalXAConnection = xaConnectionCache.getIfPresent( connectionHandle.id );
        if ( physicalXAConnection == null ) {
            LOG.debug( "client requested abortConnection unknown connection {}", connectionHandle );
            return;
        }

        final Connection physicalConnection = super.getConnectionCache().getIfPresent( connectionHandle.id );

        try {
            if ( physicalConnection != null ) {
                try {
                    physicalConnection.abort( ABORT_CONNECTION_EXECUTOR );
                } catch ( SQLException ex ) {
                    throw new RuntimeException( ex );
                } finally {
                    super.getConnectionCache().invalidate( connectionHandle.id );
                }
            }
        } finally {
            xaConnectionCache.invalidate( connectionHandle.id );
        }
    }


    @Override
    public TransactionInfos getOrStartTransaction( final ConnectionInfos connection, final TransactionHandle transactionHandle ) throws XAException {
        LOG.trace( "getOrStartTransaction( connection: {}, transactionHandle: {} )", connection, transactionHandle );

        final TransactionInfos result;
        try {
            synchronized ( transactionMap ) {
                result = transactionMap.computeIfAbsent( transactionHandle, handleOfTransactionToStart -> {
                    LOG.trace( "starting a new transaction ( {} )", handleOfTransactionToStart );

                    final XAConnection xaConnection = xaConnectionCache.getIfPresent( connection.getConnectionHandle().id );
                    if ( xaConnection == null ) {
                        LOG.debug( "client requested SoT on an unknown connection {}", connection );
                        throw new RuntimeException( "Unknown connection " + connection.getConnectionHandle() );
                    }

                    final XAResource xaResource;
                    try {
                        xaResource = xaConnection.getXAResource();
                    } catch ( SQLException ex ) {
                        throw new InternalError( ex );
                    }

                    // BEGIN HACK
                    if ( ((org.hsqldb.jdbc.pool.JDBCXAResource) xaResource).withinGlobalTransaction() ) {
                        LOG.warn( "transaction {} - ALREADY STARTED for connection {}", transactionHandle, connection.getConnectionHandle() );
                        return new TransactionInfos( connection, handleOfTransactionToStart );
                    }
                    // END HACK

                    LOG.trace( "creating transaction {} for connection {}", transactionHandle.getTransactionId(), connection.getConnectionHandle() );

                    try {
                        xaResource.start( handleOfTransactionToStart, XAResource.TMNOFLAGS );
                    } catch ( XAException ex ) {
                        throw new InternalError( ex );
                    }

                    return new TransactionInfos( connection, handleOfTransactionToStart );
                } );
            }
        } catch ( final InternalError e ) {
            final Throwable cause = e.getCause();
            if ( cause instanceof SQLException ) {
                throw new XAException( e.getMessage() );
            } else if ( cause instanceof XAException ) {
                throw (XAException) cause;
            } else {
                throw e;
            }
        }

        LOG.trace( "getOrStartTransaction( connection: {}, transactionHandle: {} ) = {}", connection, transactionHandle, result );
        return result;
    }


    @Override
    public void onePhaseCommit( final ConnectionHandle connectionHandle, final TransactionHandle transactionHandle ) throws XAException {
        LOG.trace( "onePhaseCommit( connectionHandle: {}, transactionHandle: {} )", connectionHandle, transactionHandle );

        if ( !transactionMap.containsKey( transactionHandle ) ) {
            LOG.warn( "Call of onePhaseCommit() but there is no transaction present." );
            return;
        }

        final XAResource xaResource;

        try {
            final XAConnection physicalXAConnection = xaConnectionCache.getIfPresent( connectionHandle.id );
            if ( physicalXAConnection == null ) {
                LOG.debug( "client requested commit (One Phase) unknown connection {}", connectionHandle );
                throw new NoSuchConnectionException( "Unknown connection " + connectionHandle.id );
            }
            xaResource = physicalXAConnection.getXAResource();
        } catch ( SQLException ex ) {
            throw new XAException( ex.getMessage() );
        }

        // end the transaction
        xaResource.end( transactionHandle, XAResource.TMSUCCESS );

        // commit
        xaResource.commit( transactionHandle, true );

        transactionMap.remove( transactionHandle );

        LOG.debug( "TRANSACTION {} - COMMITTED (1P) for connection {}", transactionHandle.getTransactionId(), connectionHandle );
    }


    @Override
    public boolean prepareCommit( final ConnectionHandle connectionHandle, final TransactionHandle transactionHandle ) throws XAException {
        LOG.trace( "prepareCommit( connectionHandle: {}, transactionHandle: {} )", connectionHandle, transactionHandle );

        if ( !transactionMap.containsKey( transactionHandle ) ) {
            LOG.warn( "Call of prepareCommit() but there is no transaction present." );
            return true;
        }

        final int prepareResult;
        try {
            final XAConnection physicalXAConnection = xaConnectionCache.getIfPresent( connectionHandle.id );
            if ( physicalXAConnection == null ) {
                LOG.debug( "client requested prepare unknown connection {}", connectionHandle );
                throw new NoSuchConnectionException( "Unknown connection " + connectionHandle.id );
            }

            final XAResource xaResource = physicalXAConnection.getXAResource();

            // end the transaction
            xaResource.end( transactionHandle, XAResource.TMSUCCESS );

            // prepare (2PC)
            prepareResult = xaResource.prepare( transactionHandle );
        } catch ( SQLException ex ) {
            throw new XAException( ex.getMessage() );
        }

        LOG.debug( "PREPARE COMMIT for TRANSACTION {} on connection {} = {}", transactionHandle.getTransactionId(), connectionHandle, prepareResult == XAResource.XA_OK ? "SUCCESS" : "FAIL" );

        return prepareResult == XAResource.XA_OK;
    }


    @Override
    public void commit( final ConnectionHandle connectionHandle, final TransactionHandle transactionHandle ) throws XAException {
        LOG.trace( "commit( connectionHandle: {}, transactionHandle: {} )", connectionHandle, transactionHandle );

        if ( !transactionMap.containsKey( transactionHandle ) ) {
            LOG.warn( "Call of commit() but there is no transaction present." );
            return;
        }

        try {
            final XAConnection physicalXAConnection = xaConnectionCache.getIfPresent( connectionHandle.id );
            if ( physicalXAConnection == null ) {
                LOG.debug( "client requested commit (Two Phase) unknown connection {}", connectionHandle );
                //throw new NoSuchConnectionException( "Unknown connection " + connectionHandle.id );
                return;
            }

            final XAResource xaResource = physicalXAConnection.getXAResource();

            // BEGIN HACK
            try {
                final org.hsqldb.jdbc.pool.JDBCXAResource hsqldbXaResource = (org.hsqldb.jdbc.pool.JDBCXAResource) xaResource;
                final java.lang.reflect.Field stateField = hsqldbXaResource.getClass().getDeclaredField( "state" );
                stateField.setAccessible( true );
                final int state = stateField.getInt( hsqldbXaResource );
                switch ( state ) {
                    case 0: //XA_STATE_INITIAL
                    case 1: //XA_STATE_STARTED
                    case 2: //XA_STATE_ENDED
                        LOG.trace( "ending transaction {}", transactionHandle );
                        LOG.debug( "Unprepared transaction {}. Fallback to One Phase Commit.", transactionHandle.getTransactionId() );
                        this.onePhaseCommit( connectionHandle, transactionHandle );
                        return;

                    case 3: //XA_STATE_PREPARED
                        break;

                    case 4: //XA_STATE_DISPOSED
                    default:
                        //throw new RuntimeException( "Illegal Transaction State" );
                        return;
                }
            } catch ( NoSuchFieldException | IllegalAccessException ex ) {
                throw new RuntimeException( ex );
            }
            // END HACK

            // commit (2PC)
            LOG.trace( "commiting (2P) transaction {}", transactionHandle );
            xaResource.commit( transactionHandle, false );
            transactionMap.remove( transactionHandle );
        } catch ( SQLException e ) {
            throw new XAException( e.getMessage() );
        }

        LOG.debug( "TRANSACTION {} - COMMITTED (2P) for connection {}", transactionHandle.getTransactionId(), connectionHandle );
    }


    @Override
    public void rollback( ConnectionHandle connectionHandle, TransactionHandle transactionHandle ) throws XAException {
        LOG.trace( "rollback( connectionHandle: {}, transactionHandle: {} )", connectionHandle, transactionHandle );

        if ( !transactionMap.containsKey( transactionHandle ) ) {
            LOG.warn( "Call of rollback() but there is no transaction present." );
            return;
        }

        try {
            final XAConnection physicalXAConnection = xaConnectionCache.getIfPresent( connectionHandle.id );
            if ( physicalXAConnection == null ) {
                LOG.debug( "client requested rollback unknown connection {}", connectionHandle );
                throw new NoSuchConnectionException( "Unknown connection " + connectionHandle.id );
            }

            final XAResource xaResource = physicalXAConnection.getXAResource();

            // BEGIN HACK
            try {
                final org.hsqldb.jdbc.pool.JDBCXAResource hsqldbXaResource = (org.hsqldb.jdbc.pool.JDBCXAResource) xaResource;
                final java.lang.reflect.Field stateField = hsqldbXaResource.getClass().getDeclaredField( "state" );
                stateField.setAccessible( true );
                final int state = stateField.getInt( hsqldbXaResource );
                switch ( state ) {
                    case 0: //XA_STATE_INITIAL
                    case 1: //XA_STATE_STARTED
                        LOG.trace( "ending transaction {}", transactionHandle );
                        xaResource.end( transactionHandle, XAResource.TMFAIL );
                        // intended fall through

                    case 2: //XA_STATE_ENDED
                    case 3: //XA_STATE_PREPARED
                        break;

                    case 4: //XA_STATE_DISPOSED
                    default:
                        throw new RuntimeException( "Illegal Transaction State" );
                }
            } catch ( NoSuchFieldException | IllegalAccessException ex ) {
                throw new RuntimeException( ex );
            }
            // END HACK

            // rollback
            LOG.trace( "rollback transaction {}", transactionHandle );
            xaResource.rollback( transactionHandle );
            transactionMap.remove( transactionHandle );
        } catch ( SQLException e ) {
            throw new XAException( e.getMessage() );
        }

        LOG.debug( "TRANSACTION {} - ROLLBACKED for connection {}", transactionHandle.getTransactionId(), connectionHandle );
    }


    @Override
    public ExecuteResult prepareAndExecute( StatementHandle h, String sql, long maxRowCount, PrepareCallback callback ) throws NoSuchStatementException {
        if ( maxRowCount == -1 ) {
            maxRowCount = UNLIMITED_COUNT;
        }
        return super.prepareAndExecute( h, sql, maxRowCount, callback );
    }


    @Override
    public ExecuteResult prepareAndExecute( StatementHandle h, String sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws NoSuchStatementException {
        if ( maxRowCount == -1 ) {
            maxRowCount = UNLIMITED_COUNT;
        }
        if ( maxRowsInFirstFrame == -1 ) {
            maxRowsInFirstFrame = UNLIMITED_COUNT;
        }
        //
        // This implementation is copied from org.apache.calcite.avatica.jdbc.JdbcMeta
        // and altered to use java.sql.Statement.execute( String sql, int autoGeneratedKeys)
        //
        try {
            final StatementInfo info = getStatementCache().getIfPresent( h.id );
            if ( info == null ) {
                throw new NoSuchStatementException( h );
            }
            final Statement statement = info.statement;

            // Make sure that we limit the number of rows for the query
            if ( maxRowCount > 0 ) {
                AvaticaUtils.setLargeMaxRows( statement, maxRowCount );
            } else if ( maxRowCount < 0 ) {
                statement.setMaxRows( 0 );
            }

            final boolean ret;
            if ( statement.isWrapperFor( org.hsqldb.jdbc.JDBCStatement.class ) ) {
                // HSQLDB specific
                ret = statement.execute( sql, org.hsqldb.jdbc.JDBCStatement.RETURN_PRIMARY_KEYS );
            } else {
                ret = statement.execute( sql );
            }

            info.setResultSet( statement.getResultSet() );
            // Either execute(sql) returned true or the resultSet was null
            assert ret || null == info.getResultSet();
            final List<MetaResultSet> resultSets = new ArrayList<>();
            if ( null == info.getResultSet() ) {
                // Create a special result set that just carries update count
                resultSets.add( JdbcResultSet.count( h.connectionId, h.id, AvaticaUtils.getLargeUpdateCount( statement ) ) );
            } else {
                resultSets.add( JdbcResultSet.create( h.connectionId, h.id, info.getResultSet(), maxRowsInFirstFrame ) );
            }
            LOG.trace( "prepAndExec statement {}", h );
            // TODO: review client to ensure statementId is updated when appropriate
            return new ExecuteResult( resultSets );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public ExecuteResult prepareAndExecute( StatementHandle h, String sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback, int[] columnIndexes ) throws NoSuchStatementException {
        if ( maxRowCount == -1 ) {
            maxRowCount = UNLIMITED_COUNT;
        }
        if ( maxRowsInFirstFrame == -1 ) {
            maxRowsInFirstFrame = UNLIMITED_COUNT;
        }
        //
        // This implementation is copied from org.apache.calcite.avatica.jdbc.JdbcMeta
        // and altered to use java.sql.Statement.execute( String sql, int[] columnIndexes)
        //
        try {
            final StatementInfo info = super.getStatementCache().getIfPresent( h.id );
            if ( info == null ) {
                throw new NoSuchStatementException( h );
            }
            final Statement statement = info.statement;

            // Make sure that we limit the number of rows for the query
            if ( maxRowCount > 0 ) {
                AvaticaUtils.setLargeMaxRows( statement, maxRowCount );
            } else if ( maxRowCount < 0 ) {
                statement.setMaxRows( 0 );
            }

            boolean ret = statement.execute( sql, columnIndexes );
            info.setResultSet( statement.getResultSet() );
            // Either execute(sql) returned true or the resultSet was null
            assert ret || null == info.getResultSet();
            final List<MetaResultSet> resultSets = new ArrayList<>();
            if ( null == info.getResultSet() ) {
                // Create a special result set that just carries update count
                resultSets.add( JdbcResultSet.count( h.connectionId, h.id, AvaticaUtils.getLargeUpdateCount( statement ) ) );
            } else {
                resultSets.add( JdbcResultSet.create( h.connectionId, h.id, info.getResultSet(), maxRowsInFirstFrame ) );
            }
            LOG.trace( "prepAndExec statement {}", h );

            return new ExecuteResult( resultSets );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public StatementHandle prepare( ConnectionHandle ch, String sql, long maxRowCount ) {
        if ( maxRowCount == -1 ) {
            maxRowCount = UNLIMITED_COUNT;
        }
        //
        // This implementation is copied from org.apache.calcite.avatica.jdbc.JdbcMeta
        // and altered to use java.sql.Connection.prepareStatement( String sql, int autoGeneratedKeys)
        //
        try {
            final Connection conn = getConnection( ch.id );
            final PreparedStatement statement;
            if ( conn.isWrapperFor( org.hsqldb.jdbc.JDBCConnection.class ) ) {
                // HSQLDB specific
                statement = conn.prepareStatement( sql, org.hsqldb.jdbc.JDBCStatement.RETURN_PRIMARY_KEYS );
            } else {
                statement = conn.prepareStatement( sql );
            }

            final int id = getStatementIdGenerator().getAndIncrement();
            Meta.StatementType statementType = null;
            if ( statement.isWrapperFor( AvaticaPreparedStatement.class ) ) {
                final AvaticaPreparedStatement avaticaPreparedStatement;
                avaticaPreparedStatement = statement.unwrap( AvaticaPreparedStatement.class );
                statementType = avaticaPreparedStatement.getStatementType();
            }

            // Set the maximum number of rows
            // Special handling of maxRowCount as JDBC 0 is unlimited, our meta 0 row
            if ( maxRowCount > 0 ) {
                AvaticaUtils.setLargeMaxRows( statement, maxRowCount );
            } else if ( maxRowCount < 0 ) {
                statement.setMaxRows( 0 );
            }

            getStatementCache().put( id, new StatementInfo( statement ) );
            StatementHandle h = new StatementHandle( ch.id, id, signature( statement.getMetaData(), statement.getParameterMetaData(), sql, statementType ) );
            LOG.trace( "prepared statement {}", h );
            return h;
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public StatementHandle prepare( ConnectionHandle ch, String sql, long maxRowCount, int[] columnIndexes ) {
        if ( maxRowCount == -1 ) {
            maxRowCount = UNLIMITED_COUNT;
        }
        //
        // This implementation is copied from org.apache.calcite.avatica.jdbc.JdbcMeta
        // and altered to use java.sql.Connection.prepareStatement( String sql, int[] columnIndexes)
        //
        try {
            final Connection conn = super.getConnection( ch.id );
            final PreparedStatement statement = conn.prepareStatement( sql, columnIndexes );
            final int id = super.getStatementIdGenerator().getAndIncrement();
            Meta.StatementType statementType = null;
            if ( statement.isWrapperFor( AvaticaPreparedStatement.class ) ) {
                final AvaticaPreparedStatement avaticaPreparedStatement;
                avaticaPreparedStatement = statement.unwrap( AvaticaPreparedStatement.class );
                statementType = avaticaPreparedStatement.getStatementType();
            }

            // Set the maximum number of rows
            // Special handling of maxRowCount as JDBC 0 is unlimited, our meta 0 row
            if ( maxRowCount > 0 ) {
                AvaticaUtils.setLargeMaxRows( statement, maxRowCount );
            } else if ( maxRowCount < 0 ) {
                statement.setMaxRows( 0 );
            }

            super.getStatementCache().put( id, new StatementInfo( statement ) );
            StatementHandle h = new StatementHandle( ch.id, id, signature( statement.getMetaData(), statement.getParameterMetaData(), sql, statementType ) );
            LOG.trace( "prepared statement {}", h );
            return h;
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public ExecuteResult execute( StatementHandle h, List<TypedValue> parameterValues, int maxRowsInFirstFrame ) throws NoSuchStatementException {
        if ( maxRowsInFirstFrame == -1 ) {
            maxRowsInFirstFrame = UNLIMITED_COUNT;
        }
        return super.execute( h, parameterValues, maxRowsInFirstFrame );
    }


    @Override
    public ExecuteResult execute( StatementHandle h, List<TypedValue> parameterValues, long maxRowCount ) throws NoSuchStatementException {
        if ( maxRowCount == -1 ) {
            maxRowCount = UNLIMITED_COUNT;
        }
        return super.execute( h, parameterValues, maxRowCount );
    }


    @Override
    public Frame fetch( StatementHandle h, long offset, int fetchMaxRowCount ) throws NoSuchStatementException, MissingResultsException {
        if ( fetchMaxRowCount == -1 ) {
            fetchMaxRowCount = UNLIMITED_COUNT;
        }
        return super.fetch( h, offset, fetchMaxRowCount );
    }


    @Override
    public ExecuteResult getGeneratedKeys( StatementHandle h, long maxRowCount, int maxRowsInFirstFrame ) throws NoSuchStatementException {
        if ( maxRowCount == -1 ) {
            maxRowCount = UNLIMITED_COUNT;
        }
        if ( maxRowsInFirstFrame == -1 ) {
            maxRowsInFirstFrame = UNLIMITED_COUNT;
        }
        //
        // This implementation is inspired by org.apache.calcite.avatica.jdbc.JdbcMeta
        // and altered to use java.sql.Statement.execute( String sql, int[] columnIndexes)
        //
        try {
            final StatementInfo info = getStatementCache().getIfPresent( h.id );
            if ( info == null ) {
                throw new NoSuchStatementException( h );
            }
            final Statement statement = info.statement;
            // Make sure that we limit the number of rows for the query
            if ( maxRowCount > 0 ) {
                AvaticaUtils.setLargeMaxRows( statement, maxRowCount );
            } else if ( maxRowCount < 0 ) {
                statement.setMaxRows( 0 );
            }

            info.setResultSet( statement.getGeneratedKeys() );
            // JDBC Specification: "Retrieves any auto-generated keys created as a result of executing this Statement object. If this Statement object did not generate any keys, an empty ResultSet object is returned."
            assert null != info.getResultSet();

            final List<MetaResultSet> resultSets = new ArrayList<>();
            resultSets.add( JdbcResultSet.create( h.connectionId, h.id, info.getResultSet(), maxRowsInFirstFrame ) );
            LOG.trace( "getGeneratedKeys statement {}", h );

            return new ExecuteResult( resultSets );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    /**
     * Maps between XADataSource and DataSource
     */
    private static class XADataSourceAdapter implements DataSource, XADataSource {

        private final XADataSource adaptee;


        private XADataSourceAdapter( final XADataSource adaptee ) {
            this.adaptee = adaptee;
        }


        @Override
        public Connection getConnection() throws SQLException {
            return adaptee.getXAConnection().getConnection();
        }


        @Override
        public Connection getConnection( final String username, final String password ) throws SQLException {
            return adaptee.getXAConnection( username, password ).getConnection();
        }


        @Override
        public <T> T unwrap( final Class<T> iface ) throws SQLException {
            try {
                return iface.cast( adaptee );
            } catch ( ClassCastException ex ) {
                throw new SQLException( ex );
            }
        }


        @Override
        public boolean isWrapperFor( final Class<?> iface ) throws SQLException {
            return iface.isInstance( adaptee );
        }


        @Override
        public PrintWriter getLogWriter() throws SQLException {
            return adaptee.getLogWriter();
        }


        @Override
        public void setLogWriter( final PrintWriter out ) throws SQLException {
            adaptee.setLogWriter( out );
        }


        @Override
        public void setLoginTimeout( final int seconds ) throws SQLException {
            adaptee.setLoginTimeout( seconds );
        }


        @Override
        public int getLoginTimeout() throws SQLException {
            return adaptee.getLoginTimeout();
        }


        @Override
        public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return adaptee.getParentLogger();
        }


        @Override
        public XAConnection getXAConnection() throws SQLException {
            return adaptee.getXAConnection();
        }


        @Override
        public XAConnection getXAConnection( final String user, final String password ) throws SQLException {
            return adaptee.getXAConnection( user, password );
        }
    }
}
