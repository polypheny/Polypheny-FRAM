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


import static org.polypheny.fram.standalone.Utils.VOID;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.sql.DataSource;
import javax.transaction.xa.XAException;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.ConnectionHandle;
import org.apache.calcite.avatica.Meta.ExecuteBatchResult;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.NoSuchConnectionException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.polypheny.fram.Catalog;
import org.polypheny.fram.remote.AbstractLocalNode;
import org.polypheny.fram.remote.types.RemoteConnectionHandle;
import org.polypheny.fram.remote.types.RemoteExecuteBatchResult;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.remote.types.RemoteFrame;
import org.polypheny.fram.remote.types.RemoteStatementHandle;
import org.polypheny.fram.remote.types.RemoteTransactionHandle;
import org.polypheny.fram.standalone.StatementInfos.PreparedStatementInfos;
import org.polypheny.fram.standalone.transaction.TransactionHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is the local Node.
 * <p>
 * This class uses {@link JdbcXAMeta} to access the underlying Database.
 */
class LocalNode extends AbstractLocalNode {

    private static final Logger LOGGER = LoggerFactory.getLogger( LocalNode.class );

    private final UUID nodeId;

    private final XAMeta xaMeta;
    private final DataSource dataSource;

    private final Map<String, ConnectionInfos> remoteToLocalConnectionMap = new HashMap<>();
    private final Map<String, StatementInfos> remoteToLocalStatementMap = new HashMap<>();
    private final Map<ConnectionHandle, TransactionHandle> openTransactionsMap = new HashMap<>();


    private LocalNode() {
        this.nodeId = GlobalCatalog.getInstance().getNodeId();
        this.xaMeta = DataStore.getStorage();
        this.dataSource = DataStore.getStorageDataSource();
    }


    @Override
    public Config getSqlParserConfig() {
        return DataStore.getStorageParserConfig();
    }


    @Override
    public JdbcImplementor getRelToSqlConverter() {
        return DataStore.getRelToSqlConverter();
    }


    @Override
    public SqlDialect getSqlDialect() {
        return DataStore.getStorageDialect();
    }


    @Override
    public DataSource getDataSource() {
        return this.dataSource;
    }


    @Override
    public Catalog getCatalog() {
        return GlobalCatalog.getInstance();
    }


    private ConnectionInfos getOrOpenConnection( final ConnectionHandle connectionHandle ) {
        synchronized ( remoteToLocalConnectionMap ) {
            return remoteToLocalConnectionMap.computeIfAbsent( connectionHandle.id, id -> {
                // this is an unknown connection
                // its first occurrence creates a new connection here to represent the original connection
                final ConnectionInfos localConnection = new ConnectionInfos( new ConnectionHandle( id ) );
                xaMeta.openConnection( localConnection.getConnectionHandle(), null );
                return localConnection;
            } );
        }
    }


    private ConnectionInfos getOrOpenConnection( final StatementHandle statementHandle ) {
        return this.getOrOpenConnection( new ConnectionHandle( statementHandle.connectionId ) );
    }


    private ConnectionInfos getConnection( final ConnectionHandle connectionHandle ) throws NoSuchConnectionException {
        synchronized ( remoteToLocalConnectionMap ) {
            if ( remoteToLocalConnectionMap.containsKey( connectionHandle.id ) ) {
                return remoteToLocalConnectionMap.get( connectionHandle.id );
            }
            throw new NoSuchConnectionException( connectionHandle.id );
        }
    }


    private ConnectionInfos getConnection( final StatementHandle statementHandle ) {
        return this.getConnection( new ConnectionHandle( statementHandle.connectionId ) );
    }


    @Override
    public Common.ConnectionProperties connectionSync( final RemoteConnectionHandle remoteConnectionHandle, final Common.ConnectionProperties properties ) throws RemoteException {
        synchronized ( remoteToLocalConnectionMap ) {
            final ConnectionInfos localConnection = remoteToLocalConnectionMap.get( remoteConnectionHandle.toConnectionHandle().id );
            if ( localConnection != null && false /* see comment*/ ) {
                // Tries to do setAutoCommit which leads to an SQLException with the message:  Method prohibited within a global transaction
                return xaMeta.connectionSync( localConnection.getConnectionHandle(), ConnectionPropertiesImpl.fromProto( properties ) ).toProto();
            } else {
                return null;
            }
        }
    }


    @Override
    public Void closeConnection( final RemoteConnectionHandle remoteConnectionHandle ) throws RemoteException {
        closeConnection( remoteConnectionHandle.toConnectionHandle() );

        return VOID;
    }


    private void closeConnection( final ConnectionHandle connectionHandle ) {
        synchronized ( remoteToLocalConnectionMap ) {
            final ConnectionInfos localConnection = remoteToLocalConnectionMap.remove( connectionHandle.id );
            if ( localConnection != null ) {
                xaMeta.closeConnection( localConnection.getConnectionHandle() );
            }
        }
    }


    private StatementInfos getOrCreateStatement( final ConnectionInfos connection, final StatementHandle statementHandle ) {
        synchronized ( remoteToLocalStatementMap ) {
            return remoteToLocalStatementMap.computeIfAbsent( statementHandle.toString(), statementHandleString -> {
                // this is an unknown statement
                // its first occurrence creates a new local statement here to represent the original remote statement
                return new StatementInfos( connection, xaMeta.createStatement( connection.getConnectionHandle() ) );
            } );
        }
    }


    private StatementInfos getStatement( final StatementHandle statementHandle ) throws NoSuchStatementException {
        synchronized ( remoteToLocalStatementMap ) {
            if ( remoteToLocalStatementMap.containsKey( statementHandle.toString() ) ) {
                return remoteToLocalStatementMap.get( statementHandle.toString() );
            }
            throw new NoSuchStatementException( statementHandle );
        }
    }


    private StatementInfos createStatement( final ConnectionInfos connection, final StatementHandle statementHandle ) {
        synchronized ( remoteToLocalStatementMap ) {
            return remoteToLocalStatementMap.compute( statementHandle.toString(), ( handle, statementInfos ) -> {
                if ( statementInfos == null ) {
                    return new StatementInfos( connection, xaMeta.createStatement( connection.getConnectionHandle() ) );
                } else {
                    throw new IllegalStateException( "Statement already exists." );
                }
            } );
        }
    }


    @Override
    public Map<Common.DatabaseProperty, Serializable> getDatabaseProperties( RemoteConnectionHandle remoteConnectionHandle ) throws RemoteException {
        LOGGER.trace( "getDatabaseProperties( remoteConnectionHandle: {} )", remoteConnectionHandle );

        final Map<Common.DatabaseProperty, Serializable> result = new HashMap<>();

        final ConnectionInfos connection = getOrOpenConnection( remoteConnectionHandle.toConnectionHandle() );
        xaMeta.getDatabaseProperties( connection.getConnectionHandle() )
                .forEach( ( databaseProperty, value ) -> result.put( databaseProperty.toProto(), (Serializable) value ) );

        LOGGER.trace( "getDatabaseProperties( remoteConnectionHandle: {} ) = {}", remoteConnectionHandle, result );
        return result;
    }


    @Override
    public RemoteExecuteResult prepareAndExecute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final String sql, final long maxRowCount, final int maxRowsInFirstFrame ) throws RemoteException {
        LOGGER.trace( "prepareAndExecute( remoteTransactionHandle: {}, remoteStatementHandle: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {} )", remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame );

        final TransactionHandle branchTransactionHandle = remoteTransactionHandle.toTransactionHandle().generateBranchTransactionIdentifier( this.nodeId );

        final RemoteExecuteResult result;
        try {
            final ConnectionInfos connection = getOrOpenConnection( remoteStatementHandle.toStatementHandle() );
            final StatementInfos statement = getOrCreateStatement( connection, remoteStatementHandle.toStatementHandle() );

            final TransactionInfos transaction = xaMeta.getOrStartTransaction( connection, branchTransactionHandle );
            LOGGER.debug( "executing xaMeta.prepareAndExecute( ... ) in the context of {}", transaction );

            final ExecuteResult executeResult = xaMeta.prepareAndExecute( statement.getStatementHandle(), sql, maxRowCount, maxRowsInFirstFrame, NOOP_PREPARE_CALLBACK );
            final ExecuteResult affectedPrimaryKeys = xaMeta.getGeneratedKeys( statement.getStatementHandle(), -1 /* == unlimited */, -1 /* == unlimited */ );

            if ( executeResult.resultSets.iterator().next().updateCount > -1 ) {
                if ( executeResult.resultSets.iterator().next().updateCount > 0
                        && affectedPrimaryKeys.resultSets.iterator().next().firstFrame.rows.iterator().hasNext() == false ) {
                    throw new SQLException( "Update Count > 0 but the affected Primary Keys result set is empty!" );
                }
            } else {
                // result set
                if ( executeResult.resultSets.iterator().next().firstFrame.rows.iterator().hasNext()
                        && affectedPrimaryKeys.resultSets.iterator().next().firstFrame.rows.iterator().hasNext() == false ) {
                    throw new SQLException( "Data result set has rows but the affected Primary Keys result set is empty!" );
                }
            }

            result = RemoteExecuteResult.fromExecuteResult( executeResult )
                    .withGeneratedKeys( affectedPrimaryKeys );

            // todo: Do we need to take action if the connection is set on AutoCommit?

        } catch ( Exception ex ) {
            LOGGER.debug( "[" + Thread.currentThread() + "]", ex );
            throw new RemoteException( ex.getMessage(), ex );
        }

        LOGGER.trace( "prepareAndExecute( remoteTransactionHandle: {}, remoteStatementHandle: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {} ) = {}", remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame, result );
        return result;
    }


    @Override
    public RemoteExecuteResult prepareAndExecute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final String sql, final long maxRowCount, final int maxRowsInFirstFrame, final int[] columnIndexes ) throws RemoteException {
        LOGGER.trace( "prepareAndExecute( remoteTransactionHandle: {}, remoteStatementHandle: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {} )", remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame );

        final TransactionHandle branchTransactionHandle = remoteTransactionHandle.toTransactionHandle().generateBranchTransactionIdentifier( this.nodeId );

        final RemoteExecuteResult result;
        try {
            final ConnectionInfos connection = getOrOpenConnection( remoteStatementHandle.toStatementHandle() );
            final StatementInfos statement = getOrCreateStatement( connection, remoteStatementHandle.toStatementHandle() );

            final TransactionInfos transaction = xaMeta.getOrStartTransaction( connection, branchTransactionHandle );
            LOGGER.debug( "executing xaMeta.prepareAndExecute( ... ) in the context of {}", transaction );

            final ExecuteResult executeResult = xaMeta.prepareAndExecute( statement.getStatementHandle(), sql, maxRowCount, maxRowsInFirstFrame, NOOP_PREPARE_CALLBACK, columnIndexes );
            final ExecuteResult affectedPrimaryKeys = xaMeta.getGeneratedKeys( statement.getStatementHandle(), -1 /* == unlimited */, -1 /* == unlimited */ );

            if ( executeResult.resultSets.iterator().next().updateCount > -1 ) {
                if ( executeResult.resultSets.iterator().next().updateCount > 0
                        && affectedPrimaryKeys.resultSets.iterator().next().firstFrame.rows.iterator().hasNext() == false ) {
                    throw new SQLException( "Update Count > 0 but the affected Primary Keys result set is empty!" );
                }
            } else {
                // result set
                if ( executeResult.resultSets.iterator().next().firstFrame.rows.iterator().hasNext()
                        && affectedPrimaryKeys.resultSets.iterator().next().firstFrame.rows.iterator().hasNext() == false ) {
                    throw new SQLException( "Data result set has rows but the affected Primary Keys result set is empty!" );
                }
            }

            result = RemoteExecuteResult.fromExecuteResult( executeResult )
                    .withGeneratedKeys( affectedPrimaryKeys );

            // todo: Do we need to take action if the connection is set on AutoCommit?

        } catch ( Exception ex ) {
            LOGGER.debug( "[" + Thread.currentThread() + "]", ex );
            throw new RemoteException( ex.getMessage(), ex );
        }

        LOGGER.trace( "prepareAndExecute( remoteTransactionHandle: {}, remoteStatementHandle: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {} ) = {}", remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame, result );
        return result;
    }


    @Override
    public RemoteExecuteResult prepareAndExecuteDataDefinition( RemoteTransactionHandle remoteTransactionHandle, RemoteStatementHandle remoteStatementHandle, String globalCatalogSql, String localStoreSql, long maxRowCount, int maxRowsInFirstFrame ) throws RemoteException {
        LOGGER.trace( "prepareAndExecuteDataDefinition( remoteTransactionHandle: {}, remoteStatementHandle: {}, globalCatalogSql: {}, localStoreSql: {}, maxRowCount: {}, maxRowsInFirstFrame: {} )", remoteTransactionHandle, remoteStatementHandle, globalCatalogSql, localStoreSql, maxRowCount, maxRowsInFirstFrame );

        final TransactionHandle branchTransactionHandle = remoteTransactionHandle.toTransactionHandle().generateBranchTransactionIdentifier( this.nodeId );

        final RemoteExecuteResult result;
        try {
            final ConnectionInfos connection = getOrOpenConnection( remoteStatementHandle.toStatementHandle() );
            final StatementInfos statement = getOrCreateStatement( connection, remoteStatementHandle.toStatementHandle() );

            final ExecuteResult catalogExecuteResult = this.getCatalog().prepareAndExecuteDataDefinition( branchTransactionHandle, remoteStatementHandle.toStatementHandle(), globalCatalogSql, maxRowCount, maxRowsInFirstFrame );

            final TransactionInfos transaction = xaMeta.getOrStartTransaction( connection, branchTransactionHandle );
            LOGGER.debug( "executing xaMeta.prepareAndExecuteDataDefinition( ... ) in the context of {}", transaction );
            final ExecuteResult storeExecuteResult = xaMeta.prepareAndExecute( statement.getStatementHandle(), localStoreSql, maxRowCount, maxRowsInFirstFrame, NOOP_PREPARE_CALLBACK );

            final List<Meta.MetaResultSet> resultSets = new LinkedList<>();
            resultSets.addAll( storeExecuteResult.resultSets );
            resultSets.addAll( catalogExecuteResult.resultSets );

            result = RemoteExecuteResult.fromExecuteResult( new ExecuteResult( resultSets ) );

            // todo: Do we need to take action if the connection is set on AutoCommit?

        } catch ( Exception ex ) {
            LOGGER.debug( "[" + Thread.currentThread() + "]", ex );
            throw new RemoteException( ex.getMessage(), ex );
        }

        LOGGER.trace( "prepareAndExecuteDataDefinition( remoteTransactionHandle: {}, remoteStatementHandle: {}, globalCatalogSql: {}, localStoreSql: {}, maxRowCount: {}, maxRowsInFirstFrame: {} ) = {}", remoteTransactionHandle, remoteStatementHandle, globalCatalogSql, localStoreSql, maxRowCount, maxRowsInFirstFrame, result );
        return result;
    }


    @Override
    public RemoteExecuteBatchResult prepareAndExecuteBatch( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<String> sqlCommands ) throws RemoteException {
        LOGGER.trace( "prepareAndExecuteBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, sqlCommands: {} )", remoteTransactionHandle, remoteStatementHandle, sqlCommands );

        final TransactionHandle branchTransactionHandle = remoteTransactionHandle.toTransactionHandle().generateBranchTransactionIdentifier( this.nodeId );

        final RemoteExecuteBatchResult result;
        try {
            final ConnectionInfos connection = getOrOpenConnection( remoteStatementHandle.toStatementHandle() );
            final StatementInfos statement = getOrCreateStatement( connection, remoteStatementHandle.toStatementHandle() );

            final TransactionInfos transaction = xaMeta.getOrStartTransaction( connection, branchTransactionHandle );

            final ExecuteBatchResult executeBatchResult = xaMeta.prepareAndExecuteBatch( statement.getStatementHandle(), sqlCommands );
            final ExecuteResult affectedPrimaryKeys = xaMeta.getGeneratedKeys( statement.getStatementHandle(), -1 /* == unlimited */, -1 /* == unlimited */ );

            if ( executeBatchResult.updateCounts.length > 0
                    && affectedPrimaryKeys.resultSets.iterator().next().firstFrame.rows.iterator().hasNext() == false ) {
                throw new SQLException( "Number of update counts > 0 but the affected Primary Keys result set is empty!" );
            }

            result = RemoteExecuteBatchResult.fromExecuteBatchResult( executeBatchResult )
                    .withGeneratedKeys( affectedPrimaryKeys );

            // todo: Do we need to take action if the connection is set on AutoCommit?

        } catch ( Exception ex ) {
            throw new RemoteException( ex.getMessage(), ex );
        }

        LOGGER.trace( "prepareAndExecuteBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, sqlCommands: {} ) = {}", remoteTransactionHandle, remoteStatementHandle, sqlCommands, result );
        return result;
    }


    @Override
    public RemoteStatementHandle prepare( final RemoteStatementHandle remoteStatementHandle, final String sql, final long maxRowCount ) throws RemoteException {
        LOGGER.trace( "prepare( remoteStatementHandle: {}, sql: {}, maxRowCount: {} )", remoteStatementHandle, sql, maxRowCount );

        final StatementHandle result;
        try {
            final ConnectionInfos connection = getOrOpenConnection( remoteStatementHandle.toStatementHandle() );

            // Prepare Statement is done outside of a transaction?
            final StatementInfos statement;
            synchronized ( remoteToLocalStatementMap ) {
                statement = remoteToLocalStatementMap.compute( remoteStatementHandle.toStatementHandle().toString(), ( handle, statementInfos ) -> {
                    if ( statementInfos != null ) {
                        //throw new IllegalStateException( "Illegal attempt to prepare an already present statement." );
                        LOGGER.warn( "{} for handle {} already exists!", statementInfos instanceof PreparedStatementInfos ? "PreparedStatement" : "Statement", handle );
                        xaMeta.closeStatement( statementInfos.getStatementHandle() );
                    }
                    return new StatementInfos(
                            connection,
                            xaMeta.prepare( connection.getConnectionHandle(), sql, maxRowCount )
                    );
                } );
            }

            result = statement.getStatementHandle();

        } catch ( Exception ex ) {
            throw new RemoteException( ex.getMessage(), ex );
        }

        LOGGER.trace( "prepare( remoteStatementHandle: {}, sql: {}, maxRowCount: {} ) = {}", remoteStatementHandle, sql, maxRowCount, result );
        return RemoteStatementHandle.fromStatementHandle( result );
    }


    @Override
    public RemoteStatementHandle prepare( final RemoteStatementHandle remoteStatementHandle, final String sql, final long maxRowCount, final int[] columnIndexes ) throws RemoteException {
        LOGGER.trace( "prepare( remoteStatementHandle: {}, sql: {}, maxRowCount: {}, columnIndexes: {} )", remoteStatementHandle, sql, maxRowCount, columnIndexes );

        final StatementHandle result;
        try {
            final ConnectionInfos connection = getOrOpenConnection( remoteStatementHandle.toStatementHandle() );

            // Prepare Statement is done outside of a transaction?
            final StatementInfos statement;
            synchronized ( remoteToLocalStatementMap ) {
                statement = remoteToLocalStatementMap.compute( remoteStatementHandle.toStatementHandle().toString(), ( handle, statementInfos ) -> {
                    if ( statementInfos != null ) {
                        throw new IllegalStateException( "Illegal attempt to prepare an already present statement." );
                    }
                    return new StatementInfos(
                            connection,
                            xaMeta.prepare( connection.getConnectionHandle(), sql, maxRowCount, columnIndexes ),
                            columnIndexes
                    );
                } );
            }

            result = statement.getStatementHandle();

        } catch ( Exception ex ) {
            throw new RemoteException( ex.getMessage(), ex );
        }

        LOGGER.trace( "prepare( remoteStatementHandle: {}, sql: {}, maxRowCount: {}, columnIndexes: {} ) = {}", remoteStatementHandle, sql, maxRowCount, columnIndexes, result );
        return RemoteStatementHandle.fromStatementHandle( result );
    }


    @Override
    public RemoteExecuteResult execute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<Common.TypedValue> parameterValues, final int maxRowsInFirstFrame ) throws RemoteException {
        LOGGER.trace( "execute( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {}, maxRowsInFirstFrame: {} )", remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame );

        final TransactionHandle branchTransactionHandle = remoteTransactionHandle.toTransactionHandle().generateBranchTransactionIdentifier( this.nodeId );

        final RemoteExecuteResult result;
        try {
            final ConnectionInfos connection = getConnection( remoteStatementHandle.toStatementHandle() ); // a connection should already be open since we should have prepared the statement
            final StatementInfos statement = getStatement( remoteStatementHandle.toStatementHandle() ); // get the prepared statement

            final TransactionInfos transaction = xaMeta.getOrStartTransaction( connection, branchTransactionHandle );

            final List<TypedValue> deserializedParameterValues = new LinkedList<>();
            for ( Common.TypedValue value : parameterValues ) {
                deserializedParameterValues.add( TypedValue.fromProto( value ) );
            }

            final ExecuteResult executeResult = xaMeta.execute( statement.getStatementHandle(), deserializedParameterValues, maxRowsInFirstFrame );
            final ExecuteResult affectedPrimaryKeys = xaMeta.getGeneratedKeys( statement.getStatementHandle(), -1 /* == unlimited */, -1 /* == unlimited */ );

            if ( executeResult.resultSets.iterator().next().updateCount > -1 ) {
                if ( executeResult.resultSets.iterator().next().updateCount > 0
                        && affectedPrimaryKeys.resultSets.iterator().next().firstFrame.rows.iterator().hasNext() == false ) {
                    throw new SQLException( "Update Count > 0 but the affected Primary Keys result set is empty!" );
                }
            } else {
                // result set
                if ( executeResult.resultSets.iterator().next().firstFrame.rows.iterator().hasNext()
                        && affectedPrimaryKeys.resultSets.iterator().next().firstFrame.rows.iterator().hasNext() == false ) {
                    throw new SQLException( "Data result set has rows but the affected Primary Keys result set is empty!" );
                }
            }

            result = RemoteExecuteResult.fromExecuteResult( executeResult )
                    .withGeneratedKeys( affectedPrimaryKeys );

            // todo: Do we need to take action if the connection is set on AutoCommit?

        } catch ( Exception ex ) {
            throw new RemoteException( ex.getMessage(), ex );
        }

        LOGGER.trace( "execute( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {}, maxRowsInFirstFrame: {} ) = {}", remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame, result );
        return result;
    }


    @Override
    public RemoteExecuteBatchResult executeBatch( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<UpdateBatch> parameterValues ) throws RemoteException {
        LOGGER.trace( "executeBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {} )", remoteTransactionHandle, remoteStatementHandle, parameterValues );

        final TransactionHandle branchTransactionHandle = remoteTransactionHandle.toTransactionHandle().generateBranchTransactionIdentifier( this.nodeId );

        final RemoteExecuteBatchResult result;
        try {
            final ConnectionInfos connection = getConnection( remoteStatementHandle.toStatementHandle() ); // a connection should already be open since we should have prepared the statement
            final StatementInfos statement = getStatement( remoteStatementHandle.toStatementHandle() ); // get the prepared statement

            final TransactionInfos transaction = xaMeta.getOrStartTransaction( connection, branchTransactionHandle );

            final List<List<TypedValue>> deserializedParameterValues = new LinkedList<>();
            for ( UpdateBatch batch : parameterValues ) {
                final List<TypedValue> valuesList = new LinkedList<>();
                for ( Common.TypedValue value : batch.getParameterValuesList() ) {
                    valuesList.add( TypedValue.fromProto( value ) );
                }
                deserializedParameterValues.add( valuesList );
            }

            final ExecuteBatchResult executeBatchResult = xaMeta.executeBatch( statement.getStatementHandle(), deserializedParameterValues );
            final ExecuteResult affectedPrimaryKeys = xaMeta.getGeneratedKeys( statement.getStatementHandle(), -1 /* == unlimited */, -1 /* == unlimited */ );

            if ( executeBatchResult.updateCounts.length > 0
                    && affectedPrimaryKeys.resultSets.iterator().next().firstFrame.rows.iterator().hasNext() == false ) {
                throw new SQLException( "Number of update counts > 0 but the affected Primary Keys result set is empty!" );
            }

            result = RemoteExecuteBatchResult.fromExecuteBatchResult( executeBatchResult )
                    .withGeneratedKeys( affectedPrimaryKeys );

            // todo: Do we need to take action if the connection is set on AutoCommit?

        } catch ( Exception ex ) {
            throw new RemoteException( ex.getMessage(), ex );
        }

        LOGGER.trace( "executeBatch( remoteTransactionHandle: {}, remoteStatementHandle: {}, parameterValues: {} ) = {}", remoteTransactionHandle, remoteStatementHandle, parameterValues, result );
        return result;
    }


    @Override
    public RemoteFrame fetch( final RemoteStatementHandle remoteStatementHandle, final long offset, final int fetchMaxRowCount ) throws RemoteException {
        try {
            final StatementInfos statement = getStatement( remoteStatementHandle.toStatementHandle() );
            final Frame frame = xaMeta.fetch( statement.getStatementHandle(), offset, fetchMaxRowCount );
            return RemoteFrame.fromFrame( frame );
        } catch ( Exception ex ) {
            throw new RemoteException( ex.getMessage(), ex );
        }
    }


    @Override
    public Void closeStatement( RemoteStatementHandle remoteStatementHandle ) throws RemoteException {
        LOGGER.trace( "closeStatement( remoteStatementHandle: {} )", remoteStatementHandle );

        synchronized ( remoteToLocalStatementMap ) {
            remoteToLocalStatementMap.computeIfPresent( remoteStatementHandle.toStatementHandle().toString(), ( statementHandle, statement ) -> {
                xaMeta.closeStatement( statement.getStatementHandle() );
                return null; // `null` == remove the mapping
            } );
        }

        LOGGER.trace( "closeStatement( remoteStatementHandle: {} ) = {}", remoteStatementHandle, "<VOID>" );
        return VOID;
    }


    @Override
    public Void abortConnection( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        LOGGER.trace( "abortConnection( remoteConnectionHandle: {}, remoteTransactionHandle: {} )", remoteConnectionHandle, remoteTransactionHandle );

        final TransactionHandle branchTransactionHandle = remoteTransactionHandle.toTransactionHandle().generateBranchTransactionIdentifier( this.nodeId );

        throw new RemoteException( "UnsupportedOperation", new UnsupportedOperationException( "xaMeta.abortConnection( remoteConnectionHandle.toConnectionHandle() ) not supported yet." ) );
    }


    @Override
    public Void onePhaseCommit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        LOGGER.trace( "onePhaseCommit( remoteConnectionHandle: {}, remoteTransactionHandle: {} )", remoteConnectionHandle, remoteTransactionHandle );

        if ( remoteTransactionHandle == null || remoteTransactionHandle.toTransactionHandle() == null ) {
            // legacy method
            LOGGER.warn( "Calling legacy commit() for connection {} and transaction {}", remoteConnectionHandle, remoteTransactionHandle );
            this.getCatalog().commit( remoteConnectionHandle.toConnectionHandle() );
            xaMeta.commit( remoteConnectionHandle.toConnectionHandle() );
        } else {
            final TransactionHandle branchTransactionHandle = remoteTransactionHandle.toTransactionHandle().generateBranchTransactionIdentifier( this.nodeId );
            try {
                LOGGER.info( "onePhaseCommit on connection {}", remoteConnectionHandle.toConnectionHandle().id );
                this.getCatalog().onePhaseCommit( remoteConnectionHandle.toConnectionHandle(), branchTransactionHandle );
                xaMeta.onePhaseCommit( remoteConnectionHandle.toConnectionHandle(), branchTransactionHandle );
            } catch ( XAException ex ) {
                throw new RemoteException( ex.getMessage(), ex );
            }
        }

        return VOID;
    }


    @Override
    public boolean prepareCommit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        LOGGER.trace( "prepareCommit( remoteConnectionHandle: {}, remoteTransactionHandle: {} )", remoteConnectionHandle, remoteTransactionHandle );

        if ( remoteTransactionHandle == null || remoteTransactionHandle.toTransactionHandle() == null ) {
            throw new RemoteException( "Cannot perform 2PC prepare without a transaction handle." );
        }

        final TransactionHandle branchTransactionHandle = remoteTransactionHandle.toTransactionHandle().generateBranchTransactionIdentifier( this.nodeId );

        boolean result = true;
        try {
            result &= this.getCatalog().prepareCommit( remoteConnectionHandle.toConnectionHandle(), branchTransactionHandle );
            result &= xaMeta.prepareCommit( remoteConnectionHandle.toConnectionHandle(), branchTransactionHandle );
            LOGGER.info( "prepareCommit on connection {} = {}", remoteConnectionHandle.toConnectionHandle().id, result );
        } catch ( XAException ex ) {
            throw new RemoteException( ex.getMessage(), ex );
        }

        LOGGER.trace( "prepareCommit( remoteConnectionHandle: {}, remoteTransactionHandle: {} ) = {}", remoteConnectionHandle, remoteTransactionHandle, result );
        return result;
    }


    @Override
    public Void commit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        LOGGER.trace( "commit( remoteConnectionHandle: {}, remoteTransactionHandle: {} )", remoteConnectionHandle, remoteTransactionHandle );

        if ( remoteTransactionHandle == null || remoteTransactionHandle.toTransactionHandle() == null ) {
            throw new RemoteException( "Cannot perform 2PC commit without a transaction handle." );
        }

        final TransactionHandle branchTransactionHandle = remoteTransactionHandle.toTransactionHandle().generateBranchTransactionIdentifier( this.nodeId );

        try {
            LOGGER.info( "commit on connection {}", remoteConnectionHandle.toConnectionHandle().id );
            this.getCatalog().commit( remoteConnectionHandle.toConnectionHandle(), branchTransactionHandle );
            xaMeta.commit( remoteConnectionHandle.toConnectionHandle(), branchTransactionHandle );
        } catch ( XAException ex ) {
            throw new RemoteException( ex.getMessage(), ex );
        }

        return VOID;
    }


    @Override
    public Void rollback( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException {
        LOGGER.trace( "rollback( remoteConnectionHandle: {}, remoteTransactionHandle: {} )", remoteConnectionHandle, remoteTransactionHandle );

        if ( remoteTransactionHandle == null || remoteTransactionHandle.toTransactionHandle() == null ) {
            this.getCatalog().rollback( remoteConnectionHandle.toConnectionHandle() );
            xaMeta.rollback( remoteConnectionHandle.toConnectionHandle() );
        } else {
            final TransactionHandle branchTransactionHandle = remoteTransactionHandle.toTransactionHandle().generateBranchTransactionIdentifier( this.nodeId );
            try {
                LOGGER.info( "rollback on connection {}", remoteConnectionHandle.toConnectionHandle().id );
                this.getCatalog().rollback( remoteConnectionHandle.toConnectionHandle(), branchTransactionHandle );
                xaMeta.rollback( remoteConnectionHandle.toConnectionHandle(), branchTransactionHandle );
            } catch ( NoSuchConnectionException | XAException ex ) {
                throw new RemoteException( ex.getMessage(), ex );
            }
        }

        return VOID;
    }


    private static final PrepareCallback NOOP_PREPARE_CALLBACK = new PrepareCallback() {
        @Override
        public Object getMonitor() {
            return LocalNode.class;
        }


        @Override
        public void clear() throws SQLException {
        }


        @Override
        public void assign( Signature signature, Frame firstFrame, long updateCount ) throws SQLException {
        }


        @Override
        public void execute() throws SQLException {
        }
    };


    private static class SingletonHolder {

        private static final LocalNode INSTANCE = new LocalNode();


        private SingletonHolder() {
        }
    }


    public static LocalNode getInstance() {
        return SingletonHolder.INSTANCE;
    }
}
