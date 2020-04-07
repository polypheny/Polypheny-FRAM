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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import javax.transaction.xa.XAException;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.Meta.ConnectionHandle;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.apache.calcite.avatica.Meta.Pat;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.NoSuchConnectionException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RuleSets;
import org.polypheny.fram.AbstractCatalog;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.standalone.transaction.TransactionHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
class GlobalCatalog extends AbstractCatalog {

    private static final Logger LOGGER = LoggerFactory.getLogger( GlobalCatalog.class );

    private static final String INTERNAL_SCHEMA_NAME = "$_POLYPHENY";
    private static final String INTERNAL_TABLE_UUIDs_NAME = "$_UUIDs";

    private final XAMeta xaMeta;
    private final DataSource dataSource;
    private final Config sqlParserConfig;
    private final SqlDialect sqlDialect;

    private final Map<String, ConnectionInfos> catalogConnections = new HashMap<>();
    private final Map<String, StatementInfos> remoteToLocalStatementMap = new HashMap<>();
    private final Map<String, Set<TransactionHandle>> openTransactionsMap = new HashMap<>();

    private final AtomicReference<UUID> nodeIdReference = new AtomicReference<>();


    private GlobalCatalog() {
        this.xaMeta = DataStore.getCatalog();
        this.dataSource = DataStore.getCatalogDataSource();
        this.sqlParserConfig = DataStore.getCatalogParserConfig();
        this.sqlDialect = DataStore.getCatalogDialect();

        initializePolyphenySchema();
    }


    private void initializePolyphenySchema() {
        try ( final Connection connetion = this.dataSource.getConnection() ) {
            try ( final Statement statement = connetion.createStatement() ) {
                statement.executeUpdate( "CREATE SCHEMA IF NOT EXISTS \"" + INTERNAL_SCHEMA_NAME + "\"" );
            }
            try ( final Statement statement = connetion.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE IF NOT EXISTS \"" + INTERNAL_SCHEMA_NAME + "\".\"" + INTERNAL_TABLE_UUIDs_NAME + "\" (id INTEGER IDENTITY PRIMARY KEY, serializedUUID OTHER NOT NULL)" );
                statement.executeUpdate( "COMMENT ON TABLE \"" + INTERNAL_SCHEMA_NAME + "\".\"" + INTERNAL_TABLE_UUIDs_NAME + "\" IS 'Polypheny internal table holding all UUIDs.'" );
            }
            if ( !connetion.getAutoCommit() ) {
                connetion.commit();
            }
        } catch ( SQLException e ) {
            throw Utils.extractAndThrow( e );
        }
    }


    public UUID getNodeId() {
        synchronized ( nodeIdReference ) {
            UUID nodeId = nodeIdReference.get();

            if ( nodeId == null ) {
                try ( final Connection connetion = this.dataSource.getConnection() ) {
                    try ( final Statement statement = connetion.createStatement() ) {
                        try ( final ResultSet result = statement.executeQuery( "SELECT serializedUUID FROM \"" + INTERNAL_SCHEMA_NAME + "\".\"" + INTERNAL_TABLE_UUIDs_NAME + "\" WHERE id = -1" ) ) {
                            while ( result.next() ) {
                                nodeId = result.getObject( 1, UUID.class );
                            }
                        }

                        if ( nodeId == null ) {
                            // we got empty result set

                            nodeId = UUID.randomUUID();

                            try ( final PreparedStatement insertStatement = connetion.prepareStatement( "INSERT INTO \"" + INTERNAL_SCHEMA_NAME + "\".\"" + INTERNAL_TABLE_UUIDs_NAME + "\" (id , serializedUUID) VALUES (-1, ?)" ) ) {
                                insertStatement.setObject( 1, nodeId );

                                if ( insertStatement.execute() || insertStatement.getUpdateCount() != 1 ) {
                                    throw new RuntimeException( "Cannot insert NodeId." );
                                }
                            }
                        }
                    }
                    if ( !connetion.getAutoCommit() ) {
                        connetion.commit();
                    }
                } catch ( SQLException e ) {
                    throw Utils.extractAndThrow( e );
                }
                nodeIdReference.set( nodeId );
            }

            return nodeId;
        }
    }


    @Override
    public MetaResultSet getTables( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat tableNamePattern, List<String> typeList ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getTables( catalogConnection.getConnectionHandle(), catalog, schemaPattern, tableNamePattern, typeList );
    }


    @Override
    public MetaResultSet getColumns( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getColumns( catalogConnection.getConnectionHandle(), catalog, schemaPattern, tableNamePattern, columnNamePattern );
    }


    @Override
    public MetaResultSet getSchemas( ConnectionInfos connection, String catalog, Pat schemaPattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getSchemas( catalogConnection.getConnectionHandle(), catalog, schemaPattern );
    }


    @Override
    public MetaResultSet getCatalogs( ConnectionInfos connection ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getCatalogs( catalogConnection.getConnectionHandle() );
    }


    @Override
    public MetaResultSet getTableTypes( ConnectionInfos connection ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getTableTypes( catalogConnection.getConnectionHandle() );
    }


    @Override
    public MetaResultSet getProcedures( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat procedureNamePattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getProcedures( catalogConnection.getConnectionHandle(), catalog, schemaPattern, procedureNamePattern );
    }


    @Override
    public MetaResultSet getProcedureColumns( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat procedureNamePattern, Pat columnNamePattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getProcedureColumns( catalogConnection.getConnectionHandle(), catalog, schemaPattern, procedureNamePattern, columnNamePattern );
    }


    @Override
    public MetaResultSet getColumnPrivileges( ConnectionInfos connection, String catalog, String schema, String table, Pat columnNamePattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getColumnPrivileges( catalogConnection.getConnectionHandle(), catalog, schema, table, columnNamePattern );
    }


    @Override
    public MetaResultSet getTablePrivileges( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat tableNamePattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getTablePrivileges( catalogConnection.getConnectionHandle(), catalog, schemaPattern, tableNamePattern );
    }


    @Override
    public MetaResultSet getBestRowIdentifier( ConnectionInfos connection, String catalog, String schema, String table, int scope, boolean nullable ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getBestRowIdentifier( catalogConnection.getConnectionHandle(), catalog, schema, table, scope, nullable );
    }


    @Override
    public MetaResultSet getVersionColumns( ConnectionInfos connection, String catalog, String schema, String table ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getVersionColumns( catalogConnection.getConnectionHandle(), catalog, schema, table );
    }


    @Override
    public MetaResultSet getPrimaryKeys( ConnectionInfos connection, String catalog, String schema, String table ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getPrimaryKeys( catalogConnection.getConnectionHandle(), catalog, schema, table );
    }


    @Override
    public MetaResultSet getImportedKeys( ConnectionInfos connection, String catalog, String schema, String table ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getImportedKeys( catalogConnection.getConnectionHandle(), catalog, schema, table );
    }


    @Override
    public MetaResultSet getExportedKeys( ConnectionInfos connection, String catalog, String schema, String table ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getExportedKeys( catalogConnection.getConnectionHandle(), catalog, schema, table );
    }


    @Override
    public MetaResultSet getCrossReference( ConnectionInfos connection, String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getCrossReference( catalogConnection.getConnectionHandle(), parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable );
    }


    @Override
    public MetaResultSet getTypeInfo( ConnectionInfos connection ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getTypeInfo( catalogConnection.getConnectionHandle() );
    }


    @Override
    public MetaResultSet getIndexInfo( ConnectionInfos connection, String catalog, String schema, String table, boolean unique, boolean approximate ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getIndexInfo( catalogConnection.getConnectionHandle(), catalog, schema, table, unique, approximate );
    }


    @Override
    public MetaResultSet getUDTs( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat typeNamePattern, int[] types ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getUDTs( catalogConnection.getConnectionHandle(), catalog, schemaPattern, typeNamePattern, types );
    }


    @Override
    public MetaResultSet getSuperTypes( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat typeNamePattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getSuperTypes( catalogConnection.getConnectionHandle(), catalog, schemaPattern, typeNamePattern );
    }


    @Override
    public MetaResultSet getSuperTables( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat tableNamePattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getSuperTables( catalogConnection.getConnectionHandle(), catalog, schemaPattern, tableNamePattern );
    }


    @Override
    public MetaResultSet getAttributes( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat typeNamePattern, Pat attributeNamePattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getAttributes( catalogConnection.getConnectionHandle(), catalog, schemaPattern, typeNamePattern, attributeNamePattern );
    }


    @Override
    public MetaResultSet getClientInfoProperties( ConnectionInfos connection ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getClientInfoProperties( catalogConnection.getConnectionHandle() );
    }


    @Override
    public MetaResultSet getFunctions( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat functionNamePattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getFunctions( catalogConnection.getConnectionHandle(), catalog, schemaPattern, functionNamePattern );
    }


    @Override
    public MetaResultSet getFunctionColumns( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat functionNamePattern, Pat columnNamePattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getFunctionColumns( catalogConnection.getConnectionHandle(), catalog, schemaPattern, functionNamePattern, columnNamePattern );
    }


    @Override
    public MetaResultSet getPseudoColumns( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        return xaMeta.getPseudoColumns( catalogConnection.getConnectionHandle(), catalog, schemaPattern, tableNamePattern, columnNamePattern );
    }


    private ConnectionInfos getOrOpenCatalogConnection( final ConnectionInfos connection ) {
        synchronized ( catalogConnections ) {
            return catalogConnections.computeIfAbsent( connection.getConnectionHandle().id, cid -> {
                // this is an unknown connection
                // its first occurrence creates a new connection here to represent the original connection
                xaMeta.openConnection( connection.getConnectionHandle(), null );
                return connection;
            } );
        }
    }


    private ConnectionInfos getOrOpenCatalogConnection( final ConnectionHandle connectionHandle ) {
        synchronized ( catalogConnections ) {
            return catalogConnections.computeIfAbsent( connectionHandle.id, cid -> {
                // this is an unknown connection
                // its first occurrence creates a new connection here to represent the original connection
                final ConnectionInfos connection = new ConnectionInfos( new ConnectionHandle( cid ) );
                xaMeta.openConnection( connection.getConnectionHandle(), null );
                return connection;
            } );
        }
    }


    private ConnectionInfos getOrOpenCatalogConnection( final StatementHandle statementHandle ) {
        return this.getOrOpenCatalogConnection( new ConnectionHandle( statementHandle.connectionId ) );
    }


    private ConnectionInfos getConnection( final ConnectionHandle connectionHandle ) throws NoSuchConnectionException {
        synchronized ( catalogConnections ) {
            if ( catalogConnections.containsKey( connectionHandle.id ) ) {
                return catalogConnections.get( connectionHandle.id );
            }
            throw new NoSuchConnectionException( connectionHandle.id );
        }
    }


    private ConnectionInfos getConnection( final StatementHandle statementHandle ) {
        return this.getConnection( new ConnectionHandle( statementHandle.connectionId ) );
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
    public ExecuteResult prepareAndExecuteDataDefinition( TransactionHandle transactionHandle, StatementHandle statementHandle, String sql, long maxRowCount, int maxRowsInFirstFrame ) {
        final RemoteExecuteResult result;

        final ConnectionInfos connection = getOrOpenCatalogConnection( statementHandle );
        final StatementInfos catalogStatement = getOrCreateStatement( connection, statementHandle );

        try {
            final TransactionInfos catalogTransaction = xaMeta.getOrStartTransaction( connection, transactionHandle );
            LOGGER.debug( "executing catalog.prepareAndExecuteDataDefinition( ... ) in the context of {}", catalogTransaction );

            openTransactionsMap.compute( connection.getConnectionHandle().id, ( connectionId, transactions ) -> {
                if ( transactions == null ) {
                    transactions = new HashSet<>();
                }
                transactions.add( transactionHandle );
                return transactions;
            } );
            return xaMeta.prepareAndExecute( catalogStatement.getStatementHandle(), sql, maxRowCount, maxRowsInFirstFrame, NOOP_PREPARE_CALLBACK );
        } catch ( XAException | NoSuchStatementException e ) {
            throw Utils.extractAndThrow( e );
        }
    }


    @Override
    public void onePhaseCommit( ConnectionHandle connectionHandle, TransactionHandle transactionHandle ) throws XAException {
        if ( !catalogConnections.containsKey( connectionHandle.id ) ) {
            return;
        }

        xaMeta.onePhaseCommit( connectionHandle, transactionHandle );
        openTransactionsMap.compute( connectionHandle.id, ( connectionId, transactions ) -> {
            if ( transactions == null || transactions.isEmpty() ) {
                return null;
            }
            transactions.remove( transactionHandle );
            if ( transactions.isEmpty() ) {
                return null;
            }
            return transactions;
        } );
    }


    @Override
    public void commit( ConnectionHandle connectionHandle ) {
        if ( !catalogConnections.containsKey( connectionHandle.id ) ) {
            return;
        }

        xaMeta.commit( connectionHandle );
        openTransactionsMap.remove( connectionHandle.id );
    }


    @Override
    public void commit( ConnectionHandle connectionHandle, TransactionHandle transactionHandle ) throws XAException {
        if ( !catalogConnections.containsKey( connectionHandle.id ) ) {
            return;
        }

        xaMeta.commit( connectionHandle, transactionHandle );
        openTransactionsMap.compute( connectionHandle.id, ( connectionId, transactions ) -> {
            if ( transactions == null || transactions.isEmpty() ) {
                return null;
            }
            transactions.remove( transactionHandle );
            if ( transactions.isEmpty() ) {
                return null;
            }
            return transactions;
        } );
    }


    @Override
    public boolean prepareCommit( ConnectionHandle connectionHandle, TransactionHandle transactionHandle ) throws XAException {
        if ( !catalogConnections.containsKey( connectionHandle.id ) ) {
            return true;
        }

        Set<TransactionHandle> transactions = openTransactionsMap.getOrDefault( connectionHandle.id, Collections.emptySet() );
        if ( transactions.contains( transactionHandle ) ) {
            return xaMeta.prepareCommit( connectionHandle, transactionHandle );
        } else {
            return true;
        }
    }


    @Override
    public void rollback( ConnectionHandle connectionHandle ) {
        if ( !catalogConnections.containsKey( connectionHandle.id ) ) {
            return;
        }

        xaMeta.rollback( connectionHandle );
        openTransactionsMap.remove( connectionHandle.id );
    }


    @Override
    public void rollback( ConnectionHandle connectionHandle, TransactionHandle transactionHandle ) throws XAException {
        if ( !catalogConnections.containsKey( connectionHandle.id ) ) {
            return;
        }

        xaMeta.rollback( connectionHandle, transactionHandle );
        openTransactionsMap.compute( connectionHandle.id, ( connectionId, transactions ) -> {
            if ( transactions == null || transactions.isEmpty() ) {
                return null;
            }
            transactions.remove( transactionHandle );
            if ( transactions.isEmpty() ) {
                return null;
            }
            return transactions;
        } );
    }


    @Override
    public Planner getPlanner() {
        final SchemaPlus rootSchema = Frameworks.createRootSchema( true );
        return Frameworks.getPlanner( Frameworks.newConfigBuilder()
                .parserConfig( this.sqlParserConfig )
                // CAUTION! Hard coded HSQLDB information
                .defaultSchema( rootSchema.add( "PUBLIC", JdbcSchema.create( rootSchema, "HSQLDB", this.dataSource, null, "PUBLIC" ) ) )
                .traitDefs( ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE )
                .context( Contexts.EMPTY_CONTEXT )
                .ruleSets( RuleSets.ofList() )
                .costFactory( null )
                .typeSystem( RelDataTypeSystem.DEFAULT )
                .build() );
    }


    @Override
    public void closeConnection( ConnectionInfos connection ) {
        if ( !catalogConnections.containsKey( connection.getConnectionHandle().id ) ) {
            return;
        }

        xaMeta.closeConnection( connection.getConnectionHandle() );
    }


    private static class SingletonHolder {

        private static final GlobalCatalog INSTANCE = new GlobalCatalog();


        private SingletonHolder() {
        }
    }


    public static GlobalCatalog getInstance() {
        return SingletonHolder.INSTANCE;
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

}
