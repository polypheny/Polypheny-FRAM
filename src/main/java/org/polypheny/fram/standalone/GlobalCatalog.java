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

    private final Map<ConnectionHandle, ConnectionInfos> catalogConnections = new HashMap<>();
    private final Map<StatementHandle, StatementInfos> remoteToLocalStatementMap = new HashMap<>();
    private final Map<ConnectionHandle, Set<TransactionHandle>> openTransactionsMap = new HashMap<>();

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
            throw Utils.wrapException( e );
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
                    throw Utils.wrapException( e );
                }
                nodeIdReference.set( nodeId );
            }

            return nodeId;
        }
    }


    @Override
    public MetaResultSet getTables( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat tableNamePattern, List<String> typeList ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getTables( catalogConnection.getConnectionHandle(), catalog, schemaPattern, tableNamePattern, typeList );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getColumns( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getColumns( catalogConnection.getConnectionHandle(), catalog, schemaPattern, tableNamePattern, columnNamePattern );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getSchemas( ConnectionInfos connection, String catalog, Pat schemaPattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getSchemas( catalogConnection.getConnectionHandle(), catalog, schemaPattern );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getCatalogs( ConnectionInfos connection ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getCatalogs( catalogConnection.getConnectionHandle() );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getTableTypes( ConnectionInfos connection ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getTableTypes( catalogConnection.getConnectionHandle() );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getProcedures( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat procedureNamePattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getProcedures( catalogConnection.getConnectionHandle(), catalog, schemaPattern, procedureNamePattern );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getProcedureColumns( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat procedureNamePattern, Pat columnNamePattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getProcedureColumns( catalogConnection.getConnectionHandle(), catalog, schemaPattern, procedureNamePattern, columnNamePattern );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getColumnPrivileges( ConnectionInfos connection, String catalog, String schema, String table, Pat columnNamePattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getColumnPrivileges( catalogConnection.getConnectionHandle(), catalog, schema, table, columnNamePattern );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getTablePrivileges( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat tableNamePattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getTablePrivileges( catalogConnection.getConnectionHandle(), catalog, schemaPattern, tableNamePattern );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getBestRowIdentifier( ConnectionInfos connection, String catalog, String schema, String table, int scope, boolean nullable ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getBestRowIdentifier( catalogConnection.getConnectionHandle(), catalog, schema, table, scope, nullable );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getVersionColumns( ConnectionInfos connection, String catalog, String schema, String table ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getVersionColumns( catalogConnection.getConnectionHandle(), catalog, schema, table );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getPrimaryKeys( ConnectionInfos connection, String catalog, String schema, String table ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getPrimaryKeys( catalogConnection.getConnectionHandle(), catalog, schema, table );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getImportedKeys( ConnectionInfos connection, String catalog, String schema, String table ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getImportedKeys( catalogConnection.getConnectionHandle(), catalog, schema, table );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getExportedKeys( ConnectionInfos connection, String catalog, String schema, String table ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getExportedKeys( catalogConnection.getConnectionHandle(), catalog, schema, table );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getCrossReference( ConnectionInfos connection, String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getCrossReference( catalogConnection.getConnectionHandle(), parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getTypeInfo( ConnectionInfos connection ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getTypeInfo( catalogConnection.getConnectionHandle() );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getIndexInfo( ConnectionInfos connection, String catalog, String schema, String table, boolean unique, boolean approximate ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getIndexInfo( catalogConnection.getConnectionHandle(), catalog, schema, table, unique, approximate );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getUDTs( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat typeNamePattern, int[] types ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getUDTs( catalogConnection.getConnectionHandle(), catalog, schemaPattern, typeNamePattern, types );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getSuperTypes( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat typeNamePattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getSuperTypes( catalogConnection.getConnectionHandle(), catalog, schemaPattern, typeNamePattern );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getSuperTables( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat tableNamePattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getSuperTables( catalogConnection.getConnectionHandle(), catalog, schemaPattern, tableNamePattern );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getAttributes( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat typeNamePattern, Pat attributeNamePattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getAttributes( catalogConnection.getConnectionHandle(), catalog, schemaPattern, typeNamePattern, attributeNamePattern );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getClientInfoProperties( ConnectionInfos connection ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getClientInfoProperties( catalogConnection.getConnectionHandle() );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getFunctions( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat functionNamePattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getFunctions( catalogConnection.getConnectionHandle(), catalog, schemaPattern, functionNamePattern );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getFunctionColumns( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat functionNamePattern, Pat columnNamePattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getFunctionColumns( catalogConnection.getConnectionHandle(), catalog, schemaPattern, functionNamePattern, columnNamePattern );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    @Override
    public MetaResultSet getPseudoColumns( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern ) {
        final ConnectionInfos catalogConnection = getOrOpenCatalogConnection( connection );
        final MetaResultSet result = xaMeta.getPseudoColumns( catalogConnection.getConnectionHandle(), catalog, schemaPattern, tableNamePattern, columnNamePattern );
        if ( !result.firstFrame.done ) {
            throw new UnsupportedOperationException( "Catalog.MetaResultSet.firstFrame.done == false! I guess we need something like a ResultSet implementation..." );
        }
        return result;
    }


    private ConnectionInfos getOrOpenCatalogConnection( final ConnectionInfos connection ) {
        synchronized ( catalogConnections ) {
            return catalogConnections.computeIfAbsent( connection.getConnectionHandle(), cid -> {
                // this is an unknown connection
                // its first occurrence creates a new connection here to represent the original connection
                xaMeta.openConnection( connection.getConnectionHandle(), null );
                return connection;
            } );
        }
    }


    private ConnectionInfos getOrOpenCatalogConnection( final ConnectionHandle connectionHandle ) {
        synchronized ( catalogConnections ) {
            return catalogConnections.computeIfAbsent( connectionHandle, handle -> {
                // this is an unknown connection
                // its first occurrence creates a new connection here to represent the original connection
                final ConnectionInfos connection = new ConnectionInfos( handle );
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
            if ( catalogConnections.containsKey( connectionHandle ) ) {
                return catalogConnections.get( connectionHandle );
            }
            throw new NoSuchConnectionException( connectionHandle.id );
        }
    }


    private ConnectionInfos getConnection( final StatementHandle statementHandle ) {
        return this.getConnection( new ConnectionHandle( statementHandle.connectionId ) );
    }


    private StatementInfos getOrCreateStatement( final ConnectionInfos connection, final StatementHandle statementHandle ) {
        synchronized ( remoteToLocalStatementMap ) {
            return remoteToLocalStatementMap.computeIfAbsent( statementHandle, statementHandleString -> {
                // this is an unknown statement
                // its first occurrence creates a new local statement here to represent the original remote statement
                return new StatementInfos( connection, xaMeta.createStatement( connection.getConnectionHandle() ) );
            } );
        }
    }


    private StatementInfos getStatement( final StatementHandle statementHandle ) throws NoSuchStatementException {
        synchronized ( remoteToLocalStatementMap ) {
            if ( remoteToLocalStatementMap.containsKey( statementHandle ) ) {
                return remoteToLocalStatementMap.get( statementHandle );
            }
            throw new NoSuchStatementException( statementHandle );
        }
    }


    private StatementInfos createStatement( final ConnectionInfos connection, final StatementHandle statementHandle ) {
        synchronized ( remoteToLocalStatementMap ) {
            return remoteToLocalStatementMap.compute( statementHandle, ( handle, statementInfos ) -> {
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
        final ConnectionInfos connection = getOrOpenCatalogConnection( statementHandle );
        final StatementInfos catalogStatement = getOrCreateStatement( connection, statementHandle );

        try {
            final TransactionInfos catalogTransaction = xaMeta.getOrStartTransaction( connection, transactionHandle );
            LOGGER.debug( "executing catalog.prepareAndExecuteDataDefinition( ... ) in the context of {}", catalogTransaction );

            openTransactionsMap.compute( connection.getConnectionHandle(), ( connectionId, transactions ) -> {
                if ( transactions == null ) {
                    transactions = new HashSet<>();
                }
                transactions.add( transactionHandle );
                return transactions;
            } );
            return xaMeta.prepareAndExecute( catalogStatement.getStatementHandle(), sql, maxRowCount, maxRowsInFirstFrame, NOOP_PREPARE_CALLBACK );
        } catch ( XAException | NoSuchStatementException e ) {
            throw Utils.wrapException( e );
        }
    }


    @Override
    public void onePhaseCommit( ConnectionHandle connectionHandle, TransactionHandle transactionHandle ) throws XAException {
        if ( !openTransactionsMap.containsKey( connectionHandle ) ) {
            return;
        }

        xaMeta.onePhaseCommit( connectionHandle, transactionHandle );
        openTransactionsMap.compute( connectionHandle, ( connectionId, transactions ) -> {
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
        if ( !openTransactionsMap.containsKey( connectionHandle ) ) {
            return;
        }

        xaMeta.commit( connectionHandle );
        openTransactionsMap.remove( connectionHandle );
    }


    @Override
    public void commit( ConnectionHandle connectionHandle, TransactionHandle transactionHandle ) throws XAException {
        if ( !openTransactionsMap.containsKey( connectionHandle ) ) {
            return;
        }

        xaMeta.commit( connectionHandle, transactionHandle );
        openTransactionsMap.compute( connectionHandle, ( connectionId, transactions ) -> {
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
        if ( !openTransactionsMap.containsKey( connectionHandle ) ) {
            return true;
        }

        Set<TransactionHandle> transactions = openTransactionsMap.getOrDefault( connectionHandle, Collections.emptySet() );
        if ( transactions.contains( transactionHandle ) ) {
            return xaMeta.prepareCommit( connectionHandle, transactionHandle );
        } else {
            return true;
        }
    }


    @Override
    public void rollback( ConnectionHandle connectionHandle ) {
        if ( !openTransactionsMap.containsKey( connectionHandle ) ) {
            return;
        }

        xaMeta.rollback( connectionHandle );
        openTransactionsMap.remove( connectionHandle );
    }


    @Override
    public void rollback( ConnectionHandle connectionHandle, TransactionHandle transactionHandle ) throws XAException {
        if ( !openTransactionsMap.containsKey( connectionHandle ) ) {
            return;
        }

        xaMeta.rollback( connectionHandle, transactionHandle );
        openTransactionsMap.compute( connectionHandle, ( connectionId, transactions ) -> {
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
        if ( !catalogConnections.containsKey( connection.getConnectionHandle() ) ) {
            return;
        }

        xaMeta.closeConnection( connection.getConnectionHandle() );
        catalogConnections.remove( connection.getConnectionHandle() );
        openTransactionsMap.remove( connection.getConnectionHandle() );
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
