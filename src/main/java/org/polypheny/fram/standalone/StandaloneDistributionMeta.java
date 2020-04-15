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


import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchConnectionException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.apache.calcite.avatica.remote.ProtobufMeta;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.ValidationException;
import org.polypheny.fram.AbstractDistributionMeta;
import org.polypheny.fram.protocols.Protocol;
import org.polypheny.fram.protocols.Protocols;
import org.polypheny.fram.remote.types.RemoteConnectionHandle;
import org.polypheny.fram.standalone.connection.ConnectionAlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
class StandaloneDistributionMeta extends AbstractDistributionMeta implements Meta, ProtobufMeta {


    public static final String ANONYMOUS_USERNAME = "anonymous";
    public static final String DATABASE_ADMIN_USERNAME = "pa";


    public static ProtobufMeta newMetaInstance() {
        return new StandaloneDistributionMeta();
    }


    private static final Logger LOGGER = LoggerFactory.getLogger( StandaloneDistributionMeta.class );

    protected final Map<String, ConnectionInfos> openConnections = new HashMap<>();
    protected final Map<String, StatementInfos> openStatements = new HashMap<>();

    protected final Timer executionDurationTimer;
    protected final Timer fetchDurationTimer;
    protected final Timer commitDurationTimer;
    protected final Timer rollbackDurationTimer;
    protected final Timer closeStatementDurationTimer;
    protected final Timer closeConnectionDurationTimer;
    protected final Timer connectionSyncDurationTimer;

    protected volatile Protocol protocol;


    private StandaloneDistributionMeta() {
        super( LocalNode.getInstance() );

        this.executionDurationTimer = Metrics.timer( "meta.execute", Tags.empty() );
        this.fetchDurationTimer = Metrics.timer( "meta.detch", Tags.empty() );
        this.commitDurationTimer = Metrics.timer( "meta.commit", Tags.empty() );
        this.rollbackDurationTimer = Metrics.timer( "meta.rollback", Tags.empty() );
        this.closeStatementDurationTimer = Metrics.timer( "meta.closeStatement", Tags.empty() );
        this.closeConnectionDurationTimer = Metrics.timer( "meta.closeConnection", Tags.empty() );
        this.connectionSyncDurationTimer = Metrics.timer( "meta.connectionSync", Tags.empty() );

        this.protocol = Protocols.valueOf( Main.configuration().hasPath( "fram.defaultProtocol" ) ? Main.configuration().getString( "fram.defaultProtocol" ).toUpperCase() : Protocols.PASS_THROUGH.name() );
    }


    @Override
    public Map<DatabaseProperty, Object> getDatabaseProperties( ConnectionHandle ch ) {
        try {
            final Map<DatabaseProperty, Object> databaseProperties = new EnumMap<>( DatabaseProperty.class );
            this.store.getDatabaseProperties( RemoteConnectionHandle.fromConnectionHandle( ch ) )
                    .forEach( ( databaseProperty, serializable ) -> databaseProperties.put( DatabaseProperty.fromProto( databaseProperty ), serializable ) );
            return databaseProperties;
        } catch ( RemoteException e ) {
            throw Utils.wrapException( e );
        }
    }


    @Override
    public MetaResultSet getTables( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, List<String> typeList ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getTables( connection, catalog, schemaPattern, tableNamePattern, typeList );
    }


    @Override
    public MetaResultSet getColumns( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getColumns( connection, catalog, schemaPattern, tableNamePattern, columnNamePattern );
    }


    @Override
    public MetaResultSet getSchemas( ConnectionHandle ch, String catalog, Pat schemaPattern ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getSchemas( connection, catalog, schemaPattern );
    }


    @Override
    public MetaResultSet getCatalogs( ConnectionHandle ch ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getCatalogs( connection );
    }


    @Override
    public MetaResultSet getTableTypes( ConnectionHandle ch ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getTableTypes( connection );
    }


    @Override
    public MetaResultSet getProcedures( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat procedureNamePattern ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getProcedures( connection, catalog, schemaPattern, procedureNamePattern );
    }


    @Override
    public MetaResultSet getProcedureColumns( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat procedureNamePattern, Pat columnNamePattern ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getProcedureColumns( connection, catalog, schemaPattern, procedureNamePattern, columnNamePattern );
    }


    @Override
    public MetaResultSet getColumnPrivileges( ConnectionHandle ch, String catalog, String schema, String table, Pat columnNamePattern ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getColumnPrivileges( connection, catalog, schema, table, columnNamePattern );
    }


    @Override
    public MetaResultSet getTablePrivileges( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getTablePrivileges( connection, catalog, schemaPattern, tableNamePattern );
    }


    @Override
    public MetaResultSet getBestRowIdentifier( ConnectionHandle ch, String catalog, String schema, String table, int scope, boolean nullable ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getBestRowIdentifier( connection, catalog, schema, table, scope, nullable );
    }


    @Override
    public MetaResultSet getVersionColumns( ConnectionHandle ch, String catalog, String schema, String table ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getVersionColumns( connection, catalog, schema, table );
    }


    @Override
    public MetaResultSet getPrimaryKeys( ConnectionHandle ch, String catalog, String schema, String table ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getPrimaryKeys( connection, catalog, schema, table );
    }


    @Override
    public MetaResultSet getImportedKeys( ConnectionHandle ch, String catalog, String schema, String table ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getImportedKeys( connection, catalog, schema, table );
    }


    @Override
    public MetaResultSet getExportedKeys( ConnectionHandle ch, String catalog, String schema, String table ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getExportedKeys( connection, catalog, schema, table );
    }


    @Override
    public MetaResultSet getCrossReference( ConnectionHandle ch, String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getCrossReference( connection, parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable );
    }


    @Override
    public MetaResultSet getTypeInfo( ConnectionHandle ch ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getTypeInfo( connection );
    }


    @Override
    public MetaResultSet getIndexInfo( ConnectionHandle ch, String catalog, String schema, String table, boolean unique, boolean approximate ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getIndexInfo( connection, catalog, schema, table, unique, approximate );
    }


    @Override
    public MetaResultSet getUDTs( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat typeNamePattern, int[] types ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getUDTs( connection, catalog, schemaPattern, typeNamePattern, types );
    }


    @Override
    public MetaResultSet getSuperTypes( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat typeNamePattern ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getSuperTypes( connection, catalog, schemaPattern, typeNamePattern );
    }


    @Override
    public MetaResultSet getSuperTables( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getSuperTables( connection, catalog, schemaPattern, tableNamePattern );
    }


    @Override
    public MetaResultSet getAttributes( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat typeNamePattern, Pat attributeNamePattern ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getAttributes( connection, catalog, schemaPattern, typeNamePattern, attributeNamePattern );
    }


    @Override
    public MetaResultSet getClientInfoProperties( ConnectionHandle ch ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getClientInfoProperties( connection );
    }


    @Override
    public MetaResultSet getFunctions( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat functionNamePattern ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getFunctions( connection, catalog, schemaPattern, functionNamePattern );
    }


    @Override
    public MetaResultSet getFunctionColumns( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat functionNamePattern, Pat columnNamePattern ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getFunctionColumns( connection, catalog, schemaPattern, functionNamePattern, columnNamePattern );
    }


    @Override
    public MetaResultSet getPseudoColumns( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern ) {
        final ConnectionInfos connection = getConnection( ch );
        //final TransactionInfos transaction = connection.getOrStartTransaction();
        return this.catalog.getPseudoColumns( connection, catalog, schemaPattern, tableNamePattern, columnNamePattern );
    }


    @Override
    public Iterable<Object> createIterable( StatementHandle statementHandle, QueryState state, Signature signature, List<TypedValue> parameters, Frame firstFrame ) {
        LOGGER.trace( "createIterable( statementHandle: {}, state: {}, signature: {}, parameters: {}, firstFrame: {} )", statementHandle, state, signature, parameters, firstFrame );
        final ConnectionInfos connection = getConnection( statementHandle );
        final TransactionInfos transaction = connection.getOrStartTransaction();

        try {
            final StatementInfos statement = getStatement( statementHandle );

            final List<Common.TypedValue> serializedParameterValues = new LinkedList<>();
            for ( TypedValue value : parameters ) {
                serializedParameterValues.add( value.toProto() );
            }

            final Iterable<Serializable> result = protocol.createIterable( connection, transaction, statement, state, signature, serializedParameterValues, firstFrame );

            LOGGER.trace( "createIterable( statementHandle: {}, state: {}, signature: {}, parameters: {}, firstFrame: {} ) = {}", statementHandle, state, signature, parameters, firstFrame, result );

            return () -> new Iterator<Object>() {

                private final Iterator<Serializable> delegate = result.iterator();


                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }


                @Override
                public Object next() {
                    return delegate.next();
                }
            };

        } catch ( RemoteException ex ) {
            LOGGER.warn( "Exception occured", ex );
            throw Utils.wrapException( ex );
        } catch ( Exception ex ) {
            throw Utils.wrapException( ex );
        }
    }


    @Override
    public boolean syncResults( StatementHandle statementHandle, QueryState state, long offset ) throws NoSuchStatementException {
        LOGGER.trace( "syncResults( statementHandle: {}, state: {}, offset: {} )", statementHandle, state, offset );
        final ConnectionInfos connection = getConnection( statementHandle );
        final TransactionInfos transaction = connection.getOrStartTransaction();
        final StatementInfos statement = getStatement( statementHandle );

        try {
            final boolean result = protocol.syncResults( connection, transaction, statement, state, offset );

            LOGGER.trace( "syncResults( statementHandle: {}, state: {}, offset: {} ) = {}", statementHandle, state, offset, result );
            return result;
        } catch ( RemoteException ex ) {
            LOGGER.warn( "Exception occured", ex );
            throw Utils.wrapException( ex );
        } catch ( Exception ex ) {
            throw Utils.wrapException( ex );
        }
    }


    private final Timer plannerParseStringTimer = Metrics.timer( StandaloneDistributionMeta.class.getSimpleName() + "." + "planner.parse", Tags.empty() );


    private SqlNode parseSql( final Planner planner, final String sql ) throws SqlParseException {
        LOGGER.trace( "parseSql( planner: {}, sql: {} )", planner, sql );

        try {
            final SqlNode result = plannerParseStringTimer.recordCallable( () -> planner.parse( sql ) );

            LOGGER.trace( "parseSql( planner: {}, sql: {} ) = {}", planner, sql, result );
            return result;
        } catch ( IllegalArgumentException ex ) {
            LOGGER.debug( "Wrong planner state. ", ex );
            throw Utils.wrapException( ex );
        } catch ( SqlParseException ex ) {
            throw ex;
        } catch ( Exception ex ) {
            throw Utils.wrapException( ex );
        }
    }


    private final Timer plannerValidateSqlTimer = Metrics.timer( StandaloneDistributionMeta.class.getSimpleName() + "." + "planner.validate", Tags.empty() );


    private SqlNode validateSql( final Planner planner, final SqlNode sql ) throws ValidationException {
        LOGGER.trace( "validateSql( planner: {}, sql: {} )", planner, sql );

        try {
            final SqlNode result = plannerValidateSqlTimer.recordCallable( () -> planner.validate( sql ) );

            LOGGER.trace( "validateSql( planner: {}, sql: {} ) = {}", planner, sql, result );
            return result;
        } catch ( IllegalArgumentException ex ) {
            LOGGER.debug( "Wrong planner state. ", ex );
            throw Utils.wrapException( ex );
        } catch ( ValidationException ex ) {
            throw ex;
        } catch ( Exception ex ) {
            throw Utils.wrapException( ex );
        }
    }


    private final Timer prepareTimer = Metrics.timer( StandaloneDistributionMeta.class.getSimpleName() + "." + "prepare", Tags.empty() );


    @Override
    public StatementHandle prepare( final ConnectionHandle connectionHandle, final String sql, final long maxRowCount ) {
        LOGGER.trace( "prepare( connectionHandle: {}, sql: {}, maxRowCount: {} )", connectionHandle, sql, maxRowCount );

        final ConnectionInfos connection = getConnection( connectionHandle );

        LOGGER.trace( "prepare() -- connection: {}", connection );

        final Planner planner = connection.getPlanner();

        final SqlNode sqlTreeParsed;
        try {
            sqlTreeParsed = parseSql( planner, sql );
        } catch ( SqlParseException ex ) {
            LOGGER.debug( "Exception while parsing sql: " + sql, ex );
            throw Utils.wrapException( ex );
        }

        if ( !sqlTreeParsed.isA( SqlKind.TOP_LEVEL ) ) {
            // SqlKind.TOP_LEVEL = QUERY, DML, DDL
            LOGGER.debug( "Unsupported Operation: `" + sqlTreeParsed.getKind() + "´ is not TOP_LEVEL." );
            throw Utils.wrapException( new UnsupportedOperationException( "`" + sqlTreeParsed.getKind() + "´ is not TOP_LEVEL." ) );
        }

        if ( sqlTreeParsed.isA( SqlKind.DDL ) ) {
            /*
             * Branching off DataDefinition
             */
            LOGGER.debug( "Unsupported Operation: DDL is not supported yet." );
            throw Utils.wrapException( new UnsupportedOperationException( "Not supported yet." ) );
        }

        /*
         * Continue processing the validated SQL tree.
         */

        final SqlNode sqlTreeValidated;
        try {
            sqlTreeValidated = validateSql( planner, sqlTreeParsed );
        } catch ( ValidationException ex ) {
            LOGGER.debug( "Exception while validating the statement.", ex );
            throw Utils.wrapException( ex );
        }
        final SqlNode sqlTreeGenerated = sqlTreeValidated;

        StatementInfos statement = connection.createStatement();
        if ( sqlTreeValidated.isA( SqlKind.DML ) ) {
            /*
             * Branching off DML statements (writing statements)
             */
            statement = prepareDataManipulation( connection, statement, sqlTreeGenerated, maxRowCount );
        } else if ( sqlTreeValidated.isA( SqlKind.QUERY ) ) {
            /*
             * Branching off QUERY statements (reading statements)
             */
            statement = prepareDataQuery( connection, statement, sqlTreeGenerated, maxRowCount );
        } else {
            /*
             * We should not be here. Can only be the case if the API of Apache Calcite Avatica has changed.
             */
            throw Utils.wrapException( new UnsupportedOperationException( "Not supported yet." ) );
        }

        synchronized ( openStatements ) {
            if ( openStatements.putIfAbsent( statement.getStatementHandle().toString(), statement ) != null ) {
                LOGGER.warn( "Statement already exists." );
                throw Utils.wrapException( new RuntimeException( "Statement already exists." ) );
            }
        }

        LOGGER.trace( "prepare( connectionHandle: {}, sql: {}, maxRowCount: {} ) = {}", connectionHandle, sql, maxRowCount, statement.getStatementHandle() );
        return statement.getStatementHandle();
    }


    private StatementInfos prepareDataManipulation( final ConnectionInfos connection, final StatementInfos statement, final SqlNode sql, final long maxRowCount ) {
        LOGGER.trace( "prepareDataManipulation( connection: {}, sql: {}, maxRowCount: {} )", connection, sql, maxRowCount );

        try {
            final StatementInfos result = prepareTimer.recordCallable(
                    () -> protocol.prepareDataManipulation( connection, statement, sql, maxRowCount )
            );

            LOGGER.trace( "prepareDataManipulation( connection: {}, sql: {}, maxRowCount: {} ) = {}", connection, sql, maxRowCount, result );
            return result;
        } catch ( RemoteException ex ) {
            LOGGER.warn( "Exception occured", ex );
            throw Utils.wrapException( ex );
        } catch ( Exception ex ) {
            throw Utils.wrapException( ex );
        }
    }


    private StatementInfos prepareDataQuery( final ConnectionInfos connection, final StatementInfos statement, final SqlNode sql, final long maxRowCount ) {
        try {
            LOGGER.debug( "prepareDataQuery( connection: {}, sql: {}, maxRowCount: {} )", connection, sql, maxRowCount );

            final StatementInfos result = prepareTimer.recordCallable(
                    () -> protocol.prepareDataQuery( connection, statement, sql, maxRowCount )
            );

            LOGGER.trace( "prepareDataQuery( connection: {}, sql: {}, maxRowCount: {} ) = {}", connection, sql, maxRowCount, result );
            return result;
        } catch ( RemoteException ex ) {
            throw Utils.wrapException( ex );
        } catch ( Exception ex ) {
            throw Utils.wrapException( ex );
        }
    }


    /**
     * @deprecated Deprecated by Apache Calcite Avatica
     */
    @Override
    @Deprecated
    public ExecuteResult prepareAndExecute( StatementHandle h, String sql, long maxRowCount, PrepareCallback callback ) throws NoSuchStatementException {
        return prepareAndExecute( h, sql, maxRowCount, AvaticaUtils.toSaturatedInt( maxRowCount ), callback );
    }


    @Override
    public ExecuteResult prepareAndExecute( final StatementHandle statementHandle, final String sql, final long maxRowCount, final int maxRowsInFirstFrame, final PrepareCallback callback ) throws NoSuchStatementException {
        LOGGER.trace( "prepareAndExecute( statementHandle: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, callback: {} )", statementHandle, sql, maxRowCount, maxRowsInFirstFrame, callback );

        final ConnectionInfos connection = getConnection( statementHandle );
        final TransactionInfos transaction = connection.getOrStartTransaction();
        final StatementInfos statement = getStatement( statementHandle );

        LOGGER.trace( "prepareAndExecute() -- connection: {}, transaction: {}, statement: {}", connection, transaction, statement );

        final Planner planner = connection.getPlanner();

        final SqlNode sqlTreeParsed;
        try {
            sqlTreeParsed = parseSql( planner, sql );
        } catch ( SqlParseException ex ) {
            LOGGER.debug( "Exception while parsing sql: " + sql, ex );
            throw Utils.wrapException( ex );
        }

        if ( !sqlTreeParsed.isA( SqlKind.TOP_LEVEL ) ) {
            // SqlKind.TOP_LEVEL = QUERY, DML, DDL
            LOGGER.debug( "Unsupported Operation: `{}´ is not TOP_LEVEL.", sqlTreeParsed.getKind() );
            throw Utils.wrapException( new UnsupportedOperationException( "`" + sqlTreeParsed.getKind() + "´ is not TOP_LEVEL." ) );
        }

        if ( sqlTreeParsed.isA( SqlKind.DDL ) ) {
            /*
             * DataDefinition cannot be validated (for now?). That's why we branch off here.
             */
            return prepareAndExecuteDataDefinition( connection, transaction, statement, sqlTreeParsed, maxRowCount, maxRowsInFirstFrame, callback );
        }

        /*
         * Continue processing the validated SQL tree.
         */
        final SqlNode sqlTreeValidated;
        try {
            sqlTreeValidated = validateSql( planner, sqlTreeParsed );
        } catch ( ValidationException ex ) {
            LOGGER.debug( "Exception while validating the statement.", ex );
            throw Utils.wrapException( ex );
        }
        final SqlNode sqlTreeGenerated = sqlTreeValidated;

        final ExecuteResult result;
        if ( sqlTreeGenerated.isA( SqlKind.DML ) ) {
            /*
             * Branching off DML statements (writing statements)
             */
            result = prepareAndExecuteDataManipulation( connection, transaction, statement, sqlTreeGenerated, maxRowCount, maxRowsInFirstFrame, callback );
        } else if ( sqlTreeGenerated.isA( SqlKind.QUERY ) ) {
            /*
             * Branching off QUERY statements (reading statements)
             */
            result = prepareAndExecuteDataQuery( connection, transaction, statement, sqlTreeGenerated, maxRowCount, maxRowsInFirstFrame, callback );
        } else {
            /*
             * We should not be here. Can only be the case if the API of Apache Calcite Avatica has changed.
             */
            throw new IllegalStateException( "Fell through all statement types." );
        }

        return result;
    }


    private ExecuteResult prepareAndExecuteDataDefinition( final ConnectionInfos connection, final TransactionInfos transaction, final StatementInfos statement, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final PrepareCallback callback ) {
        /*
         * Currently some TransactionControl commands are members of the DataDefinition group (See org.apache.calcite.sql.SqlKind, org.apache.calcite.sql.SqlKind.DDL).
         * We are especially interested in `COMMIT` and `ROLLBACK` statements.
         */
        switch ( sql.getKind() ) {
            case COMMIT:
                return prepareAndExecuteTransactionCommit( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );

            case ROLLBACK:
                return prepareAndExecuteTransactionRollback( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );

            case SET_OPTION:
                return prepareAndExecuteSetOption( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );

            case ALTER_SESSION:
                throw new UnsupportedOperationException( "Not supported." );

            default:
                // intentional noop
                break;
        }

        // default:
        try {
            LOGGER.trace( "prepareAndExecuteDataDefinition( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, callback: {} )", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );

            final ExecuteResult result = executionDurationTimer.recordCallable(
                    () -> {
                        return protocol.prepareAndExecuteDataDefinition( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
                    }
            );

            // force a new planner since the schema has changed
            connection.getPlanner( true );

            LOGGER.trace( "prepareAndExecuteDataDefinition( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, callback: {} ) = {}", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback, result );
            return result;
        } catch ( RemoteException ex ) {
            throw Utils.wrapException( ex );
        } catch ( Exception ex ) {
            throw Utils.wrapException( ex );
        }
    }


    private ExecuteResult prepareAndExecuteTransactionCommit( final ConnectionInfos connection, final TransactionInfos transaction, final StatementInfos statement, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final PrepareCallback callback ) {
        LOGGER.trace( "prepareAndExecuteTransactionCommit( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, callback: {} )", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );

        this.commit( connection, transaction );

        final ExecuteResult result = new ExecuteResult( Collections.singletonList( MetaResultSet.count( statement.getStatementHandle().connectionId, statement.getStatementHandle().id, 0 ) ) );

        LOGGER.trace( "prepareAndExecuteTransactionCommit( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, callback: {} ) = {}", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback, result );
        return result;
    }


    private ExecuteResult prepareAndExecuteTransactionRollback( final ConnectionInfos connection, final TransactionInfos transaction, final StatementInfos statement, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final PrepareCallback callback ) {
        LOGGER.trace( "prepareAndExecuteTransactionRollback( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, callback: {} )", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );

        this.rollback( connection, transaction );

        final ExecuteResult result = new ExecuteResult( Collections.singletonList( MetaResultSet.count( statement.getStatementHandle().connectionId, statement.getStatementHandle().id, 0 ) ) );

        LOGGER.trace( "prepareAndExecuteTransactionRollback( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, callback: {} ) = {}", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback, result );
        return result;
    }


    private ExecuteResult prepareAndExecuteSetOption( final ConnectionInfos connection, final TransactionInfos transaction, final StatementInfos statement, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final PrepareCallback callback ) {
        LOGGER.trace( "prepareAndExecuteSetOption( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, callback: {} )", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );

        String optionName = sql.accept( new SqlBasicVisitor<String>() {
            @Override
            public String visit( SqlCall call ) {
                if ( call instanceof SqlSetOption ) {
                    return ((SqlSetOption) call).getName().getSimple();
                }
                return super.visit( call );
            }
        } );

        switch ( optionName.toUpperCase() ) {
            case "FRAM_PROTOCOL":
                String protocolEnumName = sql.accept( new SqlBasicVisitor<String>() {
                    @Override
                    public String visit( SqlCall call ) {
                        if ( call instanceof SqlSetOption ) {
                            return ((SqlSetOption) call).getValue().accept( new SqlBasicVisitor<String>() {
                                @Override
                                public String visit( SqlLiteral literal ) {
                                    return literal.toValue();
                                }


                                @Override
                                public String visit( SqlIdentifier id ) {
                                    return id.getSimple();
                                }
                            } );
                        }
                        return super.visit( call );
                    }
                } );

                if ( protocolEnumName == null || protocolEnumName.isEmpty() ) {
                    throw new IllegalArgumentException( "Protocol name == null or \"\"" );
                }

                this.switchProtocol( Protocols.valueOf( protocolEnumName.toUpperCase() ) );
                break;

            default:
                throw new UnsupportedOperationException( "Not implemented yet." );
        }

        final ExecuteResult result = new ExecuteResult( Collections.singletonList( MetaResultSet.count( statement.getStatementHandle().connectionId, statement.getStatementHandle().id, 0 ) ) );
        LOGGER.trace( "prepareAndExecuteSetOption( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, callback: {} ) = {}", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback, result );
        return result;
    }


    private synchronized void switchProtocol( Protocol newProtocol ) {
        LOGGER.info( "New Protocol {} -- Old Protocol {}", newProtocol, protocol );
        this.protocol = newProtocol;
    }


    private ExecuteResult prepareAndExecuteDataManipulation( final ConnectionInfos connection, final TransactionInfos transaction, final StatementInfos statement, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final PrepareCallback callback ) {
        LOGGER.debug( "prepareAndExecuteDataManipulation(connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, callback: {} )", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );

        try {
            final ExecuteResult result = executionDurationTimer.recordCallable(
                    () -> protocol.prepareAndExecuteDataManipulation( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback )
            );

            LOGGER.trace( "prepareAndExecuteDataManipulation( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, callback: {} ) = {}", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback, result );
            return result;
        } catch ( RemoteException ex ) {
            throw Utils.wrapException( ex );
        } catch ( Exception ex ) {
            throw Utils.wrapException( ex );
        }
    }


    private ExecuteResult prepareAndExecuteDataQuery( final ConnectionInfos connection, final TransactionInfos transaction, final StatementInfos statement, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final PrepareCallback callback ) {
        LOGGER.debug( "prepareAndExecuteDataQuery( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, callback: {} )", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );

        try {
            final ExecuteResult result = executionDurationTimer.recordCallable(
                    () -> protocol.prepareAndExecuteDataQuery( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback )
            );

            LOGGER.trace( "prepareAndExecuteDataQuery( connection: {}, transaction: {}, statement: {}, sql: {}, maxRowCount: {}, maxRowsInFirstFrame: {}, callback: {} ) = {}", connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback, result );
            return result;
        } catch ( RemoteException ex ) {
            throw Utils.wrapException( ex );
        } catch ( Exception ex ) {
            throw Utils.wrapException( ex );
        }
    }


    @Override
    public ExecuteBatchResult prepareAndExecuteBatch( StatementHandle h, List<String> sqlCommands ) throws NoSuchStatementException {
        throw new UnsupportedOperationException( "Not supported yet." );
    }


    /**
     * @deprecated Deprecated by Apache Calcite Avatica
     */
    @Override
    @Deprecated
    public ExecuteResult execute( StatementHandle h, List<TypedValue> parameterValues, long maxRowCount ) throws NoSuchStatementException {
        return execute( h, parameterValues, AvaticaUtils.toSaturatedInt( maxRowCount ) );
    }


    @Override
    public ExecuteResult execute( final StatementHandle statementHandle, final List<TypedValue> parameterValues, final int maxRowsInFirstFrame ) throws NoSuchStatementException {
        LOGGER.trace( "execute( statementHandle: {}, parameterValues: {}, maxRowsInFirstFrame: {} )", statementHandle, parameterValues, maxRowsInFirstFrame );

        final List<Common.TypedValue> serializedParameterValues = new LinkedList<>();
        for ( TypedValue value : parameterValues ) {
            serializedParameterValues.add( value.toProto() );
        }

        final ExecuteResult result = this.executeProtobuf( statementHandle, serializedParameterValues, maxRowsInFirstFrame );

        LOGGER.trace( "execute( statementHandle: {}, parameterValues: {}, maxRowsInFirstFrame: {} ) = {}", statementHandle, parameterValues, maxRowsInFirstFrame, result );
        return result;
    }


    public ExecuteResult executeProtobuf( final StatementHandle statementHandle, final List<Common.TypedValue> parameterValues, final int maxRowsInFirstFrame ) throws NoSuchStatementException {
        LOGGER.trace( "executeProtobuf( statementHandle: {}, parameterValues: {}, maxRowsInFirstFrame: {} )", statementHandle, parameterValues, maxRowsInFirstFrame );
        final ConnectionInfos connection = getConnection( statementHandle );
        final TransactionInfos transaction = connection.getOrStartTransaction();
        final StatementInfos statement = getStatement( statementHandle );

        try {
            final ExecuteResult result = executionDurationTimer.recordCallable(
                    () -> protocol.execute( connection, transaction, statement, parameterValues, maxRowsInFirstFrame )
            );

            LOGGER.trace( "executeProtobuf( statementHandle: {}, parameterValues: {}, maxRowsInFirstFrame: {} ) = {}", statementHandle, parameterValues, maxRowsInFirstFrame, result );
            return result;
        } catch ( NoSuchStatementException ex ) {
            throw ex;
        } catch ( RemoteException ex ) {
            throw Utils.wrapException( ex );
        } catch ( Exception ex ) {
            throw Utils.wrapException( ex );
        }
    }


    @Override
    public ExecuteBatchResult executeBatch( final StatementHandle statementHandle, final List<List<TypedValue>> parameterValues ) throws NoSuchStatementException {
        LOGGER.trace( "executeBatch( statementHandle: {}, parameterValues: {} )", statementHandle, parameterValues );

        final List<UpdateBatch> serializedParameterValues = new LinkedList<>();
        for ( List<TypedValue> batch : parameterValues ) {
            final List<Common.TypedValue> valuesList = new LinkedList<>();
            for ( TypedValue value : batch ) {
                valuesList.add( value.toProto() );
            }
            serializedParameterValues.add( UpdateBatch.newBuilder().addAllParameterValues( valuesList ).build() );
        }

        final ExecuteBatchResult result = this.executeBatchProtobuf( statementHandle, serializedParameterValues );

        LOGGER.trace( "executeBatch( statementHandle: {}, parameterValues: {} ) = {}", statementHandle, parameterValues, result );
        return result;
    }


    @Override
    public ExecuteBatchResult executeBatchProtobuf( final StatementHandle statementHandle, final List<UpdateBatch> parameterValues ) throws NoSuchStatementException {
        LOGGER.trace( "executeBatch( statementHandle: {}, parameterValues: {} )", statementHandle, parameterValues );
        final ConnectionInfos connection = getConnection( statementHandle );
        final TransactionInfos transaction = connection.getOrStartTransaction();
        final StatementInfos statement = getStatement( statementHandle );

        try {
            final ResultSetInfos resultSet = executionDurationTimer.recordCallable(
                    () -> protocol.executeBatch( connection, transaction, statement, parameterValues )
            );

            LOGGER.trace( "prepareAndExecute( statementHandle: {}, parameterValues: {} ) = {}", statementHandle, parameterValues, resultSet );
            return resultSet.getExecuteResult();
        } catch ( NoSuchStatementException ex ) {
            throw ex;
        } catch ( RemoteException ex ) {
            throw Utils.wrapException( ex );
        } catch ( Exception ex ) {
            throw Utils.wrapException( ex );
        }
    }


    @Override
    public Frame fetch( final StatementHandle statementHandle, final long offset, final int fetchMaxRowCount ) throws NoSuchStatementException, MissingResultsException {
        LOGGER.trace( "fetch( statementHandle: {}, offset: {}, fetchMaxRowCount: {} )", statementHandle, offset, fetchMaxRowCount );

        final ConnectionInfos connection = getConnection( statementHandle );
        final StatementInfos statement = getStatement( statementHandle );

        final ResultSetInfos resultSet = statement.getResultSet();

        try {
            final Frame result = fetchDurationTimer.recordCallable(
                    () -> {
                        if ( resultSet == null ) {
                            return protocol.fetch( connection, statementHandle, offset, fetchMaxRowCount );
                        } else {
                            return resultSet.fetch( connection, statement, offset, fetchMaxRowCount );
                        }
                    }
            );

            LOGGER.trace( "fetch( statementHandle: {}, offset: {}, fetchMaxRowCount: {} ) = {}", statementHandle, offset, fetchMaxRowCount, result );
            return result;
        } catch ( NoSuchStatementException ex ) {
            throw ex;
        } catch ( RemoteException ex ) {
            throw Utils.wrapException( ex );
        } catch ( Exception ex ) {
            throw Utils.wrapException( ex );
        }
    }


    /**
     * Creates a statement; called by java.sql.Connection.createStatement().
     *
     * The StatementInfos object created represents the java.sql.Statement object created in the App connected to this Middleware.
     *
     * <code>createStatement()</code> *does not* create statements on the connections to the actual underlying databases.
     * Instead, the first statement being executed (e.g., <code>prepareAndExecute</code>) on a certain node creates the necessary statement.
     * This lazy approach prevents the creation of statements at databases which are not used during the transaction
     * at the cost of an increased overhead for every new statement execution (e.g., <code>prepareAndExecute</code>) at a node.
     * (If the App re-uses the statement object for another execution, this middleware will also re-use the created statement.)
     */
    @Override
    public StatementHandle createStatement( final ConnectionHandle connectionHandle ) {
        LOGGER.trace( "createStatement( connectionHandle: {} )", connectionHandle );

        final StatementHandle result;
        synchronized ( openConnections ) {
            StatementInfos si = getConnection( connectionHandle ).createStatement();
            openStatements.put( si.getStatementHandle().toString(), si );
            result = si.getStatementHandle();
        }

        LOGGER.trace( "createStatement( connectionHandle: {} ) = {}", connectionHandle, result );

        return result;
    }


    private StatementInfos getStatement( final StatementHandle statementHandle ) throws NoSuchStatementException {
        LOGGER.trace( "getStatement( statementHandle: {} )", statementHandle );

        final StatementInfos result;
        synchronized ( openStatements ) {
            result = openStatements.get( statementHandle.toString() );
        }
        if ( null == result ) {
            throw new NoSuchStatementException( statementHandle );
        }

        LOGGER.trace( "getStatement( statementHandle: {} ) = {}", statementHandle, result );

        return result;
    }


    @Override
    public void closeStatement( final StatementHandle statementHandle ) {
        LOGGER.trace( "getStatement( statementHandle: {} )", statementHandle );
        final ConnectionInfos connection = getConnection( statementHandle );

        final StatementInfos statement;
        synchronized ( openStatements ) {
            statement = openStatements.remove( statementHandle.toString() );
            if ( statement == null ) {
                LOGGER.debug( "`close()` on unknown statement {}", statementHandle );
                return;
            }
        }

        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "Closing statement {}", statementHandle );
        }

        // TODO: close remote statements and result sets
        //throw new UnsupportedOperationException( "Not supported yet." );

        try {
            final boolean success = closeStatementDurationTimer.recordCallable( () -> {
                protocol.closeStatement( connection, statement );
                return true;
            } );
        } catch ( RemoteException ex ) {
            throw Utils.wrapException( ex );
        } catch ( Exception ex ) {
            throw Utils.wrapException( ex );
        }
    }


    /**
     * Registers a physical connection which has already been opened by Avatica due to java.sql.DriverManager.getConnection().
     *
     * The ConnectionInfos object created represents the connection between the App and this Middleware.
     * These connections are typically opened by an App using the JDBC driver.
     *
     * <code>openConnection()</code> *does not* open connections from the middleware to the actual underlying databases.
     * Instead, the first statement being executed (e.g., <code>prepareAndExecute</code>) on a certain node creates the necessary connection.
     * This lazy approach prevents the opening of connections to databases which are not used during the transaction
     * at the cost of an increased overhead for every first statement execution (e.g., <code>prepareAndExecute</code>) at a node.
     */
    @Override
    public void openConnection( final ConnectionHandle connectionHandle, final Map<String, String> info ) {
        LOGGER.trace( "openConnection( connectionHandle: {}, info: {})", connectionHandle, info );

        synchronized ( openConnections ) {
            if ( openConnections.containsKey( connectionHandle.id ) ) {
                LOGGER.warn( "Connection {} already exists.", connectionHandle );
                throw new ConnectionAlreadyExistsException( connectionHandle.id );
            }

            // TODO: check user-password
            final String username = info == null ? ANONYMOUS_USERNAME : info.getOrDefault( "user", ANONYMOUS_USERNAME );
            UUID userId = null;
            if ( username == null ) {
                throw new NullPointerException( "username == null" );
            }
            if ( username.isEmpty() ) {
                LOGGER.debug( "No user specified. Using \"" + ANONYMOUS_USERNAME + "\"" );
                userId = Utils.USER_ANONYMOUS_UUID;
            }
            if ( username.equalsIgnoreCase( ANONYMOUS_USERNAME ) ) {
                userId = Utils.USER_ANONYMOUS_UUID;
            }
            if ( username.equalsIgnoreCase( DATABASE_ADMIN_USERNAME ) ) {
                userId = Utils.USER_PA_UUID;
            }

            LOGGER.debug( "User {}:{} successfully logged in.", username, userId );

            ConnectionInfos connection = new ConnectionInfos( this.nodeId, userId, connectionHandle );
            openConnections.put( connectionHandle.id, connection );

            LOGGER.debug( "Number of open connections: {}.", openConnections.size() );
        }
    }


    private ConnectionInfos getConnection( final ConnectionHandle connectionHandle ) {
        LOGGER.trace( "getConnection( connectionHandle: {} )", connectionHandle );

        final ConnectionInfos result;
        synchronized ( openConnections ) {
            ConnectionInfos connectionInfos = openConnections.get( connectionHandle.id );
            if ( connectionInfos == null ) {
                LOGGER.debug( "Connection {} does not exist.", connectionHandle );
                throw new NoSuchConnectionException( connectionHandle.id );
            }
            result = connectionInfos;
        }

        LOGGER.trace( "getConnection( connectionHandle: {} ) = {}", connectionHandle, result );
        return result;
    }


    private ConnectionInfos getConnection( final StatementHandle statementHandle ) {
        LOGGER.trace( "getConnection( statementHandle: {} )", statementHandle );

        final ConnectionInfos result;
        synchronized ( openConnections ) {
            ConnectionInfos connectionInfos = openConnections.get( statementHandle.connectionId );
            if ( connectionInfos == null ) {
                LOGGER.debug( "Connection {} does not exist.", statementHandle.connectionId );
                throw new NoSuchConnectionException( statementHandle.connectionId );
            }
            result = connectionInfos;
        }

        LOGGER.trace( "getConnection( statementHandle: {} ) = {}", statementHandle, result );
        return result;
    }


    @Override
    public void closeConnection( final ConnectionHandle connectionHandle ) {
        LOGGER.trace( "closeConnection( connectionHandle: {} )", connectionHandle );

        final ConnectionInfos connection;

        synchronized ( openConnections ) {
            connection = openConnections.remove( connectionHandle.id );
            if ( connection == null ) {
                LOGGER.debug( "`close()` on unknown connection {}", connectionHandle );
                return;
            }

            LOGGER.debug( "Number of remaining open connections: {}", openConnections.size() );
        }

        LOGGER.trace( "Closing connection {}", connectionHandle );

        try {
            final boolean success = closeConnectionDurationTimer.recordCallable( () -> {
                protocol.closeConnection( connection );
                return true;
            } );
        } catch ( RemoteException ex ) {
            throw Utils.wrapException( ex );
        } catch ( Exception ex ) {
            throw Utils.wrapException( ex );
        }
    }


    @Override
    public void commit( ConnectionHandle connectionHandle ) {
        LOGGER.trace( "commit( connectionHandle: {} )", connectionHandle );

        final ConnectionInfos connection = getConnection( connectionHandle );
        final TransactionInfos transaction = connection.getTransaction();

        if ( transaction == null ) {
            // null == there is no transaction currently running => NOOP
            return;
        }

        this.commit( getConnection( connectionHandle ), transaction );
    }


    private void commit( final ConnectionInfos connection, final TransactionInfos transaction ) {
        LOGGER.trace( "commit( connection: {}, transaction: {} )", connection, transaction );

        try {
            final boolean success = commitDurationTimer.recordCallable( () -> {
                protocol.commit( connection, transaction );
                return true;
            } );

            if ( success ) {
                connection.endTransaction();
            }
        } catch ( RemoteException ex ) {
            throw Utils.wrapException( ex );
        } catch ( Exception ex ) {
            throw Utils.wrapException( ex );
        }
    }


    @Override
    public void rollback( ConnectionHandle connectionHandle ) {
        LOGGER.trace( "rollback( connectionHandle: {} )", connectionHandle );

        final ConnectionInfos connection = getConnection( connectionHandle );
        final TransactionInfos transaction = connection.getTransaction();

        if ( transaction == null ) {
            // null == there is no transaction currently running => NOOP
            return;
        }

        this.rollback( connection, transaction );
    }


    private void rollback( final ConnectionInfos connection, final TransactionInfos transaction ) {
        LOGGER.trace( "rollback( connection: {}, transaction: {} )", connection, transaction );

        try {
            final boolean success = rollbackDurationTimer.recordCallable( () -> {
                protocol.rollback( connection, transaction );
                return true;
            } );

            if ( success ) {
                connection.endTransaction();
            }
        } catch ( RemoteException ex ) {
            throw Utils.wrapException( ex );
        } catch ( Exception ex ) {
            throw Utils.wrapException( ex );
        }

        LOGGER.warn( "commit( rollback: {}, transaction: {} ) SUCCESS", connection, transaction );
    }


    @Override
    public ConnectionProperties connectionSync( final ConnectionHandle connectionHandle, final ConnectionProperties newConnectionProperties ) {
        LOGGER.trace( "connectionSync( connectionHandle: {}, newConnectionProperties: {} )", connectionHandle, newConnectionProperties );

        final ConnectionProperties result;
        synchronized ( openConnections ) {
            result = openConnections.compute( connectionHandle.toString(), ( __, connectionInfos ) -> {
                if ( connectionInfos == null ) {
                    throw Utils.wrapException( new RuntimeException( "Connection does not exist." ) );
                }
                final ConnectionInfos connection = connectionInfos.merge( newConnectionProperties );
                if ( connection.isDirty() ) {
                    applyConnectionSettings( connectionHandle, connection.getConnectionProperties() );
                    try {
                        final ConnectionProperties resultFromNetwork = connectionSyncDurationTimer.recordCallable( () ->
                                protocol.connectionSync( connection, connection.getConnectionProperties() )
                        );
                    } catch ( RemoteException ex ) {
                        LOGGER.warn( "Exception while synchronizing the connection properties.", ex );
                    } catch ( Exception ex ) {
                        LOGGER.warn( "Exception while synchronizing the connection properties.", ex );
                    }
                    connection.clearDirty();
                }
                return connection;
            } ).getConnectionProperties();
        }

        LOGGER.trace( "connectionSync( connectionHandle: {}, newConnectionProperties: {} ) = {}", connectionHandle, newConnectionProperties, result );

        return result;
    }


    protected void applyConnectionSettings( ConnectionHandle connectionHandle, ConnectionProperties properties ) {
        LOGGER.trace( "applyConnectionSettings( connectionHandle: {}, properties: {}", connectionHandle, properties );

        // TODO: apply on all "sub-connections"
        if ( properties.isAutoCommit() != null ) {
            LOGGER.trace( "applyConnectionSettings() -- New value for AutoCommit: {}", properties.isAutoCommit() );
        }
        if ( properties.isReadOnly() != null ) {
            LOGGER.trace( "applyConnectionSettings() -- New value for ReadOnly: {}", properties.isReadOnly() );
        }
        if ( properties.getTransactionIsolation() != null ) {
            LOGGER.trace( "applyConnectionSettings() -- New value for TransactionIsolation: {}", properties.getTransactionIsolation() );
        }
        if ( properties.getCatalog() != null ) {
            LOGGER.trace( "applyConnectionSettings() -- New value for Catalog: {}", properties.getCatalog() );
        }
        if ( properties.getSchema() != null ) {
            LOGGER.trace( "applyConnectionSettings() -- New value for Schema: {}", properties.getSchema() );
        }
    }
}
