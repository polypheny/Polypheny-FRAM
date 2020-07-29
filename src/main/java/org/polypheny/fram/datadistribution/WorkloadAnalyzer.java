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
import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.TreeSet;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.Meta.ConnectionProperties;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.apache.calcite.avatica.Meta.Pat;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.proto.Common.TypedValue;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.polypheny.fram.datadistribution.Transaction.Operation;
import org.polypheny.fram.protocols.AbstractProtocol;
import org.polypheny.fram.protocols.Protocol;
import org.polypheny.fram.remote.AbstractRemoteNode;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.standalone.CatalogUtils;
import org.polypheny.fram.standalone.ConnectionInfos;
import org.polypheny.fram.standalone.Main;
import org.polypheny.fram.standalone.ResultSetInfos;
import org.polypheny.fram.standalone.ResultSetInfos.BatchResultSetInfos;
import org.polypheny.fram.standalone.ResultSetInfos.QueryResultSet;
import org.polypheny.fram.standalone.StatementInfos;
import org.polypheny.fram.standalone.StatementInfos.PreparedStatementInfos;
import org.polypheny.fram.standalone.TransactionInfos;
import org.polypheny.fram.standalone.parser.sql.SqlNodeUtils;


public class WorkloadAnalyzer implements Protocol {

    public static final String GET_GENERATED_KEYS_CONFIGURATION_OPTION_RETURN_PRIMARY_KEY = "RETURN_PRIMARY_KEY";
    public static final String GET_GENERATED_KEYS_CONFIGURATION_OPTION_RETURN_COLUMN_INDEXES = "RETURN_GENERATED_KEYS_COL_INDEXES";

    private final boolean rpkDML;
    private final boolean rpkDQL;
    private final boolean rpkNoPrimaryKey;

    private AbstractProtocol down;


    public WorkloadAnalyzer() {
        this( null );
    }


    public WorkloadAnalyzer( final AbstractProtocol protocol ) {
        final com.typesafe.config.Config configuration = Main.configuration();

        rpkDML = configuration.getString( "standalone.datastore.getGeneratedKeys.return_primary_key_for_dml" ).equalsIgnoreCase( GET_GENERATED_KEYS_CONFIGURATION_OPTION_RETURN_PRIMARY_KEY );
        rpkDQL = configuration.getString( "standalone.datastore.getGeneratedKeys.return_primary_key_for_dql" ).equalsIgnoreCase( GET_GENERATED_KEYS_CONFIGURATION_OPTION_RETURN_PRIMARY_KEY );
        rpkNoPrimaryKey = configuration.getString( "standalone.datastore.getGeneratedKeys.tables_without_primary_key" ).equalsIgnoreCase( GET_GENERATED_KEYS_CONFIGURATION_OPTION_RETURN_PRIMARY_KEY );

        this.down = protocol;
    }


    @Override
    public Protocol setUp( Protocol protocol ) {
        throw new IllegalStateException( "You cannot have a protocol before the workload analyzer" );
    }


    @Override
    public Protocol setDown( final Protocol protocol ) {
        if ( protocol instanceof AbstractProtocol ) {
            return this.down = (AbstractProtocol) protocol;
        } else {
            throw new IllegalArgumentException( "An instance of AbstractProtocol is required." );
        }
    }


    @Override
    public ConnectionProperties connectionSync( ConnectionInfos connection, ConnectionProperties newConnectionProperties ) throws RemoteException {
        return down.connectionSync( connection, newConnectionProperties );
    }


    @Override
    public ResultSetInfos prepareAndExecuteDataDefinition( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        return down.prepareAndExecuteDataDefinition( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
    }


    @Override
    public ResultSetInfos prepareAndExecuteDataManipulation( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {

        final SqlIdentifier targetTable;
        switch ( sql.getKind() ) {
            // See org.apache.calcite.sql.SqlKind.DML
            case INSERT:
                targetTable = SqlNodeUtils.INSERT_UTILS.getTargetTable( (SqlInsert) sql );
                break;

            case DELETE:
                targetTable = SqlNodeUtils.DELETE_UTILS.getTargetTable( (SqlDelete) sql );
                break;

            case UPDATE:
                targetTable = SqlNodeUtils.UPDATE_UTILS.getTargetTable( (SqlUpdate) sql );
                break;

            case MERGE:
            case PROCEDURE_CALL:
            default:
                throw new UnsupportedOperationException( "Not supported" );
        }

        final String catalogName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 2 );
        final String schemaName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 1 );
        final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 0 );

        final ResultSetInfos executeResult;
        if ( rpkDML && rpkNoPrimaryKey ) {
            // best case: we can use RETURN_PRIMARY_KEY even if the table does not contain a primary key (then the values of all columns are returned)
            executeResult = down.prepareAndExecuteDataManipulation( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
        } else {
            // we need to check if the table has a primary key
            final Map<String, Integer> primaryKeyColumnNamesAndIndexes = Collections.unmodifiableMap( CatalogUtils.lookupPrimaryKeyColumnNamesAndIndexes( connection, catalogName, schemaName, tableName ) );
            final boolean tableHasPrimaryKeys = !primaryKeyColumnNamesAndIndexes.isEmpty();

            if ( rpkDML && tableHasPrimaryKeys ) {
                // the table has a primary key and we can use RETURN_PRIMARY_KEY
                executeResult = down.prepareAndExecuteDataManipulation( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
            } else {
                // we cannot use RETURN_PRIMARY_KEY and thus we need to request the return of columns using an int array
                final int[] columnIndexes;
                if ( !rpkDML && tableHasPrimaryKeys ) {
                    // The table has a primary key but we cannot use the RETURN_PRIMARY_KEY function,
                    // so we use the execute(String, int[]) function to get the values of the primary key
                    columnIndexes = new int[primaryKeyColumnNamesAndIndexes.values().size()];
                    int i = 0;
                    for ( int index : new TreeSet<>( primaryKeyColumnNamesAndIndexes.values() ) ) {
                        columnIndexes[i++] = index + 1; // convert the "array" index to a "sql" index
                    }
                } else {
                    // The table has no primary key and we cannot use the RETURN_PRIMARY_KEY function,
                    // so we get all columns and want all of them returned using the execute(String, int[]) function
                    final MetaResultSet columns = connection.getCatalog().getColumns( connection, catalogName, Pat.of( schemaName ), Pat.of( tableName ), Pat.of( null ) );
                    int numberOfColumns = 0;
                    for ( final Object row : columns.firstFrame.rows ) {
                        ++numberOfColumns;
                    }
                    columnIndexes = new int[numberOfColumns];
                    for ( int ci = 0; ci < columnIndexes.length; ++ci ) {
                        columnIndexes[ci] = ci;
                    }
                }

                executeResult = down.prepareAndExecuteDataManipulation( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, columnIndexes, callback );
            }
        }

        if ( !(executeResult instanceof QueryResultSet) ) {
            throw new IllegalStateException( "Unknown ResultSetInfos implementation" );
        }

        final Map<AbstractRemoteNode, RemoteExecuteResult> origins = ((QueryResultSet) executeResult).getOrigins();
        final Transaction wlTrx = Transaction.getTransaction( transaction.getTransactionId() );

        for ( Entry<AbstractRemoteNode, RemoteExecuteResult> origin : origins.entrySet() ) {
            final AbstractRemoteNode src = origin.getKey();
            final List<MetaResultSet> resultSets = origin.getValue().toExecuteResult().resultSets;

            switch ( resultSets.size() ) {
                case 0:
                    throw new IllegalStateException( "List of resultSets returned by " + src + " is empty!" );

                case 1:
                    throw new IllegalStateException( "List of resultSets returned by " + src + " contains only one result set! The retrieval of the accessed keys must have failed!" );

                case 2:
                    break; // intentional

                default:
                    throw new UnsupportedOperationException( "List of resultSets returned by " + src + " contains more than two result sets! The return of multiple data(?) results is currently not supported!" );
            }

            final long updateCount = resultSets.get( 0 ).updateCount;
            final MetaResultSet accessedKeys = resultSets.get( 1 );

            try {
                final ResultSet apks = new AvaticaResultSet( null, null, accessedKeys.signature, new AvaticaResultSetMetaData( null, null, accessedKeys.signature ), TimeZone.getDefault(), accessedKeys.firstFrame )
                        .execute2( MetaImpl.createCursor( CursorFactory.ARRAY, accessedKeys.firstFrame.rows ), accessedKeys.signature.columns );

                final int numberOfColumns = apks.getMetaData().getColumnCount();

                int processedRows = 0;
                for ( ; apks.next(); ++processedRows ) {
                    final Serializable[] primaryKeyValues = new Serializable[numberOfColumns];

                    for ( int columnIndex = 0; columnIndex < numberOfColumns; ++columnIndex ) {
                        primaryKeyValues[columnIndex] = (Serializable) apks.getObject( columnIndex + 1 );
                    }

                    // add the requested record to the transaction
                    wlTrx.addAction( Operation.WRITE, new RecordIdentifier( catalogName, schemaName, tableName, primaryKeyValues ) );
                }

                if ( processedRows != updateCount ) {
                    throw new AssertionError( "Processed rows (" + processedRows + ") != update count (" + updateCount + ")" );
                }
            } catch ( SQLException ex ) {
                throw new RuntimeException( "Internal Exception while converting the returned MetaResultSet into a JDBC ResultSet.", ex );
            }
        }

        return executeResult;
    }


    @Override
    public ResultSetInfos prepareAndExecuteDataQuery( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {

        // more than one table and especially the required join is currently not supported!
        final SqlIdentifier targetTable;
        switch ( sql.getKind() ) {
            // See org.apache.calcite.sql.SqlKind.QUERY
            case SELECT:
                targetTable = SqlNodeUtils.SELECT_UTILS.getTargetTable( (SqlSelect) sql );
                break;

            case UNION:
            case INTERSECT:
            case EXCEPT:
            case VALUES:
            case WITH:
            case ORDER_BY:
            case EXPLICIT_TABLE:
            default:
                throw new UnsupportedOperationException( "Not supported" );
        }

        final String catalogName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 2 );
        final String schemaName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 1 );
        final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 0 );

        final ResultSetInfos executeResult;
        if ( rpkDQL && rpkNoPrimaryKey ) {
            // best case: we can use RETURN_PRIMARY_KEY even if the table does not contain a primary key (then the values of all columns are returned)
            executeResult = down.prepareAndExecuteDataQuery( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
        } else {
            // we need to check if the table has a primary key
            final Map<String, Integer> primaryKeyColumnNamesAndIndexes = Collections.unmodifiableMap( CatalogUtils.lookupPrimaryKeyColumnNamesAndIndexes( connection, catalogName, schemaName, tableName ) );
            final boolean tableHasPrimaryKeys = !primaryKeyColumnNamesAndIndexes.isEmpty();

            if ( rpkDQL && tableHasPrimaryKeys ) {
                // the table has a primary key and we can use RETURN_PRIMARY_KEY
                executeResult = down.prepareAndExecuteDataQuery( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
            } else {
                // we cannot use RETURN_PRIMARY_KEY and thus we need to request the return of columns using an int array
                final int[] columnIndexes;
                if ( !rpkDQL && tableHasPrimaryKeys ) {
                    // The table has a primary key but we cannot use the RETURN_PRIMARY_KEY function,
                    // so we use the execute(String, int[]) function to get the values of the primary key
                    columnIndexes = new int[primaryKeyColumnNamesAndIndexes.values().size()];
                    int i = 0;
                    for ( int index : new TreeSet<>( primaryKeyColumnNamesAndIndexes.values() ) ) {
                        columnIndexes[i++] = index + 1; // convert the "array" index to a "sql" index
                    }
                } else {
                    // The table has no primary key and we cannot use the RETURN_PRIMARY_KEY function,
                    // so we get all columns and want all of them returned using the execute(String, int[]) function
                    final MetaResultSet columns = connection.getCatalog().getColumns( connection, catalogName, Pat.of( schemaName ), Pat.of( tableName ), Pat.of( null ) );
                    int numberOfColumns = 0;
                    for ( final Object row : columns.firstFrame.rows ) {
                        ++numberOfColumns;
                    }
                    columnIndexes = new int[numberOfColumns];
                    for ( int ci = 0; ci < columnIndexes.length; ++ci ) {
                        columnIndexes[ci] = ci;
                    }
                }

                executeResult = down.prepareAndExecuteDataQuery( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, columnIndexes, callback );
            }
        }

        if ( !(executeResult instanceof QueryResultSet) ) {
            throw new IllegalStateException( "Unknown ResultSetInfos implementation" );
        }

        final Map<AbstractRemoteNode, RemoteExecuteResult> origins = ((QueryResultSet) executeResult).getOrigins();
        final Transaction wlTrx = Transaction.getTransaction( transaction.getTransactionId() );

        for ( Entry<AbstractRemoteNode, RemoteExecuteResult> origin : origins.entrySet() ) {
            final AbstractRemoteNode src = origin.getKey();
            final List<MetaResultSet> resultSets = origin.getValue().toExecuteResult().resultSets;

            switch ( resultSets.size() ) {
                case 0:
                    throw new IllegalStateException( "List of resultSets returned by " + src + " is empty!" );

                case 1:
                    throw new IllegalStateException( "List of resultSets returned by " + src + " contains only one result set! The retrieval of the accessed keys must have failed!" );

                case 2:
                    break; // intentional

                default:
                    throw new UnsupportedOperationException( "List of resultSets returned by " + src + " contains more than two result sets! The return of multiple data(?) results is currently not supported!" );
            }

            final MetaResultSet accessedKeys = resultSets.get( 1 );

            try {
                final ResultSet apks = new AvaticaResultSet( null, null, accessedKeys.signature, new AvaticaResultSetMetaData( null, null, accessedKeys.signature ), TimeZone.getDefault(), accessedKeys.firstFrame )
                        .execute2( MetaImpl.createCursor( CursorFactory.ARRAY, accessedKeys.firstFrame.rows ), accessedKeys.signature.columns );

                final int numberOfColumns = apks.getMetaData().getColumnCount();

                int processedRows = 0;
                for ( ; apks.next(); ++processedRows ) {
                    final Serializable[] primaryKeyValues = new Serializable[numberOfColumns];

                    for ( int columnIndex = 0; columnIndex < numberOfColumns; ++columnIndex ) {
                        primaryKeyValues[columnIndex] = (Serializable) apks.getObject( columnIndex + 1 );
                    }

                    // add the requested record to the transaction
                    wlTrx.addAction( Operation.READ, new RecordIdentifier( catalogName, schemaName, tableName, primaryKeyValues ) );
                }
            } catch ( SQLException ex ) {
                throw new RuntimeException( "Internal Exception while converting the returned MetaResultSet into a JDBC ResultSet.", ex );
            }
        }

        return executeResult;
    }


    @Override
    public ResultSetInfos prepareAndExecuteTransactionCommit( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        try {
            return down.prepareAndExecuteTransactionCommit( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
        } finally {
            final Transaction t = Transaction.getTransaction( transaction.getTransactionId() );
            Workload.THIS_IS_A_TEST_REMOVE_ME.addTransaction( t );
            Transaction.removeTransaction( t );
        }
    }


    @Override
    public ResultSetInfos prepareAndExecuteTransactionRollback( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        try {
            return down.prepareAndExecuteTransactionRollback( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
        } finally {
            Transaction.removeTransaction( Transaction.getTransaction( transaction.getTransactionId() ) );
        }
    }


    @Override
    public StatementInfos prepareDataManipulation( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {

        final SqlIdentifier targetTable;
        switch ( sql.getKind() ) {
            // See org.apache.calcite.sql.SqlKind.DML
            case INSERT:
                targetTable = SqlNodeUtils.INSERT_UTILS.getTargetTable( (SqlInsert) sql );
                break;

            case DELETE:
                targetTable = SqlNodeUtils.DELETE_UTILS.getTargetTable( (SqlDelete) sql );
                break;

            case UPDATE:
                targetTable = SqlNodeUtils.UPDATE_UTILS.getTargetTable( (SqlUpdate) sql );
                break;

            case MERGE:
            case PROCEDURE_CALL:
            default:
                throw new UnsupportedOperationException( "Not supported" );
        }

        final String catalogName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 2 );
        final String schemaName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 1 );
        final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 0 );

        final PreparedStatementInfos preparedStatement;
        if ( rpkDML && rpkNoPrimaryKey ) {
            // best case: we can use RETURN_PRIMARY_KEY even if the table does not contain a primary key (then the values of all columns are returned)
            preparedStatement = (PreparedStatementInfos) down.prepareDataManipulation( connection, statement, sql, maxRowCount );
        } else {
            // we need to check if the table has a primary key
            final Map<String, Integer> primaryKeyColumnNamesAndIndexes = Collections.unmodifiableMap( CatalogUtils.lookupPrimaryKeyColumnNamesAndIndexes( connection, catalogName, schemaName, tableName ) );
            final boolean tableHasPrimaryKeys = !primaryKeyColumnNamesAndIndexes.isEmpty();

            if ( rpkDML && tableHasPrimaryKeys ) {
                // the table has a primary key and we can use RETURN_PRIMARY_KEY
                preparedStatement = (PreparedStatementInfos) down.prepareDataManipulation( connection, statement, sql, maxRowCount );
            } else {
                // we cannot use RETURN_PRIMARY_KEY and thus we need to request the return of columns using an int array
                final int[] columnIndexes;
                if ( !rpkDML && tableHasPrimaryKeys ) {
                    // The table has a primary key but we cannot use the RETURN_PRIMARY_KEY function,
                    // so we use the execute(String, int[]) function to get the values of the primary key
                    columnIndexes = new int[primaryKeyColumnNamesAndIndexes.values().size()];
                    int i = 0;
                    for ( int index : new TreeSet<>( primaryKeyColumnNamesAndIndexes.values() ) ) {
                        columnIndexes[i++] = index + 1; // convert the "array" index to a "sql" index
                    }
                } else {
                    // The table has no primary key and we cannot use the RETURN_PRIMARY_KEY function,
                    // so we get all columns and want all of them returned using the execute(String, int[]) function
                    final MetaResultSet columns = connection.getCatalog().getColumns( connection, catalogName, Pat.of( schemaName ), Pat.of( tableName ), Pat.of( null ) );
                    int numberOfColumns = 0;
                    for ( final Object row : columns.firstFrame.rows ) {
                        ++numberOfColumns;
                    }
                    columnIndexes = new int[numberOfColumns];
                    for ( int ci = 0; ci < columnIndexes.length; ++ci ) {
                        columnIndexes[ci] = ci;
                    }
                }

                preparedStatement = (PreparedStatementInfos) down.prepareDataManipulation( connection, statement, sql, maxRowCount, columnIndexes );
            }
        }

        preparedStatement.setWorkloadAnalysisFunction( ( _connection, _transaction, _statement, _values, _result ) -> {

            if ( _result instanceof QueryResultSet ) {
                final Map<AbstractRemoteNode, RemoteExecuteResult> origins = ((QueryResultSet) _result).getOrigins();
                final Transaction wlTrx = Transaction.getTransaction( _transaction.getTransactionId() );

                for ( Entry<AbstractRemoteNode, RemoteExecuteResult> origin : origins.entrySet() ) {
                    final AbstractRemoteNode src = origin.getKey();
                    final List<MetaResultSet> resultSets = origin.getValue().toExecuteResult().resultSets;

                    switch ( resultSets.size() ) {
                        case 0:
                            throw new IllegalStateException( "List of resultSets returned by " + src + " is empty!" );

                        case 1:
                            throw new IllegalStateException( "List of resultSets returned by " + src + " contains only one result set! The retrieval of the accessed keys must have failed!" );

                        case 2:
                            break; // intentional

                        default:
                            throw new UnsupportedOperationException( "List of resultSets returned by " + src + " contains more than two result sets! The return of multiple data(?) results is currently not supported!" );
                    }

                    final long updateCount = resultSets.get( 0 ).updateCount;
                    final MetaResultSet accessedKeys = resultSets.get( 1 );

                    try {
                        final ResultSet apks = new AvaticaResultSet( null, null, accessedKeys.signature, new AvaticaResultSetMetaData( null, null, accessedKeys.signature ), TimeZone.getDefault(), accessedKeys.firstFrame )
                                .execute2( MetaImpl.createCursor( CursorFactory.ARRAY, accessedKeys.firstFrame.rows ), accessedKeys.signature.columns );

                        final int numberOfColumns = apks.getMetaData().getColumnCount();

                        int processedRows = 0;
                        for ( ; apks.next(); ++processedRows ) {
                            final Serializable[] primaryKeyValues = new Serializable[numberOfColumns];

                            for ( int columnIndex = 0; columnIndex < numberOfColumns; ++columnIndex ) {
                                primaryKeyValues[columnIndex] = (Serializable) apks.getObject( columnIndex + 1 );
                            }

                            // add the requested record to the transaction
                            wlTrx.addAction( Operation.WRITE, new RecordIdentifier( catalogName, schemaName, tableName, primaryKeyValues ) );
                        }

                        if ( processedRows != updateCount ) {
                            throw new AssertionError( "Processed rows (" + processedRows + ") != update count (" + updateCount + ")" );
                        }
                    } catch ( SQLException ex ) {
                        throw new RuntimeException( "Internal Exception while converting the returned MetaResultSet into a JDBC ResultSet.", ex );
                    }
                }

            } else if ( _result instanceof BatchResultSetInfos ) {
                throw new UnsupportedOperationException( "Not implemented yet." );
            } else {
                throw new IllegalStateException( "Unknown ResultSetInfos implementation" );
            }

            return /*Void*/ null;
        } );

        return preparedStatement;
    }


    @Override
    public StatementInfos prepareDataQuery( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {

        // more than one table and especially the required join is currently not supported!
        final SqlIdentifier targetTable;
        switch ( sql.getKind() ) {
            // See org.apache.calcite.sql.SqlKind.QUERY
            case SELECT:
                targetTable = SqlNodeUtils.SELECT_UTILS.getTargetTable( (SqlSelect) sql );
                break;

            case UNION:
            case INTERSECT:
            case EXCEPT:
            case VALUES:
            case WITH:
            case ORDER_BY:
            case EXPLICIT_TABLE:
            default:
                throw new UnsupportedOperationException( "Not supported" );
        }

        final String catalogName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 2 );
        final String schemaName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 1 );
        final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 0 );

        final PreparedStatementInfos preparedStatement;
        if ( rpkDQL && rpkNoPrimaryKey ) {
            // best case: we can use RETURN_PRIMARY_KEY even if the table does not contain a primary key (then the values of all columns are returned)
            preparedStatement = (PreparedStatementInfos) down.prepareDataQuery( connection, statement, sql, maxRowCount );
        } else {
            // we need to check if the table has a primary key
            final Map<String, Integer> primaryKeyColumnNamesAndIndexes = Collections.unmodifiableMap( CatalogUtils.lookupPrimaryKeyColumnNamesAndIndexes( connection, catalogName, schemaName, tableName ) );
            final boolean tableHasPrimaryKeys = !primaryKeyColumnNamesAndIndexes.isEmpty();

            if ( rpkDQL && tableHasPrimaryKeys ) {
                // the table has a primary key and we can use RETURN_PRIMARY_KEY
                preparedStatement = (PreparedStatementInfos) down.prepareDataQuery( connection, statement, sql, maxRowCount );
            } else {
                // we cannot use RETURN_PRIMARY_KEY and thus we need to request the return of columns using an int array
                final int[] columnIndexes;
                if ( !rpkDQL && tableHasPrimaryKeys ) {
                    // The table has a primary key but we cannot use the RETURN_PRIMARY_KEY function,
                    // so we use the execute(String, int[]) function to get the values of the primary key
                    columnIndexes = new int[primaryKeyColumnNamesAndIndexes.values().size()];
                    int i = 0;
                    for ( int index : new TreeSet<>( primaryKeyColumnNamesAndIndexes.values() ) ) {
                        columnIndexes[i++] = index + 1; // convert the "array" index to a "sql" index
                    }
                } else {
                    // The table has no primary key and we cannot use the RETURN_PRIMARY_KEY function,
                    // so we get all columns and want all of them returned using the execute(String, int[]) function
                    final MetaResultSet columns = connection.getCatalog().getColumns( connection, catalogName, Pat.of( schemaName ), Pat.of( tableName ), Pat.of( null ) );
                    int numberOfColumns = 0;
                    for ( final Object row : columns.firstFrame.rows ) {
                        ++numberOfColumns;
                    }
                    columnIndexes = new int[numberOfColumns];
                    for ( int ci = 0; ci < columnIndexes.length; ++ci ) {
                        columnIndexes[ci] = ci;
                    }
                }

                preparedStatement = (PreparedStatementInfos) down.prepareDataQuery( connection, statement, sql, maxRowCount, columnIndexes );
            }
        }

        preparedStatement.setWorkloadAnalysisFunction( ( _connection, _transaction, _statement, _values, _result ) -> {

            if ( _result instanceof QueryResultSet ) {
                final Map<AbstractRemoteNode, RemoteExecuteResult> origins = ((QueryResultSet) _result).getOrigins();
                final Transaction wlTrx = Transaction.getTransaction( _transaction.getTransactionId() );

                for ( Entry<AbstractRemoteNode, RemoteExecuteResult> origin : origins.entrySet() ) {
                    final AbstractRemoteNode src = origin.getKey();
                    final List<MetaResultSet> resultSets = origin.getValue().toExecuteResult().resultSets;

                    switch ( resultSets.size() ) {
                        case 0:
                            throw new IllegalStateException( "List of resultSets returned by " + src + " is empty!" );

                        case 1:
                            throw new IllegalStateException( "List of resultSets returned by " + src + " contains only one result set! The retrieval of the accessed keys must have failed!" );

                        case 2:
                            break; // intentional

                        default:
                            throw new UnsupportedOperationException( "List of resultSets returned by " + src + " contains more than two result sets! The return of multiple data(?) results is currently not supported!" );
                    }

                    final MetaResultSet accessedKeys = resultSets.get( 1 );

                    try {
                        final ResultSet apks = new AvaticaResultSet( null, null, accessedKeys.signature, new AvaticaResultSetMetaData( null, null, accessedKeys.signature ), TimeZone.getDefault(), accessedKeys.firstFrame )
                                .execute2( MetaImpl.createCursor( CursorFactory.ARRAY, accessedKeys.firstFrame.rows ), accessedKeys.signature.columns );

                        final int numberOfColumns = apks.getMetaData().getColumnCount();

                        int processedRows = 0;
                        for ( ; apks.next(); ++processedRows ) {
                            final Serializable[] primaryKeyValues = new Serializable[numberOfColumns];

                            for ( int columnIndex = 0; columnIndex < numberOfColumns; ++columnIndex ) {
                                primaryKeyValues[columnIndex] = (Serializable) apks.getObject( columnIndex + 1 );
                            }

                            // add the requested record to the transaction
                            wlTrx.addAction( Operation.READ, new RecordIdentifier( catalogName, schemaName, tableName, primaryKeyValues ) );
                        }
                    } catch ( SQLException ex ) {
                        throw new RuntimeException( "Internal Exception while converting the returned MetaResultSet into a JDBC ResultSet.", ex );
                    }
                }

            } else if ( _result instanceof BatchResultSetInfos ) {
                throw new UnsupportedOperationException( "SELECTs are not supported in batch mode." );
            } else {
                throw new IllegalStateException( "Unknown ResultSetInfos implementation" );
            }

            return /*Void*/ null;
        } );

        return preparedStatement;
    }


    @Override
    public ResultSetInfos execute( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<TypedValue> parameterValues, int maxRowsInFirstFrame ) throws NoSuchStatementException, RemoteException {

        if ( !(statement instanceof PreparedStatementInfos) ) {
            // The given statement is not a prepared statement.
            throw new NoSuchStatementException( statement.getStatementHandle() );
        }

        final PreparedStatementInfos preparedStatement = (PreparedStatementInfos) statement;

        final ResultSetInfos resultSet = down.execute( connection, transaction, preparedStatement, parameterValues, maxRowsInFirstFrame );

        // execute the workload analysis function which was created during the prepare call
        preparedStatement.getWorkloadAnalysisFunction().apply( connection, transaction, preparedStatement, parameterValues, resultSet );

        return resultSet;
    }


    @Override
    public ResultSetInfos executeBatch( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<UpdateBatch> parameterValues ) throws NoSuchStatementException, RemoteException {
        return down.executeBatch( connection, transaction, statement, parameterValues );
    }


    @Override
    public Frame fetch( final ConnectionInfos connection, StatementHandle statementHandle, long offset, int fetchMaxRowCount ) throws NoSuchStatementException, MissingResultsException, RemoteException {
        throw new UnsupportedOperationException( "The workload analysis does not work for fragmented results (yet)." );
    }


    @Override
    public void commit( ConnectionInfos connection, TransactionInfos transaction ) throws RemoteException {
        try {
            down.commit( connection, transaction );
        } finally {
            final Transaction t = Transaction.getTransaction( transaction.getTransactionId() );
            Workload.THIS_IS_A_TEST_REMOVE_ME.addTransaction( t );
            Transaction.removeTransaction( t );
        }
    }


    @Override
    public void rollback( ConnectionInfos connection, TransactionInfos transaction ) throws RemoteException {
        try {
            down.rollback( connection, transaction );
        } finally {
            Transaction.removeTransaction( Transaction.getTransaction( transaction.getTransactionId() ) );
        }
    }


    @Override
    public void closeStatement( ConnectionInfos connection, StatementInfos statement ) throws RemoteException {
        down.closeStatement( connection, statement );
    }


    @Override
    public void closeConnection( ConnectionInfos connection ) throws RemoteException {
        down.closeConnection( connection );
    }


    @Override
    public Iterable<Serializable> createIterable( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, QueryState state, Signature signature, List<TypedValue> parameterValues, Frame firstFrame ) throws RemoteException {
        return down.createIterable( connection, transaction, statement, state, signature, parameterValues, firstFrame );
    }


    @Override
    public boolean syncResults( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, QueryState state, long offset ) throws RemoteException {
        return down.syncResults( connection, transaction, statement, state, offset );
    }
}
