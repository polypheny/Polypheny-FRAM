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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeSet;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.Meta.ConnectionProperties;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.Meta.ExecuteResult;
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
import org.polypheny.fram.standalone.CatalogUtils;
import org.polypheny.fram.standalone.ConnectionInfos;
import org.polypheny.fram.standalone.Main;
import org.polypheny.fram.standalone.ResultSetInfos;
import org.polypheny.fram.standalone.ResultSetInfos.QueryResultSet;
import org.polypheny.fram.standalone.StatementInfos;
import org.polypheny.fram.standalone.StatementInfos.PreparedStatementInfos;
import org.polypheny.fram.standalone.TransactionInfos;
import org.polypheny.fram.standalone.Utils;
import org.polypheny.fram.standalone.parser.sql.SqlNodeUtils;


public class WorkloadAnalyzer implements Protocol {

    public static final String GET_GENERATED_KEYS_CONFIGURATION_OPTION_RETURN_PRIMARY_KEY = "RETURN_PRIMARY_KEY";
    public static final String GET_GENERATED_KEYS_CONFIGURATION_OPTION_RETURN_COLUMN_INDEXES = "RETURN_GENERATED_KEYS_COL_INDEXES";

    private final boolean rpkDML;
    private final boolean rpkDQL;
    private final boolean rpkNoPrimaryKey;
    private final boolean analyzeBatches;

    private AbstractProtocol delegate;


    public WorkloadAnalyzer( final AbstractProtocol protocol ) {
        final com.typesafe.config.Config configuration = Main.configuration();

        rpkDML = configuration.getString( "standalone.datastore.getGeneratedKeys.return_primary_key_for_dml" ).equalsIgnoreCase( GET_GENERATED_KEYS_CONFIGURATION_OPTION_RETURN_PRIMARY_KEY );
        rpkDQL = configuration.getString( "standalone.datastore.getGeneratedKeys.return_primary_key_for_dql" ).equalsIgnoreCase( GET_GENERATED_KEYS_CONFIGURATION_OPTION_RETURN_PRIMARY_KEY );
        rpkNoPrimaryKey = configuration.getString( "standalone.datastore.getGeneratedKeys.tables_without_primary_key" ).equalsIgnoreCase( GET_GENERATED_KEYS_CONFIGURATION_OPTION_RETURN_PRIMARY_KEY );
        analyzeBatches = configuration.getBoolean( "fram.analyzeBatches" );

        this.delegate = protocol;
    }


    @Override
    public ConnectionProperties connectionSync( ConnectionInfos connection, ConnectionProperties newConnectionProperties ) throws RemoteException {
        return delegate.connectionSync( connection, newConnectionProperties );
    }


    @Override
    public ResultSetInfos prepareAndExecuteDataDefinition( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        return delegate.prepareAndExecuteDataDefinition( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
    }


    @Override
    public ResultSetInfos prepareAndExecuteDataDefinition( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode catalogSql, SqlNode storeSql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        return delegate.prepareAndExecuteDataDefinition( connection, transaction, statement, catalogSql, storeSql, maxRowCount, maxRowsInFirstFrame, callback );
    }


    @Override
    public ResultSetInfos prepareAndExecuteDataManipulation( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {

        final SqlIdentifier targetTable;
        switch ( sql.getKind() ) {
            // See org.apache.calcite.sql.SqlKind.DML
            case INSERT:
                targetTable = SqlNodeUtils.INSERT_UTILS.getTargetTable( (SqlInsert) sql );
                break;

            case UPDATE:
                targetTable = SqlNodeUtils.UPDATE_UTILS.getTargetTable( (SqlUpdate) sql );
                break;

            case DELETE:
                targetTable = SqlNodeUtils.DELETE_UTILS.getTargetTable( (SqlDelete) sql );
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
            executeResult = delegate.prepareAndExecuteDataManipulation( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
        } else {
            // we need to check if the table has a primary key
            final Map<String, Integer> primaryKeyColumnNamesAndIndexes = Collections.unmodifiableMap( CatalogUtils.lookupPrimaryKeyColumnsNamesAndIndexes( connection, catalogName, schemaName, tableName ) );
            final boolean tableHasPrimaryKeys = !primaryKeyColumnNamesAndIndexes.isEmpty();

            if ( rpkDML && tableHasPrimaryKeys ) {
                // the table has a primary key and we can use RETURN_PRIMARY_KEY
                executeResult = delegate.prepareAndExecuteDataManipulation( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
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
                throw new UnsupportedOperationException( "Not implemented yet." );
                //executeResult = down.prepareAndExecuteDataManipulation( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, columnIndexes, callback );
            }
        }

        if ( !(executeResult instanceof QueryResultSet) ) {
            throw new IllegalStateException( "Unknown ResultSetInfos implementation" );
        }

        final ExecuteResult generatedKeysResult = ((ResultSetInfos) executeResult).getGeneratedKeys();
        if ( generatedKeysResult == null ) {
            throw new IllegalStateException( "generatedKeysResult == null" );
        }

        final List<MetaResultSet> generatedKeysMetaResultSets = generatedKeysResult.resultSets;
        if ( generatedKeysMetaResultSets.isEmpty() ) {
            throw new IllegalStateException( "List of generatedKeysResult.resultSets is empty!" );
        }

        try {

            final MetaResultSet generatedKeys = generatedKeysMetaResultSets.get( 0 );
            final ResultSet generateKeysResultSet = new AvaticaResultSet( null, null, generatedKeys.signature, new AvaticaResultSetMetaData( null, null, generatedKeys.signature ), TimeZone.getDefault(), generatedKeys.firstFrame )
                    .execute2( MetaImpl.createCursor( CursorFactory.ARRAY, generatedKeys.firstFrame.rows ), generatedKeys.signature.columns );

            final int numberOfColumns = generateKeysResultSet.getMetaData().getColumnCount();

            int processedRows = 0;
            for ( ; generateKeysResultSet.next(); ++processedRows ) {
                final Serializable[] primaryKeyValues = new Serializable[numberOfColumns];

                for ( int columnIndex = 0; columnIndex < numberOfColumns; ++columnIndex ) {
                    primaryKeyValues[columnIndex] = (Serializable) generateKeysResultSet.getObject( columnIndex + 1 );
                }

                // add the requested record to the transaction
                switch ( sql.getKind() ) {
                    case INSERT:
                        Transaction.getTransaction( ((TransactionInfos) transaction).getTransactionId() )
                                .addAction( Operation.INSERT, new RecordIdentifier( catalogName, schemaName, tableName, primaryKeyValues ) );
                        break;

                    case UPDATE:
                        Transaction.getTransaction( ((TransactionInfos) transaction).getTransactionId() )
                                .addAction( Operation.UPDATE, new RecordIdentifier( catalogName, schemaName, tableName, primaryKeyValues ) );
                        break;

                    case DELETE:
                        Transaction.getTransaction( ((TransactionInfos) transaction).getTransactionId() )
                                .addAction( Operation.DELETE, new RecordIdentifier( catalogName, schemaName, tableName, primaryKeyValues ) );
                        break;
                }
            }

            // todo: check processed rows against (the sum of) the update count(s)

        } catch ( SQLException ex ) {
            throw new RuntimeException( "Internal Exception while converting the returned MetaResultSet into a JDBC ResultSet.", ex );
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
            executeResult = delegate.prepareAndExecuteDataQuery( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
        } else {
            // we need to check if the table has a primary key
            final Map<String, Integer> primaryKeyColumnNamesAndIndexes = Collections.unmodifiableMap( CatalogUtils.lookupPrimaryKeyColumnsNamesAndIndexes( connection, catalogName, schemaName, tableName ) );
            final boolean tableHasPrimaryKeys = !primaryKeyColumnNamesAndIndexes.isEmpty();

            if ( rpkDQL && tableHasPrimaryKeys ) {
                // the table has a primary key and we can use RETURN_PRIMARY_KEY
                executeResult = delegate.prepareAndExecuteDataQuery( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
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
                throw new UnsupportedOperationException( "Not implemented yet." );
                //executeResult = down.prepareAndExecuteDataQuery( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, columnIndexes, callback );
            }
        }

        if ( !(executeResult instanceof QueryResultSet) ) {
            throw new IllegalStateException( "Unknown ResultSetInfos implementation" );
        }

        final ExecuteResult generatedKeysResult = ((ResultSetInfos) executeResult).getGeneratedKeys();
        if ( generatedKeysResult == null ) {
            throw new IllegalStateException( "generatedKeysResult == null" );
        }

        final List<MetaResultSet> generatedKeysMetaResultSets = generatedKeysResult.resultSets;
        if ( generatedKeysMetaResultSets.isEmpty() ) {
            throw new IllegalStateException( "List of generatedKeysResult.resultSets is empty!" );
        }

        try {

            final MetaResultSet generatedKeys = generatedKeysMetaResultSets.get( 0 );
            final ResultSet generateKeysResultSet = new AvaticaResultSet( null, null, generatedKeys.signature, new AvaticaResultSetMetaData( null, null, generatedKeys.signature ), TimeZone.getDefault(), generatedKeys.firstFrame )
                    .execute2( MetaImpl.createCursor( CursorFactory.ARRAY, generatedKeys.firstFrame.rows ), generatedKeys.signature.columns );

            final int numberOfColumns = generateKeysResultSet.getMetaData().getColumnCount();

            int processedRows = 0;
            for ( ; generateKeysResultSet.next(); ++processedRows ) {
                final Serializable[] primaryKeyValues = new Serializable[numberOfColumns];

                for ( int columnIndex = 0; columnIndex < numberOfColumns; ++columnIndex ) {
                    primaryKeyValues[columnIndex] = (Serializable) generateKeysResultSet.getObject( columnIndex + 1 );
                }

                // add the requested record to the transaction
                switch ( sql.getKind() ) {
                    case SELECT:
                        Transaction.getTransaction( ((TransactionInfos) transaction).getTransactionId() )
                                .addAction( Operation.SELECT, new RecordIdentifier( catalogName, schemaName, tableName, primaryKeyValues ) );
                        break;
                }
            }

            // todo: check processed rows against (the sum of) the update count(s)

        } catch ( SQLException ex ) {
            throw new RuntimeException( "Internal Exception while converting the returned MetaResultSet into a JDBC ResultSet.", ex );
        }

        return executeResult;
    }


    @Override
    public ResultSetInfos prepareAndExecuteTransactionCommit( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        try {
            return delegate.prepareAndExecuteTransactionCommit( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
        } finally {
            final Transaction t = Transaction.getTransaction( transaction.getTransactionId() );
            Workload.THIS_IS_A_TEST_REMOVE_ME.addTransaction( t );
            Transaction.removeTransaction( t );
        }
    }


    @Override
    public ResultSetInfos prepareAndExecuteTransactionRollback( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        try {
            return delegate.prepareAndExecuteTransactionRollback( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
        } finally {
            Transaction.removeTransaction( Transaction.getTransaction( transaction.getTransactionId() ) );
        }
    }


    @Override
    public PreparedStatementInfos prepareDataManipulation( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {

        final SqlIdentifier targetTable;
        switch ( sql.getKind() ) {
            // See org.apache.calcite.sql.SqlKind.DML
            case INSERT:
                targetTable = SqlNodeUtils.INSERT_UTILS.getTargetTable( (SqlInsert) sql );
                break;

            case UPDATE:
                targetTable = SqlNodeUtils.UPDATE_UTILS.getTargetTable( (SqlUpdate) sql );
                break;

            case DELETE:
                targetTable = SqlNodeUtils.DELETE_UTILS.getTargetTable( (SqlDelete) sql );
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
            preparedStatement = (PreparedStatementInfos) delegate.prepareDataManipulation( connection, statement, sql, maxRowCount );
        } else {
            // we need to check if the table has a primary key
            final Map<String, Integer> primaryKeyColumnNamesAndIndexes = Collections.unmodifiableMap( CatalogUtils.lookupPrimaryKeyColumnsNamesAndIndexes( connection, catalogName, schemaName, tableName ) );
            final boolean tableHasPrimaryKeys = !primaryKeyColumnNamesAndIndexes.isEmpty();

            if ( rpkDML && tableHasPrimaryKeys ) {
                // the table has a primary key and we can use RETURN_PRIMARY_KEY
                preparedStatement = (PreparedStatementInfos) delegate.prepareDataManipulation( connection, statement, sql, maxRowCount );
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
                throw new UnsupportedOperationException( "Not implemented yet." );
                //preparedStatement = (PreparedStatementInfos) down.prepareDataManipulation( connection, statement, sql, maxRowCount, columnIndexes );
            }
        }

        preparedStatement.setWorkloadAnalysisFunction( ( _connection, _transaction, _statement, _values, _result ) -> {

            final ExecuteResult generatedKeysResult = ((ResultSetInfos) _result).getGeneratedKeys();
            if ( generatedKeysResult == null ) {
                throw new IllegalStateException( "generatedKeysResult == null" );
            }

            final List<MetaResultSet> generatedKeysMetaResultSets = generatedKeysResult.resultSets;
            if ( generatedKeysMetaResultSets.isEmpty() ) {
                throw new IllegalStateException( "List of generatedKeysResult.resultSets is empty!" );
            }

            for ( MetaResultSet generatedKeys : generatedKeysMetaResultSets ) {
                for ( Object rawRow : generatedKeys.firstFrame.rows ) {
                    final Serializable[] keyValues;

                    if ( rawRow instanceof Object[] ) {
                        final Object[] arrayRow = (Object[]) rawRow;
                        keyValues = new Serializable[arrayRow.length];
                        for ( int columnIndex = 0; columnIndex < keyValues.length; ++columnIndex ) {
                            keyValues[columnIndex] = (Serializable) arrayRow[columnIndex];
                        }
                    } else if ( rawRow instanceof List ) {
                        final List<Object> listRow = (List<Object>) rawRow;
                        keyValues = new Serializable[listRow.size()];
                        Iterator<Object> listRowIterator = listRow.iterator();
                        for ( int columnIndex = 0; columnIndex < keyValues.length && listRowIterator.hasNext(); ++columnIndex ) {
                            keyValues[columnIndex] = (Serializable) listRowIterator.next();
                        }
                    } else {
                        throw new UnsupportedOperationException( "Not implemented yet." );
                    }

                    // add the requested record to the transaction
                    switch ( sql.getKind() ) {
                        case INSERT:
                            Transaction.getTransaction( ((TransactionInfos) _transaction).getTransactionId() )
                                    .addAction( Operation.INSERT, new RecordIdentifier( catalogName, schemaName, tableName, keyValues ) );
                            break;

                        case UPDATE:
                            Transaction.getTransaction( ((TransactionInfos) _transaction).getTransactionId() )
                                    .addAction( Operation.UPDATE, new RecordIdentifier( catalogName, schemaName, tableName, keyValues ) );
                            break;

                        case DELETE:
                            Transaction.getTransaction( ((TransactionInfos) _transaction).getTransactionId() )
                                    .addAction( Operation.DELETE, new RecordIdentifier( catalogName, schemaName, tableName, keyValues ) );
                            break;
                    }
                }
            }

            // todo: check processed rows against (the sum of) the update count(s)

            return Utils.VOID;
        } );

        return preparedStatement;
    }


    @Override
    public PreparedStatementInfos prepareDataQuery( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {

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
            preparedStatement = (PreparedStatementInfos) delegate.prepareDataQuery( connection, statement, sql, maxRowCount );
        } else {
            // we need to check if the table has a primary key
            final Map<String, Integer> primaryKeyColumnNamesAndIndexes = Collections.unmodifiableMap( CatalogUtils.lookupPrimaryKeyColumnsNamesAndIndexes( connection, catalogName, schemaName, tableName ) );
            final boolean tableHasPrimaryKeys = !primaryKeyColumnNamesAndIndexes.isEmpty();

            if ( rpkDQL && tableHasPrimaryKeys ) {
                // the table has a primary key and we can use RETURN_PRIMARY_KEY
                preparedStatement = (PreparedStatementInfos) delegate.prepareDataQuery( connection, statement, sql, maxRowCount );
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
                throw new UnsupportedOperationException( "Not implemented yet." );
                //preparedStatement = (PreparedStatementInfos) down.prepareDataQuery( connection, statement, sql, maxRowCount, columnIndexes );
            }
        }

        preparedStatement.setWorkloadAnalysisFunction( ( _connection, _transaction, _statement, _values, _result ) -> {

            final ExecuteResult generatedKeysResult = ((ResultSetInfos) _result).getGeneratedKeys();
            if ( generatedKeysResult == null ) {
                throw new IllegalStateException( "generatedKeysResult == null" );
            }

            final List<MetaResultSet> generatedKeysMetaResultSets = generatedKeysResult.resultSets;
            if ( generatedKeysMetaResultSets.isEmpty() ) {
                throw new IllegalStateException( "List of generatedKeysResult.resultSets is empty!" );
            }

            try {

                final MetaResultSet generatedKeys = generatedKeysMetaResultSets.get( 0 );
                final ResultSet generateKeysResultSet = new AvaticaResultSet( null, null, generatedKeys.signature, new AvaticaResultSetMetaData( null, null, generatedKeys.signature ), TimeZone.getDefault(), generatedKeys.firstFrame )
                        .execute2( MetaImpl.createCursor( CursorFactory.ARRAY, generatedKeys.firstFrame.rows ), generatedKeys.signature.columns );

                final int numberOfColumns = generateKeysResultSet.getMetaData().getColumnCount();

                int processedRows = 0;
                for ( ; generateKeysResultSet.next(); ++processedRows ) {
                    final Serializable[] primaryKeyValues = new Serializable[numberOfColumns];

                    for ( int columnIndex = 0; columnIndex < numberOfColumns; ++columnIndex ) {
                        primaryKeyValues[columnIndex] = (Serializable) generateKeysResultSet.getObject( columnIndex + 1 );
                    }

                    // add the requested record to the transaction
                    switch ( sql.getKind() ) {
                        case SELECT:
                            Transaction.getTransaction( ((TransactionInfos) _transaction).getTransactionId() )
                                    .addAction( Operation.SELECT, new RecordIdentifier( catalogName, schemaName, tableName, primaryKeyValues ) );
                            break;
                    }
                }

                // todo: check processed rows against (the sum of) the update count(s)

            } catch ( SQLException ex ) {
                throw new RuntimeException( "Internal Exception while converting the returned MetaResultSet into a JDBC ResultSet.", ex );
            }

            return Utils.VOID;
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

        final ResultSetInfos resultSet = delegate.execute( connection, transaction, preparedStatement, parameterValues, maxRowsInFirstFrame );

        // execute the workload analysis function which was created during the prepare call
        preparedStatement.getWorkloadAnalysisFunction().apply( connection, transaction, preparedStatement, parameterValues, resultSet );

        return resultSet;
    }


    @Override
    public ResultSetInfos executeBatch( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, List<UpdateBatch> parameterValues ) throws NoSuchStatementException, RemoteException {

        if ( !(statement instanceof PreparedStatementInfos) ) {
            // The given statement is not a prepared statement.
            throw new NoSuchStatementException( statement.getStatementHandle() );
        }

        final PreparedStatementInfos preparedStatement = (PreparedStatementInfos) statement;

        final ResultSetInfos resultSet = delegate.executeBatch( connection, transaction, preparedStatement, parameterValues );

        // execute the workload analysis function which was created during the prepare call
        preparedStatement.getWorkloadAnalysisFunction().apply( connection, transaction, preparedStatement, parameterValues, resultSet );

        return resultSet;
    }


    @Override
    public Frame fetch( final ConnectionInfos connection, StatementHandle statementHandle, long offset, int fetchMaxRowCount ) throws NoSuchStatementException, MissingResultsException, RemoteException {
        throw new UnsupportedOperationException( "The workload analysis does not work for fragmented results (yet)." );
    }


    @Override
    public void commit( ConnectionInfos connection, TransactionInfos transaction ) throws RemoteException {
        try {
            delegate.commit( connection, transaction );
        } finally {
            final Transaction t = Transaction.getTransaction( transaction.getTransactionId() );
            Workload.THIS_IS_A_TEST_REMOVE_ME.addTransaction( t );
            Transaction.removeTransaction( t );
        }
    }


    @Override
    public void rollback( ConnectionInfos connection, TransactionInfos transaction ) throws RemoteException {
        try {
            delegate.rollback( connection, transaction );
        } finally {
            Transaction.removeTransaction( Transaction.getTransaction( transaction.getTransactionId() ) );
        }
    }


    @Override
    public void closeStatement( ConnectionInfos connection, StatementInfos statement ) throws RemoteException {
        delegate.closeStatement( connection, statement );
    }


    @Override
    public void closeConnection( ConnectionInfos connection ) throws RemoteException {
        delegate.closeConnection( connection );
    }


    @Override
    public Iterable<Serializable> createIterable( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, QueryState state, Signature signature, List<TypedValue> parameterValues, Frame firstFrame ) throws RemoteException {
        return delegate.createIterable( connection, transaction, statement, state, signature, parameterValues, firstFrame );
    }


    @Override
    public boolean syncResults( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, QueryState state, long offset ) throws RemoteException {
        return delegate.syncResults( connection, transaction, statement, state, offset );
    }
}
