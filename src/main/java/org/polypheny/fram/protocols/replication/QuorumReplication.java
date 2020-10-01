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

package org.polypheny.fram.protocols.replication;


import io.vavr.Function1;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.calcite.avatica.Meta.ConnectionHandle;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlDdlNodes;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jgroups.util.RspList;
import org.polypheny.fram.Node;
import org.polypheny.fram.protocols.AbstractProtocol;
import org.polypheny.fram.protocols.Protocol.ReplicationProtocol;
import org.polypheny.fram.remote.AbstractRemoteNode;
import org.polypheny.fram.remote.ClusterUtils;
import org.polypheny.fram.remote.types.RemoteConnectionHandle;
import org.polypheny.fram.remote.types.RemoteExecuteBatchResult;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.remote.types.RemoteStatementHandle;
import org.polypheny.fram.remote.types.RemoteTransactionHandle;
import org.polypheny.fram.standalone.CatalogUtils;
import org.polypheny.fram.standalone.ConnectionInfos;
import org.polypheny.fram.standalone.ResultSetInfos;
import org.polypheny.fram.standalone.StatementInfos;
import org.polypheny.fram.standalone.TransactionInfos;
import org.polypheny.fram.standalone.Utils;
import org.polypheny.fram.standalone.parser.sql.SqlNodeUtils;
import org.polypheny.fram.standalone.parser.sql.ddl.SqlAlterTable;
import org.polypheny.fram.standalone.parser.sql.ddl.SqlCreateIndex;


/**
 * """
 * &gt; Read and write quorums must fulfill the following constraints:
 * &gt; 2 · wq &gt; n and rq + wq &gt; n, being n the number of sites.
 * """
 * -- Ricardo Jiménez-Peris, Marta Patiño-Martínez, Gustavo Alonso, and Bettina Kemme. 2003. Are Quorums an Alternative for Data Replication? ACM Trans. Database Syst. 28, 3 (September 2003), 257–294. DOI:https://doi.org/10.1145/937598.937601
 */
public class QuorumReplication extends AbstractProtocol implements ReplicationProtocol {

    /**
     *
     */
    public static final QuorumReplication ROWA = new ROWA();

    /**
     * """
     * &gt; The minimum quorum sizes satisfying the constraints are:
     * &gt; 2 · wq = n + 1 and rq + wq = n + 1 and therefore,
     * &gt; wq = floor(n / 2) + 1 and rq = ceil(n / 2 ) = floor((n+1) / 2).
     * """
     * -- Ricardo Jiménez-Peris, Marta Patiño-Martínez, Gustavo Alonso, and Bettina Kemme. 2003. Are Quorums an Alternative for Data Replication? ACM Trans. Database Syst. 28, 3 (September 2003), 257–294. DOI:https://doi.org/10.1145/937598.937601
     */
    public static final QuorumReplication MAJORITY = new QuorumReplication(
            connection -> {
                List<AbstractRemoteNode> candidates = new LinkedList<>( connection.getCluster().getMembers() );
                Collections.shuffle( candidates );
                Set<AbstractRemoteNode> readQuorum = new LinkedHashSet<>(); // floor((n+1) / 2)
                final int n = candidates.size();
                final int rq = Math.floorDiv( n + 1, 2 );
                for ( AbstractRemoteNode candidate : candidates ) {
                    readQuorum.add( candidate );
                    if ( readQuorum.size() >= rq ) {
                        break;
                    }
                }
                return Collections.unmodifiableSet( readQuorum );
            },
            connection -> {
                List<AbstractRemoteNode> candidates = new LinkedList<>( connection.getCluster().getMembers() );
                Collections.shuffle( candidates );
                Set<AbstractRemoteNode> writeQuorum = new LinkedHashSet<>(); // floor(n / 2) + 1
                final int n = candidates.size();
                final int wq = Math.floorDiv( n, 2 ) + 1;
                for ( AbstractRemoteNode candidate : candidates ) {
                    writeQuorum.add( candidate );
                    if ( writeQuorum.size() >= wq ) {
                        break;
                    }
                }
                return Collections.unmodifiableSet( writeQuorum );
            }
    );

    public final Function1<ConnectionInfos, Collection<AbstractRemoteNode>> readQuorumFunction;
    public final Function1<ConnectionInfos, Collection<AbstractRemoteNode>> writeQuorumFunction;

    public final boolean WORK_IN_PROGRESS = true;

    private final Map<TransactionInfos, Collection<AbstractRemoteNode>> transactionToReadQuorum = new ConcurrentHashMap<>();
    private final Map<TransactionInfos, Collection<AbstractRemoteNode>> transactionToWriteQuorum = new ConcurrentHashMap<>();


    public QuorumReplication( final Function1<ConnectionInfos, Collection<AbstractRemoteNode>> readQuorumFunction, final Function1<ConnectionInfos, Collection<AbstractRemoteNode>> writeQuorumFunction ) {
        this.readQuorumFunction = readQuorumFunction;
        this.writeQuorumFunction = writeQuorumFunction;
    }


    /**
     * For 1SR consistency, it is required that the read and the write quorum have at least one node in common.
     */
    protected Collection<AbstractRemoteNode> getReadQuorum( final ConnectionInfos connection ) {
        return this.transactionToReadQuorum.computeIfAbsent( connection.getTransaction(), __ -> this.readQuorumFunction.apply( connection ) );
    }


    /**
     * For 1SR consistency, it is required that the read and the write quorum have at least one node in common.
     */
    protected Collection<AbstractRemoteNode> getWriteQuorum( final ConnectionInfos connection ) {
        return this.transactionToWriteQuorum.computeIfAbsent( connection.getTransaction(), __ -> this.writeQuorumFunction.apply( connection ) );
    }


    @Override
    public ResultSetInfos prepareAndExecuteDataDefinition( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        if ( WORK_IN_PROGRESS ) {
            LOGGER.warn( "Quorum Replication currently without(!) version columns!" );
            return super.prepareAndExecuteDataDefinition( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
        }

        switch ( sql.getKind() ) {
            case CREATE_TABLE:
                return prepareAndExecuteDataDefinitionCreateTable( connection, transaction, statement, (SqlCreateTable) sql, maxRowCount, maxRowsInFirstFrame, callback );

            case DROP_TABLE:
                return super.prepareAndExecuteDataDefinition( connection, transaction, statement, (SqlDropTable) sql, maxRowCount, maxRowsInFirstFrame, callback );

            case CREATE_INDEX: // todo - do we need to alter the index, too? See TPC-C DDL script.
                return super.prepareAndExecuteDataDefinition( connection, transaction, statement, (SqlCreateIndex) sql, maxRowCount, maxRowsInFirstFrame, callback );

            case ALTER_TABLE:
                return prepareAndExecuteDataDefinitionAlterTable( connection, transaction, statement, (SqlAlterTable) sql, maxRowCount, maxRowsInFirstFrame, callback );

            default:
                throw new UnsupportedOperationException( "Not implemented yet." );
        }
    }


    protected ResultSetInfos prepareAndExecuteDataDefinitionCreateTable( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlCreateTable origin, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {

        final SqlNodeList originColumnList = origin.operand( 1 );
        final List<SqlNode> columns = new LinkedList<>();

        for ( SqlNode originColumnAsNode : originColumnList.getList() ) {
            if ( originColumnAsNode instanceof SqlColumnDeclaration ) {

                final SqlColumnDeclaration originColumn = (SqlColumnDeclaration) originColumnAsNode;
                columns.add( originColumn );

                final SqlParserPos originColumnPos = originColumn.getParserPosition();
                final SqlIdentifier originColumnName = originColumn.operand( 0 );
                final List<String> names = new LinkedList<>();
                for ( Iterator<String> originColumnNamesIterator = originColumnName.names.iterator(); originColumnNamesIterator.hasNext(); ) {
                    String columnNamePart = originColumnNamesIterator.next();
                    if ( !originColumnNamesIterator.hasNext() ) {
                        columnNamePart += "_$V";
                    }
                    names.add( columnNamePart );
                }
                final List<SqlParserPos> versionColumnNamesPositions = new LinkedList<>();
                for ( int i = 0; i < originColumnName.names.size(); ++i ) {
                    SqlParserPos versionColumnNamePosition = originColumnName.getComponentParserPosition( i );
                    if ( i + 1 == originColumnName.names.size() ) {
                        versionColumnNamePosition = versionColumnNamePosition.withQuoting( true );
                    }
                    versionColumnNamesPositions.add( versionColumnNamePosition );
                }
                final SqlIdentifier versionColumnName = (SqlIdentifier) originColumnName.clone( originColumnPos.withQuoting( true ) );
                versionColumnName.setNames( names, versionColumnNamesPositions );

                final SqlDataTypeSpec datatype = new SqlDataTypeSpec( new SqlBasicTypeNameSpec( SqlTypeName.BIGINT, originColumnPos ), originColumnPos );
                final SqlNode expression = null;
                final ColumnStrategy strategy = ColumnStrategy.NULLABLE;

                columns.add( SqlDdlNodes.column( originColumnPos.withQuoting( true ), versionColumnName, datatype, expression, strategy ) );

            } else {

                // e.g., table constraints
                if ( originColumnAsNode instanceof SqlKeyConstraint ) {
                    // PRIMARY KEY or UNIQUE
                    final SqlKeyConstraint originKeyConstraint = (SqlKeyConstraint) originColumnAsNode;
                    final List<SqlNode> keyConstraintIdentifiers = new LinkedList<>();

                    for ( SqlNode originalIdentifierAsNode : originKeyConstraint.<SqlNodeList>operand( 1 ).getList() ) {
                        if ( !(originalIdentifierAsNode instanceof SqlIdentifier) ) {
                            throw new UnsupportedOperationException( "Not implemented yet." );
                        }

                        final SqlIdentifier originalIdentifier = (SqlIdentifier) originalIdentifierAsNode;
                        keyConstraintIdentifiers.add( originalIdentifier );

                        final SqlParserPos originColumnPos = originalIdentifier.getParserPosition();
                        final List<String> names = new LinkedList<>();
                        for ( Iterator<String> originIdentifierNamesIterator = originalIdentifier.names.iterator(); originIdentifierNamesIterator.hasNext(); ) {
                            String columnNamePart = originIdentifierNamesIterator.next();
                            if ( !originIdentifierNamesIterator.hasNext() ) {
                                columnNamePart += "_$V";
                            }
                            names.add( columnNamePart );
                        }
                        final List<SqlParserPos> versionIdentifierNamesPositions = new LinkedList<>();
                        for ( int i = 0; i < originalIdentifier.names.size(); ++i ) {
                            SqlParserPos versionColumnNamePosition = originalIdentifier.getComponentParserPosition( i );
                            if ( i + 1 == originalIdentifier.names.size() ) {
                                versionColumnNamePosition = versionColumnNamePosition.withQuoting( true );
                            }
                            versionIdentifierNamesPositions.add( versionColumnNamePosition );
                        }
                        final SqlIdentifier versionIdentifierName = (SqlIdentifier) originalIdentifier.clone( originColumnPos.withQuoting( true ) );
                        versionIdentifierName.setNames( names, versionIdentifierNamesPositions );
                        keyConstraintIdentifiers.add( versionIdentifierName );
                    }

                    final SqlNodeList keyConstraintIdentifiersAsNodeList = new SqlNodeList( keyConstraintIdentifiers, originKeyConstraint.<SqlNodeList>operand( 1 ).getParserPosition() );
                    final SqlKeyConstraint newKeyConstraint;
                    switch ( originKeyConstraint.getOperator().kind ) {
                        case UNIQUE:
                            newKeyConstraint = SqlKeyConstraint.unique( originKeyConstraint.getParserPosition(), originKeyConstraint.operand( 0 ), keyConstraintIdentifiersAsNodeList );
                            break;
                        case PRIMARY_KEY:
                            newKeyConstraint = SqlKeyConstraint.primary( originKeyConstraint.getParserPosition(), originKeyConstraint.operand( 0 ), keyConstraintIdentifiersAsNodeList );
                            break;
                        default:
                            throw new UnsupportedOperationException( "Not implemented yet." );
                    }
                    columns.add( newKeyConstraint );
                } else {
                    throw new UnsupportedOperationException( "Not implemented yet." );
                }

            }
        }

        final SqlParserPos pos = origin.getParserPosition();
        final boolean replace = origin.getReplace();
        final boolean ifNotExists = origin.toSqlString( AnsiSqlDialect.DEFAULT ).getSql().contains( "IF NOT EXISTS" ); // the only way to get the info without using reflection
        final SqlIdentifier name = origin.operand( 0 );
        final SqlNodeList columnList = new SqlNodeList( columns, originColumnList.getParserPosition() );
        final SqlNode query = origin.operand( 2 );

        final SqlCreateTable catalogSql = origin;
        final SqlCreateTable storeSql = SqlDdlNodes.createTable( pos, replace, ifNotExists, name, columnList, query );

        final Collection<AbstractRemoteNode> quorum = this.getAllNodes( connection.getCluster() );
        LOGGER.trace( "prepareAndExecute[DataDefinition][CreateTable] on {}", quorum );

        final RspList<RemoteExecuteResult> responseList = connection.getCluster().prepareAndExecuteDataDefinition( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), catalogSql, storeSql, maxRowCount, maxRowsInFirstFrame, quorum );

        final Map<AbstractRemoteNode, RemoteExecuteResult> remoteResults = new HashMap<>();

        responseList.forEach( ( address, remoteStatementHandleRsp ) -> {
            if ( remoteStatementHandleRsp.hasException() ) {
                throw new RuntimeException( "Exception at " + address + " occurred.", remoteStatementHandleRsp.getException() );
            }
            final AbstractRemoteNode currentRemote = connection.getCluster().getRemoteNode( address );

            remoteResults.put( currentRemote, remoteStatementHandleRsp.getValue() );

            remoteStatementHandleRsp.getValue().toExecuteResult().resultSets.forEach( resultSet -> {
                connection.addAccessedNode( currentRemote, RemoteConnectionHandle.fromConnectionHandle( new ConnectionHandle( resultSet.connectionId ) ) );
                statement.addAccessedNode( currentRemote, RemoteStatementHandle.fromStatementHandle( new StatementHandle( resultSet.connectionId, resultSet.statementId, resultSet.signature ) ) );
            } );
        } );

        return statement.createResultSet( remoteResults, origins -> origins.entrySet().iterator().next().getValue().toExecuteResult(), ( origins, conn, stmt, offset, fetchMaxRowCount ) -> {
            try {
                return origins.entrySet().iterator().next().getKey().fetch( RemoteStatementHandle.fromStatementHandle( stmt.getStatementHandle() ), offset, fetchMaxRowCount ).toFrame();
            } catch ( RemoteException e ) {
                throw Utils.wrapException( e );
            }
        } );
    }


    protected ResultSetInfos prepareAndExecuteDataDefinitionAlterTable( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlAlterTable origin, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    @Override
    public ResultSetInfos prepareAndExecuteDataManipulation( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        final Collection<AbstractRemoteNode> quorum = this.getWriteQuorum( connection );
        LOGGER.trace( "prepareAndExecute[DataManipulation] on {}", quorum );

        // EXECUTE the statement
        final RspList<RemoteExecuteResult> responseList = connection.getCluster().prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, maxRowsInFirstFrame,
                quorum
        );
        // handle the responses
        final Map<AbstractRemoteNode, RemoteExecuteResult> remoteResults = ClusterUtils.getRemoteResults( connection.getCluster(), responseList );

        for ( AbstractRemoteNode _node : remoteResults.keySet() ) {
            connection.addAccessedNode( _node, RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ) );
        }

        return statement.createResultSet( remoteResults,
                /* mergeResults */ ( origins ) -> origins.entrySet().iterator().next().getValue().toExecuteResult(),
                /* fetch */ ( origins, conn, stmt, offset, fetchMaxRowCount ) -> {
                    try {
                        return origins.entrySet().iterator().next().getKey().fetch( RemoteStatementHandle.fromStatementHandle( stmt.getStatementHandle() ), offset, fetchMaxRowCount ).toFrame();
                    } catch ( RemoteException e ) {
                        throw Utils.wrapException( e );
                    }
                } );
    }


    @Override
    protected <NodeType extends Node> Map<NodeType, RemoteExecuteResult> prepareAndExecuteDataManipulation( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, Collection<NodeType> executionTargets ) throws RemoteException {
        return null;
    }


    @Override
    public ResultSetInfos prepareAndExecuteDataQuery( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        final Collection<AbstractRemoteNode> quorum = this.getReadQuorum( connection );
        LOGGER.trace( "prepareAndExecute[DataQuery] on {}", quorum );

        // EXECUTE the statement
        final RspList<RemoteExecuteResult> responseList = connection.getCluster().prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, maxRowsInFirstFrame,
                quorum
        );
        // handle the responses
        final Map<AbstractRemoteNode, RemoteExecuteResult> remoteResults = ClusterUtils.getRemoteResults( connection.getCluster(), responseList );

        for ( AbstractRemoteNode _node : remoteResults.keySet() ) {
            connection.addAccessedNode( _node, RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ) );
        }

        return statement.createResultSet( remoteResults,
                /* mergeResults */ ( _origins ) -> _origins.entrySet().iterator().next().getValue().toExecuteResult(),
                /* fetch */ ( _origins, _connection, _statement, _offset, _fetchMaxRowCount ) -> {
                    try {
                        return _origins.entrySet().iterator().next().getKey().fetch( RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _offset, _fetchMaxRowCount ).toFrame();
                    } catch ( RemoteException e ) {
                        throw Utils.wrapException( e );
                    }
                } );
    }


    @Override
    protected <NodeType extends Node> Map<NodeType, RemoteExecuteResult> prepareAndExecuteDataQuery( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, Collection<NodeType> executionTargets ) throws RemoteException {
        return null;
    }


    @Override
    public StatementInfos prepareDataManipulation( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {
        LOGGER.trace( "prepareDataManipulation( connection: {}, statement: {}, sql: {}, maxRowCount: {} )", connection, statement, sql, maxRowCount );
        switch ( sql.getKind() ) {
            // See org.apache.calcite.sql.SqlKind.DML
            case INSERT:
                if ( WORK_IN_PROGRESS ) {
                    break;
                } else {
                    return prepareDataManipulationInsert( connection, statement, (SqlInsert) sql, maxRowCount );
                }

            case DELETE:
            case UPDATE:
                // intentional empty case
                break;

            case MERGE:
            case PROCEDURE_CALL:
            default:
                throw new UnsupportedOperationException( "Not supported" );
        }

        // PREPARE on ALL nodes
        final RspList<RemoteStatementHandle> prepareResponseList = connection.getCluster().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount );
        // handle the responses of the PREPARE execution
        final Map<AbstractRemoteNode, RemoteStatementHandle> prepareRemoteResults = ClusterUtils.getRemoteResults( connection.getCluster(), prepareResponseList );

        return connection.createPreparedStatement( statement, prepareRemoteResults,
                //
                /* signatureMerge */ ( _remoteStatements ) -> {
                    // BEGIN HACK
                    return _remoteStatements.values().iterator().next().toStatementHandle().signature;
                    // END HACK
                },
                //
                /* execute */ ( _connection, _transaction, _statement, _parameterValues, _maxRowsInFirstFrame ) -> {
                    //
                    final Collection<AbstractRemoteNode> _writeQuorum = this.getWriteQuorum( _connection );
                    LOGGER.trace( "execute on {}", _writeQuorum );

                    final Map<AbstractRemoteNode, RemoteExecuteResult> _executeRemoteResults;
                    try {
                        // EXECUTE the statements
                        final RspList<RemoteExecuteResult> _executeResponseList = _connection.getCluster().execute( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _parameterValues, _maxRowsInFirstFrame,
                                _writeQuorum
                        );
                        // handle the EXECUTE responses
                        _executeRemoteResults = ClusterUtils.getRemoteResults( _connection.getCluster(), _executeResponseList );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    for ( AbstractRemoteNode _node : _executeRemoteResults.keySet() ) {
                        _connection.addAccessedNode( _node, RemoteConnectionHandle.fromConnectionHandle( _connection.getConnectionHandle() ) );
                    }

                    return _statement.createResultSet( _executeRemoteResults,
                            /* mergeResults */ ( __origins ) -> __origins.entrySet().iterator().next().getValue().toExecuteResult(),
                            /* fetch */ ( __origins, __connection, __statement, __offset, __fetchMaxRowCount ) -> {
                                try {
                                    return __origins.entrySet().iterator().next().getKey().fetch( RemoteStatementHandle.fromStatementHandle( __statement.getStatementHandle() ), __offset, __fetchMaxRowCount ).toFrame();
                                } catch ( RemoteException e ) {
                                    throw Utils.wrapException( e );
                                }
                            } );
                },
                //
                /* executeBatch */ ( _connection, _transaction, _statement, _listOfUpdateBatches ) -> {
                    //
                    final Collection<AbstractRemoteNode> _writeQuorum = this.getWriteQuorum( _connection );
                    LOGGER.trace( "executeBatch on {}", _writeQuorum );

                    final Map<AbstractRemoteNode, RemoteExecuteBatchResult> _executeRemoteBatchResults;
                    try {
                        // EXECUTE the statements
                        final RspList<RemoteExecuteBatchResult> _executeBatchResponseList = _connection.getCluster().executeBatch( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _listOfUpdateBatches,
                                _writeQuorum
                        );
                        // handle the EXECUTE responses
                        _executeRemoteBatchResults = ClusterUtils.getRemoteResults( _connection.getCluster(), _executeBatchResponseList );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    for ( AbstractRemoteNode _node : _executeRemoteBatchResults.keySet() ) {
                        _connection.addAccessedNode( _node, RemoteConnectionHandle.fromConnectionHandle( _connection.getConnectionHandle() ) );
                    }

                    return _statement.createBatchResultSet( _executeRemoteBatchResults,
                            /* mergeResults */ ( __origins ) -> __origins.entrySet().iterator().next().getValue().toExecuteBatchResult() );
                } );
    }


    @Override
    public Map<AbstractRemoteNode, RemoteStatementHandle> prepareDataManipulation( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount, Collection<AbstractRemoteNode> executionTargets ) throws RemoteException {
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    protected StatementInfos prepareDataManipulationInsert( ConnectionInfos connection, StatementInfos statement, SqlInsert sql, long maxRowCount ) throws RemoteException {
        LOGGER.trace( "prepareDataManipulationInsert( connection: {}, statement: {}, sql: {}, maxRowCount: {} )", connection, statement, sql, maxRowCount );

        final SqlIdentifier targetTable = SqlNodeUtils.INSERT_UTILS.getTargetTable( sql );
        final SqlNodeList targetColumns = SqlNodeUtils.INSERT_UTILS.getTargetColumns( sql );
        if ( targetColumns == null || targetColumns.size() == 0 ) {
            /*
                todo: Inserts: duplicate the amount of dynamic parameters ("?"), map the old ones to the new by applying index*2 (0->0, 1->2, 2->4, 3->6, ...)
             */
            final String catalogName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 2 );
            final String schemaName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 1 );
            final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetTable, 0 );
            final List<SqlNode> columnsNames = new LinkedList<>();
            for ( final String columnName : CatalogUtils.lookupColumnsNames( connection, catalogName, schemaName, tableName ) ) {
                columnsNames.add( new SqlIdentifier( columnName, SqlParserPos.QUOTED_ZERO ) );
            }
            SqlNodeList targetColumnList = new SqlNodeList( columnsNames, SqlParserPos.ZERO );
            sql.setOperand( 3, targetColumnList );
        }

        final Collection<AbstractRemoteNode> quorum = this.getWriteQuorum( connection );
        LOGGER.trace( "prepare[DataManipulation][Insert] on {}", quorum );

        final RspList<RemoteStatementHandle> responseList = connection.getCluster().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, quorum );
        final Map<AbstractRemoteNode, RemoteStatementHandle> prepareRemoteResults = ClusterUtils.getRemoteResults( connection.getCluster(), responseList );

        return connection.createPreparedStatement( statement, prepareRemoteResults,
                //
                /* signatureMerge */ ( _remoteStatements ) -> {
                    // BEGIN HACK
                    return _remoteStatements.values().iterator().next().toStatementHandle().signature;
                    // END HACK
                },
                //
                /* execute */ ( _connection, _transaction, _statement, _parameterValues, _maxRowsInFirstFrame ) -> {
                    //
                    final Collection<AbstractRemoteNode> _writeQuorum = this.getWriteQuorum( _connection );
                    LOGGER.trace( "execute on {}", _writeQuorum );

                    final Map<AbstractRemoteNode, RemoteExecuteResult> _executeRemoteResults;
                    try {
                        // EXECUTE the statements
                        final RspList<RemoteExecuteResult> _executeResponseList = _connection.getCluster().execute( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _parameterValues, _maxRowsInFirstFrame,
                                _writeQuorum
                        );
                        // handle the EXECUTE responses
                        _executeRemoteResults = ClusterUtils.getRemoteResults( _connection.getCluster(), _executeResponseList );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    for ( AbstractRemoteNode _node : _executeRemoteResults.keySet() ) {
                        _connection.addAccessedNode( _node, RemoteConnectionHandle.fromConnectionHandle( _connection.getConnectionHandle() ) );
                    }

                    return _statement.createResultSet( _executeRemoteResults,
                            /* mergeResults */ ( __origins ) -> __origins.entrySet().iterator().next().getValue().toExecuteResult(),
                            /* fetch */ ( __origins, __connection, __statement, __offset, __fetchMaxRowCount ) -> {
                                try {
                                    return __origins.entrySet().iterator().next().getKey().fetch( RemoteStatementHandle.fromStatementHandle( __statement.getStatementHandle() ), __offset, __fetchMaxRowCount ).toFrame();
                                } catch ( RemoteException e ) {
                                    throw Utils.wrapException( e );
                                }
                            } );
                },
                //
                /* executeBatch */ ( _connection, _transaction, _statement, _listOfUpdateBatches ) -> {
                    //
                    final Collection<AbstractRemoteNode> _writeQuorum = this.getWriteQuorum( _connection );
                    LOGGER.trace( "executeBatch on {}", _writeQuorum );

                    final Map<AbstractRemoteNode, RemoteExecuteBatchResult> _executeRemoteBatchResults;
                    try {
                        // EXECUTE the statements
                        final RspList<RemoteExecuteBatchResult> _executeBatchResponseList = _connection.getCluster().executeBatch( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _listOfUpdateBatches,
                                _writeQuorum
                        );
                        // handle the EXECUTE responses
                        _executeRemoteBatchResults = ClusterUtils.getRemoteResults( _connection.getCluster(), _executeBatchResponseList );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    for ( AbstractRemoteNode _node : _executeRemoteBatchResults.keySet() ) {
                        _connection.addAccessedNode( _node, RemoteConnectionHandle.fromConnectionHandle( _connection.getConnectionHandle() ) );
                    }

                    return _statement.createBatchResultSet( _executeRemoteBatchResults,
                            /* mergeResults */ ( __origins ) -> __origins.entrySet().iterator().next().getValue().toExecuteBatchResult() );
                } );
    }


    @Override
    public StatementInfos prepareDataQuery( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {

        // PREPARE on ALL nodes
        final RspList<RemoteStatementHandle> prepareResponseList = connection.getCluster().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount );
        // handle the responses of the PREPARE execution
        final Map<AbstractRemoteNode, RemoteStatementHandle> prepareRemoteResults = ClusterUtils.getRemoteResults( connection.getCluster(), prepareResponseList );

        return connection.createPreparedStatement( statement, prepareRemoteResults,
                //
                /* signatureMerge */ ( _remoteStatements ) -> {
                    // BEGIN HACK
                    return _remoteStatements.values().iterator().next().toStatementHandle().signature;
                    // END HACK
                },
                //
                /* execute */ ( _connection, _transaction, _statement, _parameterValues, _maxRowsInFirstFrame ) -> {
                    //
                    final Collection<AbstractRemoteNode> _readQuorum = this.getReadQuorum( _connection );
                    LOGGER.trace( "execute on {}", _readQuorum );

                    final Map<AbstractRemoteNode, RemoteExecuteResult> _executeRemoteResults;
                    try {
                        // EXECUTE the statements
                        final RspList<RemoteExecuteResult> _executeResponseList = _connection.getCluster().execute( RemoteTransactionHandle.fromTransactionHandle( _transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _parameterValues, _maxRowsInFirstFrame,
                                _readQuorum
                        );
                        // handle the EXECUTE responses
                        _executeRemoteResults = ClusterUtils.getRemoteResults( _connection.getCluster(), _executeResponseList );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    for ( AbstractRemoteNode _node : _executeRemoteResults.keySet() ) {
                        _connection.addAccessedNode( _node, RemoteConnectionHandle.fromConnectionHandle( _connection.getConnectionHandle() ) );
                    }

                    return _statement.createResultSet( _executeRemoteResults,
                            /* mergeResults */ ( __origins ) -> __origins.entrySet().iterator().next().getValue().toExecuteResult(),
                            /* fetch */ ( __origins, __connection, __statement, __offset, __fetchMaxRowCount ) -> {
                                try {
                                    return __origins.entrySet().iterator().next().getKey().fetch( RemoteStatementHandle.fromStatementHandle( __statement.getStatementHandle() ), __offset, __fetchMaxRowCount ).toFrame();
                                } catch ( RemoteException e ) {
                                    throw Utils.wrapException( e );
                                }
                            } );
                },
                //
                /* executeBatch */ ( _connection, _transaction, _statement, _listOfUpdateBatches ) -> {
                    //
                    throw Utils.wrapException( new SQLException( "SELECT statements cannot be executed in a batch context." ) );
                } );
    }


    @Override
    public Map<AbstractRemoteNode, RemoteStatementHandle> prepareDataQuery( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount, Collection<AbstractRemoteNode> executionTargets ) throws RemoteException {
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    @Override
    public ResultSetInfos prepareAndExecuteTransactionCommit( final ConnectionInfos connection, final TransactionInfos transaction, final StatementInfos statement, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final PrepareCallback callback ) throws RemoteException {
        this.transactionToReadQuorum.remove( transaction );
        this.transactionToWriteQuorum.remove( transaction );
        return super.prepareAndExecuteTransactionCommit( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
    }


    @Override
    public ResultSetInfos prepareAndExecuteTransactionRollback( final ConnectionInfos connection, final TransactionInfos transaction, final StatementInfos statement, final SqlNode sql, final long maxRowCount, final int maxRowsInFirstFrame, final PrepareCallback callback ) throws RemoteException {
        this.transactionToReadQuorum.remove( transaction );
        this.transactionToWriteQuorum.remove( transaction );
        return super.prepareAndExecuteTransactionRollback( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, callback );
    }


    @Override
    public void commit( final ConnectionInfos connection, final TransactionInfos transaction ) throws RemoteException {
        this.transactionToReadQuorum.remove( transaction );
        this.transactionToWriteQuorum.remove( transaction );
        super.commit( connection, transaction );
    }


    @Override
    public void rollback( final ConnectionInfos connection, final TransactionInfos transaction ) throws RemoteException {
        this.transactionToReadQuorum.remove( transaction );
        this.transactionToWriteQuorum.remove( transaction );
        super.rollback( connection, transaction );
    }


    @Override
    public FragmentationProtocol setFragmentationProtocol( FragmentationProtocol fragmentationProtocol ) {
        return null;
    }


    @Override
    public PlacementProtocol setAllocationProtocol( PlacementProtocol placementProtocol ) {
        return null;
    }


    public static class ROWA extends QuorumReplication {

        public ROWA() {
            super(
                    connection -> Collections.singletonList( connection.getCluster().getLocalNodeAsRemoteNode() ), // read quorum
                    connection -> Collections.unmodifiableList( connection.getCluster().getMembers() ) // write quorum
            );
        }


        /**
         * For ROWA we do not need to alter the tables to include version columns.
         */
        @Override
        public ResultSetInfos prepareAndExecuteDataDefinition( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
            final Collection<AbstractRemoteNode> quorum = this.getAllNodes( connection.getCluster() );
            LOGGER.trace( "prepareAndExecute[DataDefinition] on {}", quorum );

            // execute on ALL nodes
            final RspList<RemoteExecuteResult> responseList = connection.getCluster().prepareAndExecuteDataDefinition( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, sql, maxRowCount, maxRowsInFirstFrame, quorum );
            // handle the responses of the execution
            final Map<AbstractRemoteNode, RemoteExecuteResult> remoteResults = ClusterUtils.getRemoteResults( connection.getCluster(), responseList );

            for ( AbstractRemoteNode _node : remoteResults.keySet() ) {
                connection.addAccessedNode( _node, RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ) );
            }

            return statement.createResultSet( remoteResults,
                    /* mergeResults */ ( _origins ) -> _origins.entrySet().iterator().next().getValue().toExecuteResult(),
                    /* fetch */ ( _origins, _connection, _statement, _offset, _fetchMaxRowCount ) -> {
                        try {
                            return _origins.entrySet().iterator().next().getKey().fetch( RemoteStatementHandle.fromStatementHandle( _statement.getStatementHandle() ), _offset, _fetchMaxRowCount ).toFrame();
                        } catch ( RemoteException e ) {
                            throw Utils.wrapException( e );
                        }
                    } );
        }
    }
}
