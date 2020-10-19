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
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.calcite.avatica.Meta.PrepareCallback;
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
import org.polypheny.fram.protocols.fragmentation.MergeFunctions;
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
import org.polypheny.fram.standalone.StatementInfos.PreparedStatementInfos;
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

    public final Function1<ConnectionInfos, Set<AbstractRemoteNode>> readQuorumFunction;
    public final Function1<ConnectionInfos, Set<AbstractRemoteNode>> writeQuorumFunction;

    public final boolean WORK_IN_PROGRESS = true;

    private final Map<TransactionInfos, Set<AbstractRemoteNode>> transactionToReadQuorum = new ConcurrentHashMap<>();
    private final Map<TransactionInfos, Set<AbstractRemoteNode>> transactionToWriteQuorum = new ConcurrentHashMap<>();


    public QuorumReplication( final Function1<ConnectionInfos, Set<AbstractRemoteNode>> readQuorumFunction, final Function1<ConnectionInfos, Set<AbstractRemoteNode>> writeQuorumFunction ) {
        this.readQuorumFunction = readQuorumFunction;
        this.writeQuorumFunction = writeQuorumFunction;
    }


    /**
     * For 1SR consistency, it is required that the read and the write quorum have at least one node in common.
     */
    protected Set<AbstractRemoteNode> getReadQuorum( final ConnectionInfos connection ) {
        return this.transactionToReadQuorum.computeIfAbsent( connection.getTransaction(), __ -> this.readQuorumFunction.apply( connection ) );
    }


    /**
     * For 1SR consistency, it is required that the read and the write quorum have at least one node in common.
     */
    protected Set<AbstractRemoteNode> getWriteQuorum( final ConnectionInfos connection ) {
        return this.transactionToWriteQuorum.computeIfAbsent( connection.getTransaction(), __ -> this.writeQuorumFunction.apply( connection ) );
    }


    @Override
    public ResultSetInfos prepareAndExecuteDataDefinition( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, final SqlNode catalogSql, SqlNode storeSql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        if ( WORK_IN_PROGRESS ) {
            LOGGER.warn( "Quorum Replication currently without(!) version columns!" );

            // execute on all nodes
            final RspList<RemoteExecuteResult> executeResponseList = connection.getCluster().prepareAndExecuteDataDefinition( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), catalogSql, storeSql, maxRowCount, maxRowsInFirstFrame );

            final Map<AbstractRemoteNode, RemoteExecuteResult> executeResults = ClusterUtils.getRemoteResults( connection.getCluster(), executeResponseList );

            for ( AbstractRemoteNode node : executeResults.keySet() ) {
                connection.addAccessedNode( node, RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ) );
                statement.addAccessedNode( node, RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ) );
            }

            return statement.createResultSet( executeResults.keySet(),
                    /* executeResultSupplier */ () -> executeResults.values().iterator().next().toExecuteResult(),
                    /* generatedKeysSupplier */ () -> executeResults.values().iterator().next().getGeneratedKeys(),
                    /* fetch */ ( fetchOrigins, fetchConnection, fetchStatement, fetchOffset, fetchMaxRowCount ) -> {
                        try {
                            return fetchOrigins.iterator().next().fetch( RemoteStatementHandle.fromStatementHandle( fetchStatement.getStatementHandle() ), fetchOffset, fetchMaxRowCount ).toFrame();
                        } catch ( RemoteException ex ) {
                            throw Utils.wrapException( ex );
                        }
                    } );
        }

        switch ( catalogSql.getKind() ) {
            case CREATE_TABLE:
                return prepareAndExecuteDataDefinitionCreateTable( connection, transaction, statement, (SqlCreateTable) catalogSql, (SqlCreateTable) storeSql, maxRowCount, maxRowsInFirstFrame, callback );

            case DROP_TABLE:
                return prepareAndExecuteDataDefinitionDropTable( connection, transaction, statement, (SqlDropTable) catalogSql, (SqlDropTable) storeSql, maxRowCount, maxRowsInFirstFrame, callback );

            case CREATE_INDEX: // todo - do we need to alter the index, too? See TPC-C DDL script.
                return prepareAndExecuteDataDefinitionCreateIndex( connection, transaction, statement, (SqlCreateIndex) catalogSql, (SqlCreateIndex) storeSql, maxRowCount, maxRowsInFirstFrame, callback );

            case ALTER_TABLE:
                return prepareAndExecuteDataDefinitionAlterTable( connection, transaction, statement, (SqlAlterTable) catalogSql, (SqlAlterTable) storeSql, maxRowCount, maxRowsInFirstFrame, callback );

            default:
                throw new UnsupportedOperationException( "Not implemented yet." );
        }
    }


    @Override
    public <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteDataDefinition( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode catalogSql, SqlNode storeSql, long maxRowCount, int maxRowsInFirstFrame, Set<NodeType> executionTargets ) throws RemoteException {
        throw new UnsupportedOperationException( "Not supported." );
    }


    protected ResultSetInfos prepareAndExecuteDataDefinitionCreateTable( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, final SqlCreateTable catalogSql, SqlCreateTable storeSql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {

        final SqlNodeList originColumnList = storeSql.operand( 1 );
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

        final SqlParserPos pos = storeSql.getParserPosition();
        final boolean replace = storeSql.getReplace();
        final boolean ifNotExists = storeSql.toSqlString( AnsiSqlDialect.DEFAULT ).getSql().contains( "IF NOT EXISTS" ); // the only way to get the info without using reflection
        final SqlIdentifier name = storeSql.operand( 0 );
        final SqlNodeList columnList = new SqlNodeList( columns, originColumnList.getParserPosition() );
        final SqlNode query = storeSql.operand( 2 );

        storeSql = SqlDdlNodes.createTable( pos, replace, ifNotExists, name, columnList, query );

        final Set<AbstractRemoteNode> quorum = this.getAllNodes( connection.getCluster() );
        LOGGER.trace( "prepareAndExecute[DataDefinition][CreateTable] on {}", quorum );

        final RspList<RemoteExecuteResult> executeResponseList = connection.getCluster().prepareAndExecuteDataDefinition( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), catalogSql, storeSql, maxRowCount, maxRowsInFirstFrame, quorum );

        final Map<AbstractRemoteNode, RemoteExecuteResult> executeResults = ClusterUtils.getRemoteResults( connection.getCluster(), executeResponseList );

        for ( AbstractRemoteNode node : executeResults.keySet() ) {
            connection.addAccessedNode( node, RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ) );
            statement.addAccessedNode( node, RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ) );
        }

        return statement.createResultSet( executeResults.keySet(),
                /* executeResultSupplier */ () -> executeResults.values().iterator().next().toExecuteResult(),
                /* generatedKeysSupplier */ () -> executeResults.values().iterator().next().getGeneratedKeys(),
                /* fetch */ ( fetchOrigins, fetchConnection, fetchStatement, fetchOffset, fetchMaxRowCount ) -> {
                    try {
                        return fetchOrigins.iterator().next().fetch( RemoteStatementHandle.fromStatementHandle( fetchStatement.getStatementHandle() ), fetchOffset, fetchMaxRowCount ).toFrame();
                    } catch ( RemoteException e ) {
                        throw Utils.wrapException( e );
                    }
                } );
    }


    protected ResultSetInfos prepareAndExecuteDataDefinitionDropTable( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlDropTable catalogSql, SqlDropTable storeSql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    protected ResultSetInfos prepareAndExecuteDataDefinitionAlterTable( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlAlterTable catalogSql, SqlAlterTable storeSql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    protected ResultSetInfos prepareAndExecuteDataDefinitionCreateIndex( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlCreateIndex catalogSql, SqlCreateIndex storeSql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    @Override
    public ResultSetInfos prepareAndExecuteDataManipulation( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        final Set<AbstractRemoteNode> writeQuorum = this.getWriteQuorum( connection );
        LOGGER.trace( "prepareAndExecute[DataManipulation] on {}", writeQuorum );

        // EXECUTE the statement
        final RspList<RemoteExecuteResult> executeResponseList = connection.getCluster().prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, maxRowsInFirstFrame,
                writeQuorum
        );
        // handle the responses
        final Map<AbstractRemoteNode, RemoteExecuteResult> executeResults = ClusterUtils.getRemoteResults( connection.getCluster(), executeResponseList );

        for ( AbstractRemoteNode node : executeResults.keySet() ) {
            connection.addAccessedNode( node, RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ) );
        }

        return statement.createResultSet( executeResults.keySet(),
                /* executeResultSupplier */ () -> executeResults.values().iterator().next().toExecuteResult(),
                /* generatedKeysSupplier */ () -> executeResults.values().iterator().next().getGeneratedKeys(),
                /* fetch */ ( fetchOrigins, fetchConnection, fetchStatement, fetchOffset, fetchMaxRowCount ) -> {
                    try {
                        return fetchOrigins.iterator().next().fetch( RemoteStatementHandle.fromStatementHandle( fetchStatement.getStatementHandle() ), fetchOffset, fetchMaxRowCount ).toFrame();
                    } catch ( RemoteException e ) {
                        throw Utils.wrapException( e );
                    }
                } );
    }


    @Override
    public <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteDataManipulation( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, Set<NodeType> executionTargets ) throws RemoteException {
        throw new UnsupportedOperationException( "Not supported." );
    }


    @Override
    public ResultSetInfos prepareAndExecuteDataQuery( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        final Set<AbstractRemoteNode> readQuorum = this.getReadQuorum( connection );
        LOGGER.trace( "prepareAndExecute[DataQuery] on {}", readQuorum );

        // EXECUTE the statement
        final RspList<RemoteExecuteResult> executeResponseList = connection.getCluster().prepareAndExecute( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, maxRowsInFirstFrame,
                readQuorum
        );
        // handle the responses
        final Map<AbstractRemoteNode, RemoteExecuteResult> executeResults = ClusterUtils.getRemoteResults( connection.getCluster(), executeResponseList );

        for ( AbstractRemoteNode node : executeResults.keySet() ) {
            connection.addAccessedNode( node, RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ) );
        }

        return statement.createResultSet( executeResults.keySet(),
                /* executeResultSupplier */ () -> executeResults.values().iterator().next().toExecuteResult(),
                /* generatedKeysResults */ () -> executeResults.values().iterator().next().getGeneratedKeys(),
                /* fetch */ ( fetchOrigins, fetchConnection, fetchStatement, fetchOffset, fetchMaxRowCount ) -> {
                    try {
                        return fetchOrigins.iterator().next().fetch( RemoteStatementHandle.fromStatementHandle( fetchStatement.getStatementHandle() ), fetchOffset, fetchMaxRowCount ).toFrame();
                    } catch ( RemoteException e ) {
                        throw Utils.wrapException( e );
                    }
                } );
    }


    @Override
    public <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteDataQuery( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, Set<NodeType> executionTargets ) throws RemoteException {
        throw new UnsupportedOperationException( "Not supported." );
    }


    @Override
    public PreparedStatementInfos prepareDataManipulation( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {
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
                /* signatureSupplier */ () -> MergeFunctions.mergeSignature( statement, prepareRemoteResults, sql ),
                //
                /* execute */ ( executeConnection, executeTransaction, executeStatement, executeParameterValues, executeMaxRowsInFirstFrame ) -> {
                    //
                    final Set<AbstractRemoteNode> writeQuorum = this.getWriteQuorum( executeConnection );
                    LOGGER.trace( "execute on {}", writeQuorum );

                    final Map<AbstractRemoteNode, RemoteExecuteResult> executeResults;
                    try {
                        // EXECUTE the statements
                        final RspList<RemoteExecuteResult> executeResponseList = executeConnection.getCluster().execute( RemoteTransactionHandle.fromTransactionHandle( executeTransaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( executeStatement.getStatementHandle() ), executeParameterValues, executeMaxRowsInFirstFrame,
                                writeQuorum
                        );
                        // handle the EXECUTE responses
                        executeResults = ClusterUtils.getRemoteResults( executeConnection.getCluster(), executeResponseList );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    for ( AbstractRemoteNode node : executeResults.keySet() ) {
                        executeConnection.addAccessedNode( node, RemoteConnectionHandle.fromConnectionHandle( executeConnection.getConnectionHandle() ) );
                    }

                    return executeStatement.createResultSet( executeResults.keySet(),
                            /* executeResultSupplier */ () -> executeResults.values().iterator().next().toExecuteResult(),
                            /* generatedKeysResults */ () -> executeResults.values().iterator().next().getGeneratedKeys(),
                            /* fetch */ ( fetchOrigins, fetchConnection, fetchStatement, fetchOffset, fetchMaxRowCount ) -> {
                                try {
                                    return fetchOrigins.iterator().next().fetch( RemoteStatementHandle.fromStatementHandle( fetchStatement.getStatementHandle() ), fetchOffset, fetchMaxRowCount ).toFrame();
                                } catch ( RemoteException e ) {
                                    throw Utils.wrapException( e );
                                }
                            } );
                },
                //
                /* executeBatch */ ( executeBatchConnection, executeBatchTransaction, executeBatchStatement, executeBatchListOfUpdateBatches ) -> {
                    //
                    final Set<AbstractRemoteNode> writeQuorum = this.getWriteQuorum( executeBatchConnection );
                    LOGGER.trace( "executeBatch on {}", writeQuorum );

                    final Map<AbstractRemoteNode, RemoteExecuteBatchResult> executeBatchResults;
                    try {
                        // EXECUTE the statements
                        final RspList<RemoteExecuteBatchResult> executeBatchResponseList = executeBatchConnection.getCluster().executeBatch( RemoteTransactionHandle.fromTransactionHandle( executeBatchTransaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( executeBatchStatement.getStatementHandle() ), executeBatchListOfUpdateBatches,
                                writeQuorum
                        );
                        // handle the EXECUTE responses
                        executeBatchResults = ClusterUtils.getRemoteResults( executeBatchConnection.getCluster(), executeBatchResponseList );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    for ( AbstractRemoteNode node : executeBatchResults.keySet() ) {
                        executeBatchConnection.addAccessedNode( node, RemoteConnectionHandle.fromConnectionHandle( executeBatchConnection.getConnectionHandle() ) );
                    }

                    return executeBatchStatement.createBatchResultSet( executeBatchResults.keySet(),
                            /* executeBatchResultSupplier */ () -> executeBatchResults.values().iterator().next().toExecuteBatchResult(),
                            /* generatedKeysSupplier */ () -> executeBatchResults.values().iterator().next().getGeneratedKeys() );
                } );
    }


    protected PreparedStatementInfos prepareDataManipulationInsert( ConnectionInfos connection, StatementInfos statement, SqlInsert sql, long maxRowCount ) throws RemoteException {
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

        final Set<AbstractRemoteNode> quorum = this.getWriteQuorum( connection );
        LOGGER.trace( "prepare[DataManipulation][Insert] on {}", quorum );

        final RspList<RemoteStatementHandle> responseList = connection.getCluster().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount, quorum );
        final Map<AbstractRemoteNode, RemoteStatementHandle> prepareRemoteResults = ClusterUtils.getRemoteResults( connection.getCluster(), responseList );

        return connection.createPreparedStatement( statement, prepareRemoteResults,
                //
                /* signatureSupplier */ () -> MergeFunctions.mergeSignature( statement, prepareRemoteResults, sql ),
                //
                /* execute */ ( executeConnection, executeTransaction, executeStatement, executeParameterValues, executeMaxRowsInFirstFrame ) -> {
                    //
                    final Set<AbstractRemoteNode> writeQuorum = this.getWriteQuorum( executeConnection );
                    LOGGER.trace( "execute on {}", writeQuorum );

                    final Map<AbstractRemoteNode, RemoteExecuteResult> executeResults;
                    try {
                        // EXECUTE the statements
                        final RspList<RemoteExecuteResult> executeResponseList = executeConnection.getCluster().execute( RemoteTransactionHandle.fromTransactionHandle( executeTransaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( executeStatement.getStatementHandle() ), executeParameterValues, executeMaxRowsInFirstFrame,
                                writeQuorum
                        );
                        // handle the EXECUTE responses
                        executeResults = ClusterUtils.getRemoteResults( executeConnection.getCluster(), executeResponseList );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    for ( AbstractRemoteNode node : executeResults.keySet() ) {
                        executeConnection.addAccessedNode( node, RemoteConnectionHandle.fromConnectionHandle( executeConnection.getConnectionHandle() ) );
                    }

                    return executeStatement.createResultSet( executeResults.keySet(),
                            /* executeResultSupplier */ () -> executeResults.values().iterator().next().toExecuteResult(),
                            /* generatedKeysResults */ () -> executeResults.values().iterator().next().getGeneratedKeys(),
                            /* fetch */ ( fetchOrigins, fetchConnection, fetchStatement, fetchOffset, fetchMaxRowCount ) -> {
                                try {
                                    return fetchOrigins.iterator().next().fetch( RemoteStatementHandle.fromStatementHandle( fetchStatement.getStatementHandle() ), fetchOffset, fetchMaxRowCount ).toFrame();
                                } catch ( RemoteException e ) {
                                    throw Utils.wrapException( e );
                                }
                            } );
                },
                //
                /* executeBatch */ ( executeBatchConnection, executeBatchTransaction, executeBatchStatement, executeBatchListOfUpdateBatches ) -> {
                    //
                    final Set<AbstractRemoteNode> writeQuorum = this.getWriteQuorum( executeBatchConnection );
                    LOGGER.trace( "executeBatch on {}", writeQuorum );

                    final Map<AbstractRemoteNode, RemoteExecuteBatchResult> executeBatchResults;
                    try {
                        // EXECUTE the statements
                        final RspList<RemoteExecuteBatchResult> executeBatchResponseList = executeBatchConnection.getCluster().executeBatch( RemoteTransactionHandle.fromTransactionHandle( executeBatchTransaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( executeBatchStatement.getStatementHandle() ), executeBatchListOfUpdateBatches,
                                writeQuorum
                        );
                        // handle the EXECUTE responses
                        executeBatchResults = ClusterUtils.getRemoteResults( executeBatchConnection.getCluster(), executeBatchResponseList );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    for ( AbstractRemoteNode node : executeBatchResults.keySet() ) {
                        executeBatchConnection.addAccessedNode( node, RemoteConnectionHandle.fromConnectionHandle( executeBatchConnection.getConnectionHandle() ) );
                    }

                    return executeBatchStatement.createBatchResultSet( executeBatchResults.keySet(),
                            /* executeBatchResultSupplier */ () -> executeBatchResults.values().iterator().next().toExecuteBatchResult(),
                            /* generatedKeysSupplier */ () -> executeBatchResults.values().iterator().next().getGeneratedKeys() );
                } );
    }


    @Override
    public <NodeType extends Node> Map<NodeType, PreparedStatementInfos> prepareDataManipulation( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount, Set<NodeType> executionTargets ) throws RemoteException {
        throw new UnsupportedOperationException( "Not supported." );
    }


    @Override
    public PreparedStatementInfos prepareDataQuery( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {

        // PREPARE on ALL nodes
        final RspList<RemoteStatementHandle> prepareResponseList = connection.getCluster().prepare( RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), sql, maxRowCount );
        // handle the responses of the PREPARE execution
        final Map<AbstractRemoteNode, RemoteStatementHandle> prepareRemoteResults = ClusterUtils.getRemoteResults( connection.getCluster(), prepareResponseList );

        return connection.createPreparedStatement( statement, prepareRemoteResults,
                //
                /* signatureSupplier */ () -> MergeFunctions.mergeSignature( statement, prepareRemoteResults, sql ),
                //
                /* execute */ ( executeConnection, executeTransaction, executeStatement, executeParameterValues, executeMaxRowsInFirstFrame ) -> {
                    //
                    final Set<AbstractRemoteNode> readQuorum = this.getReadQuorum( executeConnection );
                    LOGGER.trace( "execute on {}", readQuorum );

                    final Map<AbstractRemoteNode, RemoteExecuteResult> executeResults;
                    try {
                        // EXECUTE the statements
                        final RspList<RemoteExecuteResult> executeResponseList = executeConnection.getCluster().execute( RemoteTransactionHandle.fromTransactionHandle( executeTransaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( executeStatement.getStatementHandle() ), executeParameterValues, executeMaxRowsInFirstFrame,
                                readQuorum
                        );
                        // handle the EXECUTE responses
                        executeResults = ClusterUtils.getRemoteResults( executeConnection.getCluster(), executeResponseList );
                    } catch ( RemoteException ex ) {
                        throw Utils.wrapException( ex );
                    }

                    for ( AbstractRemoteNode node : executeResults.keySet() ) {
                        executeConnection.addAccessedNode( node, RemoteConnectionHandle.fromConnectionHandle( executeConnection.getConnectionHandle() ) );
                    }

                    return executeStatement.createResultSet( executeResults.keySet(),
                            /* executeResultSupplier */ () -> executeResults.values().iterator().next().toExecuteResult(),
                            /* generatedKeysSupplier */ () -> executeResults.values().iterator().next().getGeneratedKeys(),
                            /* fetch */ ( fetchOrigins, fetchConnection, fetchStatement, fetchOffset, fetchMaxRowCount ) -> {
                                try {
                                    return fetchOrigins.iterator().next().fetch( RemoteStatementHandle.fromStatementHandle( fetchStatement.getStatementHandle() ), fetchOffset, fetchMaxRowCount ).toFrame();
                                } catch ( RemoteException e ) {
                                    throw Utils.wrapException( e );
                                }
                            } );
                },
                //
                /* executeBatch */ ( executeBatchConnection, executeBatchTransaction, executeBatchStatement, executeBatchListOfUpdateBatches ) -> {
                    //
                    throw Utils.wrapException( new SQLFeatureNotSupportedException( "SELECT statements cannot be executed in a batch context." ) );
                } );
    }


    @Override
    public <NodeType extends Node> Map<NodeType, PreparedStatementInfos> prepareDataQuery( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount, Set<NodeType> executionTargets ) throws RemoteException {
        throw new UnsupportedOperationException( "Not supported." );
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
                    connection -> Collections.singleton( connection.getCluster().getLocalNodeAsRemoteNode() ), // read quorum
                    connection -> Collections.unmodifiableSet( connection.getCluster().getMembers() ) // write quorum
            );
        }


        /**
         * For ROWA we do not need to alter the tables to include version columns.
         */
        @Override
        public ResultSetInfos prepareAndExecuteDataDefinition( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, final SqlNode catalogSql, SqlNode storeSql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
            final Set<AbstractRemoteNode> quorum = this.getAllNodes( connection.getCluster() );
            LOGGER.trace( "prepareAndExecute[DataDefinition] on {}", quorum );

            // execute on ALL nodes
            final RspList<RemoteExecuteResult> responseList = connection.getCluster().prepareAndExecuteDataDefinition( RemoteTransactionHandle.fromTransactionHandle( transaction.getTransactionHandle() ), RemoteStatementHandle.fromStatementHandle( statement.getStatementHandle() ), catalogSql, storeSql, maxRowCount, maxRowsInFirstFrame, quorum );
            // handle the responses of the execution
            final Map<AbstractRemoteNode, RemoteExecuteResult> remoteResults = ClusterUtils.getRemoteResults( connection.getCluster(), responseList );

            for ( AbstractRemoteNode node : remoteResults.keySet() ) {
                connection.addAccessedNode( node, RemoteConnectionHandle.fromConnectionHandle( connection.getConnectionHandle() ) );
            }

            return statement.createResultSet( remoteResults.keySet(),
                    /* executeResultSupplier */ () -> remoteResults.values().iterator().next().toExecuteResult(),
                    /* generatedKeysSupplier */ () -> remoteResults.values().iterator().next().getGeneratedKeys(),
                    /* fetch */ ( fetchOrigins, fetchConnection, fetchStatement, fetchOffset, fetchMaxRowCount ) -> {
                        try {
                            return fetchOrigins.iterator().next().fetch( RemoteStatementHandle.fromStatementHandle( fetchStatement.getStatementHandle() ), fetchOffset, fetchMaxRowCount ).toFrame();
                        } catch ( RemoteException e ) {
                            throw Utils.wrapException( e );
                        }
                    } );
        }
    }
}
