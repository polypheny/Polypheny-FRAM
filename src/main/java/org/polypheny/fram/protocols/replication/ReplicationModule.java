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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlDdlNodes;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.polypheny.fram.Node;
import org.polypheny.fram.protocols.AbstractProtocol;
import org.polypheny.fram.protocols.Protocol.ReplicationProtocol;
import org.polypheny.fram.protocols.fragmentation.Fragment;
import org.polypheny.fram.protocols.fragmentation.MergeFunctions;
import org.polypheny.fram.remote.PhysicalNode;
import org.polypheny.fram.standalone.ConnectionInfos;
import org.polypheny.fram.standalone.ResultSetInfos;
import org.polypheny.fram.standalone.ResultSetInfos.BatchResultSet;
import org.polypheny.fram.standalone.ResultSetInfos.QueryResultSet;
import org.polypheny.fram.standalone.StatementInfos;
import org.polypheny.fram.standalone.StatementInfos.PreparedStatementInfos;
import org.polypheny.fram.standalone.TransactionInfos;
import org.polypheny.fram.standalone.Utils;
import org.polypheny.fram.standalone.Utils.WrappingException;
import org.polypheny.fram.standalone.parser.sql.SqlNodeUtils;
import org.polypheny.fram.standalone.parser.sql.ddl.SqlCreateIndex;
import org.polypheny.fram.standalone.parser.sql.ddl.SqlDdlIndexNodes;
import org.polypheny.fram.standalone.parser.sql.ddl.SqlDropIndex;


public class ReplicationModule extends AbstractProtocol implements ReplicationProtocol {


    public static ReplicationSchema currentSchema = new ReplicationSchema();


    /**
     * For 1SR consistency, it is required that the read and the write quorum have at least one node in common.
     */
    protected Set<PhysicalNode> getReadQuorum( final ConnectionInfos connection ) {
        return Collections.singleton( connection.getCluster().getLocalNode() );
    }


    /**
     * For 1SR consistency, it is required that the read and the write quorum have at least one node in common.
     */
    protected Set<PhysicalNode> getWriteQuorum( final ConnectionInfos connection ) {
        return Collections.singleton( connection.getCluster().getLocalNode() );
    }


    protected <NodeType extends Node> Set<NodeType> getReadQuorum( final Set<NodeType> nodes ) {
        return Collections.singleton( nodes.iterator().next() );
    }


    protected <NodeType extends Node> Set<NodeType> getWriteQuorum( final Set<NodeType> nodes ) {
        return nodes;
    }


    private <NodeType extends Node> Set<? extends Replica> lookupReplicas( NodeType node ) {
        throw new UnsupportedOperationException( "Not implemented yet." );
    }


    @Override
    public ResultSetInfos prepareAndExecuteDataDefinition( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode catalogSql, SqlNode storeSql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {

        // execute on ALL replicas
        final Map<Replica, ResultSetInfos> remoteResults = this.prepareAndExecuteDataDefinition( connection, transaction, statement, catalogSql, storeSql, maxRowCount, maxRowsInFirstFrame,
                this.currentSchema.allReplicas()
        );

        return statement.createResultSet( remoteResults.keySet(),
                /* executeResultSupplier */ () -> remoteResults.values().iterator().next().getExecuteResult(),
                /* generatedKeysSupplier */ () -> remoteResults.values().iterator().next().getGeneratedKeys()
        );
    }


    @Override
    public <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteDataDefinition( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode catalogSql, SqlNode storeSql, long maxRowCount, int maxRowsInFirstFrame, Set<NodeType> executionTargets ) throws RemoteException {
        // TODO: forall Replica -> append replica_id and call down.prepareAndExecuteDataDefinition with the new sql

        final Function1<Replica, SqlNode> alteredSqlFunction;
        switch ( storeSql.getKind() ) {
            case CREATE_TABLE:
                alteredSqlFunction = ( replica ) -> {
                    final SqlCreateTable original = (SqlCreateTable) storeSql;
                    final SqlIdentifier originalTableIdentifier = original.operand( 0 );
                    final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( originalTableIdentifier, /* table name */ 0 )
                            + "_" + replica.id.toString();

                    return SqlDdlNodes.createTable( original.getParserPosition(), original.getReplace(), original.toSqlString( AnsiSqlDialect.DEFAULT ).getSql().contains( "IF NOT EXISTS" ),
                            SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( originalTableIdentifier, /* table name */ 0, tableName ), original.operand( 1 ), original.operand( 2 ) );
                };
                break;

            case DROP_TABLE:
                alteredSqlFunction = ( replica ) -> {
                    final SqlDropTable original = (SqlDropTable) storeSql;
                    final SqlIdentifier originalTableIdentifier = original.operand( 0 );
                    final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( originalTableIdentifier, 0 )
                            + "_" + replica.id.toString();

                    return SqlDdlNodes.dropTable( original.getParserPosition(), original.toSqlString( AnsiSqlDialect.DEFAULT ).getSql().contains( "IF EXISTS" ),
                            SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( originalTableIdentifier, 0, tableName ) );
                };
                break;

            case CREATE_INDEX:
                alteredSqlFunction = ( replica ) -> {
                    final SqlCreateIndex original = (SqlCreateIndex) storeSql;
                    final SqlIdentifier originalIndexIdentifier = original.operand( 0 );
                    final String indexName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( originalIndexIdentifier, 0 )
                            + "_" + replica.id.toString();
                    final SqlIdentifier originalTableIdentifier = original.operand( 1 );
                    final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( originalTableIdentifier, 0 )
                            + "_" + replica.id.toString();

                    return SqlDdlIndexNodes.createIndex( original.getParserPosition(), original.getReplace(), original.toSqlString( AnsiSqlDialect.DEFAULT ).getSql().contains( "IF NOT EXISTS" ),
                            SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( originalIndexIdentifier, 0, indexName ),
                            SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( originalTableIdentifier, 0, tableName ),
                            original.operand( 2 ) );
                };
                break;

            case DROP_INDEX:
                alteredSqlFunction = ( replica ) -> {
                    final SqlDropIndex original = (SqlDropIndex) storeSql;
                    final SqlIdentifier originalIndexIdentifier = original.operand( 0 );
                    final String indexName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( originalIndexIdentifier, 0 )
                            + "_" + replica.id.toString();

                    return SqlDdlIndexNodes.dropIndex( original.getParserPosition(), original.toSqlString( AnsiSqlDialect.DEFAULT ).getSql().contains( "IF EXISTS" ),
                            SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( originalIndexIdentifier, 0, indexName ) );
                };
                break;

            default:
                throw new UnsupportedOperationException( "Case `" + storeSql.getKind() + "` not implemented yet." );
        }

        final Map<NodeType, ResultSetInfos> executeResults = new ConcurrentHashMap<>();
        try {
            executionTargets.parallelStream().forEach( executionTarget -> {
                final Map<Replica, ResultSetInfos> executeOnTargetResults = new HashMap<>();
                final Set<Replica> replicas;
                if ( executionTarget instanceof Replica ) {
                    replicas = Collections.singleton( (Replica) executionTarget );
                } else if ( executionTarget instanceof Fragment ) {
                    replicas = this.currentSchema.lookupReplicas( (Fragment) executionTarget );
                } else {
                    throw new IllegalArgumentException( "Type of `executionTarget` is not supported." );
                }

                replicas.parallelStream().forEach( replica -> {
                    final SqlNode alteredStoreSql = alteredSqlFunction.apply( replica );
                    try {
                        final Map<Replica, ResultSetInfos> executeResult = down.prepareAndExecuteDataDefinition( connection, transaction, statement, catalogSql, alteredStoreSql, maxRowCount, maxRowsInFirstFrame, Collections.singleton( replica ) );
                        executeOnTargetResults.put( replica, executeResult.get( replica ) );
                    } catch ( RemoteException e ) {
                        throw Utils.wrapException( e );
                    }
                } );

                executeResults.put( executionTarget, statement.createResultSet( executeOnTargetResults.keySet(),
                        /* executeResultSupplier */ () -> executeOnTargetResults.values().iterator().next().getExecuteResult(),
                        /* generatedKeysSupplier */ () -> executeOnTargetResults.values().iterator().next().getGeneratedKeys()
                ) );
            } );
        } catch ( WrappingException we ) {
            final Throwable t = Utils.xtractException( we );
            if ( t instanceof RemoteException ) {
                throw (RemoteException) t;
            } else {
                throw we;
            }
        }

        return executeResults;
    }


    @Override
    public ResultSetInfos prepareAndExecuteDataManipulation( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {
        return null;
    }


    @Override
    public <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteDataManipulation( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, Set<NodeType> executionTargets ) throws RemoteException {
        return null;
    }


    @Override
    public ResultSetInfos prepareAndExecuteDataQuery( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws RemoteException {

        // execute on ONE replica
        final Set<Replica> readQuorum = this.getReadQuorum( this.currentSchema.allReplicas() );

        final Map<Replica, ResultSetInfos> prepareAndExecuteResults = this.prepareAndExecuteDataQuery( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, readQuorum );

        return statement.createResultSet( prepareAndExecuteResults.keySet(),
                /* executeResultSupplier */ () -> prepareAndExecuteResults.values().iterator().next().getExecuteResult(),
                /* generatedKeysSupplier */ () -> prepareAndExecuteResults.values().iterator().next().getGeneratedKeys()
        );
    }


    @Override
    public <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteDataQuery( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, Set<NodeType> executionTargets ) throws RemoteException {

        final Function1<Replica, SqlNode> alteredSqlFunction;
        switch ( sql.getKind() ) {
            case SELECT:
                alteredSqlFunction = ( replica ) -> {
                    final SqlSelect original = (SqlSelect) sql;

                    final SqlNode alteredFrom;
                    switch ( original.getFrom().getKind() ) {
                        case AS: {
                            final SqlBasicCall as = (SqlBasicCall) original.getFrom();
                            final String replicaTableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( as.operand( 0 ), /* table name */ 0 )
                                    + "_" + replica.id.toString();
                            alteredFrom = SqlStdOperatorTable.AS.createCall( as.getParserPosition(), SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( as.operand( 0 ), 0, replicaTableName ), as.operand( 1 ) );
                            break;
                        }

                        case IDENTIFIER: {
                            final SqlIdentifier identifier = (SqlIdentifier) original.getFrom();
                            final String replicaTableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( identifier, /* table name */ 0 )
                                    + "_" + replica.id.toString();
                            alteredFrom = SqlStdOperatorTable.AS.createCall( identifier.getParserPosition(), SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( identifier, 0, replicaTableName ), identifier.getComponent( 0 ) );
                            break;
                        }

                        default:
                            throw new UnsupportedOperationException( "Case `" + original.getFrom().getKind() + "` not implemented yet." );
                    }

                    return new SqlSelect( original.getParserPosition(), /* keywordList */ original.operand( 0 ), /* selectList */ original.operand( 1 ),
                            alteredFrom,
                            /* where */ original.operand( 3 ), /* groupBy */ original.operand( 4 ), /* having */ original.operand( 5 ), /* windowDecls */ original.operand( 6 ), /* orderBy */ original.operand( 7 ), /* offset */ original.operand( 8 ), /* fetch */ original.operand( 9 ), /* hints */ original.operand( 10 ) );
                };
                break;

            default:
                throw new UnsupportedOperationException( "Case `" + sql.getKind() + "` not implemented yet." );
        }

        final Map<NodeType, ResultSetInfos> executeResults = new HashMap<>();
        try {
            executionTargets.parallelStream().forEach( ( executionTarget ) -> {
                final Map<Replica, ResultSetInfos> executeOnTargetResults = new HashMap<>();
                final Set<Replica> readQuorum;
                if ( executionTarget instanceof Replica ) {
                    readQuorum = Collections.singleton( (Replica) executionTarget );
                } else if ( executionTarget instanceof Fragment ) {
                    readQuorum = this.getReadQuorum(
                            this.currentSchema.lookupReplicas( (Fragment) executionTarget )
                    );
                } else {
                    throw new IllegalArgumentException( "Type of `executionTarget` is not supported." );
                }

                readQuorum.parallelStream().forEach( ( replica ) -> {
                    final SqlNode alteredSql = alteredSqlFunction.apply( replica );
                    try {
                        final Map<Replica, ResultSetInfos> executeResult = down.prepareAndExecuteDataQuery( connection, transaction, statement, alteredSql, maxRowCount, maxRowsInFirstFrame, Collections.singleton( replica ) );
                        executeOnTargetResults.put( replica, executeResult.get( replica ) );
                    } catch ( RemoteException e ) {
                        throw Utils.wrapException( e );
                    }
                } );

                executeResults.put( executionTarget, statement.createResultSet( executeOnTargetResults.keySet(),
                        /* executeResultSupplier */ () -> executeOnTargetResults.values().iterator().next().getExecuteResult(),
                        /* generatedKeysSupplier */ () -> executeOnTargetResults.values().iterator().next().getGeneratedKeys()
                ) );
            } );
        } catch ( WrappingException we ) {
            final Throwable t = Utils.xtractException( we );
            if ( t instanceof RemoteException ) {
                throw (RemoteException) t;
            } else {
                throw we;
            }
        }

        return executeResults;
    }


    @Override
    public PreparedStatementInfos prepareDataManipulation( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {

        // prepare on ALL replicas
        final Map<Replica, PreparedStatementInfos> preparedStatements = this.prepareDataManipulation( connection, statement, sql, maxRowCount,
                this.currentSchema.allReplicas()
        );

        return connection.createPreparedStatement( statement, preparedStatements,
                /* signatureSupplier */ () -> MergeFunctions.mergeSignature( statement, preparedStatements, sql ),
                //
                /* execute */ ( executeConnection, executeTransaction, executeStatement, executeParameterValues, executeMaxRowsInFirstFrame ) -> {
                    //
                    final Map<Replica, QueryResultSet> executeResults = new HashMap<>();

                    // todo: we might want to filter which nodes to execute the statements on (quorum case)
                    final Set<Replica> writeQuorum = this.getWriteQuorum(
                            this.currentSchema.allReplicas()
                    );

                    writeQuorum.parallelStream().forEach( executeReplica -> {
                        final PreparedStatementInfos preparedStatement;
                        synchronized ( preparedStatements ) {
                            if ( preparedStatements.get( executeReplica ) == null ) {
                                // if not prepared on that new? replica --> prepare and then use for execution
                                try {
                                    preparedStatements.putAll( ReplicationModule.this.prepareDataManipulation( connection, statement, sql, maxRowCount, Collections.singleton( executeReplica ) ) );
                                } catch ( RemoteException e ) {
                                    throw Utils.wrapException( e );
                                }
                            }
                            preparedStatement = preparedStatements.get( executeReplica );
                        }
                        executeResults.put( executeReplica, preparedStatement.execute( executeConnection, executeTransaction, preparedStatement, executeParameterValues, executeMaxRowsInFirstFrame ) );
                    } );

                    return executeStatement.createResultSet( executeResults.keySet(),
                            /* executeResultSupplier */ () -> executeResults.values().iterator().next().getExecuteResult(),
                            /* generatedKeysSupplier */ () -> executeResults.values().iterator().next().getGeneratedKeys()
                    );
                },
                //
                /* executeBatch */ ( executeBatchConnection, executeBatchTransaction, executeBatchStatement, executeBatchListOfUpdateBatches ) -> {
                    //
                    final Map<Replica, BatchResultSet> executeBatchResults = new HashMap<>();

                    // todo: we might want to filter which nodes to execute the statements on (quorum case)
                    final Set<Replica> writeQuorum = this.getWriteQuorum(
                            this.currentSchema.allReplicas()
                    );

                    writeQuorum.parallelStream().forEach( executeReplica -> {
                        final PreparedStatementInfos preparedStatement;
                        synchronized ( preparedStatements ) {
                            if ( preparedStatements.get( executeReplica ) == null ) {
                                // if not prepared on that new? replica --> prepare and then use for execution
                                try {
                                    preparedStatements.putAll( ReplicationModule.this.prepareDataManipulation( connection, statement, sql, maxRowCount, Collections.singleton( executeReplica ) ) );
                                } catch ( RemoteException e ) {
                                    throw Utils.wrapException( e );
                                }
                            }
                            preparedStatement = preparedStatements.get( executeReplica );
                        }
                        executeBatchResults.put( executeReplica, preparedStatement.executeBatch( executeBatchConnection, executeBatchTransaction, preparedStatement, executeBatchListOfUpdateBatches ) );
                    } );

                    return executeBatchStatement.createBatchResultSet( executeBatchResults.keySet(),
                            /* executeBatchResultSupplier */ () -> executeBatchResults.values().iterator().next().getExecuteResult(),
                            /* generatedKeysSupplier */ () -> executeBatchResults.values().iterator().next().getGeneratedKeys()
                    );
                } );
    }


    @Override
    public <NodeType extends Node> Map<NodeType, PreparedStatementInfos> prepareDataManipulation( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount, Set<NodeType> executionTargets ) throws RemoteException {

        /*
         * Semantics: execute the given SQL on the given Fragments/Replicas
         */

        /*
         * executionTargets: Type: e.g. Fragment or Replica or PhysicalNode
         * Can be a single Fragment or multiple
         */

        final Function1<Replica, SqlNode> alteredSqlFunction;
        switch ( sql.getKind() ) {
            case INSERT:
                alteredSqlFunction = ( replica ) -> {
                    final SqlInsert original = (SqlInsert) sql;
                    final SqlIdentifier originalTableIdentifier = SqlNodeUtils.INSERT_UTILS.getTargetTable( original );
                    final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( originalTableIdentifier, /* table name */ 0 )
                            + "_" + replica.id.toString();

                    return new SqlInsert( original.getParserPosition(), original.operand( 0 ),
                            SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( originalTableIdentifier, 0, tableName ),
                            original.operand( 2 ), original.operand( 3 ) );
                };
                break;

            case UPDATE:
                alteredSqlFunction = ( replica ) -> {
                    final SqlUpdate original = (SqlUpdate) sql;
                    final SqlIdentifier originalTableIdentifier = SqlNodeUtils.UPDATE_UTILS.getTargetTable( original );
                    final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( originalTableIdentifier, /* table name */ 0 )
                            + "_" + replica.id.toString();

                    return new SqlUpdate( original.getParserPosition(),
                            SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( originalTableIdentifier, 0, tableName ),
                            original.operand( 1 ), original.operand( 2 ), original.operand( 3 ), original.getSourceSelect(), original.operand( 4 ) );
                };
                break;

            case DELETE:
                alteredSqlFunction = ( replica ) -> {
                    final SqlDelete original = (SqlDelete) sql;
                    final SqlIdentifier originalTableIdentifier = SqlNodeUtils.DELETE_UTILS.getTargetTable( original );
                    final String tableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( originalTableIdentifier, /* table name */ 0 )
                            + "_" + replica.id.toString();

                    return new SqlDelete( original.getParserPosition(), SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( originalTableIdentifier, 0, tableName ),
                            original.operand( 1 ), original.getSourceSelect(), original.operand( 2 ) );
                };
                break;

            default:
                throw new UnsupportedOperationException( "Case `" + sql.getKind() + "` not implemented yet." );
        }

        final Map<NodeType, PreparedStatementInfos> preparedStatements = new ConcurrentHashMap<>();
        try {
            executionTargets.parallelStream().forEach( ( executionTarget ) -> {
                final Map<Replica, PreparedStatementInfos> prepareOnTargetResult = new HashMap<>();
                final Set<Replica> prepareTargets;
                if ( executionTarget instanceof Replica ) {
                    prepareTargets = Collections.singleton( (Replica) executionTarget );
                } else if ( executionTarget instanceof Fragment ) {
                    prepareTargets = this.currentSchema.lookupReplicas( (Fragment) executionTarget ); // on all replicas
                } else {
                    throw new IllegalArgumentException( "Type of `executionTarget` is not supported." );
                }

                prepareTargets.parallelStream().forEach( ( prepareReplica ) -> {
                    final SqlNode alteredSql = alteredSqlFunction.apply( prepareReplica );
                    try {
                        final Map<Replica, PreparedStatementInfos> prepareResult = down.prepareDataManipulation( connection, statement, alteredSql, maxRowCount, Collections.singleton( prepareReplica ) );
                        prepareOnTargetResult.put( prepareReplica, prepareResult.get( prepareReplica ) );
                    } catch ( RemoteException e ) {
                        throw Utils.wrapException( e );
                    }
                } );

                preparedStatements.put( executionTarget, connection.createPreparedStatement( statement, prepareOnTargetResult,
                        //
                        /* signatureSupplier */ () -> MergeFunctions.mergeSignature( statement, prepareOnTargetResult, sql ),
                        //
                        /* execute */ ( executeConnection, executeTransaction, executeStatement, executeParameterValues, executeMaxRowsInFirstFrame ) -> {
                            //
                            final Set<Replica> writeQuorum; // get the write quorum required for the execution on `executionTarget`
                            if ( executionTarget instanceof Replica ) {
                                writeQuorum = Collections.singleton( (Replica) executionTarget );
                            } else if ( executionTarget instanceof Fragment ) {
                                writeQuorum = this.getWriteQuorum(
                                        this.currentSchema.lookupReplicas( (Fragment) executionTarget )
                                );
                            } else {
                                throw new IllegalArgumentException( "Type of `executionTarget` is not supported." );
                            }

                            final Map<Replica, QueryResultSet> executeResults = new HashMap<>();

                            // EXECUTE the statements
                            writeQuorum.parallelStream().forEach( executeReplica -> {
                                final PreparedStatementInfos preparedStatement;
                                synchronized ( prepareOnTargetResult ) {
                                    if ( prepareOnTargetResult.get( executeReplica ) == null ) {
                                        // if not prepared on that new? replica --> prepare and then use for execution
                                        try {
                                            prepareOnTargetResult.putAll( ReplicationModule.this.down.prepareDataManipulation( connection, statement, alteredSqlFunction.apply( executeReplica ), maxRowCount, Collections.singleton( executeReplica ) ) );
                                        } catch ( RemoteException e ) {
                                            throw Utils.wrapException( e );
                                        }
                                    }
                                    preparedStatement = prepareOnTargetResult.get( executeReplica );
                                }
                                executeResults.put( executeReplica,
                                        preparedStatement.execute( executeConnection, executeTransaction, preparedStatement, executeParameterValues, executeMaxRowsInFirstFrame )
                                );
                            } );

                            return executeStatement.createResultSet( executeResults.keySet(),
                                    /* executeResultSupplier */ () -> executeResults.values().iterator().next().getExecuteResult(),
                                    /* generatedKeysSupplier */ () -> executeResults.values().iterator().next().getGeneratedKeys()  // todo: this might not be correct!
                            );
                        },
                        //
                        /* executeBatch */ ( executeBatchConnection, executeBatchTransaction, executeBatchStatement, executeBatchListOfUpdateBatches ) -> {
                            //
                            final Collection<Replica> writeQuorum; // get the write quorum required for the execution on `executionTarget`
                            if ( executionTarget instanceof Replica ) {
                                writeQuorum = Collections.singleton( (Replica) executionTarget );
                            } else if ( executionTarget instanceof Fragment ) {
                                writeQuorum = this.getWriteQuorum(
                                        this.currentSchema.lookupReplicas( (Fragment) executionTarget )
                                );
                            } else {
                                throw new IllegalArgumentException( "Type of `executionTarget` is not supported." );
                            }

                            final Map<Replica, BatchResultSet> executeBatchResults = new HashMap<>();

                            // EXECUTE the statements
                            writeQuorum.parallelStream().forEach( executeReplica -> {
                                final PreparedStatementInfos preparedStatement;
                                synchronized ( prepareOnTargetResult ) {
                                    if ( prepareOnTargetResult.get( executeReplica ) == null ) {
                                        // if not prepared on that new? replica --> prepare and then use for execution
                                        try {
                                            prepareOnTargetResult.putAll( ReplicationModule.this.down.prepareDataManipulation( connection, statement, alteredSqlFunction.apply( executeReplica ), maxRowCount, Collections.singleton( executeReplica ) ) );
                                        } catch ( RemoteException e ) {
                                            throw Utils.wrapException( e );
                                        }
                                    }
                                    preparedStatement = prepareOnTargetResult.get( executeReplica );
                                }
                                executeBatchResults.put( executeReplica,
                                        preparedStatement.executeBatch( executeBatchConnection, executeBatchTransaction, preparedStatement, executeBatchListOfUpdateBatches )
                                );
                            } );

                            return executeBatchStatement.createBatchResultSet( executeBatchResults.keySet(),
                                    /* executeBatchResultSupplier */ () -> executeBatchResults.values().iterator().next().getExecuteResult(),
                                    /* generatedKeysSupplier */ () -> executeBatchResults.values().iterator().next().getGeneratedKeys()  // todo: this might not be correct!
                            );
                        } )
                );
            } );
        } catch ( WrappingException we ) {
            final Throwable t = Utils.xtractException( we );
            if ( t instanceof RemoteException ) {
                throw (RemoteException) t;
            } else {
                throw we;
            }
        }

        return preparedStatements;
    }


    @Override
    public PreparedStatementInfos prepareDataQuery( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount ) throws RemoteException {

        // prepare on ALL replicas
        final Map<Replica, PreparedStatementInfos> preparedStatements = this.prepareDataQuery( connection, statement, sql, maxRowCount,
                this.currentSchema.allReplicas()
        );

        return connection.createPreparedStatement( statement, preparedStatements,
                /* signatureSupplier */ () -> MergeFunctions.mergeSignature( statement, preparedStatements, sql ),
                //
                /* execute */ ( executeConnection, executeTransaction, executeStatement, executeParameterValues, executeMaxRowsInFirstFrame ) -> {
                    //
                    final Map<Replica, QueryResultSet> executeResults = new HashMap<>();

                    // todo: we might want to filter which nodes to execute the statements on (quorum case)
                    final Set<Replica> readQuorum = this.getReadQuorum(
                            this.currentSchema.allReplicas()
                    );

                    readQuorum.parallelStream().forEach( executeReplica -> {
                        final PreparedStatementInfos preparedStatement;
                        synchronized ( preparedStatements ) {
                            if ( preparedStatements.get( executeReplica ) == null ) {
                                // if not prepared on that new? replica --> prepare and then use for execution
                                try {
                                    preparedStatements.putAll( ReplicationModule.this.prepareDataQuery( connection, statement, sql, maxRowCount, Collections.singleton( executeReplica ) ) );
                                } catch ( RemoteException e ) {
                                    throw Utils.wrapException( e );
                                }
                            }
                            preparedStatement = preparedStatements.get( executeReplica );
                        }
                        executeResults.put( executeReplica, preparedStatement.execute( executeConnection, executeTransaction, preparedStatement, executeParameterValues, executeMaxRowsInFirstFrame ) );
                    } );

                    return executeStatement.createResultSet( executeResults.keySet(),
                            /* executeResultSupplier */ () -> executeResults.values().iterator().next().getExecuteResult(),
                            /* generatedKeysSupplier */ () -> executeResults.values().iterator().next().getGeneratedKeys() // todo: if we accessed more than one node, we need to know which row on which nodes
                    );
                },
                //
                /* executeBatch */ ( executeBatchConnection, executeBatchTransaction, executeBatchStatement, executeBatchListOfUpdateBatches ) -> {
                    throw Utils.wrapException( new SQLFeatureNotSupportedException( "`SELECT` statements cannot be executed in a batch context." ) );
                } );
    }


    @Override
    public <NodeType extends Node> Map<NodeType, PreparedStatementInfos> prepareDataQuery( ConnectionInfos connection, StatementInfos statement, SqlNode sql, long maxRowCount, Set<NodeType> executionTargets ) throws RemoteException {

        /*
         * Semantics: execute the given SQL on the given Fragments/Replicas
         */

        /*
         * executionTargets: Type: e.g. Fragment or Replica or PhysicalNode
         * Can be a single Fragment or multiple
         */

        final Function1<Replica, SqlNode> alteredSqlFunction;
        switch ( sql.getKind() ) {
            case SELECT:
                alteredSqlFunction = ( replica ) -> {
                    final SqlSelect original = (SqlSelect) sql;

                    final SqlNode alteredFrom;
                    switch ( original.getFrom().getKind() ) {
                        case AS: {
                            final SqlBasicCall as = (SqlBasicCall) original.getFrom();
                            final String replicaTableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( as.operand( 0 ), /* table name */ 0 )
                                    + "_" + replica.id.toString();
                            alteredFrom = SqlStdOperatorTable.AS.createCall( as.getParserPosition(), SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( as.operand( 0 ), 0, replicaTableName ), as.operand( 1 ) );
                            break;
                        }

                        case IDENTIFIER: {
                            final SqlIdentifier identifier = (SqlIdentifier) original.getFrom();
                            final String replicaTableName = SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( identifier, /* table name */ 0 )
                                    + "_" + replica.id.toString();
                            alteredFrom = SqlStdOperatorTable.AS.createCall( identifier.getParserPosition(), SqlNodeUtils.IDENTIFIER_UTILS.setPartOfCompoundIdentifier( identifier, 0, replicaTableName ), identifier.getComponent( 0 ) );
                            break;
                        }

                        default:
                            throw new UnsupportedOperationException( "Case `" + original.getFrom().getKind() + "` not implemented yet." );
                    }

                    return new SqlSelect( original.getParserPosition(), /* keywordList */ original.operand( 0 ), /* selectList */ original.operand( 1 ),
                            alteredFrom,
                            /* where */ original.operand( 3 ), /* groupBy */ original.operand( 4 ), /* having */ original.operand( 5 ), /* windowDecls */ original.operand( 6 ), /* orderBy */ original.operand( 7 ), /* offset */ original.operand( 8 ), /* fetch */ original.operand( 9 ), /* hints */ original.operand( 10 ) );
                };
                break;

            default:
                throw new UnsupportedOperationException( "Case `" + sql.getKind() + "` not implemented yet." );
        }

        final Map<NodeType, PreparedStatementInfos> preparedStatements = new ConcurrentHashMap<>();
        try {
            executionTargets.parallelStream().forEach( ( executionTarget ) -> {
                final Map<Replica, PreparedStatementInfos> prepareOnTargetResult = new HashMap<>();
                final Set<Replica> prepareTargets;
                if ( executionTarget instanceof Replica ) {
                    prepareTargets = Collections.singleton( (Replica) executionTarget );
                } else if ( executionTarget instanceof Fragment ) {
                    prepareTargets = this.currentSchema.lookupReplicas( (Fragment) executionTarget );
                } else {
                    throw new IllegalArgumentException( "Type of `executionTarget` is not supported." );
                }

                prepareTargets.parallelStream().forEach( ( prepareReplica ) -> {
                    final SqlNode alteredSql = alteredSqlFunction.apply( prepareReplica );
                    try {
                        final Map<Replica, PreparedStatementInfos> prepareResult = down.prepareDataQuery( connection, statement, alteredSql, maxRowCount, Collections.singleton( prepareReplica ) );
                        prepareOnTargetResult.put( prepareReplica, prepareResult.get( prepareReplica ) );
                    } catch ( RemoteException e ) {
                        throw Utils.wrapException( e );
                    }
                } );

                preparedStatements.put( executionTarget, connection.createPreparedStatement( statement, prepareOnTargetResult,
                        //
                        /* signatureSupplier */ () -> MergeFunctions.mergeSignature( statement, prepareOnTargetResult, sql ),
                        //
                        /* execute */ ( executeConnection, executeTransaction, executeStatement, executeParameterValues, executeMaxRowsInFirstFrame ) -> {
                            //
                            final Collection<Replica> readQuorum; // get the read quorum required for the execution on `executionTarget`
                            if ( executionTarget instanceof Replica ) {
                                readQuorum = Collections.singleton( (Replica) executionTarget );
                            } else if ( executionTarget instanceof Fragment ) {
                                readQuorum = this.getReadQuorum(
                                        this.currentSchema.lookupReplicas( (Fragment) executionTarget )
                                );
                            } else {
                                throw new IllegalArgumentException( "Type of `executionTarget` is not supported." );
                            }

                            final Map<Replica, QueryResultSet> executeResults = new HashMap<>();

                            // EXECUTE the statements
                            readQuorum.parallelStream().forEach( executeReplica -> {
                                final PreparedStatementInfos preparedStatement;
                                synchronized ( prepareOnTargetResult ) {
                                    if ( prepareOnTargetResult.get( executeReplica ) == null ) {
                                        // if not prepared on that new? replica --> prepare and then use for execution
                                        try {
                                            prepareOnTargetResult.putAll( ReplicationModule.this.down.prepareDataQuery( connection, statement, alteredSqlFunction.apply( executeReplica ), maxRowCount, Collections.singleton( executeReplica ) ) );
                                        } catch ( RemoteException e ) {
                                            throw Utils.wrapException( e );
                                        }
                                    }
                                    preparedStatement = prepareOnTargetResult.get( executeReplica );
                                }
                                executeResults.put( executeReplica,
                                        preparedStatement.execute( executeConnection, executeTransaction, preparedStatement, executeParameterValues, executeMaxRowsInFirstFrame )
                                );
                            } );

                            return executeStatement.createResultSet( executeResults.keySet(),
                                    /* executeResultSupplier */ () -> executeResults.values().iterator().next().getExecuteResult(),
                                    /* generatedKeysSupplier */ () -> executeResults.values().iterator().next().getGeneratedKeys()  // todo: this might not be correct!
                            );
                        },
                        //
                        /* executeBatch */ ( executeBatchConnection, executeBatchTransaction, executeBatchStatement, executeBatchListOfUpdateBatches ) -> {
                            //
                            throw Utils.wrapException( new SQLFeatureNotSupportedException( "`SELECT` statements cannot be executed in a batch context." ) );
                        } )
                );
            } );
        } catch ( WrappingException we ) {
            final Throwable t = Utils.xtractException( we );
            if ( t instanceof RemoteException ) {
                throw (RemoteException) t;
            } else {
                throw we;
            }
        }

        return preparedStatements;
    }


    @Override
    public FragmentationProtocol setFragmentationProtocol( FragmentationProtocol fragmentationProtocol ) {
        return null;
    }


    @Override
    public PlacementProtocol setAllocationProtocol( PlacementProtocol placementProtocol ) {
        return null;
    }
}
