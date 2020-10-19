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

package org.polypheny.fram.protocols.fragmentation;


import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.Meta.ExecuteBatchResult;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.Meta.StatementType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.HsqldbSqlDialect;
import org.polypheny.fram.Node;
import org.polypheny.fram.protocols.Protocol;
import org.polypheny.fram.remote.types.RemoteExecuteBatchResult;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.remote.types.RemoteStatementHandle;
import org.polypheny.fram.standalone.Meta.Result;
import org.polypheny.fram.standalone.ResultSetInfos.BatchResultSet;
import org.polypheny.fram.standalone.ResultSetInfos.QueryResultSet;
import org.polypheny.fram.standalone.StatementInfos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MergeFunctions {

    protected static final Logger LOGGER = LoggerFactory.getLogger( Protocol.class );


    public static Signature mergeSignature( StatementInfos statement, Map<? extends Node, ?> sources, final SqlNode sql ) {
        // todo: if vertical partitioning is a thing, or we have windows on horizontal --> properly merge
        //
        List<ColumnMetaData> columns = null;
        List<AvaticaParameter> parameters = null;
        CursorFactory cursorFactory = null;
        StatementType statementType = null;

        for ( Entry<? extends Node, ?> source : sources.entrySet() ) {
            Node node = source.getKey();
            Object preparedStatement = source.getValue();
            Signature signature;

            if ( preparedStatement instanceof StatementInfos ) {
                signature = ((StatementInfos) preparedStatement).getStatementHandle().signature;
            } else if ( preparedStatement instanceof RemoteStatementHandle ) {
                signature = ((RemoteStatementHandle) preparedStatement).toStatementHandle().signature;
            } else if ( preparedStatement instanceof StatementHandle ) {
                signature = ((StatementHandle) preparedStatement).signature;
            } else {
                throw new IllegalArgumentException( "Illegal argument for source!" );
            }

            if ( columns == null ) {
                columns = signature.columns;
            } else {
                // todo
            }

            if ( parameters == null ) {
                parameters = signature.parameters;
            } else {
                //todo
            }

            if ( cursorFactory == null ) {
                cursorFactory = signature.cursorFactory;
            } else {
                //todo
            }

            if ( statementType == null ) {
                statementType = signature.statementType;
            } else {
                // todo
            }
        }

        return Meta.Signature.create( columns, sql.toSqlString( HsqldbSqlDialect.DEFAULT ).getSql(), parameters, cursorFactory, statementType );
    }


    public static ExecuteResult mergeExecuteResults( StatementInfos statement, Map<? extends Node, ? extends Result> sources, Integer maxRowsInFirstFrame ) {
        if ( sources == null || sources.isEmpty() ) {
            throw new IllegalArgumentException( "origin == null || origin.isEmpty" );
        }
        if ( sources.size() == 1 ) {
            final Result result = sources.values().iterator().next();
            if ( result instanceof ExecuteResult ) {
                return ((ExecuteResult) result);
            } else if ( result instanceof RemoteExecuteResult ) {
                return ((RemoteExecuteResult) result).toExecuteResult();
            } else if ( result instanceof QueryResultSet ) {
                return ((QueryResultSet) result).getExecuteResult();
            } else {
                throw new UnsupportedOperationException( "Type of result not supported yet." );
            }
        }

        // results merge
        boolean done = true;
        List<Long> updateCounts = new LinkedList<>();
        List<Iterator<Object>> iterators = new LinkedList<>();
        for ( Result result : sources.values() ) {
            final List<MetaResultSet> resultSets;
            if ( result instanceof QueryResultSet ) {
                resultSets = ((QueryResultSet) result).getExecuteResult().resultSets;
            } else if ( result instanceof RemoteExecuteResult ) {
                resultSets = ((RemoteExecuteResult) result).toExecuteResult().resultSets;
            } else if ( result instanceof ExecuteResult ) {
                resultSets = ((ExecuteResult) result).resultSets;
            } else {
                throw new UnsupportedOperationException( "Type of result not supported yet." );
            }

            if ( resultSets.isEmpty() ) {
                throw new InternalError( "The result set list is empty." );
            }

            MetaResultSet executionResult = resultSets.get( 0 );
            {
                if ( executionResult.updateCount > -1L ) {
                    updateCounts.add( executionResult.updateCount );
                } else {
                    done &= executionResult.firstFrame.done;
                    iterators.add( executionResult.firstFrame.rows.iterator() );
                }
            }
        }

        if ( !updateCounts.isEmpty() && !iterators.isEmpty() ) {
            throw new IllegalStateException( "Mixed update counts with actual results." );
        }

        if ( updateCounts.isEmpty() ) {
            // Merge frames
            List<Object> rows = new LinkedList<>();
            boolean _continue;
            do {
                _continue = false;
                for ( Iterator<Object> iterator : iterators ) {
                    if ( iterator.hasNext() ) {
                        rows.add( iterator.next() );
                        _continue = true;
                    }
                }
            } while ( _continue );

            if ( !done ) {
                LOGGER.trace( "The merge of the frames did not finish." );
            }

            return new ExecuteResult( Collections.singletonList(
                    MetaResultSet.create( statement.getStatementHandle().connectionId, statement.getStatementHandle().id, false, statement.getStatementHandle().signature, Frame.create( 0, done, rows ) )
            ) );
        } else {
            // Merge update counts
            long mergedUpdateCount = 0;
            for ( long updateCount : updateCounts ) {
                mergedUpdateCount += updateCount;
            }
            return new ExecuteResult( Collections.singletonList(
                    MetaResultSet.count( statement.getStatementHandle().connectionId, statement.getStatementHandle().id, mergedUpdateCount )
            ) );
        }
    }


    public static <NodeType extends Node> ExecuteBatchResult mergeExecuteBatchResults( StatementInfos statement, Map<NodeType, ? extends Result> sources, Map<Integer, Set<NodeType>> lineToNodeMap ) {
        if ( sources == null || sources.isEmpty() ) {
            throw new IllegalArgumentException( "origin == null || origin.isEmpty" );
        }
        if ( sources.size() == 1 ) {
            final Result result = sources.values().iterator().next();
            if ( result instanceof ExecuteBatchResult ) {
                return ((ExecuteBatchResult) result);
            } else if ( result instanceof RemoteExecuteBatchResult ) {
                return ((RemoteExecuteBatchResult) result).toExecuteBatchResult();
            } else if ( result instanceof BatchResultSet ) {
                return ((BatchResultSet) result).getExecuteResult();
            } else {
                throw new UnsupportedOperationException( "Type of result not supported yet." );
            }
        }

        final Map<NodeType, Integer> sourceToUpdateCountIndexMap = new HashMap<>();

        final long[] updateCounts = new long[lineToNodeMap.keySet().size()];
        for ( int updateCountIndex = 0; updateCountIndex < updateCounts.length; ++updateCountIndex ) {
            final Set<NodeType> nodes = lineToNodeMap.get( updateCountIndex );
            for ( NodeType node : nodes ) {
                final int originUpdateCountIndex = sourceToUpdateCountIndexMap.getOrDefault( node, 0 );

                final Result result = sources.get( node );
                if ( result instanceof ExecuteBatchResult ) {
                    updateCounts[updateCountIndex] = ((ExecuteBatchResult) result).updateCounts[originUpdateCountIndex];
                } else if ( result instanceof RemoteExecuteBatchResult ) {
                    updateCounts[updateCountIndex] = ((RemoteExecuteBatchResult) result).toExecuteBatchResult().updateCounts[originUpdateCountIndex];
                } else if ( result instanceof BatchResultSet ) {
                    updateCounts[updateCountIndex] = ((BatchResultSet) result).getExecuteResult().updateCounts[originUpdateCountIndex];
                } else {
                    throw new UnsupportedOperationException( "Type of result not supported yet." );
                }

                sourceToUpdateCountIndexMap.put( node, originUpdateCountIndex + 1 );
            }
        }

        return new ExecuteBatchResult( updateCounts );
    }


    public static ExecuteResult mergeGeneratedKeys( StatementInfos statement, Map<? extends Node, ? extends Result> sources ) {
        if ( sources.size() == 0 ) {
            return null;
        }
        if ( sources.size() == 1 ) {
            return sources.entrySet().iterator().next().getValue().getGeneratedKeys();
        }

        // results merge
        boolean done = true;
        List<Long> updateCounts = new LinkedList<>();
        List<Iterator<Object>> iterators = new LinkedList<>();
        for ( Result res : sources.values() ) {
            if ( res.getGeneratedKeys() == null || res.getGeneratedKeys().resultSets == null ) {
                continue;
            }

            if ( res.getGeneratedKeys().resultSets.isEmpty() ) {
                throw new InternalError( "The generated keys list is empty." );
            }
            MetaResultSet executionResult = res.getGeneratedKeys().resultSets.get( 0 );
            {
                if ( executionResult.updateCount > -1L ) {
                    updateCounts.add( executionResult.updateCount );
                } else {
                    done &= executionResult.firstFrame.done;
                    iterators.add( executionResult.firstFrame.rows.iterator() );
                }
            }
        }

        if ( updateCounts.isEmpty() && iterators.isEmpty() ) {
            // res.getGeneratedKeys() was always null
            return null;
        }

        if ( !updateCounts.isEmpty() && !iterators.isEmpty() ) {
            throw new IllegalStateException( "Mixed update counts with actual results." );
        }

        if ( updateCounts.isEmpty() ) {
            // Merge frames
            List<Object> rows = new LinkedList<>();
            boolean _continue;
            do {
                _continue = false;
                for ( Iterator<Object> iterator : iterators ) {
                    if ( iterator.hasNext() ) {
                        rows.add( iterator.next() );
                        _continue = true;
                    }
                }
            } while ( _continue );

            if ( !done ) {
                LOGGER.trace( "The merge of the frames did not finish." );
            }

            return new ExecuteResult( Collections.singletonList(
                    MetaResultSet.create( statement.getStatementHandle().connectionId, statement.getStatementHandle().id, false, statement.getStatementHandle().signature, Frame.create( 0, done, rows ) )
            ) );
        } else {
            // Merge update counts
            long mergedUpdateCount = 0;
            for ( long updateCount : updateCounts ) {
                mergedUpdateCount += updateCount;
            }
            return new ExecuteResult( Collections.singletonList(
                    MetaResultSet.count( statement.getStatementHandle().connectionId, statement.getStatementHandle().id, mergedUpdateCount )
            ) );
        }
    }


    public static ExecuteResult mergeAggregatedResults( StatementInfos statement, Map<? extends Node, ? extends Result> origins, final SqlKind[] aggregateFunctions ) {
        if ( origins == null || origins.isEmpty() ) {
            throw new IllegalArgumentException( "origin == null || origin.isEmpty" );
        }
        if ( origins.size() == 1 ) {
            final Result result = origins.values().iterator().next();
            if ( result instanceof QueryResultSet ) {
                return ((QueryResultSet) result).getExecuteResult();
            } else if ( result instanceof RemoteExecuteResult ) {
                return ((RemoteExecuteResult) result).toExecuteResult();
            } else if ( result instanceof ExecuteResult ) {
                return ((ExecuteResult) result);
            } else {
                throw new UnsupportedOperationException( "Type of result not supported yet." );
            }
        }

        Signature signature = null;
        Number[] aggregateValues = new Number[aggregateFunctions.length];
        for ( Result res : origins.values() ) {
            final List<MetaResultSet> resultSets;
            if ( res instanceof QueryResultSet ) {
                resultSets = ((QueryResultSet) res).getExecuteResult().resultSets;
            } else if ( res instanceof RemoteExecuteResult ) {
                resultSets = ((RemoteExecuteResult) res).toExecuteResult().resultSets;
            } else if ( res instanceof ExecuteResult ) {
                resultSets = ((ExecuteResult) res).resultSets;
            } else {
                throw new UnsupportedOperationException( "Type of res not supported yet." );
            }

            for ( MetaResultSet rs : resultSets ) {
                if ( signature == null ) {
                    signature = rs.signature;
                }

                Object row = rs.firstFrame.rows.iterator().next();
                for ( int columnIndex = 0; columnIndex < aggregateFunctions.length; ++columnIndex ) {
                    final Number next;
                    switch ( rs.signature.cursorFactory.style ) {
                        case LIST:
                            if ( row instanceof List ) {
                                next = (Number) ((List) row).get( columnIndex );
                            } else if ( row instanceof Object[] ) {
                                next = (Number) ((Object[]) row)[columnIndex];
                            } else {
                                throw new UnsupportedOperationException( "Not implemented yet." );
                            }
                            break;

                        default:
                            throw new UnsupportedOperationException( "Not implemented yet." );
                    }

                    switch ( aggregateFunctions[columnIndex] ) {
                        case MAX:
                            aggregateValues[columnIndex] = aggregateValues[columnIndex] == null ? next :
                                    Math.max( aggregateValues[columnIndex].doubleValue(), next.doubleValue() );
                            break;

                        case MIN:
                            aggregateValues[columnIndex] = aggregateValues[columnIndex] == null ? next :
                                    Math.min( aggregateValues[columnIndex].doubleValue(), next.doubleValue() );
                            break;

                        case SUM:
                        case COUNT:
                            aggregateValues[columnIndex] = aggregateValues[columnIndex] == null ? next :
                                    aggregateValues[columnIndex].doubleValue() + next.doubleValue();
                            break;

                        default:
                            throw new UnsupportedOperationException( "Not implemented yet." );
                    }
                }
            }
        }

        final Frame frame = Frame.create( 0, true, Collections.singletonList( (Object[]) aggregateValues ) );
        return new Meta.ExecuteResult( Collections.singletonList( MetaResultSet.create( statement.getStatementHandle().connectionId, statement.getStatementHandle().id, false, signature, frame ) ) );
    }
}
