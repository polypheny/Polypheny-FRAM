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


import io.vavr.Function5;
import java.rmi.RemoteException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.calcite.avatica.Meta.ExecuteBatchResult;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.Frame;
import org.polypheny.fram.Node;
import org.polypheny.fram.remote.types.RemoteExecuteBatchResult;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.standalone.Meta.Result;
import org.polypheny.fram.standalone.Utils.WrappingException;


public abstract class ResultSetInfos implements Result {

    protected final StatementInfos statement;
    protected ExecuteResult generatedKeysResult;


    protected ResultSetInfos( StatementInfos statement ) {
        this.statement = statement;
    }


    public abstract <NodeType extends Node> Set<NodeType> getSources();


    public abstract <ExecuteResultType> ExecuteResultType getExecuteResult();


    @Override
    public ExecuteResult getGeneratedKeys() {
        return this.generatedKeysResult;
    }


    public Frame fetch( ConnectionInfos connection, StatementInfos statement, long offset, int fetchMaxRowCount ) throws RemoteException {
        throw new UnsupportedOperationException( "Not supported yet." );
    }


    public static class QueryResultSet<NodeType extends Node, ResultType extends Result> extends ResultSetInfos {

        private final Set<NodeType> sources;
        private final ExecuteResult executeResult;
        private final Function5<Set<NodeType>, ConnectionInfos, StatementInfos, Long, Integer, Frame> resultsFetchFunction;


        public QueryResultSet( StatementInfos statement, NodeType source, ResultType result ) {
            this( statement, Collections.singleton( source ),
                    () -> {
                        if ( result instanceof ExecuteResult ) {
                            return (ExecuteResult) result;
                        }
                        if ( result instanceof RemoteExecuteResult ) {
                            return ((RemoteExecuteResult) result).toExecuteResult();
                        }
                        if ( result instanceof QueryResultSet ) {
                            return ((QueryResultSet) result).getExecuteResult();
                        }
                        throw new IllegalArgumentException( "Type of result is not of ExecuteResult, RemoteExecuteResult, or QueryResultSet." );
                    },
                    () -> result.getGeneratedKeys(),
                    ( _origins, _connection, _statement, _offset, _fetchMaxRowCount ) -> {
                        throw Utils.wrapException( new SQLFeatureNotSupportedException( "Fetch is not supported." ) );
                    } );
        }


        public QueryResultSet( StatementInfos statement, Set<NodeType> sources,
                Supplier<ExecuteResult> executeResultSupplier,
                Supplier<ExecuteResult> generatedKeysSupplier,
                Function5<Set<NodeType>, ConnectionInfos, StatementInfos, Long, Integer, Frame> resultsFetchFunction ) {
            super( statement );

            final Set<NodeType> s = new LinkedHashSet<>();
            s.addAll( s );
            this.sources = Collections.unmodifiableSet( s );

            this.executeResult = executeResultSupplier.get();
            this.generatedKeysResult = generatedKeysSupplier.get();
            this.resultsFetchFunction = resultsFetchFunction;
        }


        @Override
        public Set<NodeType> getSources() {
            return this.sources;
        }


        @Override
        public ExecuteResult getExecuteResult() {
            return this.executeResult;
        }


        @Override
        public Frame fetch( ConnectionInfos connection, StatementInfos statement, long offset, int fetchMaxRowCount ) throws RemoteException {
            try {
                return resultsFetchFunction.apply( sources, connection, statement, offset, fetchMaxRowCount );
            } catch ( WrappingException we ) {
                Throwable t = Utils.xtractException( we );
                if ( t instanceof RemoteException ) {
                    throw (RemoteException) t;
                }
                throw we;
            }
        }
    }


    public static class BatchResultSet<NodeType extends Node, ResultType extends Result> extends ResultSetInfos {

        private final Set<NodeType> sources;
        private final ExecuteBatchResult executeBatchResult;


        public BatchResultSet( StatementInfos statement, NodeType source, ResultType result ) {
            this( statement, Collections.singleton( source ),
                    () -> {
                        if ( result instanceof ExecuteBatchResult ) {
                            return (ExecuteBatchResult) result;
                        }
                        if ( result instanceof RemoteExecuteBatchResult ) {
                            return ((RemoteExecuteBatchResult) result).toExecuteBatchResult();
                        }
                        if ( result instanceof BatchResultSet ) {
                            return ((BatchResultSet) result).getExecuteResult();
                        }
                        throw new IllegalArgumentException( "Type of result is not of ExecuteBatchResult, RemoteExecuteBatchResult, or BatchResultSet." );
                    },
                    () -> result.getGeneratedKeys() );
        }


        public BatchResultSet( StatementInfos statement, Set<NodeType> sources,
                Supplier<ExecuteBatchResult> executeBatchResultSupplier,
                Supplier<ExecuteResult> generatedKeysSupplier ) {
            super( statement );

            final Set<NodeType> s = new LinkedHashSet<>();
            s.addAll( sources );
            this.sources = Collections.unmodifiableSet( s );

            this.executeBatchResult = executeBatchResultSupplier.get();
            this.generatedKeysResult = generatedKeysSupplier.get();
        }


        @Override
        public Set<NodeType> getSources() {
            return this.sources;
        }


        @Override
        public ExecuteBatchResult getExecuteResult() {
            return this.executeBatchResult;
        }
    }
}
