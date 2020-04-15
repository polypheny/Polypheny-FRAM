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


import java.rmi.RemoteException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.Meta.ExecuteBatchResult;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.Frame;
import org.jooq.lambda.function.Function1;
import org.jooq.lambda.function.Function5;
import org.polypheny.fram.remote.AbstractRemoteNode;
import org.polypheny.fram.remote.RemoteMeta;
import org.polypheny.fram.remote.types.RemoteExecuteBatchResult;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.standalone.Utils.WrappingException;


public abstract class ResultSetInfos {

    private final StatementInfos statement;


    protected ResultSetInfos( StatementInfos statement ) {
        this.statement = statement;
    }


    public abstract List<RemoteMeta> getOrigins();


    public abstract <ExecuteResultType> ExecuteResultType getExecuteResult();


    public Frame fetch( ConnectionInfos connection, StatementInfos statement, long offset, int fetchMaxRowCount ) throws RemoteException {
        throw new UnsupportedOperationException( "Not supported yet." );
    }


    public static class QueryResultSet extends ResultSetInfos {

        private final Map<AbstractRemoteNode, RemoteExecuteResult> origins = new LinkedHashMap<>();
        private final ExecuteResult executeResult;
        private final Function5<Map<AbstractRemoteNode, RemoteExecuteResult>, ConnectionInfos, StatementInfos, Long, Integer, Frame> resultsFetchFunction;


        public QueryResultSet( StatementInfos statement, Map<AbstractRemoteNode, RemoteExecuteResult> remoteResults, Function1<Map<AbstractRemoteNode, RemoteExecuteResult>, ExecuteResult> resultsMergeFunction, Function5<Map<AbstractRemoteNode, RemoteExecuteResult>, ConnectionInfos, StatementInfos, Long, Integer, Frame> resultsFetchFunction ) {
            super( statement );
            this.origins.putAll( remoteResults );
            this.executeResult = resultsMergeFunction.apply( this.origins );
            this.resultsFetchFunction = resultsFetchFunction;
        }


        @Override
        public List<RemoteMeta> getOrigins() {
            return Collections.unmodifiableList( new LinkedList<>( this.origins.keySet() ) );
        }


        @Override
        public ExecuteResult getExecuteResult() {
            return this.executeResult;
        }


        @Override
        public Frame fetch( ConnectionInfos connection, StatementInfos statement, long offset, int fetchMaxRowCount ) throws RemoteException {
            try {
                return resultsFetchFunction.apply( origins, connection, statement, offset, fetchMaxRowCount );
            } catch ( WrappingException wrapper ) {
                Throwable ex = Utils.xtractException( wrapper );
                if ( ex instanceof RemoteException ) {
                    throw (RemoteException) ex;
                }
                throw Utils.wrapException( ex );
            }
        }
    }


    public static class BatchResultSetInfos extends ResultSetInfos {

        private final Map<AbstractRemoteNode, RemoteExecuteBatchResult> origins = new LinkedHashMap<>();
        private final ExecuteBatchResult executeBatchResult;


        public BatchResultSetInfos( StatementInfos statement, AbstractRemoteNode origin, RemoteExecuteBatchResult remoteBatchResult ) {
            super( statement );
            this.origins.put( origin, remoteBatchResult );
            this.executeBatchResult = remoteBatchResult.toExecuteBatchResult();
        }


        public BatchResultSetInfos( StatementInfos statement, Map<AbstractRemoteNode, RemoteExecuteBatchResult> remoteBatchResults, Function1<Map<AbstractRemoteNode, RemoteExecuteBatchResult>, ExecuteBatchResult> batchResultsMergeFunction ) {
            super( statement );
            this.origins.putAll( remoteBatchResults );
            this.executeBatchResult = batchResultsMergeFunction.apply( this.origins );
        }


        @Override
        public List<RemoteMeta> getOrigins() {
            return Collections.unmodifiableList( new LinkedList<>( this.origins.keySet() ) );
        }


        @Override
        public ExecuteBatchResult getExecuteResult() {
            return this.executeBatchResult;
        }
    }
}
