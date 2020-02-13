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


import org.polypheny.fram.remote.RemoteMeta;
import org.polypheny.fram.remote.RemoteNode;
import org.polypheny.fram.remote.types.RemoteExecuteBatchResult;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.remote.types.RemoteStatementHandle;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.calcite.avatica.Meta.ExecuteBatchResult;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.Frame;


public abstract class ResultSetInfos {

    private final StatementInfos statement;


    protected ResultSetInfos( StatementInfos statement ) {
        this.statement = statement;
    }


    public abstract List<RemoteMeta> getOrigins();


    public abstract <ExecuteResultType> ExecuteResultType getExecuteResult();


    public Frame fetch( RemoteStatementHandle fromStatementHandle, long offset, int fetchMaxRowCount ) throws RemoteException {
        throw new UnsupportedOperationException( "Not supported yet." );
    }


    public static class SingleResult extends ResultSetInfos {

        private final Map<RemoteNode, RemoteExecuteResult> origins = new LinkedHashMap<>();
        private final ExecuteResult executeResult;

        private final RemoteNode THIS_IS_A_HACK__PLEASE_REPLACE_ME;


        public SingleResult( StatementInfos statement, List<Entry<RemoteNode, RemoteExecuteResult>> remoteResults ) {
            super( statement );
            if ( remoteResults == null || remoteResults.size() < 1 ) {
                throw new IllegalArgumentException( "The size of remoteResults must be at least 1" );
            }

            remoteResults.forEach( entry -> SingleResult.this.origins.put( entry.getKey(), entry.getValue() ) );

            //this.executeResult = new ExecuteResult( Collections.singletonList(  ) );
            this.executeResult = remoteResults.get( 0 ) == null ? null : remoteResults.get( 0 ).getValue().toExecuteResult();
            this.THIS_IS_A_HACK__PLEASE_REPLACE_ME = remoteResults.get( 0 ) == null ? null : remoteResults.get( 0 ).getKey();
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
        public Frame fetch( RemoteStatementHandle fromStatementHandle, long offset, int fetchMaxRowCount ) throws RemoteException {
            return THIS_IS_A_HACK__PLEASE_REPLACE_ME.fetch( fromStatementHandle, offset, fetchMaxRowCount ).toFrame();
        }
    }


    public static class BatchResult extends ResultSetInfos {

        private final Map<RemoteNode, RemoteExecuteBatchResult> origins = new LinkedHashMap<>();
        private final ExecuteBatchResult executeBatchResult;


        public BatchResult( StatementInfos statement, List<Entry<RemoteNode, RemoteExecuteBatchResult>> remoteBatchResults ) {
            super( statement );

            if ( remoteBatchResults == null || remoteBatchResults.size() < 1 ) {
                throw new IllegalArgumentException( "The size of remoteBatchResults must be at least 1" );
            }

            remoteBatchResults.forEach( entry -> BatchResult.this.origins.put( entry.getKey(), entry.getValue() ) );

            //this.executeResult = new ExecuteBatchResult(  );
            this.executeBatchResult = remoteBatchResults.get( 0 ).getValue().toExecuteBatchResult();
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
