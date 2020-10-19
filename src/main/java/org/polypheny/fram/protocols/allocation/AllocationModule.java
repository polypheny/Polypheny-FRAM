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

package org.polypheny.fram.protocols.allocation;


import java.rmi.RemoteException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.sql.SqlNode;
import org.polypheny.fram.Node;
import org.polypheny.fram.protocols.AbstractProtocol;
import org.polypheny.fram.protocols.Protocol.PlacementProtocol;
import org.polypheny.fram.remote.PhysicalNode;
import org.polypheny.fram.standalone.ConnectionInfos;
import org.polypheny.fram.standalone.ResultSetInfos;
import org.polypheny.fram.standalone.StatementInfos;
import org.polypheny.fram.standalone.StatementInfos.PreparedStatementInfos;
import org.polypheny.fram.standalone.TransactionInfos;
import org.polypheny.fram.standalone.Utils;
import org.polypheny.fram.standalone.Utils.WrappingException;


public class AllocationModule extends AbstractProtocol implements PlacementProtocol {

    public final AllocationSchema currentSchema = new AllocationSchema();


    @Override
    public <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteDataDefinition( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode catalogSql, SqlNode storeSql, long maxRowCount, int maxRowsInFirstFrame, Set<NodeType> executionTargets ) throws RemoteException {

        final Map<NodeType, ResultSetInfos> results = new HashMap<>();

        try {
            executionTargets.parallelStream().forEach( executionTarget -> {
                final PhysicalNode assignedNode = this.currentSchema.lookupNode( executionTarget );
                try {
                    final Map<PhysicalNode, ResultSetInfos> executeResult = down.prepareAndExecuteDataDefinition( connection, transaction, statement, catalogSql, storeSql, maxRowCount, maxRowsInFirstFrame, Collections.singleton( assignedNode ) );
                    results.put( executionTarget, executeResult.get( assignedNode ) );
                } catch ( RemoteException e ) {
                    throw Utils.wrapException( e );
                }
            } );
        } catch ( WrappingException we ) {
            final Throwable t = Utils.xtractException( we );
            if ( t instanceof RemoteException ) {
                throw (RemoteException) t;
            }
            throw we;
        }

        return results;
    }


    @Override
    public <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteDataManipulation( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, Set<NodeType> executionTargets ) throws RemoteException {

        final Map<NodeType, ResultSetInfos> results = new HashMap<>();

        try {
            executionTargets.parallelStream().forEach( executionTarget -> {
                final PhysicalNode assignedNode = this.currentSchema.lookupNode( executionTarget );
                try {
                    final Map<PhysicalNode, ResultSetInfos> executeResult = down.prepareAndExecuteDataManipulation( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, Collections.singleton( assignedNode ) );
                    results.put( executionTarget, executeResult.get( assignedNode ) );
                } catch ( RemoteException e ) {
                    throw Utils.wrapException( e );
                }
            } );
        } catch ( WrappingException we ) {
            final Throwable t = Utils.xtractException( we );
            if ( t instanceof RemoteException ) {
                throw (RemoteException) t;
            }
            throw we;
        }

        return results;
    }


    @Override
    public <NodeType extends Node> Map<NodeType, ResultSetInfos> prepareAndExecuteDataQuery( ConnectionInfos connection, TransactionInfos transaction, StatementInfos statement, SqlNode sql, long maxRowCount, int maxRowsInFirstFrame, Set<NodeType> executionTargets ) throws RemoteException {

        final Map<NodeType, ResultSetInfos> results = new HashMap<>();

        try {
            executionTargets.parallelStream().forEach( executionTarget -> {
                final PhysicalNode assignedNode = this.currentSchema.lookupNode( executionTarget );
                try {
                    final Map<PhysicalNode, ResultSetInfos> executeResult = down.prepareAndExecuteDataQuery( connection, transaction, statement, sql, maxRowCount, maxRowsInFirstFrame, Collections.singleton( assignedNode ) );
                    results.put( executionTarget, executeResult.get( assignedNode ) );
                } catch ( RemoteException e ) {
                    throw Utils.wrapException( e );
                }
            } );
        } catch ( WrappingException we ) {
            final Throwable t = Utils.xtractException( we );
            if ( t instanceof RemoteException ) {
                throw (RemoteException) t;
            }
            throw we;
        }

        return results;
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

        final Map<NodeType, PreparedStatementInfos> preparedStatements = new HashMap<>();

        try {
            executionTargets.parallelStream().forEach( executionTarget -> {
                final PhysicalNode assignedNode = this.currentSchema.lookupNode( executionTarget );
                try {
                    final Map<PhysicalNode, PreparedStatementInfos> prepareResult = down.prepareDataManipulation( connection, statement, sql, maxRowCount, Collections.singleton( assignedNode ) );
                    preparedStatements.put( executionTarget, prepareResult.get( assignedNode ) );
                } catch ( RemoteException e ) {
                    throw Utils.wrapException( e );
                }
            } );
        } catch ( WrappingException we ) {
            final Throwable t = Utils.xtractException( we );
            if ( t instanceof RemoteException ) {
                throw (RemoteException) t;
            }
            throw we;
        }

        return preparedStatements;
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

        final Map<NodeType, PreparedStatementInfos> preparedStatements = new HashMap<>();

        try {
            executionTargets.parallelStream().forEach( executionTarget -> {
                final PhysicalNode assignedNode = this.currentSchema.lookupNode( executionTarget );
                try {
                    final Map<PhysicalNode, PreparedStatementInfos> prepareResult = down.prepareDataQuery( connection, statement, sql, maxRowCount, Collections.singleton( assignedNode ) );
                    preparedStatements.put( executionTarget, prepareResult.get( assignedNode ) );
                } catch ( RemoteException e ) {
                    throw Utils.wrapException( e );
                }
            } );
        } catch ( WrappingException we ) {
            final Throwable t = Utils.xtractException( we );
            if ( t instanceof RemoteException ) {
                throw (RemoteException) t;
            }
            throw we;
        }

        return preparedStatements;
    }


    @Override
    public ReplicationProtocol setReplicationProtocol( ReplicationProtocol replicationProtocol ) {
        return null;
    }


    @Override
    public MigrationProtocol setMigrationProtocol( MigrationProtocol migrationProtocol ) {
        return null;
    }
}
