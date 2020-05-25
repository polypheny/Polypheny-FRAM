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

package org.polypheny.fram;


import java.util.List;
import java.util.UUID;
import javax.transaction.xa.XAException;
import org.apache.calcite.avatica.Meta.ConnectionHandle;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.apache.calcite.avatica.Meta.Pat;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.tools.Planner;
import org.polypheny.fram.standalone.ConnectionInfos;
import org.polypheny.fram.standalone.transaction.TransactionHandle;


public interface Catalog {

    UUID getNodeId();


    MetaResultSet getTables( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat tableNamePattern, List<String> typeList );

    MetaResultSet getColumns( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern );

    MetaResultSet getSchemas( ConnectionInfos connection, String catalog, Pat schemaPattern );

    MetaResultSet getCatalogs( ConnectionInfos connection );

    MetaResultSet getTableTypes( ConnectionInfos connection );

    MetaResultSet getProcedures( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat procedureNamePattern );

    MetaResultSet getProcedureColumns( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat procedureNamePattern, Pat columnNamePattern );

    MetaResultSet getColumnPrivileges( ConnectionInfos connection, String catalog, String schema, String table, Pat columnNamePattern );

    MetaResultSet getTablePrivileges( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat tableNamePattern );

    MetaResultSet getBestRowIdentifier( ConnectionInfos connection, String catalog, String schema, String table, int scope, boolean nullable );

    MetaResultSet getVersionColumns( ConnectionInfos connection, String catalog, String schema, String table );

    MetaResultSet getPrimaryKeys( ConnectionInfos connection, String catalog, String schema, String table );

    MetaResultSet getImportedKeys( ConnectionInfos connection, String catalog, String schema, String table );

    MetaResultSet getExportedKeys( ConnectionInfos connection, String catalog, String schema, String table );

    MetaResultSet getCrossReference( ConnectionInfos connection, String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable );

    MetaResultSet getTypeInfo( ConnectionInfos connection );

    MetaResultSet getIndexInfo( ConnectionInfos connection, String catalog, String schema, String table, boolean unique, boolean approximate );

    MetaResultSet getUDTs( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat typeNamePattern, int[] types );

    MetaResultSet getSuperTypes( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat typeNamePattern );

    MetaResultSet getSuperTables( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat tableNamePattern );

    MetaResultSet getAttributes( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat typeNamePattern, Pat attributeNamePattern );

    MetaResultSet getClientInfoProperties( ConnectionInfos connection );

    MetaResultSet getFunctions( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat functionNamePattern );

    MetaResultSet getFunctionColumns( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat functionNamePattern, Pat columnNamePattern );

    MetaResultSet getPseudoColumns( ConnectionInfos connection, String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern );


    ExecuteResult prepareAndExecuteDataDefinition( TransactionHandle transactionHandle, StatementHandle statementHandle, String sql, long maxRowCount, int maxRowsInFirstFrame );

    void onePhaseCommit( ConnectionHandle connectionHandle, TransactionHandle transactionHandle ) throws XAException;

    void commit( ConnectionHandle connectionHandle );

    void commit( ConnectionHandle connectionHandle, TransactionHandle transactionHandle ) throws XAException;

    boolean prepareCommit( ConnectionHandle connectionHandle, TransactionHandle transactionHandle ) throws XAException;

    void rollback( ConnectionHandle connectionHandle );

    void rollback( ConnectionHandle connectionHandle, TransactionHandle transactionHandle ) throws XAException;


    Planner getPlanner();

    void closeConnection( ConnectionInfos connection );
}
