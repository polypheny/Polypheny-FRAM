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

package org.polypheny.fram.standalone.parser.sql.ddl;


import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;


public class SqlDdlAlterNodes {

    private SqlDdlAlterNodes() {
    }


    /**
     * Creates a ALTER INDEX.
     */
    public static SqlAlterIndex alterIndex( SqlParserPos pos, SqlIdentifier indexName, SqlIdentifier newName ) {
        return new SqlAlterIndex( pos, indexName, newName );
    }


    /**
     * Creates a ALTER SCHEMA.
     */
    public static SqlAlterSchema alterSchema( SqlParserPos pos, SqlIdentifier schemaName, SqlIdentifier newName ) {
        return new SqlAlterSchema( pos, schemaName, newName );
    }


    public static class AlterTable {

        private AlterTable() {
        }


        public static SqlAlterTable rename( SqlParserPos pos, SqlIdentifier tableName, SqlIdentifier newName ) {
            return new SqlAlterTable( pos, tableName, newName );
        }


        public static SqlAlterTable addForeignKey( SqlParserPos pos, SqlIdentifier tableName, SqlIdentifier constraintName, SqlNodeList columnList, SqlIdentifier refName, SqlNodeList refColumnList, String onDelete, String onUpdate ) {
            return new SqlAlterTable( pos, tableName, constraintName, columnList, refName, refColumnList, onDelete, onUpdate );
        }
    }
}
