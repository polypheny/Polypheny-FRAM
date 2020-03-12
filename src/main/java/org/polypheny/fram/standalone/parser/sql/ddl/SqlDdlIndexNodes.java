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


public class SqlDdlIndexNodes {

    private SqlDdlIndexNodes() {
    }


    /**
     * Creates a CREATE INDEX.
     */
    public static SqlCreateIndex createIndex( SqlParserPos pos, SqlIdentifier indexName, SqlIdentifier tableName, SqlNodeList columns ) {
        return new SqlCreateIndex( pos, indexName, tableName, columns );
    }


    /**
     * Creates a DROP INDEX.
     */
    public static SqlDropIndex dropIndex( SqlParserPos pos, SqlIdentifier indexName, boolean ifExists ) {
        return new SqlDropIndex( pos, indexName, ifExists );
    }
}
