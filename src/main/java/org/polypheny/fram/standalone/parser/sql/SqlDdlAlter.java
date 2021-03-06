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

package org.polypheny.fram.standalone.parser.sql;


import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;


/**
 * Base class for COMMIT, ROLLBACK, and other control statements.
 */
public abstract class SqlDdlAlter extends SqlDdl {

    /**
     * Use this operator only if you don't have a better one.
     */
    protected static final SqlOperator ALTER_OPERATOR = new SqlSpecialOperator( "ALTER", SqlKind.OTHER_DDL );


    /**
     * Creates a node.
     *
     * @param pos Parser position, must not be null.
     */
    public SqlDdlAlter( SqlOperator operator, SqlParserPos pos ) {
        super( operator, pos );
    }
}
