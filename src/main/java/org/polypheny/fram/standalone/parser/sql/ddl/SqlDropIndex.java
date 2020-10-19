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


import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;


/**
 * Parse tree for {@code DROP INDEX index [IF EXISTS];} statement.
 */
public class SqlDropIndex extends SqlDrop {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "DROP INDEX", SqlKind.DROP_INDEX );

    private final SqlIdentifier indexName;


    public SqlDropIndex( SqlParserPos pos, boolean ifExists, SqlIdentifier indexName ) {
        super( OPERATOR, pos, ifExists );
        this.indexName = Objects.requireNonNull( indexName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "DROP" );
        writer.keyword( "INDEX" );
        if ( ifExists ) {
            writer.keyword( "IF" );
            writer.keyword( "EXISTS" );
        }
        indexName.unparse( writer, leftPrec, rightPrec );
    }


    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( indexName );
    }
}
