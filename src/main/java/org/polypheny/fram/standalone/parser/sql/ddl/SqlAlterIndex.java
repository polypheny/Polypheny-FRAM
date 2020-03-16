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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.polypheny.fram.standalone.parser.sql.SqlDdlAlter;


/**
 * Parse tree for {@code ALTER INDEX <indexname> RENAME TO <newname>;} statement.
 */
public class SqlAlterIndex extends SqlDdlAlter {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "ALTER INDEX", SqlKind.ALTER_INDEX );

    private final SqlIdentifier indexName;
    private final SqlIdentifier newName;


    public SqlAlterIndex( SqlParserPos pos, SqlIdentifier indexName, SqlIdentifier newName ) {
        super( OPERATOR, pos );
        this.indexName = Objects.requireNonNull( indexName );
        this.newName = Objects.requireNonNull( newName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "INDEX" );
        indexName.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "RENAME" );
        writer.keyword( "TO" );
        newName.unparse( writer, leftPrec, rightPrec );
    }


    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( indexName, newName );
    }
}
