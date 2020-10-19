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
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriter.FrameTypeEnum;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;


/**
 * Parse tree for {@code CREATE INDEX <index> ON <table> (<column> [, ...]);} statement.
 */
public class SqlCreateIndex extends SqlCreate {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "CREATE INDEX", SqlKind.CREATE_INDEX );

    private final SqlIdentifier indexName;
    private final SqlIdentifier tableName;
    private final SqlNodeList columns;


    public SqlCreateIndex( SqlParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier indexName, SqlIdentifier tableName, SqlNodeList columns ) {
        super( OPERATOR, pos, replace, ifNotExists );
        this.indexName = Objects.requireNonNull( indexName );
        this.tableName = Objects.requireNonNull( tableName );
        this.columns = Objects.requireNonNull( columns );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "CREATE" );
        writer.keyword( "INDEX" );
        if ( ifNotExists ) {
            writer.keyword( "IF NOT EXISTS" );
        }
        indexName.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "ON" );
        tableName.unparse( writer, leftPrec, rightPrec );
        {//NOSONAR "squid:S1199" - Justification: better readability
            final SqlWriter.Frame list = writer.startList( FrameTypeEnum.PARENTHESES, "(", ")" );
            columns.unparse( writer, leftPrec, rightPrec );
            writer.endList( list );
        }
    }


    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( indexName, tableName, columns );
    }
}
