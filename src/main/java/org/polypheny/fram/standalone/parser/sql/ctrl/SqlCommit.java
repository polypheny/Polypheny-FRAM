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

package org.polypheny.fram.standalone.parser.sql.ctrl;


import org.polypheny.fram.standalone.parser.sql.SqlCtrl;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;


/**
 * Parse tree for {@code COMMIT} statement.
 */
public class SqlCommit extends SqlCtrl {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "COMMIT", SqlKind.COMMIT );


    public SqlCommit( SqlParserPos pos ) {
        super( OPERATOR, pos );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "COMMIT" );
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.<SqlNode>builder().build();
    }
}
