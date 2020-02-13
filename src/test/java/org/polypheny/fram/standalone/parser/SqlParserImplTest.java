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

package org.polypheny.fram.standalone.parser;


import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.test.ServerParserTest;
import org.junit.Test;


/**
 *
 */
public class SqlParserImplTest extends ServerParserTest {

    @Override
    protected SqlParserImplFactory parserImplFactory() {
        return org.polypheny.fram.standalone.parser.SqlParserImpl.FACTORY;
    }


    @Override
    public void testGenerateKeyWords() {
        // by design, method only works in base class; no-ops in this sub-class
    }


    @Test
    public void testCommit() {
        sql( "commit" )
                .ok( "COMMIT" );
    }


    @Test
    public void testRollback() {
        sql( "rollback" )
                .ok( "ROLLBACK" );
    }
}
