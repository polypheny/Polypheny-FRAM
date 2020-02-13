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

package org.polypheny.fram.remote;


import org.polypheny.fram.AbstractCatalog;
import javax.sql.DataSource;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.jgroups.blocks.MethodLookup;


/**
 *
 */
public abstract class AbstractLocalNode implements RemoteMeta, MethodLookup {

    protected AbstractLocalNode() {

    }


    @Override
    public final java.lang.reflect.Method findMethod( final short id ) {
        return RemoteMeta.Method.findMethod( id );
    }


    public abstract Config getSqlParserConfig();

    public abstract JdbcImplementor getRelToSqlConverter();

    public abstract SqlDialect getSqlDialect();

    public abstract DataSource getDataSource();

    public abstract AbstractCatalog getCatalog();
}
