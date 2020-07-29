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

package org.polypheny.fram.standalone;


import org.apache.calcite.avatica.NoSuchStatementException;


public interface Meta extends org.apache.calcite.avatica.Meta {

    /**
     * Prepares a statement.
     *
     * @param ch Connection handle
     * @param sql SQL query
     * @param maxRowCount Negative for no limit (different meaning than JDBC)
     * @param columnIndexes An array of column indexes indicating the columns that should be returned from the inserted row or rows
     * @return Signature of prepared statement
     */
    StatementHandle prepare( ConnectionHandle ch, String sql, long maxRowCount, int[] columnIndexes );


    /**
     * Prepares and executes a statement.
     *
     * @param h Statement handle
     * @param sql SQL query
     * @param maxRowCount Maximum number of rows for the entire query. Negative for no limit
     * (different meaning than JDBC).
     * @param maxRowsInFirstFrame Maximum number of rows for the first frame. This value should
     * always be less than or equal to {@code maxRowCount} as the number of results are guaranteed
     * to be restricted by {@code maxRowCount} and the underlying database.
     * @param callback Callback to lock, clear and assign cursor
     * @param columnIndexes An array of column indexes indicating the columns that should be returned from the inserted row or rows
     * @return Result containing statement ID, and if a query, a result set and
     * first frame of data
     */
    ExecuteResult prepareAndExecute( StatementHandle h, String sql,
            long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback, int[] columnIndexes )
            throws NoSuchStatementException;


    /**
     * Retrieves any auto-generated keys created as a result of executing this Statement object.
     *
     * @param h Statement handle
     * @param maxRowCount Maximum number of rows for the entire query. Negative for no limit
     * (different meaning than JDBC).
     * @param maxRowsInFirstFrame Maximum number of rows to return in the Frame.
     * @return ResultSet object containing the auto-generated key(s)
     */
    ExecuteResult getGeneratedKeys( StatementHandle h, long maxRowCount, int maxRowsInFirstFrame )
            throws NoSuchStatementException;
}
