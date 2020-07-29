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


import com.google.common.collect.Maps;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.polypheny.fram.remote.types.RemoteConnectionHandle;
import org.polypheny.fram.remote.types.RemoteExecuteBatchResult;
import org.polypheny.fram.remote.types.RemoteExecuteResult;
import org.polypheny.fram.remote.types.RemoteFrame;
import org.polypheny.fram.remote.types.RemoteStatementHandle;
import org.polypheny.fram.remote.types.RemoteTransactionHandle;


/**
 *
 */
public interface RemoteMeta extends java.rmi.Remote {

    /**
     * Returns a map of static database properties.
     *
     * <p>The provider can omit properties whose value is the same as the
     * default.
     */
    Map<Common.DatabaseProperty, Serializable> getDatabaseProperties( RemoteConnectionHandle remoteConnectionHandle ) throws RemoteException;


    /**
     * Prepares a statement.
     *
     * @param remoteStatementHandle Statement handle
     * @param sql SQL query
     * @param maxRowCount Negative for no limit (different meaning than JDBC)
     * @return Signature of prepared statement
     */
    RemoteStatementHandle prepare( RemoteStatementHandle remoteStatementHandle, String sql, long maxRowCount ) throws RemoteException;

    /**
     * Prepares a statement.
     *
     * @param remoteStatementHandle Statement handle
     * @param sql SQL query
     * @param maxRowCount Negative for no limit (different meaning than JDBC)
     * @param columnIndexes An array of column indexes indicating the columns that should be returned from the inserted row or rows
     * @return Signature of prepared statement
     */
    RemoteStatementHandle prepare( RemoteStatementHandle remoteStatementHandle, String sql, long maxRowCount, int[] columnIndexes ) throws RemoteException;

    /**
     * Prepares and executes a statement.
     *
     * @param remoteTransactionHandle (Global) transaction identifier
     * @param sql SQL query
     * @param maxRowCount Maximum number of rows for the entire query. Negative for no limit
     * (different meaning than JDBC).
     * @param maxRowsInFirstFrame Maximum number of rows for the first frame. This value should
     * always be less than or equal to {@code maxRowCount} as the number of results are guaranteed
     * to be restricted by {@code maxRowCount} and the underlying database.
     * @return Result containing statement ID, and if a query, a result set and
     * first frame of data
     */
    RemoteExecuteResult prepareAndExecute( RemoteTransactionHandle remoteTransactionHandle, RemoteStatementHandle remoteStatementHandle, String sql, long maxRowCount, int maxRowsInFirstFrame ) throws RemoteException;

    /**
     * Prepares and executes a statement.
     *
     * @param remoteTransactionHandle (Global) transaction identifier
     * @param sql SQL query
     * @param maxRowCount Maximum number of rows for the entire query. Negative for no limit
     * (different meaning than JDBC).
     * @param maxRowsInFirstFrame Maximum number of rows for the first frame. This value should
     * always be less than or equal to {@code maxRowCount} as the number of results are guaranteed
     * to be restricted by {@code maxRowCount} and the underlying database.
     * @param columnIndexes An array of column indexes indicating the columns that should be returned from the inserted row or rows
     * @return Result containing statement ID, and if a query, a result set and
     * first frame of data
     */
    RemoteExecuteResult prepareAndExecute( RemoteTransactionHandle remoteTransactionHandle, RemoteStatementHandle remoteStatementHandle, String sql, long maxRowCount, int maxRowsInFirstFrame, int[] columnIndexes ) throws RemoteException;

    /**
     * Prepares and executes a statement.
     *
     * @param remoteTransactionHandle (Global) transaction identifier
     * @param globalCatalogSql the query for the global catalog
     * @param localStoreSql the quiery for the local store
     * @param maxRowCount Maximum number of rows for the entire query. Negative for no limit
     * (different meaning than JDBC).
     * @param maxRowsInFirstFrame Maximum number of rows for the first frame. This value should
     * always be less than or equal to {@code maxRowCount} as the number of results are guaranteed
     * to be restricted by {@code maxRowCount} and the underlying database.
     * @return Result containing statement ID, and if a query, a result set and
     * first frame of data
     */
    RemoteExecuteResult prepareAndExecuteDataDefinition( RemoteTransactionHandle remoteTransactionHandle, RemoteStatementHandle remoteStatementHandle, String globalCatalogSql, String localStoreSql, long maxRowCount, int maxRowsInFirstFrame ) throws RemoteException;

    /**
     * Prepares a statement and then executes a number of SQL commands in one pass.
     *
     * @param remoteTransactionHandle (Global) transaction identifier
     * @param sqlCommands SQL commands to run
     * @return An array of update counts containing one element for each command in the batch.
     */
    RemoteExecuteBatchResult prepareAndExecuteBatch( RemoteTransactionHandle remoteTransactionHandle, RemoteStatementHandle remoteStatementHandle, List<String> sqlCommands ) throws RemoteException;

    /**
     * Executes a collection of bound parameter values on a prepared statement.
     *
     * @param remoteTransactionHandle (Global) transaction identifier
     * @param parameterValues A collection of list of typed values, one list per batch
     * @return An array of update counts containing one element for each command in the batch.
     */
    RemoteExecuteBatchResult executeBatch( RemoteTransactionHandle remoteTransactionHandle, RemoteStatementHandle remoteStatementHandle, List<UpdateBatch> parameterValues ) throws RemoteException;

    /**
     * Returns a frame of rows.
     *
     * <p>The frame describes whether there may be another frame. If there is not
     * another frame, the current iteration is done when we have finished the
     * rows in the this frame.
     *
     * <p>The default implementation always returns null.
     *
     * @param remoteStatementHandle Statement handle
     * @param offset Zero-based offset of first row in the requested frame
     * @param fetchMaxRowCount Maximum number of rows to return; negative means
     * no limit
     * @return Frame, or null if there are no more
     */
    RemoteFrame fetch( RemoteStatementHandle remoteStatementHandle, long offset, int fetchMaxRowCount ) throws RemoteException;

    /**
     * Executes a prepared statement.
     *
     * @param remoteTransactionHandle (Global) transaction identifier
     * @param remoteStatementHandle Statement handle
     * @param parameterValues A list of parameter values; may be empty, not null
     * @param maxRowsInFirstFrame Maximum number of rows to return in the Frame.
     * @return Execute result
     */
    RemoteExecuteResult execute( RemoteTransactionHandle remoteTransactionHandle, RemoteStatementHandle remoteStatementHandle, List<Common.TypedValue> parameterValues, int maxRowsInFirstFrame ) throws RemoteException;

//    /**
//     * Called during the creation of a statement to allocate a new handle.
//     *
//     * @param ch Connection handle
//     */
//    StatementHandle createStatement( ConnectionHandle ch );

    /**
     * Closes a statement.
     *
     * <p>If the statement handle is not known, or is already closed, does
     * nothing.
     *
     * @param remoteStatementHandle Statement handle
     */
    Void closeStatement( RemoteStatementHandle remoteStatementHandle ) throws RemoteException;

//    /**
//     * Opens (creates) a connection. The client allocates its own connection ID which the server is
//     * then made aware of through the {@link ConnectionHandle}. The Map {@code info} argument is
//     * analogous to the {@link Properties} typically passed to a "normal" JDBC Driver. Avatica
//     * specific properties should not be included -- only properties for the underlying driver.
//     *
//     * @param ch A ConnectionHandle encapsulates information about the connection to be opened
//     * as provided by the client.
//     * @param info A Map corresponding to the Properties typically passed to a JDBC Driver.
//     */
//    void openConnection( ConnectionHandle ch, Map<String, String> info );

    /**
     * Closes a connection
     */
    Void closeConnection( RemoteConnectionHandle remoteConnectionHandle ) throws RemoteException;

    /**
     * Aborts a connection
     */
    Void abortConnection( RemoteConnectionHandle remoteConnectionHandle, RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException;

//    /**
//     * Re-sets the {@link ResultSet} on a Statement. Not a JDBC method.
//     *
//     * @return True if there are results to fetch after resetting to the given offset. False otherwise
//     */
//    boolean syncResults( StatementHandle sh, QueryState state, long offset ) throws RemoteException;
//

    /**
     * Synchronizes client and server view of connection properties.
     *
     * <p>Note: this interface is considered "experimental" and may undergo further changes as this
     * functionality is extended to other aspects of state management for
     * {@link java.sql.Connection}, {@link java.sql.Statement}, and {@link java.sql.ResultSet}.</p>
     */
    Common.ConnectionProperties connectionSync( RemoteConnectionHandle ch, Common.ConnectionProperties connProps ) throws RemoteException;

    /**
     * @param remoteTransactionHandle (Global) transaction identifier
     */
    Void onePhaseCommit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException;

    /**
     * @param remoteTransactionHandle (Global) transaction identifier
     */
    boolean prepareCommit( RemoteConnectionHandle remoteConnectionHandle, RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException;

    /**
     * @param remoteTransactionHandle (Global) transaction identifier
     */
    Void commit( RemoteConnectionHandle remoteConnectionHandle, RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException;

    /**
     * @param remoteTransactionHandle (Global) transaction identifier
     */
    Void rollback( RemoteConnectionHandle remoteConnectionHandle, RemoteTransactionHandle remoteTransactionHandle ) throws RemoteException;


    /**
     * Used in the middleware to combine the RemoteFrames from the sources.
     * The call to <code>fetch()</code> returns the Frame to be returned to the App.
     */
    interface RemoteResultSet {

        List<RemoteMeta> getOrigins();

        /**
         * Returns a frame of rows.
         *
         * <p>The frame describes whether there may be another frame. If there is not
         * another frame, the current iteration is done when we have finished the
         * rows in the this frame.
         *
         * <p>The default implementation always returns null.
         *
         * @param remoteStatementHandle Statement handle
         * @param offset Zero-based offset of first row in the requested frame
         * @param fetchMaxRowCount Maximum number of rows to return; negative means
         * no limit
         * @return Frame, or null if there are no more
         */
        Common.Frame fetch( RemoteStatementHandle remoteStatementHandle, long offset, int fetchMaxRowCount ) throws RemoteException;
    }


    /**
     *
     */
    enum Method {
        ABORT( (short) 10, "abortConnection", RemoteConnectionHandle.class, RemoteTransactionHandle.class ),
        PREPARE_COMMIT( (short) 20, "prepareCommit", RemoteConnectionHandle.class, RemoteTransactionHandle.class ),
        COMMIT( (short) 30, "commit", RemoteConnectionHandle.class, RemoteTransactionHandle.class ),
        ONE_PHASE_COMMIT( (short) 31, "onePhaseCommit", RemoteConnectionHandle.class, RemoteTransactionHandle.class ),
        ROLLBACK( (short) 40, "rollback", RemoteConnectionHandle.class, RemoteTransactionHandle.class ),
        PREPARE( (short) 50, "prepare", RemoteStatementHandle.class, String.class, long.class ),
        PREPARE2( (short) 51, "prepare", RemoteStatementHandle.class, String.class, long.class, int[].class ),
        EXECUTE( (short) 60, "execute", RemoteTransactionHandle.class, RemoteStatementHandle.class, List.class, int.class ),
        PREPARE_AND_EXECUTE( (short) 70, "prepareAndExecute", RemoteTransactionHandle.class, RemoteStatementHandle.class, String.class, long.class, int.class ),
        PREPARE_AND_EXECUTE2( (short) 71, "prepareAndExecute", RemoteTransactionHandle.class, RemoteStatementHandle.class, String.class, long.class, int.class, int[].class ),
        PREPARE_AND_EXECUTE_DATA_DEFINITION( (short) 75, "prepareAndExecuteDataDefinition", RemoteTransactionHandle.class, RemoteStatementHandle.class, String.class, String.class, long.class, int.class ),
        PREPARE_AND_EXECUTE_BATCH( (short) 79, "prepareAndExecuteBatch", RemoteTransactionHandle.class, RemoteStatementHandle.class, List.class ),
        EXECUTE_BATCH( (short) 80, "executeBatch", RemoteTransactionHandle.class, RemoteStatementHandle.class, List.class ),
        FETCH( (short) 90, "fetch", RemoteStatementHandle.class, long.class, int.class ),
        CLOSE_STATEMENT( (short) 100, "closeStatement", RemoteStatementHandle.class ),
        CLOSE_CONNECTION( (short) 110, "closeConnection", RemoteConnectionHandle.class ),
        CONNECTION_SYNC( (short) 120, "connectionSync", RemoteConnectionHandle.class, Common.ConnectionProperties.class ),
        ;
        //

        //


        public static org.jgroups.blocks.MethodCall abort( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) {
            return ABORT.call( remoteConnectionHandle, remoteTransactionHandle );
        }


        public static org.jgroups.blocks.MethodCall onePhaseCommit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) {
            return ONE_PHASE_COMMIT.call( remoteConnectionHandle, remoteTransactionHandle );
        }


        public static org.jgroups.blocks.MethodCall prepareCommit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) {
            return PREPARE_COMMIT.call( remoteConnectionHandle, remoteTransactionHandle );
        }


        public static org.jgroups.blocks.MethodCall commit( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) {
            return COMMIT.call( remoteConnectionHandle, remoteTransactionHandle );
        }


        public static org.jgroups.blocks.MethodCall rollback( final RemoteConnectionHandle remoteConnectionHandle, final RemoteTransactionHandle remoteTransactionHandle ) {
            return ROLLBACK.call( remoteConnectionHandle, remoteTransactionHandle );
        }


        public static org.jgroups.blocks.MethodCall prepare( final RemoteStatementHandle remoteStatementHandle, final String sql, final long maxRowCount ) {
            return PREPARE.call( remoteStatementHandle, sql, maxRowCount );
        }


        public static org.jgroups.blocks.MethodCall prepare( final RemoteStatementHandle remoteStatementHandle, final String sql, final long maxRowCount, final int[] columnIndexes ) {
            return PREPARE2.call( remoteStatementHandle, sql, maxRowCount, columnIndexes );
        }


        public static org.jgroups.blocks.MethodCall execute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<Common.TypedValue> parameterValues, final int maxRowsInFirstFrame ) {
            return EXECUTE.call( remoteTransactionHandle, remoteStatementHandle, parameterValues, maxRowsInFirstFrame );
        }


        public static org.jgroups.blocks.MethodCall prepareAndExecute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final String sql, final long maxRowCount, final int maxRowsInFirstFrame ) {
            return PREPARE_AND_EXECUTE.call( remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame );
        }


        public static org.jgroups.blocks.MethodCall prepareAndExecute( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final String sql, final long maxRowCount, final int maxRowsInFirstFrame, final int[] columnIndexes ) {
            return PREPARE_AND_EXECUTE2.call( remoteTransactionHandle, remoteStatementHandle, sql, maxRowCount, maxRowsInFirstFrame, columnIndexes );
        }


        public static org.jgroups.blocks.MethodCall prepareAndExecuteDataDefinition( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final String globalCatalogSql, final String localStoreSql, final long maxRowCount, final int maxRowsInFirstFrame ) {
            return PREPARE_AND_EXECUTE_DATA_DEFINITION.call( remoteTransactionHandle, remoteStatementHandle, globalCatalogSql, localStoreSql, maxRowCount, maxRowsInFirstFrame );
        }


        public static org.jgroups.blocks.MethodCall prepareAndExecuteBatch( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<String> sqlCommands ) {
            return PREPARE_AND_EXECUTE_BATCH.call( remoteStatementHandle, remoteStatementHandle, sqlCommands );
        }


        public static org.jgroups.blocks.MethodCall executeBatch( final RemoteTransactionHandle remoteTransactionHandle, final RemoteStatementHandle remoteStatementHandle, final List<UpdateBatch> parameterValues ) {
            return EXECUTE_BATCH.call( remoteTransactionHandle, remoteStatementHandle, parameterValues );
        }


        public static org.jgroups.blocks.MethodCall fetch( final RemoteStatementHandle remoteStatementHandle, final long offset, final int fetchMaxRowCount ) {
            return FETCH.call( remoteStatementHandle, offset, fetchMaxRowCount );
        }


        public static org.jgroups.blocks.MethodCall closeStatement( final RemoteStatementHandle remoteStatementHandle ) {
            return CLOSE_STATEMENT.call( remoteStatementHandle );
        }


        public static org.jgroups.blocks.MethodCall closeConnection( final RemoteConnectionHandle remoteConnectionHandle ) {
            return CLOSE_CONNECTION.call( remoteConnectionHandle );
        }


        public static org.jgroups.blocks.MethodCall connectionSync( final RemoteConnectionHandle remoteConnectionHandle, final Common.ConnectionProperties properties ) {
            return CONNECTION_SYNC.call( remoteConnectionHandle, properties );
        }


        /**
         * @param id The ID of the method
         * @return Method for the given ID, or null
         * @see org.jgroups.blocks.MethodLookup
         */
        public static java.lang.reflect.Method findMethod( final short id ) {
            return valueOf( id ).getMethod();
        }


        public static Method valueOf( short methodId ) {
            return LOOKUP.get( methodId );
        }


        private static final Map<Short, Method> LOOKUP = Maps.uniqueIndex( Arrays.asList( Method.values() ), Method::getId );

        private final short id;
        private final transient java.lang.reflect.Method method;


        Method( final short id, final String methodName, final Class<?>... parameterTypes ) {
            this.id = id;
            try {
                this.method = RemoteMeta.class.getMethod( methodName, parameterTypes );
            } catch ( NoSuchMethodException e ) {
                throw new Error( "This should not happen. Check the enum declarations vs. the declared methods in `" + RemoteMeta.class.getCanonicalName() + "´.", e );
            }
        }


        public short getId() {
            return id;
        }


        public java.lang.reflect.Method getMethod() {
            return method;
        }


        /**
         * Creates a org.jgroups.blocks.MethodCall object from this Enum using the given parameters.
         *
         * @param parameter The parameters used by the MethodCall
         * @return The MethodCall object calling this Method using the given parameters.
         */
        public org.jgroups.blocks.MethodCall call( final Object... parameter ) {
            return new org.jgroups.blocks.MethodCall( this.id, parameter );
        }
    }
}
