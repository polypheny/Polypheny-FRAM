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


import org.polypheny.fram.AbstractCatalog;
import org.polypheny.fram.standalone.SimpleNode.DatabaseHolder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
class SimpleCatalog extends AbstractCatalog {

    private static final Logger LOGGER = LoggerFactory.getLogger( SimpleCatalog.class );


    private final XAMeta xaMeta;
    private final DataSource dataSource;

    private final AtomicReference<UUID> nodeIdReference = new AtomicReference<>();


    private SimpleCatalog() {
        this.xaMeta = DatabaseHolder.storage;
        this.dataSource = DatabaseHolder.storageDataSource;
    }


    public UUID getNodeId() {
        synchronized ( nodeIdReference ) {
            UUID nodeId = nodeIdReference.get();

            if ( nodeId == null ) {
                try ( final Connection connetion = this.dataSource.getConnection() ) {
                    try ( final Statement statement = connetion.createStatement() ) {
                        try {
                            statement.executeUpdate( "CREATE TABLE IF NOT EXISTS UUIDs (id INTEGER IDENTITY PRIMARY KEY, serializedUUID OTHER NOT NULL)" );
                        } catch ( SQLException ignored ) {
                        }

                        try ( final ResultSet result = statement.executeQuery( "SELECT serializedUUID FROM UUIDs WHERE id = -1" ) ) {
                            while ( result.next() ) {
                                nodeId = result.getObject( 1, UUID.class );
                            }
                        }

                        if ( nodeId == null ) {
                            // we got empty result set

                            nodeId = UUID.randomUUID();

                            try ( final PreparedStatement insertStatement = connetion.prepareStatement( "INSERT INTO UUIDs (id , serializedUUID) VALUES (-1, ?)" ) ) {
                                insertStatement.setObject( 1, nodeId );

                                if ( insertStatement.execute() || insertStatement.getUpdateCount() != 1 ) {
                                    throw new RuntimeException( "Cannot insert NodeId." );
                                }
                            }
                        }
                    }
                    if ( !connetion.getAutoCommit() ) {
                        connetion.commit();
                    }
                } catch ( SQLException e ) {
                    throw Utils.extractAndThrow( e );
                }
                nodeIdReference.set( nodeId );
            }

            return nodeId;
        }
    }


    private static class SingletonHolder {

        private static final SimpleCatalog INSTANCE = new SimpleCatalog();


        private SingletonHolder() {
        }
    }


    static SimpleCatalog getInstance() {
        return SingletonHolder.INSTANCE;
    }

}
