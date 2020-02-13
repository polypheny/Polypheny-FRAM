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


import java.io.IOException;
import java.rmi.RemoteException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelJsonReader;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;


/**
 *
 */
public class Utils {

    private Utils() {
    }


    public static final UUID EMPTY_UUID = new UUID( 0L, 0L );
    public static final UUID USER_PA_UUID = new UUID( 0L, 1L );
    public static final UUID USER_ANONYMOUS_UUID = new UUID( -1L, -1L );


    /*
     * From https://stackoverflow.com/questions/4485128/how-do-i-convert-long-to-byte-and-back-in-java/29132118#29132118
     */
    public static byte[] longToBytes( long l ) {
        byte[] result = new byte[Long.BYTES];
        for ( int i = (Long.BYTES - 1); i >= 0; i-- ) {
            result[i] = (byte) (l & 0xFF);
            l >>= Byte.SIZE;
        }
        return result;
    }


    /*
     * From https://stackoverflow.com/questions/4485128/how-do-i-convert-long-to-byte-and-back-in-java/29132118#29132118
     */
    public static long bytesToLong( byte[] b ) {
        long result = 0;
        for ( int i = 0; i < Long.BYTES; i++ ) {
            result <<= Byte.SIZE;
            result |= (b[i] & 0xFF);
        }
        return result;
    }


    public static String relToString( RelNode relTree ) {
        RelJsonWriter writer = new RelJsonWriter();
        writer.done( relTree );
        return writer.asString();
    }


    public static RelNode stringToRel( final String relTreeAsJson, FrameworkConfig config ) throws IOException {
        final AtomicReference<RelJsonReader> readerReferece = new AtomicReference<>();
        Frameworks.withPlanner( ( cluster, relOptSchema, rootSchema ) -> {
            readerReferece.set( new RelJsonReader( cluster, relOptSchema, config.getDefaultSchema() ) );
            return null;
        }, config );

        return readerReferece.get().read( relTreeAsJson );
    }


    public static RuntimeException extractAndThrow( final Throwable t ) throws RuntimeException, Error {

        if ( t instanceof RemoteException == false ) {
            return new RuntimeException( t.getMessage(), t.getCause() );
        }

        final Throwable cause = t.getCause() == null ? t : t.getCause();
        if ( cause instanceof RuntimeException ) {
            throw (RuntimeException) cause;
        }
        if ( cause instanceof Error ) {
            throw (Error) cause;
        }
        throw new RuntimeException( cause.getMessage(), cause );
    }
}
