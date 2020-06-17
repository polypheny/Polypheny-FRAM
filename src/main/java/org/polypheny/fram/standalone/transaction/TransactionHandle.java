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

package org.polypheny.fram.standalone.transaction;


import java.io.Serializable;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.UUID;
import javax.transaction.xa.Xid;
import org.apache.commons.codec.binary.Hex;
import org.polypheny.fram.standalone.Utils;


/**
 *
 */
public class TransactionHandle implements Xid, Serializable {

    private static final long serialVersionUID = 1L;

    // LSB = least significant bit/byte
    // MSB = most significant bit/byte

    private static final int NODE_ID_LSB_INDEX = 0;
    private static final int USER_ID_LSB_INDEX = 16;
    private static final int CONNECTION_ID_LSB_INDEX = 32;
    private static final int TRANSACTION_ID_LSB_INDEX = 48;
    private static final int STORE_ID_LSB_INDEX = 16;
    private static final int RESERVED_ID_LSB_INDEX = 32;
    private static final int CUSTOM_ID_LSB_INDEX = 48;

    /**
     * |----------------- globalTransactionId (64 Bytes) --------------------------------------------------------|
     * |- UUID: nodeId ---------| |- UUID: userId ---------| |- UUID: connectionId ---| |- UUID: transactionId --|
     * |- 8 Bytes -||- 8 Bytes -| |- 8 Bytes -||- 8 Bytes -| |- 8 Bytes -||- 8 Bytes -| |- 8 Bytes -||- 8 Bytes -|
     * most sig                                                                                        least sig
     */
    private final byte[] globalTransactionId = new byte[javax.transaction.xa.Xid.MAXGTRIDSIZE];
    /**
     * |----------------- branchQualifier (64 Bytes) ----------------------------------------------------------------|
     * |- UUID: branchId (nodeId) --| |- UUID: storeId --------| |- UUID: reserved -------| |- UUID: custom ---------|
     * |- 8 Bytes ---||--- 8 Bytes -| |- 8 Bytes -||- 8 Bytes -| |- 8 Bytes -||- 8 Bytes -| |- 8 Bytes -||- 8 Bytes -|
     * most sig                                                                                        least sig
     */
    private final byte[] branchQualifier = new byte[javax.transaction.xa.Xid.MAXBQUALSIZE];


    /**
     * Identifier for a global transaction
     *
     * Scope: GLOBAL, coordinator layer (i.e., facing the client; before the network)
     */
    public static TransactionHandle generateGlobalTransactionIdentifier( final UUID nodeId, final UUID userId, final UUID connectionId, final UUID transactionId ) {
        return new TransactionHandle( nodeId, userId, connectionId, transactionId );
    }


    /**
     * Identifier which needs to be generated on a remote branch out of the global transaction identifier
     *
     * Scope: GLOBAL, storage layer (i.e., facing the storage; after the network)
     */
    public static TransactionHandle generateBranchTransactionIdentifier( final Xid globalTransactionIdentifier, final UUID nodeId ) {
        return new TransactionHandle( globalTransactionIdentifier, nodeId );
    }


    /**
     * Identifier which is used for locally running -- mostly maintenance -- transactions
     *
     * This identifier has only the transaction id part of the global part set, as well as the node part of the branch part.
     *
     * Scope: LOCAL
     */
    public static TransactionHandle generateLocalTransactionIdentifier( final UUID nodeId, final UUID transactionId ) {
        return new TransactionHandle( nodeId, transactionId );
    }


    /*
     * global transaction
     */
    private TransactionHandle( final UUID nodeId, final UUID userId, final UUID connectionId, final UUID transactionId ) {
        //
        setField( this.globalTransactionId, NODE_ID_LSB_INDEX, nodeId.getMostSignificantBits(), nodeId.getLeastSignificantBits() );
        setField( this.globalTransactionId, USER_ID_LSB_INDEX, userId.getMostSignificantBits(), userId.getLeastSignificantBits() );
        setField( this.globalTransactionId, CONNECTION_ID_LSB_INDEX, connectionId.getMostSignificantBits(), connectionId.getLeastSignificantBits() );
        setField( this.globalTransactionId, TRANSACTION_ID_LSB_INDEX, transactionId.getMostSignificantBits(), transactionId.getLeastSignificantBits() );
    }


    /*
     * branch transaction
     */
    private TransactionHandle( final Xid xid, final UUID nodeId ) {
        //
        final byte[] otherGlobalTransactionId = xid.getGlobalTransactionId();
        System.arraycopy( otherGlobalTransactionId, 0, this.globalTransactionId, 0, Math.min( otherGlobalTransactionId.length, this.globalTransactionId.length ) );

        //
        setField( this.branchQualifier, NODE_ID_LSB_INDEX, nodeId.getMostSignificantBits(), nodeId.getLeastSignificantBits() );
    }


    /*
     * local transaction
     */
    private TransactionHandle( final UUID nodeId, final UUID transactionId ) {
        //
        // NO (origin) NODE
        // NO USER
        // NO CONNECTION
        setField( this.globalTransactionId, TRANSACTION_ID_LSB_INDEX, transactionId.getMostSignificantBits(), transactionId.getLeastSignificantBits() );

        //
        setField( this.branchQualifier, NODE_ID_LSB_INDEX, nodeId.getMostSignificantBits(), nodeId.getLeastSignificantBits() );
    }


    private UUID extractUUID( final byte[] xidPart, final int leastSignificantBytesIndex ) {
        long[] field = getField( xidPart, leastSignificantBytesIndex );
        return new UUID( field[0], field[1] );
    }


    private long[] getField( final byte[] xidPart, final int leastSignificantBytesIndex ) {
        byte[] leastSignificantBytes = new byte[Long.BYTES];
        System.arraycopy( xidPart, leastSignificantBytesIndex, leastSignificantBytes, 0, leastSignificantBytes.length );
        byte[] mostSignificantBytes = new byte[Long.BYTES];
        System.arraycopy( xidPart, leastSignificantBytesIndex + Long.BYTES, mostSignificantBytes, 0, mostSignificantBytes.length );

        return new long[]{ Utils.bytesToLong( leastSignificantBytes ), Utils.bytesToLong( mostSignificantBytes ) };
    }


    private void setField( final byte[] xidPart, final int leastSignificantBytesIndex, final long mostSignificantBytes, final long leastSignificantBytes ) {
        System.arraycopy( Utils.longToBytes( mostSignificantBytes ), 0, xidPart, leastSignificantBytesIndex, Long.BYTES );
        System.arraycopy( Utils.longToBytes( leastSignificantBytes ), 0, xidPart, leastSignificantBytesIndex + Long.BYTES, Long.BYTES );
    }


    public UUID getNodeId() {
        return extractUUID( this.globalTransactionId, NODE_ID_LSB_INDEX );
    }


    public UUID getConnectionId() {
        return extractUUID( this.globalTransactionId, CONNECTION_ID_LSB_INDEX );
    }


    public UUID getUserId() {
        return extractUUID( this.globalTransactionId, USER_ID_LSB_INDEX );
    }


    public UUID getTransactionId() {
        return extractUUID( this.globalTransactionId, TRANSACTION_ID_LSB_INDEX );
    }


    public UUID getBranchId() {
        return extractUUID( this.branchQualifier, NODE_ID_LSB_INDEX );
    }


    public UUID getStoreId() {
        return extractUUID( this.branchQualifier, STORE_ID_LSB_INDEX );
    }


    public TransactionHandle setStoreId( final UUID storeId ) {
        return setStoreField( storeId.getMostSignificantBits(), storeId.getLeastSignificantBits() );
    }


    public long[] getStoreField() {
        return getField( this.branchQualifier, STORE_ID_LSB_INDEX );
    }


    public TransactionHandle setStoreField( final long mostSignificantBytes, final long leastSignificantBytes ) {
        TransactionHandle withStore = this.clone();
        withStore.setField( this.branchQualifier, STORE_ID_LSB_INDEX, mostSignificantBytes, leastSignificantBytes );
        return withStore;
    }


    public UUID getReservedId() {
        return extractUUID( this.branchQualifier, RESERVED_ID_LSB_INDEX );
    }


    public TransactionHandle setReservedId( final UUID reservedId ) {
        return setReservedField( reservedId.getMostSignificantBits(), reservedId.getLeastSignificantBits() );
    }


    public long[] getReservedField() {
        return getField( this.branchQualifier, RESERVED_ID_LSB_INDEX );
    }


    public TransactionHandle setReservedField( final long mostSignificantBytes, final long leastSignificantBytes ) {
        TransactionHandle withReserved = this.clone();
        withReserved.setField( this.branchQualifier, RESERVED_ID_LSB_INDEX, mostSignificantBytes, leastSignificantBytes );
        return withReserved;
    }


    public UUID getCustomId() {
        return extractUUID( this.branchQualifier, CUSTOM_ID_LSB_INDEX );
    }


    public TransactionHandle setCustomId( final UUID customId ) {
        return setCustomField( customId.getMostSignificantBits(), customId.getLeastSignificantBits() );
    }


    public long[] getCustomField() {
        return getField( this.branchQualifier, CUSTOM_ID_LSB_INDEX );
    }


    public TransactionHandle setCustomField( final long mostSignificantBytes, final long leastSignificantBytes ) {
        TransactionHandle withCustom = this.clone();
        withCustom.setField( this.branchQualifier, CUSTOM_ID_LSB_INDEX, mostSignificantBytes, leastSignificantBytes );
        return withCustom;
    }


    public boolean isGlobalTransactionIdentifier() {
        // globalTransactionId set
        // NO branchQualifier
        return !getNodeId().equals( Utils.EMPTY_UUID ) && !getUserId().equals( Utils.EMPTY_UUID ) && !getConnectionId().equals( Utils.EMPTY_UUID ) && !getTransactionId().equals( Utils.EMPTY_UUID )
                && getBranchId().equals( Utils.EMPTY_UUID ) && getStoreId().equals( Utils.EMPTY_UUID );
    }


    public boolean isBranchTransactionIdentifier() {
        // globalTransactionId set
        // branchQualifier
        return !getNodeId().equals( Utils.EMPTY_UUID ) && !getUserId().equals( Utils.EMPTY_UUID ) && !getConnectionId().equals( Utils.EMPTY_UUID ) && !getTransactionId().equals( Utils.EMPTY_UUID )
                && !getBranchId().equals( Utils.EMPTY_UUID );
    }


    public boolean isLocalTransactionIdentifier() {
        // NO (origin) NODE
        // NO USER
        // NO CONNECTION
        return getNodeId().equals( Utils.EMPTY_UUID ) && getUserId().equals( Utils.EMPTY_UUID ) && getConnectionId().equals( Utils.EMPTY_UUID ) && !getTransactionId().equals( Utils.EMPTY_UUID )
                && !getBranchId().equals( Utils.EMPTY_UUID );
    }


    public TransactionHandle generateBranchTransactionIdentifier( final UUID nodeId ) {
        return new TransactionHandle( this, nodeId );
    }


    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }
        TransactionHandle that = (TransactionHandle) o;
        return Arrays.equals( globalTransactionId, that.globalTransactionId ) &&
                Arrays.equals( branchQualifier, that.branchQualifier );
    }


    @Override
    public int hashCode() {
        int result = Arrays.hashCode( globalTransactionId );
        result = 31 * result + Arrays.hashCode( branchQualifier );
        return result;
    }


    @Override
    public String toString() {
        return "Xid: {GID: "
                + identifierToString( this.globalTransactionId )
                + ", BID: "
                + identifierToString( this.branchQualifier )
                + "}";
    }


    private String identifierToString( final byte[] identifier ) {
        // Use regex to format the hex string by inserting hyphens in the canonical format: 8-4-4-4-12
        return Hex.encodeHexString( identifier ).replaceFirst(
                "([0-9a-fA-F]{8})([0-9a-fA-F]{4})([0-9a-fA-F]{4})([0-9a-fA-F]{4})([0-9a-fA-F]{12})" + //NOSONAR squid:S1192
                        "([0-9a-fA-F]{8})([0-9a-fA-F]{4})([0-9a-fA-F]{4})([0-9a-fA-F]{4})([0-9a-fA-F]{12})" + //NOSONAR squid:S1192
                        "([0-9a-fA-F]{8})([0-9a-fA-F]{4})([0-9a-fA-F]{4})([0-9a-fA-F]{4})([0-9a-fA-F]{12})" + //NOSONAR squid:S1192
                        "([0-9a-fA-F]{8})([0-9a-fA-F]{4})([0-9a-fA-F]{4})([0-9a-fA-F]{4})([0-9a-fA-F]{12})", "$1-$2-$3-$4-$5--$6-$7-$8-$9-$10--$11-$12-$13-$14-$15--$16-$17-$18-$19-$20" ); //NOSONAR squid:S1192
    }


    private static final Object[] CLONE_LOCK = new Object[0];


    @Override
    public TransactionHandle clone() {
        try {
            final TransactionHandle clone = (TransactionHandle) super.clone();

            final Field globalTransactionId_Field = TransactionHandle.class.getDeclaredField( "globalTransactionId" );
            final Field branchQualifier_Field = TransactionHandle.class.getDeclaredField( "branchQualifier" );

            synchronized ( CLONE_LOCK ) {
                AccessController.doPrivileged( (PrivilegedAction<Void>) () -> {
                    globalTransactionId_Field.setAccessible( true );
                    branchQualifier_Field.setAccessible( true );
                    return (Void) null;
                } );

                globalTransactionId_Field.set( clone, this.globalTransactionId.clone() );
                branchQualifier_Field.set( clone, this.branchQualifier.clone() );

                AccessController.doPrivileged( (PrivilegedAction<Void>) () -> {
                    globalTransactionId_Field.setAccessible( false );
                    branchQualifier_Field.setAccessible( false );
                    return (Void) null;
                } );
            }

            return clone;
        } catch ( CloneNotSupportedException | NoSuchFieldException ex ) {
            throw new RuntimeException( "This should not happen.", ex );
        } catch ( IllegalAccessException ex ) {
            throw new RuntimeException( "Cannot clone " + this.getClass().getName() + ". It seems that a SecurityManager is installed.", ex );
        }
    }


    public boolean belongsTo( final Xid transactionId ) {
        return this.getFormatId() == transactionId.getFormatId() && Arrays.equals( this.globalTransactionId, transactionId.getGlobalTransactionId() );
    }


    /**
     * @return Format identifier. O means the OSI CCR format.
     */
    @Override
    public int getFormatId() {
        return 0;
    }


    @Override
    public byte[] getGlobalTransactionId() {
        return this.globalTransactionId.clone();
    }


    @Override
    public byte[] getBranchQualifier() {
        return this.branchQualifier.clone();
    }
}
