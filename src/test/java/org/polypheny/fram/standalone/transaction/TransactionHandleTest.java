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


import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


/**
 *
 */
public class TransactionHandleTest {

    ///////////////////

    private UUID nodeId;
    private UUID userId;
    private UUID connectionId;
    private UUID transactionId;


    @BeforeEach
    public void beforeTest() {
        nodeId = UUID.randomUUID();
        userId = UUID.randomUUID();
        connectionId = UUID.randomUUID();
        transactionId = UUID.randomUUID();
    }


    @AfterEach
    public void afterTest() {
        nodeId = null;
        userId = null;
        connectionId = null;
        transactionId = null;
    }

    ///////////////////


    @Test
    public void testStoringOfTheIds() {
        final UUID expectedNodeId = nodeId;
        final UUID expectedUserId = userId;
        final UUID expectedConnectionId = connectionId;
        final UUID expectedTransactionId = transactionId;

        final TransactionHandle testSubject = TransactionHandle.generateGlobalTransactionIdentifier( nodeId, userId, connectionId, transactionId );
        final UUID actualNodeId = testSubject.getNodeId();
        final UUID actualUserId = testSubject.getUserId();
        final UUID actualConnectionId = testSubject.getConnectionId();
        final UUID actualTransactionId = testSubject.getTransactionId();

        Assertions.assertArrayEquals( new UUID[]{ expectedNodeId, expectedUserId, expectedConnectionId, expectedTransactionId }, new UUID[]{ actualNodeId, actualUserId, actualConnectionId, actualTransactionId } );
    }


    @Test
    public void testToString() {
        final TransactionHandle testSubject = TransactionHandle.generateGlobalTransactionIdentifier( nodeId, userId, connectionId, transactionId );

        final String expected = "Xid: {"
                + "GID: " + testSubject.getNodeId() + "--" + testSubject.getUserId() + "--" + testSubject.getConnectionId() + "--" + testSubject.getTransactionId()
                + ", "
                + "BID: " + testSubject.getBranchId() + "--" + testSubject.getStoreId() + "--" + /* <reserved> */"00000000-0000-0000-0000-000000000000" /* </reserved> */ + "--" + testSubject.getCustomId()
                + "}";

        final String actual = testSubject.toString();
        Assertions.assertEquals( expected, actual );
    }


    @Test
    public void isLocalTransactionIdentifier_positive() {
        final boolean expected = true;

        TransactionHandle testSubject = TransactionHandle.generateLocalTransactionIdentifier( nodeId, transactionId );
        final boolean actual = testSubject.isLocalTransactionIdentifier();

        Assertions.assertEquals( expected, actual );
    }


    @Test
    public void isLocalTransactionIdentifier_negative1() {
        final boolean expected = false;

        TransactionHandle testSubject = TransactionHandle.generateGlobalTransactionIdentifier( nodeId, userId, connectionId, transactionId );
        final boolean actual = testSubject.isLocalTransactionIdentifier();

        Assertions.assertEquals( expected, actual );
    }


    @Test
    public void isLocalTransactionIdentifier_negative2() {
        final boolean expected = false;

        TransactionHandle testSubject = TransactionHandle.generateBranchTransactionIdentifier( TransactionHandle.generateGlobalTransactionIdentifier( nodeId, userId, connectionId, transactionId ), nodeId );
        final boolean actual = testSubject.isLocalTransactionIdentifier();

        Assertions.assertEquals( expected, actual );
    }
}
