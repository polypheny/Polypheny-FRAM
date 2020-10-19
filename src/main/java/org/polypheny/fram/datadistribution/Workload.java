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

package org.polypheny.fram.datadistribution;


import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.polypheny.fram.datadistribution.Transaction.Action;


public class Workload implements Serializable {

    public static Workload THIS_IS_A_TEST_REMOVE_ME = new Workload();


    static {
        Timer t = new Timer( true );
        t.scheduleAtFixedRate( new TimerTask() {
            @Override
            public void run() {
                synchronized ( THIS_IS_A_TEST_REMOVE_ME ) {
                    System.out.println( "***" );
                    System.out.println( "WORKLOAD: trx_r=" + THIS_IS_A_TEST_REMOVE_ME.numberOfReadTransactions + " - trx_w=" + THIS_IS_A_TEST_REMOVE_ME.numberOfWriteTransactions );
                    System.out.println( "RECORD COUNTERS: size=" + THIS_IS_A_TEST_REMOVE_ME.recordCounters.size() );
                    System.out.println( "***" );
                }
            }
        }, 0, TimeUnit.SECONDS.toMillis( 10 ) );
        /*t.scheduleAtFixedRate( new TimerTask() {
            @Override
            public void run() {
                synchronized ( THIS_IS_A_TEST_REMOVE_ME ) {
                    System.out.println( "***" );
                    System.out.println( "RECORD COUNTERS: size=" + THIS_IS_A_TEST_REMOVE_ME.recordCounters.size() );
                    MapUtils.debugPrint( System.out, "recordCounters", THIS_IS_A_TEST_REMOVE_ME.recordCounters );
                    System.out.println( "***" );
                }
            }
        }, 0, TimeUnit.SECONDS.toMillis( 15 ) );*/
    }


    private final UUID identifier = UUID.randomUUID();
    private final Instant start = Instant.EPOCH;
    private final Duration duration = Duration.ZERO;


    private final List<Transaction> transactions = new LinkedList<>();
    private final Map<Transaction, Integer> transactionCounters = new HashMap<>();
    private final Map<RecordIdentifier, Integer> recordCounters = new HashMap<>();


    private int numberOfReadTransactions = 0;
    private int numberOfWriteTransactions = 0;


    public final Instant getStartTimeInstant() {
        return start;
    }


    public final Instant getEndTimeInstant() {
        return start.plus( duration );
    }


    public void addTransaction( final Transaction transaction ) {
        synchronized ( this ) {
            if ( transaction.isEmpty() ) {
                return;
            }

            transactions.add( transaction );
            if ( transaction.isReadOnly() ) {
                ++numberOfReadTransactions;
            } else {
                ++numberOfWriteTransactions;
            }

            int transactionCounter = transactionCounters.getOrDefault( transaction, 0 );
            ++transactionCounter;
            transactionCounters.put( transaction, transactionCounter );

            for ( final Action action : transaction ) {
                switch ( action.operation ) {
                    case INSERT:
                        recordCounters.put( action.record, 0 );
                        break;

                    case SELECT:
                    case UPDATE:
                        int actionCounter = recordCounters.getOrDefault( action.record, 0 );
                        ++actionCounter;
                        recordCounters.put( action.record, actionCounter );
                        break;

                    case DELETE:
                        recordCounters.remove( action.record );
                        break;
                }
            }
        }
    }


    @Override
    public String toString() {
        return "Workload{" +
                "identifier=" + identifier +
                ", start=" + start +
                ", duration=" + duration +
                ", transactions=" + transactions +
                ", transactionCounters=" + transactionCounters +
                ", recordCounters=" + recordCounters +
                ", numberOfReadTransactions=" + numberOfReadTransactions +
                ", numberOfWriteTransactions=" + numberOfWriteTransactions +
                '}';
    }
}
