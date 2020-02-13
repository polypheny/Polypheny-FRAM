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

package org.polypheny.fram.metrics.avatica;


import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.calcite.avatica.metrics.Counter;


public class CounterAdapter implements Counter {

    private final AtomicLong theCounterValue;


    public CounterAdapter( String name ) {
        this.theCounterValue = Metrics.gauge( name, Tags.empty(), new AtomicLong( 0 ) );
    }


    @Override
    public void increment() {
        this.theCounterValue.incrementAndGet();
    }


    @Override
    public void increment( long n ) {
        this.theCounterValue.addAndGet( n );
    }


    @Override
    public void decrement() {
        this.theCounterValue.decrementAndGet();
    }


    @Override
    public void decrement( long n ) {
        this.theCounterValue.addAndGet( -n );
    }
}
