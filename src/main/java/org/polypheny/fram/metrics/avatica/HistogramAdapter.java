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


import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import org.apache.calcite.avatica.metrics.Histogram;


public class HistogramAdapter implements Histogram {

    private final DistributionSummary adaptee;


    public HistogramAdapter( final String name ) {
        this.adaptee = Metrics.summary( name, Tags.empty() );
    }


    @Override
    public void update( int value ) {
        this.adaptee.record( value );
    }


    @Override
    public void update( long value ) {
        this.adaptee.record( value );
    }
}
