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

package org.polypheny.fram.metis;


import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;


@Slf4j
public class Metis {

    private static final String EXECUTABLE_NAME = "gpmetis";
    private static final int METIS_TIMEOUT_SECONDS = 10;

    private final File gpmetisExecutable;


    private Metis() {

        final URL gpmetisExecutableUrl = Metis.class.getResource( '/' + "native" + '/' + EXECUTABLE_NAME + '.' + (SystemUtils.IS_OS_WINDOWS ? "windows" : "linux") );
        if ( gpmetisExecutableUrl == null ) {
            log.error( "Failed to find " + EXECUTABLE_NAME + '.' + (SystemUtils.IS_OS_WINDOWS ? "windows" : "linux") );
            throw new InstantiationError( "Failed to find " + EXECUTABLE_NAME + '.' + (SystemUtils.IS_OS_WINDOWS ? "windows" : "linux") );
        }
        log.debug( "Found " + EXECUTABLE_NAME + ": " + gpmetisExecutableUrl );

        try {
            gpmetisExecutable = File.createTempFile( EXECUTABLE_NAME, ".exe" );
            gpmetisExecutable.deleteOnExit();
            FileUtils.copyInputStreamToFile( gpmetisExecutableUrl.openStream(), gpmetisExecutable );
        } catch ( IOException ex ) {
            log.error( "Failed to extract " + EXECUTABLE_NAME + '.' + (SystemUtils.IS_OS_WINDOWS ? "windows" : "linux") );
            throw new InstantiationError( "Failed to extract " + EXECUTABLE_NAME + '.' + (SystemUtils.IS_OS_WINDOWS ? "windows" : "linux") );
        }
        if ( !SystemUtils.IS_OS_WINDOWS && !gpmetisExecutable.setExecutable( true ) ) {
            log.error( "Failed to set the executable bit on " + EXECUTABLE_NAME + ".exe (" + (SystemUtils.IS_OS_WINDOWS ? "windows" : "linux") + ")" );
            throw new InstantiationError( "Failed to set the executable bit on " + EXECUTABLE_NAME + ".exe (" + (SystemUtils.IS_OS_WINDOWS ? "windows" : "linux") + ")" );
        }
    }


    /**
     * Usage: gpmetis [options] graphfile nparts
     *
     * Required parameters
     * graphfile   Stores the graph to be partitioned.
     * nparts      The number of partitions to split the graph.
     *
     * Optional parameters
     * -ptype=string
     * Specifies the scheme to be used for computing the k-way partitioning.
     * The possible values are:
     * rb       - Recursive bisectioning
     * kway     - Direct k-way partitioning [default]
     *
     * -ctype=string
     * Specifies the scheme to be used to match the vertices of the graph
     * during the coarsening.
     * The possible values are:
     * rm       - Random matching
     * shem     - Sorted heavy-edge matching [default]
     *
     * -iptype=string [applies only when -ptype=rb]
     * Specifies the scheme to be used to compute the initial partitioning
     * of the graph.
     * The possible values are:
     * grow     - Grow a bisection using a greedy scheme [default for ncon=1]
     * random   - Compute a bisection at random [default for ncon>1]
     *
     * -objtype=string [applies only when -ptype=kway]
     * Specifies the objective that the partitioning routines will optimize.
     * The possible values are:
     * cut      - Minimize the edgecut [default]
     * vol      - Minimize the total communication volume
     *
     * -no2hop
     * Specifies that the coarsening will not perform any 2-hop matchings
     * when the standard matching fails to sufficiently contract the graph.
     *
     * -contig [applies only when -ptype=kway]
     * Specifies that the partitioning routines should try to produce
     * partitions that are contiguous. Note that if the input graph is not
     * connected this option is ignored.
     *
     * -minconn [applies only when -ptype=kway]
     * Specifies that the partitioning routines should try to minimize the
     * maximum degree of the subdomain graph, i.e., the graph in which each
     * partition is a node, and edges connect subdomains with a shared
     * interface.
     *
     * -tpwgts=filename
     * Specifies the name of the file that stores the target weights for
     * each partition. By default, all partitions are assumed to be of
     * the same size.
     *
     * -ufactor=int
     * Specifies the maximum allowed load imbalance among the partitions.
     * A value of x indicates that the allowed load imbalance is 1+x/1000.
     * For ptype=rb, the load imbalance is measured as the ratio of the
     * 2*max(left,right)/(left+right), where left and right are the sizes
     * of the respective partitions at each bisection.
     * For ptype=kway, the load imbalance is measured as the ratio of
     * max_i(pwgts[i])/avgpwgt, where pwgts[i] is the weight of the ith
     * partition and avgpwgt is the sum of the total vertex weights divided
     * by the number of partitions requested.
     * For ptype=rb, the default value is 1 (i.e., load imbalance of 1.001).
     * For ptype=kway, the default value is 30 (i.e., load imbalance of 1.03).
     *
     * -ubvec=string
     * Applies only for multi-constraint partitioning and specifies the per
     * constraint allowed load imbalance among partitions. The required
     * parameter corresponds to a space separated set of floating point
     * numbers, one for each of the constraints. For example, for three
     * constraints, the string can be "1.02 1.2 1.35" indicating a
     * desired maximum load imbalance of 2%, 20%, and 35%, respectively.
     * The load imbalance is defined in a way similar to ufactor.
     * If supplied, this parameter takes priority over ufactor.
     *
     * -niter=int
     * Specifies the number of iterations for the refinement algorithms
     * at each stage of the uncoarsening process. Default is 10.
     *
     * -ncuts=int
     * Specifies the number of different partitionings that it will compute.
     * The final partitioning is the one that achieves the best edgecut or
     * communication volume. Default is 1.
     *
     * -nooutput
     * Specifies that no partitioning file should be generated.
     *
     * -seed=int
     * Selects the seed of the random number generator.
     *
     * -dbglvl=int
     * Selects the dbglvl.
     *
     * -help
     * Prints this message.
     *
     * @return exit value
     */
    private int executeMetis( final String[] options, final File graphFile, final int numberOfPartitions ) throws IOException, InterruptedException, TimeoutException {
        final ProcessBuilder metisProcessBuilder = new ProcessBuilder( gpmetisExecutable.getAbsolutePath(), options == null ? "" : String.join( " ", options ), graphFile.getAbsolutePath(), Integer.toString( numberOfPartitions ) );

        final Process metis = metisProcessBuilder.start();

        if ( metis.waitFor( METIS_TIMEOUT_SECONDS, TimeUnit.SECONDS ) ) {
            return metis.exitValue();
        } else {
            // Timeout reached
            metis.destroyForcibly();
            throw new TimeoutException( "The metis process did not finish in the given timeout of " + METIS_TIMEOUT_SECONDS + " seconds." );
        }
    }


    public static Metis getInstance() {
        return SingletonHolder.INSTANCE;
    }


    private static class SingletonHolder {

        private static final Metis INSTANCE = new Metis();
    }
}
