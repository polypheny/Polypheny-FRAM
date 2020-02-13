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


import org.polypheny.fram.metrics.avatica.MetricsSystemAdapter;
import org.polypheny.fram.standalone.SimpleNode.DatabaseHolder;
import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.influx.InfluxConfig;
import io.micrometer.influx.InfluxMeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import javax.inject.Inject;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.Driver.Serialization;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.server.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
@Command(name = "polypheny-fram", description = "Distributed DBMS.")
public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger( Main.class );

    public static final int DEFAULT_PORT = 20591;
    public static final int DEFAULT_CLUSTER_PORT = 20592;
    public static final int DEFAULT_STORAGE_PORT = 9001;

    @Inject
    protected HelpOption<Main> help;

    @Option(name = { "-p", "--port" }, description = "Host port. Default: " + DEFAULT_PORT)
    private int port = DEFAULT_PORT;

    @Option(name = { "--storage-port" })
    private int storagePort = DEFAULT_STORAGE_PORT;

    @Option(name = { "--cluster-port" })
    private int clusterPort = DEFAULT_CLUSTER_PORT;

    @Option(name = { "--influxDbUrl" })
    private String influxDbUrl = "";


    /**
     *
     */
    public static void main( String[] args ) throws Exception {
        SingleCommand<Main> parser = SingleCommand.singleCommand( Main.class );
        parser.parse( args ).run();
    }


    private void run() throws Exception {
        if ( help.showHelpIfRequested() ) {
            return;
        }

        Configuration.databasePort = port;
        Configuration.clusterPort = clusterPort;
        Configuration.storagePort = storagePort;

        // Register the JmxMeterRegistry
        LOGGER.info( "Registering JMX metrics exporter" );
        JmxMeterRegistry jmxMeterRegistry = new JmxMeterRegistry( JmxConfig.DEFAULT, Clock.SYSTEM );
        Metrics.addRegistry( jmxMeterRegistry );

        if ( !(this.influxDbUrl == null || this.influxDbUrl.isEmpty()) ) {
            // Register the InfluxMeterRegistry
            LOGGER.info( "Registering InfluxDB metrics exporter" );
            InfluxMeterRegistry influxMeterRegistry = new InfluxMeterRegistry( new InfluxConfig() {
                @Override
                public Duration step() {
                    return Duration.ofSeconds( 1 ); // report every second
                }


                @Override
                public String uri() {
                    return Main.this.influxDbUrl;
                }


                @Override
                public String get( String key ) {
                    return null; // use default values for everything else OR lookup the key in the config and return its value.
                }
            }, Clock.SYSTEM );
            Metrics.addRegistry( influxMeterRegistry );
        }

        // Register the PrometheusMeterRegistry
        LOGGER.info( "Registering Prometheus metrics exporter" );
        PrometheusMeterRegistry prometheusMeterRegistry = new PrometheusMeterRegistry( PrometheusConfig.DEFAULT );
        com.sun.net.httpserver.HttpServer metricsServer = com.sun.net.httpserver.HttpServer.create();
        metricsServer.bind( new InetSocketAddress( Configuration.databasePort - 1 ), 0 );
        metricsServer.createContext( "/", httpExchange -> {
            String response = prometheusMeterRegistry.scrape();
            httpExchange.sendResponseHeaders( 200, response.getBytes().length );
            try ( OutputStream os = httpExchange.getResponseBody() ) {
                os.write( response.getBytes() );
            }
        } );
        Metrics.addRegistry( prometheusMeterRegistry );

        LOGGER.info( "Creating local HSQLDB service" );
        LocalService hsqldb = new LocalService( new JdbcMeta( DatabaseHolder.jdbcConnectionUrl, "SA", "" ) );

        LOGGER.info( "Creating PolyMesh service" );
        LocalService pdbddu = new LocalService( DataDistributionUnitMeta.newMetaInstance(), new MetricsSystemAdapter() );

        // Construct the hsqldbServer
        LOGGER.info( "Creating the server for the local HSQLDB service" );
        HttpServer hsqldbServer = new HttpServer.Builder()
                .withHandler( hsqldb, Serialization.PROTOBUF )
                .withPort( Configuration.databasePort - 2 )
                .build();
        // Construct the pdbdduServer
        LOGGER.info( "Creating the server for the PolyMesh service" );
        HttpServer pdbdduServer = new HttpServer.Builder()
                .withHandler( pdbddu, Serialization.PROTOBUF )
                .withPort( Configuration.databasePort )
                .build();

        // Add shutdown hook
        LOGGER.info( "Adding shutdown hooks to stop all servers" );
        Runtime.getRuntime().addShutdownHook( new Thread( () -> {
            if ( hsqldbServer != null ) {
                hsqldbServer.stop();
            }
            if ( pdbdduServer != null ) {
                pdbdduServer.stop();
            }
            if ( metricsServer != null ) {
                metricsServer.stop( 0 );
            }
        }, "HTTPServer ShutdownHook" ) );

        // Then start it
        LOGGER.info( "Starting the HSQLDB server" );
        hsqldbServer.start();
        LOGGER.info( "Starting the HSQLDB server" );
        pdbdduServer.start();
        LOGGER.info( "Starting metrics publishing server(s)" );
        metricsServer.start();

        // Wait for termination
        System.out.println( "*** STARTUP FINISHED ***" );
        pdbdduServer.join();
    }


    public static class Configuration {

        public static int databasePort = DEFAULT_PORT;
        public static int clusterPort = DEFAULT_CLUSTER_PORT;
        public static int storagePort = DEFAULT_STORAGE_PORT;
    }
}
