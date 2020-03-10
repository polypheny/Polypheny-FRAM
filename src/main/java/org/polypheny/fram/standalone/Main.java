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


import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.influx.InfluxMeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.util.Objects;
import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.Driver.Serialization;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.server.HttpServer;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.polypheny.fram.metrics.avatica.MetricsSystemAdapter;
import org.polypheny.fram.standalone.SimpleNode.DatabaseHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
@Command(name = "polypheny-fram", description = "Distributed DBMS.")
public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger( Main.class );
    private static final int UNSET_INTEGER_OPTION = Integer.MIN_VALUE;
    private static final String UNSET_STRING_OPTION = "";

    private static Config configuration;

    @Inject
    protected HelpOption<Main> help;

    @Option(name = { "-c", "--config" }, description = "Configuration file. See sample-application.conf for valid configuration options.")
    private String configurationFile = UNSET_STRING_OPTION;

    @Option(name = { "-p", "--port" }, description = "JDBC host port.")
    private int port = UNSET_INTEGER_OPTION;

    @Option(name = { "--storage-port" })
    private int storagePort = UNSET_INTEGER_OPTION;

    @Option(name = { "--cluster-port" })
    private int clusterPort = UNSET_INTEGER_OPTION;

    @Option(name = { "--influxDbUrl" })
    private String influxDbUrl = UNSET_STRING_OPTION;


    /**
     *
     */
    public static void main( final String[] args ) throws Exception {
        Objects.requireNonNull( args );
        SingleCommand<Main> parser = SingleCommand.singleCommand( Main.class );
        parser.parse( args ).run();
    }


    private static void setConfiguration( final Config configuration ) {
        Main.configuration = configuration;
    }


    public static Config configuration() {
        return configuration;
    }


    public static Config configuration( final String prefix ) {
        return configuration.withOnlyPath( prefix );
    }


    private void run() throws Exception {
        if ( help.showHelpIfRequested() ) {
            return;
        }

        Config loadedConfiguration;
        if ( !configurationFile.equals( UNSET_STRING_OPTION ) ) {
            loadedConfiguration = ConfigFactory.load( ConfigFactory.parseFile( new File( configurationFile ) ) );
        } else {
            loadedConfiguration = ConfigFactory.load();
        }

        if ( port != UNSET_INTEGER_OPTION ) {
            loadedConfiguration = loadedConfiguration.withValue( "standalone.jdbc.port", ConfigValueFactory.fromAnyRef( port ) );
        }
        if ( clusterPort != UNSET_INTEGER_OPTION ) {
            loadedConfiguration = loadedConfiguration.withValue( "cluster.port", ConfigValueFactory.fromAnyRef( clusterPort ) );
        }
        if ( storagePort != UNSET_INTEGER_OPTION ) {
            loadedConfiguration = loadedConfiguration.withValue( "standalone.datastore.jdbc.port", ConfigValueFactory.fromAnyRef( storagePort ) );
        }
        if ( !influxDbUrl.equals( UNSET_STRING_OPTION ) ) {
            loadedConfiguration = loadedConfiguration.withValue( "standalone.metricsregistry.influx.enabled", ConfigValueFactory.fromAnyRef( true ) );
            loadedConfiguration = loadedConfiguration.withValue( "standalone.metricsregistry.influx.uri", ConfigValueFactory.fromAnyRef( influxDbUrl ) );
        }

        Main.setConfiguration( loadedConfiguration );

        //
        //
        //

        if ( Main.configuration().getBoolean( "standalone.metricsregistry.jmx.enabled" ) ) {
            // Register the JmxMeterRegistry
            LOGGER.info( "Registering JMX metrics exporter" );
            final JmxMeterRegistry jmxMeterRegistry = new JmxMeterRegistry( JmxConfig.DEFAULT, Clock.SYSTEM );
            Metrics.addRegistry( jmxMeterRegistry );
        }

        if ( Main.configuration().getBoolean( "standalone.metricsregistry.influx.enabled" ) && !Main.configuration().getString( "standalone.metricsregistry.influx.uri" ).isEmpty() ) {
            // Register the InfluxMeterRegistry
            LOGGER.info( "Registering InfluxDB metrics exporter" );
            final InfluxMeterRegistry influxMeterRegistry = new InfluxMeterRegistry( key -> Main.configuration().getString( "standalone.metricsregistry." + key ), Clock.SYSTEM );
            Metrics.addRegistry( influxMeterRegistry );
        }

        if ( Main.configuration().getBoolean( "standalone.metricsregistry.prometheus.enabled" ) ) {
            // Register the PrometheusMeterRegistry
            LOGGER.info( "Registering Prometheus metrics exporter" );
            PrometheusMeterRegistry prometheusMeterRegistry = new PrometheusMeterRegistry( key -> Main.configuration().getString( "standalone.metricsregistry." + key ) );

            // Jetty Server
            Server prometheusEndpointServer = new Server( Main.configuration().getInt( "standalone.metricsregistry.prometheus.port" ) );
            prometheusEndpointServer.setHandler( new AbstractHandler() {
                @Override
                public void handle( final String target, final Request baseRequest, final HttpServletRequest request, final HttpServletResponse response ) throws IOException, ServletException {
                    if ( target.equalsIgnoreCase( Main.configuration().getString( "standalone.metricsregistry.prometheus.path" ) ) ) {
                        response.setStatus( HttpServletResponse.SC_OK );
                        prometheusMeterRegistry.scrape( response.getWriter() );
                        baseRequest.setHandled( true );
                    }
                }
            } );

            Metrics.addRegistry( prometheusMeterRegistry );

            Runtime.getRuntime().addShutdownHook( new Thread( () -> {
                try {
                    prometheusEndpointServer.stop();
                } catch ( Exception e ) {
                    throw new RuntimeException( e );
                }
            }, "MetricsServer ShutdownHook" ) );

            LOGGER.info( "Starting metrics publishing server(s)" );
            prometheusEndpointServer.start();
        }

        if ( Main.configuration().getBoolean( "standalone.datastore.passthrough.enabled" ) ) {
            LOGGER.info( "Creating local HSQLDB service" );
            final LocalService hsqldb = new LocalService( new JdbcMeta( DatabaseHolder.jdbcConnectionUrl, "SA", "" ) );
            final int passthroughPort = Main.configuration().getInt( "standalone.datastore.passthrough.port" );

            // Construct the hsqldbPassThroughServer
            LOGGER.info( "Creating the server for the local HSQLDB service" );
            final HttpServer hsqldbPassThroughServer = new HttpServer.Builder()
                    .withHandler( hsqldb, Serialization.valueOf( Main.configuration().getString( "standalone.jdbc.serialization" ).toUpperCase() ) )
                    .withPort( passthroughPort )
                    .build();

            Runtime.getRuntime().addShutdownHook( new Thread( hsqldbPassThroughServer::stop, "HSQLDB PassThrough ShutdownHook" ) );

            LOGGER.info( "Starting the HSQLDB server at port {}", passthroughPort );
            hsqldbPassThroughServer.start();
        }

        //
        //
        //

        LOGGER.info( "Creating Polypheny-FRAM service" );
        LocalService polyphenyFram = new LocalService( DataDistributionUnitMeta.newMetaInstance(), new MetricsSystemAdapter() );

        // Construct the polyphenyFramServer
        LOGGER.info( "Creating the server for the Polypheny-FRAM service" );
        final int standalonePort = Main.configuration().getInt( "standalone.jdbc.port" );
        HttpServer polyphenyFramServer = new HttpServer.Builder()
                .withHandler( polyphenyFram, Serialization.valueOf( Main.configuration().getString( "standalone.jdbc.serialization" ).toUpperCase() ) )
                .withPort( standalonePort )
                .build();

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook( new Thread( polyphenyFramServer::stop, "HTTPServer ShutdownHook" ) );

        // Then start it
        LOGGER.info( "Starting the Polypheny-FRAM server at port {}", standalonePort );
        polyphenyFramServer.start();

        // Wait for termination
        System.out.println( "*** STARTUP FINISHED ***" ); //NOSONAR squid:S106 - Justification: We *always* want to have this printed, regardless of the logger configuration.
        polyphenyFramServer.join();
    }
}
