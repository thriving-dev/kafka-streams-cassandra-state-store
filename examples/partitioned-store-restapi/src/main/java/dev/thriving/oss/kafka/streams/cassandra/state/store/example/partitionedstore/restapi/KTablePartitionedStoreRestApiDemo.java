package dev.thriving.oss.kafka.streams.cassandra.state.store.example.partitionedstore.restapi;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.core.util.JacksonFeature;
import dev.thriving.oss.kafka.streams.cassandra.state.store.CassandraStores;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static dev.thriving.oss.kafka.streams.cassandra.state.store.CassandraStateStore.readOnlyPartitionedKeyValueStore;

/**
 * Demonstrates, using the high-level KStream DSL, how to implement a regular KTable
 * that keeps a simple KV state store (String, String) but persisted to CassandraStores.partitionedKeyValueStore -
 * accessible via REST API
 * - GET /keyvalue/{key}
 * - GET /all
 * - GET /reverseAll
 * - GET /range/{from}/{to}
 * - GET /reverseRange/{from}/{to}
 * - GET /prefixScan/{prefix}
 * - GET /approximateNumEntries
 * <p>
 * Inspired by: https://github.com/confluentinc/kafka-streams-examples/tree/7.1.1-post/src/main/java/io/confluent/examples/streams/interactivequeries
 * The main difference is there's no proxy request required, since all keys are accessible from each running instance
 * of this Demo app (backed by a cassandra table with the `key` as the sole table PK).
 * <p>
 * Before running this example you must create the input topic (e.g. via
 * {@code bin/kafka-topics.sh --create ...}), and write some data to the input topic (e.g. via
 * {@code bin/kafka-console-producer.sh}).
 */
@Path("/")
public final class KTablePartitionedStoreRestApiDemo {

    private static final Logger LOG = LoggerFactory.getLogger(KTablePartitionedStoreRestApiDemo.class);

    public static final String INPUT_TOPIC = "streams-plaintext-input";
    public static final String STORE_NAME = "partitioned-table";

    private final KafkaStreams streams;
    private final CqlSession session;
    private Server jettyServer;
    private ReadOnlyKeyValueStore<String, String> readOnlyKeyValueStore;

    public KTablePartitionedStoreRestApiDemo(KafkaStreams streams, CqlSession session) {
        this.streams = streams;
        this.session = session;
    }

    static Properties getStreamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "partitioned-store-restapi");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                Optional.ofNullable(System.getenv("BOOTSTRAP_SERVERS_CONFIG"))
                        .orElse("localhost:19092"));
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, String.format("%s:8080", System.getProperty("HOSTNAME", "localhost")));

        // setting offset reset to 'earliest' so that we can re-run the demo code with the same preloaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    static void createStream(CqlSession session, final StreamsBuilder builder) {
        final KStream<String, String> source = builder.stream(INPUT_TOPIC);
        final Serde<String> stringSerde = Serdes.String();

        source.peek((k, v) -> LOG.debug("in => {}::{}", k, v))
                .repartition() // note: this is required because the example's README uses 'kcat' which seems causes partition misalignment, probably due to not serializing and partitioning keys as Strings (StringSerde) but maybe bytes
                .toTable(Materialized.<String, String>as(
                                CassandraStores.builder(session, STORE_NAME)
                                        .partitionedKeyValueStore()
                        )
                        .withLoggingDisabled()
                        .withCachingDisabled()
                        .withKeySerde(stringSerde)
                        .withValueSerde(stringSerde));
    }

    /**
     * Get a key-value pair from a KeyValue Store
     *
     * @param key the key to get
     * @return {@link KeyValue} representing the key-value pair
     */
    @GET
    @Path("/keyvalue/{key}")
    @Produces(MediaType.APPLICATION_JSON)
    public KeyValue byKey(@PathParam("key") final String key) {
        // Get the value from the store
        final String value = store().get(key);

        if (value == null) {
            throw new NotFoundException(); // responds with 404
        }

        return new KeyValue(key, value);
    }

    /**
     * Get the approximate number of entries.
     *
     * @return {@link KeyValue} representing the key-value pair
     */
    @GET
    @Path("/approximateNumEntries")
    @Produces(MediaType.APPLICATION_JSON)
    public KeyValue approximateNumEntries() {
        // query store & return
        return new KeyValue("approximateNumEntries", store().approximateNumEntries());
    }

    /**
     * Get all entries.
     *
     * @return {@link List<KeyValue>} of entries
     */
    @GET
    @Path("/all")
    @Produces(MediaType.APPLICATION_JSON)
    public List<KeyValue> all() {
        // query store
        List<KeyValue> entries = new ArrayList<>();
        try (KeyValueIterator<String, String> result = store().all()) {
            result.forEachRemaining(it -> entries.add(new KeyValue(it.key, it.value)));
        }
        return entries;
    }

    /**
     * Get all entries in reverse order.
     *
     * @return {@link List<KeyValue>} of entries
     */
    @GET
    @Path("/reverseAll")
    @Produces(MediaType.APPLICATION_JSON)
    public List<KeyValue> reverseAll() {
        // query store
        List<KeyValue> entries = new ArrayList<>();
        try (KeyValueIterator<String, String> result = store().reverseAll()) {
            result.forEachRemaining(it -> entries.add(new KeyValue(it.key, it.value)));
        }
        return entries;
    }

    /**
     * Get all entries by range.
     *
     * @param from from key
     * @param to to keys
     * @return {@link List<KeyValue>} of entries
     */
    @GET
    @Path("/range/{from}/{to}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<KeyValue> range(@PathParam("from") final String from, @PathParam("to") final String to) {
        // query store
        List<KeyValue> entries = new ArrayList<>();
        try (KeyValueIterator<String, String> result = store().range(from, to)) {
            result.forEachRemaining(it -> entries.add(new KeyValue(it.key, it.value)));
        }
        return entries;
    }

    /**
     * Get all entries by range in reverse order.
     *
     * @param from from key
     * @param to to keys
     * @return {@link List<KeyValue>} of entries
     */
    @GET
    @Path("/reverseRange/{from}/{to}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<KeyValue> reverseRange(@PathParam("from") final String from, @PathParam("to") final String to) {
        // query store
        List<KeyValue> entries = new ArrayList<>();
        try (KeyValueIterator<String, String> result = store().reverseRange(from, to)) {
            result.forEachRemaining(it -> entries.add(new KeyValue(it.key, it.value)));
        }
        return entries;
    }

    /**
     * Get all entries by prefix.
     *
     * @param prefix the prefix
     * @return {@link List<KeyValue>} of entries
     */
    @GET
    @Path("/prefixScan/{prefix}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<KeyValue> prefixScan(@PathParam("prefix") final String prefix) {
        // query store
        List<KeyValue> entries = new ArrayList<>();
        try (KeyValueIterator<String, String> result = store().prefixScan(prefix, Serdes.String().serializer())) {
            result.forEachRemaining(it -> entries.add(new KeyValue(it.key, it.value)));
        }
        return entries;
    }

    /**
     * Get the optimised special implementation of ReadOnlyKeyValueStore (CassandraPartitionedReadOnlyKeyValueStore).
     * Lazy init, re-use.
     * @return instance of ReadOnlyKeyValueStore
     */
    private ReadOnlyKeyValueStore<String, String> store() {
        if (readOnlyKeyValueStore == null) {
            Serde<String> stringSerde = Serdes.String();
            readOnlyKeyValueStore = readOnlyPartitionedKeyValueStore(
                    streams,
                    STORE_NAME,
                    session,
                    null,
                    true,
                    null,
                    stringSerde,
                    stringSerde
            );
        }
        return readOnlyKeyValueStore;
    }

    protected void startJetty() throws Exception {
        final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        jettyServer = new Server();
        jettyServer.setHandler(context);

        final ResourceConfig rc = new ResourceConfig();
        rc.register(this);
        rc.register(JacksonFeature.class);

        final ServletContainer sc = new ServletContainer(rc);
        final ServletHolder holder = new ServletHolder(sc);
        context.addServlet(holder, "/*");

        final ServerConnector connector = new ServerConnector(jettyServer);
        connector.setHost("localhost");
        connector.setPort(8080);
        jettyServer.addConnector(connector);

        context.start();

        try {
            jettyServer.start();
        } catch (final java.net.SocketException exception) {
            throw new Exception(exception.toString());
        }
    }

    /**
     * Stop the Jetty Server
     *
     * @throws Exception if jetty can't stop
     */
    protected void stopJetty() throws Exception {
        if (jettyServer != null) {
            jettyServer.stop();
        }
    }

    public static void main(final String[] args) {
        final Properties props = getStreamsConfig();

        // init session
        CqlSession session = CqlSession.builder().build();

        final StreamsBuilder builder = new StreamsBuilder();
        createStream(session, builder);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        KTablePartitionedStoreRestApiDemo app = new KTablePartitionedStoreRestApiDemo(streams, session);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                try {
                    app.stopJetty();
                    streams.close();
                    latch.countDown();
                } catch (final Throwable e) {
                    System.exit(1);
                }
            }
        });

        try {
            streams.start();
            app.startJetty();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    public record KeyValue(String key, Object value) {}
}
