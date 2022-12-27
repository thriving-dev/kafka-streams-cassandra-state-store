package io.confluent.developer;

import com.datastax.oss.driver.api.core.CqlSession;
import io.confluent.developer.store.CassandraKeyValueBytesStoreSupplier;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaStreamsApplication {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsApplication.class);

    static void runKafkaStreams(final KafkaStreams streams) {
        final CountDownLatch latch = new CountDownLatch(1);
        streams.setStateListener((newState, oldState) -> {
            if (oldState == KafkaStreams.State.RUNNING && newState != KafkaStreams.State.RUNNING) {
                latch.countDown();
            }
        });

        streams.start();

        try {
            latch.await();
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }

        logger.info("Streams Closed");
    }

    static Topology buildTopology(String inputTopic) {
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();

        // init session
        CqlSession session = CqlSession.builder()
//                .addContactPoint(new InetSocketAddress("scylla-1", 9042))
//                .withKeyspace(CqlIdentifier.fromCql("test"))
                .build();

        builder
                .stream(inputTopic, Consumed.with(stringSerde, stringSerde))
                .peek((k, v) -> logger.info("Observed event: {}::{}", k, v))
                .toTable(
                        Materialized.<String, String>as(new CassandraKeyValueBytesStoreSupplier(session, "test1"))
                                .withLoggingDisabled()
                                .withCachingDisabled()
                                .withKeySerde(stringSerde)
                                .withValueSerde(stringSerde));

        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: the path to a configuration file.");
        }

        Properties props = new Properties();
        try (InputStream inputStream = new FileInputStream(args[0])) {
            props.load(inputStream);
        }

        final String inputTopic = props.getProperty("input.topic.name");

        // Ramdomizer only used to produce sample data for this application, not typical usage
        KafkaStreams kafkaStreams = new KafkaStreams(
                buildTopology(inputTopic),
                props);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        logger.info("Kafka Streams 101 App Started");
        runKafkaStreams(kafkaStreams);
    }
}
