package dev.thriving.oss.kafka.streams.cassandra.state.store.example.processor.api.misc;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.thriving.oss.kafka.streams.cassandra.state.store.CassandraStores;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

/**
 * Demonstrates, using the low-level Processor API, how to implement the WordCount program
 * that computes a simple word occurrence histogram from an input text.
 * <p>
 * In addition, following ReadOnlyKeyValueStore methods are used (via scheduled Punctuator):
 * - {@link ReadOnlyKeyValueStore#all()}
 * - {@link ReadOnlyKeyValueStore#range(Object, Object)}
 * - {@link ReadOnlyKeyValueStore#prefixScan(Object, Serializer)}
 * - {@link ReadOnlyKeyValueStore#approximateNumEntries()}
 * <p>
 * In this example, the input stream reads from a topic named "streams-plaintext-input", where the values of messages
 * represent lines of text; and the histogram output is written to topic "streams-wordcount-output" where each record
 * is an updated count of a single word.
 * <p>
 * Before running this example you must create the input topic and the output topic (e.g. via
 * {@code bin/kafka-topics.sh --create ...}), and write some data to the input topic (e.g. via
 * {@code bin/kafka-console-producer.sh}). Otherwise, you won't see any data arriving in the output topic.
 */
public final class ProcessorApiAllRangePrefixCountDemo {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessorApiAllRangePrefixCountDemo.class);

    public static final String INPUT_TOPIC = "streams-plaintext-input";
    public static final String OUTPUT_TOPIC = "streams-wordcount-output";

    public static final String WORD_GROUPED_COUNT_STORE = "word-grouped-count";

    static Properties getStreamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-cassandra4");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                Optional.ofNullable(System.getenv("BOOTSTRAP_SERVERS_CONFIG"))
                        .orElse("localhost:19092"));
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // setting offset reset to 'earliest' so that we can re-run the demo code with the same preloaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    static void createWordCountStream(CqlSession session, final StreamsBuilder builder) {
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        CassandraStores.builder(session, WORD_GROUPED_COUNT_STORE)
                                .withCountAllEnabled()
                                .partitionedKeyValueStore(),
                        stringSerde,
                        longSerde)
                        .withLoggingDisabled()
                        .withCachingDisabled()
        );

        builder.<String, String>stream(INPUT_TOPIC)
                .peek((k, v) -> LOG.debug("in => {}::{}", k, v))
                .flatMap((key, value) -> {
                    String[] words = value.toLowerCase(Locale.getDefault()).split(" ");
                    return Stream.of(words)
                            .filter(it -> !it.isBlank())
                            .map(it -> KeyValue.pair(it, it)).toList();
                })
                .repartition()
                .process(WordCountProcessor::new, Named.as("wordCountProcessor"), WORD_GROUPED_COUNT_STORE)
                .peek((k, v) -> LOG.debug("out => {}::{}", k, v))
                // need to override value serde to Long type
                .to(OUTPUT_TOPIC, Produced.with(stringSerde, longSerde));
    }

    public static void main(final String[] args) {
        final Properties props = getStreamsConfig();

        // init session
        CqlSession session = CqlSession.builder().build();

        final StreamsBuilder builder = new StreamsBuilder();
        createWordCountStream(session, builder);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
