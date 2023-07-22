package dev.thriving.oss.kafka.streams.cassandra.state.store;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static dev.thriving.oss.kafka.streams.cassandra.state.store.CassandraStateStore.readOnlyGlobalKeyValueStore;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.assertj.core.api.Assertions.assertThat;

class WordCountGlobalKeyValueStoreTest extends AbstractIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(WordCountGlobalKeyValueStoreTest.class);

    private static final String INPUT_TOPIC = "inputTopic";
    private static final String OUTPUT_TOPIC = "outputTopic";
    private static final String STORE_NAME = "word-grouped-count";

    @Test
    public void shouldCountWords() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        final List<String> inputValues = Arrays.asList(
                "Hello Kafka Streams",
                "All streams lead to Kafka",
                "Join Kafka Summit",
                "И теперь пошли русские слова"
        );
        final Map<String, Long> expectedWordCounts = mkMap(
                mkEntry("hello", 1L),
                mkEntry("all", 1L),
                mkEntry("streams", 2L),
                mkEntry("lead", 1L),
                mkEntry("to", 1L),
                mkEntry("join", 1L),
                mkEntry("kafka", 3L),
                mkEntry("summit", 1L),
                mkEntry("и", 1L),
                mkEntry("теперь", 1L),
                mkEntry("пошли", 1L),
                mkEntry("русские", 1L),
                mkEntry("слова", 1L)
        );

        // configure and start the processor topology.
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        final Properties props = getStreamsProperties();

        // when
        try (
                final AdminClient adminClient = initAdminClient();
                final KafkaProducer<String, String> producer = initProducer(stringSerde, stringSerde);
                final KafkaConsumer<String, Long> consumer = initConsumer(stringSerde, longSerde);
                final CqlSession session = initSession();
                final KafkaStreams streams = initStreams(props, session)
        ) {
            // setup input and output topics.
            int partitions = 6;
            Collection<NewTopic> topics = Arrays.asList(
                    new NewTopic(INPUT_TOPIC, partitions, (short) 1),
                    new NewTopic(OUTPUT_TOPIC, partitions, (short) 1)
            );
            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

            consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC));

            // start streams.
            streams.start();

            // produce some input data to the input topic.
            inputValues.forEach(it -> {
                try {
                    producer.send(new ProducerRecord<>(INPUT_TOPIC, "testcontainers", it)).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });

            // consume and collect streams output
            final Map<String, Long> results = new HashMap<>();
            Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () -> {
                        ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(500));
                        records.iterator().forEachRemaining(record -> results.put(record.key(), record.value()));

                        return results.size() >= expectedWordCounts.size();
                    }
            );

            // then verify the application's output data.
            assertThat(results).containsExactlyInAnyOrderEntriesOf(expectedWordCounts);

            // when (2)
            // get a store to exec interactive queries
            final ReadOnlyKeyValueStore<String, Long> store = readOnlyGlobalKeyValueStore(streams, STORE_NAME);

            // then (2)
            final Long valueForUnknownKey = store.get("unknown");
            assertThat(valueForUnknownKey).isNull();

            final Long valueForHello = store.get("hello");
            assertThat(valueForHello).isNotNull().isEqualTo(expectedWordCounts.get("hello"));

            final long approximateNumEntries = store.approximateNumEntries();
            assertThat(approximateNumEntries).isEqualTo(expectedWordCounts.size());
        }
    }

    // note: adapted from https://github.com/confluentinc/kafka-streams-examples/blob/v7.5.0-148/src/test/java/io/confluent/examples/streams/WordCountLambdaIntegrationTest.java
    @NotNull
    private KafkaStreams initStreams(Properties streamsConfiguration, CqlSession session) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> source = builder.stream(INPUT_TOPIC);
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        final KTable<String, Long> counts = source
                .peek((k, v) -> LOG.info("in => {}::{}", k, v))
                .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
                .groupBy((key, value) -> value)
                .count(Materialized.<String, Long>as(
                                CassandraStores.builder(session, STORE_NAME)
                                        .withKeyspace(CASSANDRA_KEYSPACE)
                                        .withCountAllEnabled()
                                        .globalKeyValueStore()
                        )
                        .withLoggingDisabled()
                        .withCachingDisabled()
                        .withKeySerde(stringSerde)
                        .withValueSerde(longSerde));

        // need to override value serde to Long type
        counts.toStream()
                .peek((k, v) -> LOG.info("out => {}::{}", k, v))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        return new KafkaStreams(builder.build(), streamsConfiguration);
    }

}
