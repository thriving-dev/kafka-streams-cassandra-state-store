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
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
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

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.StoreQueryParameters.fromNameAndType;
import static org.assertj.core.api.Assertions.assertThat;

class WordCountInteractiveQueriesTest extends AbstractIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(WordCountInteractiveQueriesTest.class);

    private static final String INPUT_TOPIC = "inputTopic";
    private static final String OUTPUT_TOPIC = "outputTopic";
    private static final String STORE_NAME = "word-grouped-count";

    @Test
    public void shouldCountWordsAndAllowInteractiveQueries() throws ExecutionException, InterruptedException, TimeoutException {
        // given
        final List<String> inputValues = Arrays.asList(
                "Albania Andorra Armenia Austria Azerbaijan Belarus Belgium BosniaAndHerzegovina Bulgaria Croatia Cyprus Czechia",
                "Albania Andorra Armenia Austria Azerbaijan Belarus Belgium BosniaAndHerzegovina Bulgaria Croatia Cyprus Czechia Denmark Estonia Finland France Georgia Germany Greece Hungary Iceland Ireland Italy Kazakhstan Kosovo Latvia Liechtenstein Lithuania Luxembourg Malta Moldova Monaco Montenegro Netherlands NorthMacedonia Norway Poland Portugal Romania Russia SanMarino Serbia Slovakia Slovenia Spain Sweden Switzerland Turkey Ukraine UnitedKingdom VaticanCity"
        );
        final Map<String, Long> expectedWordCounts = mkMap(
                mkEntry("albania", 2L),
                mkEntry("andorra", 2L),
                mkEntry("armenia", 2L),
                mkEntry("austria", 2L),
                mkEntry("azerbaijan", 2L),
                mkEntry("belarus", 2L),
                mkEntry("belgium", 2L),
                mkEntry("bosniaandherzegovina", 2L),
                mkEntry("bulgaria", 2L),
                mkEntry("croatia", 2L),
                mkEntry("cyprus", 2L),
                mkEntry("czechia", 2L),
                mkEntry("denmark", 1L),
                mkEntry("estonia", 1L),
                mkEntry("finland", 1L),
                mkEntry("france", 1L),
                mkEntry("georgia", 1L),
                mkEntry("germany", 1L),
                mkEntry("greece", 1L),
                mkEntry("hungary", 1L),
                mkEntry("iceland", 1L),
                mkEntry("ireland", 1L),
                mkEntry("italy", 1L),
                mkEntry("kazakhstan", 1L),
                mkEntry("kosovo", 1L),
                mkEntry("latvia", 1L),
                mkEntry("liechtenstein", 1L),
                mkEntry("lithuania", 1L),
                mkEntry("luxembourg", 1L),
                mkEntry("malta", 1L),
                mkEntry("moldova", 1L),
                mkEntry("monaco", 1L),
                mkEntry("montenegro", 1L),
                mkEntry("netherlands", 1L),
                mkEntry("northmacedonia", 1L),
                mkEntry("norway", 1L),
                mkEntry("poland", 1L),
                mkEntry("portugal", 1L),
                mkEntry("romania", 1L),
                mkEntry("russia", 1L),
                mkEntry("sanmarino", 1L),
                mkEntry("serbia", 1L),
                mkEntry("slovakia", 1L),
                mkEntry("slovenia", 1L),
                mkEntry("spain", 1L),
                mkEntry("sweden", 1L),
                mkEntry("switzerland", 1L),
                mkEntry("turkey", 1L),
                mkEntry("ukraine", 1L),
                mkEntry("unitedkingdom", 1L),
                mkEntry("vaticancity", 1L)
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

            // then (1) verify the application's output data.
            assertThat(results).containsExactlyInAnyOrderEntriesOf(expectedWordCounts);

            // when (2)
            // Lookup the KeyValueStore
            final ReadOnlyKeyValueStore<String, Long> store =
                    streams.store(fromNameAndType(STORE_NAME, QueryableStoreTypes.keyValueStore()));

            // then (2)
            final Long valueForUnknownKey = store.get("unknown");
            assertThat(valueForUnknownKey).isNull();

            final Long valueForBelgium = store.get("belgium");
            assertThat(valueForBelgium).isNotNull().isEqualTo(2L);

            final KeyValueIterator<String, Long> allIterator = store.all();
            assertThat(allIterator).isNotNull();
            final Map<String, Long> allResults = new HashMap<>();
            allIterator.forEachRemaining(it -> allResults.put(it.key, it.value));
            assertThat(allResults).hasSize(expectedWordCounts.size());
            assertThat(allResults).containsExactlyInAnyOrderEntriesOf(expectedWordCounts);

            final KeyValueIterator<String, Long> prefixScanIterator1 = store.prefixScan("bel", stringSerde.serializer());
            assertThat(prefixScanIterator1).isNotNull();
            final Map<String, Long> prefixScanResults1 = new HashMap<>();
            prefixScanIterator1.forEachRemaining(it -> prefixScanResults1.put(it.key, it.value));
            assertThat(prefixScanResults1).hasSize(2);
            assertThat(prefixScanResults1).containsExactlyInAnyOrderEntriesOf(
                    mkMap(
                            mkEntry("belgium", 2L),
                            mkEntry("belarus", 2L)
                    )
            );

            final KeyValueIterator<String, Long> prefixScanIterator2 = store.prefixScan("f", stringSerde.serializer());
            assertThat(prefixScanIterator2).isNotNull();
            final Map<String, Long> prefixScanResults2 = new HashMap<>();
            prefixScanIterator2.forEachRemaining(it -> prefixScanResults2.put(it.key, it.value));
            assertThat(prefixScanResults2).hasSize(2);
            assertThat(prefixScanResults2).containsExactlyInAnyOrderEntriesOf(
                    mkMap(
                            mkEntry("finland", 1L),
                            mkEntry("france", 1L)
                    )
            );

            final KeyValueIterator<String, Long> rangeIterator = store.range("lithuania", "moldova");
            assertThat(rangeIterator).isNotNull();
            final Map<String, Long> rangeResults = new HashMap<>();
            rangeIterator.forEachRemaining(it -> rangeResults.put(it.key, it.value));
            assertThat(rangeResults).hasSize(4);
            assertThat(rangeResults).containsExactlyInAnyOrderEntriesOf(
                    mkMap(
                            mkEntry("lithuania", 1L),
                            mkEntry("luxembourg", 1L),
                            mkEntry("malta", 1L),
                            mkEntry("moldova", 1L)
                    )
            );

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
                .peek((k, v) -> LOG.debug("in => {}::{}", k, v))
                .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
                .groupBy((key, value) -> value)
                .count(Materialized.<String, Long>as(
                                CassandraStores.builder(session, STORE_NAME)
                                        .withKeyspace(CASSANDRA_KEYSPACE)
                                        .keyValueStore()
                        )
                        .withLoggingDisabled()
                        .withCachingDisabled()
                        .withKeySerde(stringSerde)
                        .withValueSerde(longSerde));

        // need to override value serde to Long type
        counts.toStream()
                .peek((k, v) -> LOG.debug("out => {}::{}", k, v))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        return new KafkaStreams(builder.build(), streamsConfiguration);
    }

}
