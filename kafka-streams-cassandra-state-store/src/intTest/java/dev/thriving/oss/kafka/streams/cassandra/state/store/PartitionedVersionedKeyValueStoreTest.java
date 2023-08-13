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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;
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

import static org.assertj.core.api.Assertions.assertThat;

class PartitionedVersionedKeyValueStoreTest extends AbstractIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionedVersionedKeyValueStoreTest.class);

    private static final String INPUT_TOPIC_PRICES = "prices";
    private static final String INPUT_TOPIC_ORDERS = "orders";
    private static final String OUTPUT_TOPIC_PRICED_ORDERS = "priced_orders";
    private static final String STORE_NAME = "meal-prices";

    // Note: this is a hack required to get access to the underlying state store. Ref -> StoreProdivingProcessor
    VersionedKeyValueStore<String, Long> store;

    @Test
    public void shouldAllowToGetRecordsFromVersionedStore() throws ExecutionException, InterruptedException, TimeoutException {
        // GIVEN
        final String TEST_KEY = "curry";
        final List<ProducerRecord<String, Long>> inputPrices = Arrays.asList(
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 0L, TEST_KEY, 8L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 4L, TEST_KEY, 10L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 17L, TEST_KEY, 12L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 38L, TEST_KEY, 11L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 53L, TEST_KEY, 9L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 64L, "any", 99L) // advance stream time
        );
        final List<ProducerRecord<String, String>> inputOrders = Arrays.asList(
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 65L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 60L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 50L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 35L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 30L, TEST_KEY, TEST_KEY)
        );

        // configure and start the processor topology.
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        final Properties props = getStreamsProperties();

        // WHEN
        try (
                final AdminClient adminClient = initAdminClient();
                final KafkaProducer<String, Long> pricesProducer = initProducer(stringSerde, longSerde);
                final KafkaProducer<String, String> ordersProducer = initProducer(stringSerde, stringSerde);
                final KafkaConsumer<String, Long> consumer = initConsumer(stringSerde, longSerde);
                final CqlSession session = initSession();
                final KafkaStreams streams = initStreams(props, session)
        ) {
            // setup input and output topics.
            int partitions = 1;
            Collection<NewTopic> topics = Arrays.asList(
                    new NewTopic(INPUT_TOPIC_PRICES, partitions, (short) 1),
                    new NewTopic(INPUT_TOPIC_ORDERS, partitions, (short) 1),
                    new NewTopic(OUTPUT_TOPIC_PRICED_ORDERS, partitions, (short) 1)
            );
            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

            consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC_PRICED_ORDERS));

            // start streams.
            streams.start();

            // produce some input data to the input topics.
            inputPrices.forEach(it -> {
                try {
                    pricesProducer.send(it).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
            inputOrders.forEach(it -> {
                try {
                    ordersProducer.send(it).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });

            // consume and collect streams output
            final List<KeyValue<String, Long>> results = new ArrayList<>();
            Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () -> {
                        ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(500));
                        records.iterator().forEachRemaining(record -> results.add(KeyValue.pair(record.key(), record.value())));

                        return results.size() >= 4;
                    }
            );

            // THEN
            assertThat(results.size()).isEqualTo(4);
            assertThat(results.get(0)).isEqualTo(KeyValue.pair(TEST_KEY, 9L));
            assertThat(results.get(1)).isEqualTo(KeyValue.pair(TEST_KEY, 9L));
            assertThat(results.get(2)).isEqualTo(KeyValue.pair(TEST_KEY, 11L));
            assertThat(results.get(3)).isEqualTo(KeyValue.pair(TEST_KEY, 12L));

            assertThat(store.get(TEST_KEY).value()).isEqualTo(9L);
            assertThat(store.get(TEST_KEY, 60L).value()).isEqualTo(9L);
            assertThat(store.get(TEST_KEY, 50L).value()).isEqualTo(11L);
            assertThat(store.get(TEST_KEY, 35L).value()).isEqualTo(12L);
            assertThat(store.get(TEST_KEY, 33L)).isNull();
            assertThat(store.get(TEST_KEY, 30L)).isNull();
        }
    }

    @Test
    public void shouldPutNewLatest() throws ExecutionException, InterruptedException, TimeoutException {
        // GIVEN
        final String TEST_KEY = "bibimbap";
        final List<ProducerRecord<String, Long>> inputPrices = Arrays.asList(
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 0L, TEST_KEY, 8L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 4L, TEST_KEY, 10L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 17L, TEST_KEY, 12L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 38L, TEST_KEY, 11L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 53L, TEST_KEY, 9L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 64L, "any", 99L) // advance stream time
        );
        final List<ProducerRecord<String, String>> inputOrders = Arrays.asList(
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 65L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 60L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 50L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 35L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 30L, TEST_KEY, TEST_KEY)
        );

        // configure and start the processor topology.
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        final Properties props = getStreamsProperties();

        // WHEN
        try (
                final AdminClient adminClient = initAdminClient();
                final KafkaProducer<String, Long> pricesProducer = initProducer(stringSerde, longSerde);
                final KafkaProducer<String, String> ordersProducer = initProducer(stringSerde, stringSerde);
                final KafkaConsumer<String, Long> consumer = initConsumer(stringSerde, longSerde);
                final CqlSession session = initSession();
                final KafkaStreams streams = initStreams(props, session)
        ) {
            // setup input and output topics.
            int partitions = 1;
            Collection<NewTopic> topics = Arrays.asList(
                    new NewTopic(INPUT_TOPIC_PRICES, partitions, (short) 1),
                    new NewTopic(INPUT_TOPIC_ORDERS, partitions, (short) 1),
                    new NewTopic(OUTPUT_TOPIC_PRICED_ORDERS, partitions, (short) 1)
            );
            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

            consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC_PRICED_ORDERS));

            // start streams.
            streams.start();

            // produce some input data to the input topics.
            inputPrices.forEach(it -> {
                try {
                    pricesProducer.send(it).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
            inputOrders.forEach(it -> {
                try {
                    ordersProducer.send(it).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });

            // consume and collect streams output
            final List<KeyValue<String, Long>> results = new ArrayList<>();
            Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () -> {
                        ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(500));
                        records.iterator().forEachRemaining(record -> results.add(KeyValue.pair(record.key(), record.value())));

                        return results.size() >= 4;
                    }
            );

            // THEN
            assertThat(results.size()).isEqualTo(4);
            assertThat(results.get(0)).isEqualTo(KeyValue.pair(TEST_KEY, 9L));
            assertThat(results.get(1)).isEqualTo(KeyValue.pair(TEST_KEY, 9L));
            assertThat(results.get(2)).isEqualTo(KeyValue.pair(TEST_KEY, 11L));
            assertThat(results.get(3)).isEqualTo(KeyValue.pair(TEST_KEY, 12L));

            assertThat(store.get(TEST_KEY).value()).isEqualTo(9L);
            assertThat(store.get(TEST_KEY, 60L).value()).isEqualTo(9L);
            assertThat(store.get(TEST_KEY, 50L).value()).isEqualTo(11L);
            assertThat(store.get(TEST_KEY, 35L).value()).isEqualTo(12L);
            assertThat(store.get(TEST_KEY, 30L)).isNull();

            // WHEN 2
            long validTo = store.put(TEST_KEY, 8L, 58L);

            // THEN 2
            assertThat(validTo).isEqualTo(-1L);
            assertThat(store.get(TEST_KEY).value()).isEqualTo(8L);
            assertThat(store.get(TEST_KEY, 60L).value()).isEqualTo(8L);
            assertThat(store.get(TEST_KEY, 55L).value()).isEqualTo(9L);
        }
    }

    @Test
    public void shouldPutNewPointInTime() throws ExecutionException, InterruptedException, TimeoutException {
        // GIVEN
        final String TEST_KEY = "sushi";
        final List<ProducerRecord<String, Long>> inputPrices = Arrays.asList(
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 0L, TEST_KEY, 8L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 4L, TEST_KEY, 10L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 17L, TEST_KEY, 12L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 38L, TEST_KEY, 11L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 53L, TEST_KEY, 9L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 64L, "any", 99L) // advance stream time
        );
        final List<ProducerRecord<String, String>> inputOrders = Arrays.asList(
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 65L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 60L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 50L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 35L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 30L, TEST_KEY, TEST_KEY)
        );

        // configure and start the processor topology.
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        final Properties props = getStreamsProperties();

        // WHEN
        try (
                final AdminClient adminClient = initAdminClient();
                final KafkaProducer<String, Long> pricesProducer = initProducer(stringSerde, longSerde);
                final KafkaProducer<String, String> ordersProducer = initProducer(stringSerde, stringSerde);
                final KafkaConsumer<String, Long> consumer = initConsumer(stringSerde, longSerde);
                final CqlSession session = initSession();
                final KafkaStreams streams = initStreams(props, session)
        ) {
            // setup input and output topics.
            int partitions = 1;
            Collection<NewTopic> topics = Arrays.asList(
                    new NewTopic(INPUT_TOPIC_PRICES, partitions, (short) 1),
                    new NewTopic(INPUT_TOPIC_ORDERS, partitions, (short) 1),
                    new NewTopic(OUTPUT_TOPIC_PRICED_ORDERS, partitions, (short) 1)
            );
            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

            consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC_PRICED_ORDERS));

            // start streams.
            streams.start();

            // produce some input data to the input topics.
            inputPrices.forEach(it -> {
                try {
                    pricesProducer.send(it).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
            inputOrders.forEach(it -> {
                try {
                    ordersProducer.send(it).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });

            // consume and collect streams output
            final List<KeyValue<String, Long>> results = new ArrayList<>();
            Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () -> {
                        ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(500));
                        records.iterator().forEachRemaining(record -> results.add(KeyValue.pair(record.key(), record.value())));

                        return results.size() >= 4;
                    }
            );

            // THEN
            assertThat(results.size()).isEqualTo(4);
            assertThat(results.get(0)).isEqualTo(KeyValue.pair(TEST_KEY, 9L));
            assertThat(results.get(1)).isEqualTo(KeyValue.pair(TEST_KEY, 9L));
            assertThat(results.get(2)).isEqualTo(KeyValue.pair(TEST_KEY, 11L));
            assertThat(results.get(3)).isEqualTo(KeyValue.pair(TEST_KEY, 12L));

            assertThat(store.get(TEST_KEY).value()).isEqualTo(9L);
            assertThat(store.get(TEST_KEY, 60L).value()).isEqualTo(9L);
            assertThat(store.get(TEST_KEY, 50L).value()).isEqualTo(11L);
            assertThat(store.get(TEST_KEY, 35L).value()).isEqualTo(12L);
            assertThat(store.get(TEST_KEY, 30L)).isNull();

            // WHEN 2
            long validTo = store.put(TEST_KEY, 8L, 48L);

            // THEN 2
            assertThat(validTo).isEqualTo(53L);
            assertThat(store.get(TEST_KEY).value()).isEqualTo(9L);
            assertThat(store.get(TEST_KEY, 50L).value()).isEqualTo(8L); // was 11$ before
            assertThat(store.get(TEST_KEY, 53L).value()).isEqualTo(9L);
        }
    }

    @Test
    public void shouldPutOutsideRetentionPeriod() throws ExecutionException, InterruptedException, TimeoutException {
        // GIVEN
        final String TEST_KEY = "ramen";
        final List<ProducerRecord<String, Long>> inputPrices = Arrays.asList(
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 0L, TEST_KEY, 8L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 4L, TEST_KEY, 10L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 17L, TEST_KEY, 12L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 38L, TEST_KEY, 11L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 53L, TEST_KEY, 9L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 64L, "any", 99L) // advance stream time
        );
        final List<ProducerRecord<String, String>> inputOrders = Arrays.asList(
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 65L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 60L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 50L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 35L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 30L, TEST_KEY, TEST_KEY)
        );

        // configure and start the processor topology.
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        final Properties props = getStreamsProperties();

        // WHEN
        try (
                final AdminClient adminClient = initAdminClient();
                final KafkaProducer<String, Long> pricesProducer = initProducer(stringSerde, longSerde);
                final KafkaProducer<String, String> ordersProducer = initProducer(stringSerde, stringSerde);
                final KafkaConsumer<String, Long> consumer = initConsumer(stringSerde, longSerde);
                final CqlSession session = initSession();
                final KafkaStreams streams = initStreams(props, session)
        ) {
            // setup input and output topics.
            int partitions = 1;
            Collection<NewTopic> topics = Arrays.asList(
                    new NewTopic(INPUT_TOPIC_PRICES, partitions, (short) 1),
                    new NewTopic(INPUT_TOPIC_ORDERS, partitions, (short) 1),
                    new NewTopic(OUTPUT_TOPIC_PRICED_ORDERS, partitions, (short) 1)
            );
            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

            consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC_PRICED_ORDERS));

            // start streams.
            streams.start();

            // produce some input data to the input topics.
            inputPrices.forEach(it -> {
                try {
                    pricesProducer.send(it).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
            inputOrders.forEach(it -> {
                try {
                    ordersProducer.send(it).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });

            // consume and collect streams output
            final List<KeyValue<String, Long>> results = new ArrayList<>();
            Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () -> {
                        ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(500));
                        records.iterator().forEachRemaining(record -> results.add(KeyValue.pair(record.key(), record.value())));

                        return results.size() >= 4;
                    }
            );

            // THEN
            assertThat(results.size()).isEqualTo(4);
            assertThat(results.get(0)).isEqualTo(KeyValue.pair(TEST_KEY, 9L));
            assertThat(results.get(1)).isEqualTo(KeyValue.pair(TEST_KEY, 9L));
            assertThat(results.get(2)).isEqualTo(KeyValue.pair(TEST_KEY, 11L));
            assertThat(results.get(3)).isEqualTo(KeyValue.pair(TEST_KEY, 12L));

            assertThat(store.get(TEST_KEY).value()).isEqualTo(9L);
            assertThat(store.get(TEST_KEY, 60L).value()).isEqualTo(9L);
            assertThat(store.get(TEST_KEY, 50L).value()).isEqualTo(11L);
            assertThat(store.get(TEST_KEY, 35L).value()).isEqualTo(12L);
            assertThat(store.get(TEST_KEY, 30L)).isNull();

            // WHEN 2
            long validTo = store.put(TEST_KEY, 8L, 33L);

            // THEN 2
            assertThat(validTo).isEqualTo(Long.MIN_VALUE);
            assertThat(store.get(TEST_KEY).value()).isEqualTo(9L);
            assertThat(store.get(TEST_KEY, 35L).value()).isEqualTo(12L);
        }
    }

    @Test
    public void shouldPutNullPointInTime() throws ExecutionException, InterruptedException, TimeoutException {
        // GIVEN
        final String TEST_KEY = "pizza";
        final List<ProducerRecord<String, Long>> inputPrices = Arrays.asList(
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 0L, TEST_KEY, 8L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 4L, TEST_KEY, 10L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 17L, TEST_KEY, 12L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 38L, TEST_KEY, 11L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 53L, TEST_KEY, 9L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 64L, "any", 99L) // advance stream time
        );
        final List<ProducerRecord<String, String>> inputOrders = Arrays.asList(
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 65L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 60L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 50L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 35L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 30L, TEST_KEY, TEST_KEY)
        );

        // configure and start the processor topology.
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        final Properties props = getStreamsProperties();

        // WHEN
        try (
                final AdminClient adminClient = initAdminClient();
                final KafkaProducer<String, Long> pricesProducer = initProducer(stringSerde, longSerde);
                final KafkaProducer<String, String> ordersProducer = initProducer(stringSerde, stringSerde);
                final KafkaConsumer<String, Long> consumer = initConsumer(stringSerde, longSerde);
                final CqlSession session = initSession();
                final KafkaStreams streams = initStreams(props, session)
        ) {
            // setup input and output topics.
            int partitions = 1;
            Collection<NewTopic> topics = Arrays.asList(
                    new NewTopic(INPUT_TOPIC_PRICES, partitions, (short) 1),
                    new NewTopic(INPUT_TOPIC_ORDERS, partitions, (short) 1),
                    new NewTopic(OUTPUT_TOPIC_PRICED_ORDERS, partitions, (short) 1)
            );
            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

            consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC_PRICED_ORDERS));

            // start streams.
            streams.start();

            // produce some input data to the input topics.
            inputPrices.forEach(it -> {
                try {
                    pricesProducer.send(it).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
            inputOrders.forEach(it -> {
                try {
                    ordersProducer.send(it).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });

            // consume and collect streams output
            final List<KeyValue<String, Long>> results = new ArrayList<>();
            Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () -> {
                        ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(500));
                        records.iterator().forEachRemaining(record -> results.add(KeyValue.pair(record.key(), record.value())));

                        return results.size() >= 4;
                    }
            );

            // THEN
            assertThat(results.size()).isEqualTo(4);
            assertThat(results.get(0)).isEqualTo(KeyValue.pair(TEST_KEY, 9L));
            assertThat(results.get(1)).isEqualTo(KeyValue.pair(TEST_KEY, 9L));
            assertThat(results.get(2)).isEqualTo(KeyValue.pair(TEST_KEY, 11L));
            assertThat(results.get(3)).isEqualTo(KeyValue.pair(TEST_KEY, 12L));

            assertThat(store.get(TEST_KEY).value()).isEqualTo(9L);
            assertThat(store.get(TEST_KEY, 60L).value()).isEqualTo(9L);
            assertThat(store.get(TEST_KEY, 50L).value()).isEqualTo(11L);
            assertThat(store.get(TEST_KEY, 35L).value()).isEqualTo(12L);
            assertThat(store.get(TEST_KEY, 30L)).isNull();

            // WHEN 2
            long validTo = store.put(TEST_KEY, null, 48L);

            // THEN 2
            assertThat(validTo).isEqualTo(53L);
            assertThat(store.get(TEST_KEY).value()).isEqualTo(9L);
            assertThat(store.get(TEST_KEY, 50L)).isNull(); // was 11$ before
            assertThat(store.get(TEST_KEY, 53L).value()).isEqualTo(9L);
        }
    }

    @Test
    public void shouldDeleteLatest() throws ExecutionException, InterruptedException, TimeoutException {
        // GIVEN
        final String TEST_KEY = "spaghetti";
        final List<ProducerRecord<String, Long>> inputPrices = Arrays.asList(
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 0L, TEST_KEY, 8L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 4L, TEST_KEY, 10L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 17L, TEST_KEY, 12L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 38L, TEST_KEY, 11L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 53L, TEST_KEY, 9L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 64L, "any", 99L) // advance stream time
        );
        final List<ProducerRecord<String, String>> inputOrders = Arrays.asList(
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 65L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 60L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 50L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 35L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 30L, TEST_KEY, TEST_KEY)
        );

        // configure and start the processor topology.
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        final Properties props = getStreamsProperties();

        // WHEN
        try (
                final AdminClient adminClient = initAdminClient();
                final KafkaProducer<String, Long> pricesProducer = initProducer(stringSerde, longSerde);
                final KafkaProducer<String, String> ordersProducer = initProducer(stringSerde, stringSerde);
                final KafkaConsumer<String, Long> consumer = initConsumer(stringSerde, longSerde);
                final CqlSession session = initSession();
                final KafkaStreams streams = initStreams(props, session)
        ) {
            // setup input and output topics.
            int partitions = 1;
            Collection<NewTopic> topics = Arrays.asList(
                    new NewTopic(INPUT_TOPIC_PRICES, partitions, (short) 1),
                    new NewTopic(INPUT_TOPIC_ORDERS, partitions, (short) 1),
                    new NewTopic(OUTPUT_TOPIC_PRICED_ORDERS, partitions, (short) 1)
            );
            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

            consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC_PRICED_ORDERS));

            // start streams.
            streams.start();

            // produce some input data to the input topics.
            inputPrices.forEach(it -> {
                try {
                    pricesProducer.send(it).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
            inputOrders.forEach(it -> {
                try {
                    ordersProducer.send(it).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });

            // consume and collect streams output
            final List<KeyValue<String, Long>> results = new ArrayList<>();
            Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () -> {
                        ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(500));
                        records.iterator().forEachRemaining(record -> results.add(KeyValue.pair(record.key(), record.value())));

                        return results.size() >= 4;
                    }
            );

            // THEN
            assertThat(results.size()).isEqualTo(4);
            assertThat(results.get(0)).isEqualTo(KeyValue.pair(TEST_KEY, 9L));
            assertThat(results.get(1)).isEqualTo(KeyValue.pair(TEST_KEY, 9L));
            assertThat(results.get(2)).isEqualTo(KeyValue.pair(TEST_KEY, 11L));
            assertThat(results.get(3)).isEqualTo(KeyValue.pair(TEST_KEY, 12L));

            assertThat(store.get(TEST_KEY).value()).isEqualTo(9L);
            assertThat(store.get(TEST_KEY, 60L).value()).isEqualTo(9L);
            assertThat(store.get(TEST_KEY, 50L).value()).isEqualTo(11L);
            assertThat(store.get(TEST_KEY, 35L).value()).isEqualTo(12L);
            assertThat(store.get(TEST_KEY, 30L)).isNull();

            // WHEN 2
            VersionedRecord<Long> deleted = store.delete(TEST_KEY, 55L);

            // THEN 2
            assertThat(deleted).isNotNull();
            assertThat(deleted.timestamp()).isEqualTo(53L);
            assertThat(deleted.value()).isEqualTo(9L);
            assertThat(store.get(TEST_KEY)).isNull();
            assertThat(store.get(TEST_KEY, 53L).value()).isEqualTo(9L);
            assertThat(store.get(TEST_KEY, 54L).value()).isEqualTo(9L);
            assertThat(store.get(TEST_KEY, 55L)).isNull();
        }
    }

    @Test
    public void shouldDeletePointInTime() throws ExecutionException, InterruptedException, TimeoutException {
        // GIVEN
        final String TEST_KEY = "tarte-flambé";
        final List<ProducerRecord<String, Long>> inputPrices = Arrays.asList(
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 0L, TEST_KEY, 8L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 4L, TEST_KEY, 10L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 17L, TEST_KEY, 12L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 38L, TEST_KEY, 11L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 53L, TEST_KEY, 9L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 64L, "any", 99L) // advance stream time
        );
        final List<ProducerRecord<String, String>> inputOrders = Arrays.asList(
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 65L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 60L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 50L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 35L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 30L, TEST_KEY, TEST_KEY)
        );

        // configure and start the processor topology.
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        final Properties props = getStreamsProperties();

        // WHEN
        try (
                final AdminClient adminClient = initAdminClient();
                final KafkaProducer<String, Long> pricesProducer = initProducer(stringSerde, longSerde);
                final KafkaProducer<String, String> ordersProducer = initProducer(stringSerde, stringSerde);
                final KafkaConsumer<String, Long> consumer = initConsumer(stringSerde, longSerde);
                final CqlSession session = initSession();
                final KafkaStreams streams = initStreams(props, session)
        ) {
            // setup input and output topics.
            int partitions = 1;
            Collection<NewTopic> topics = Arrays.asList(
                    new NewTopic(INPUT_TOPIC_PRICES, partitions, (short) 1),
                    new NewTopic(INPUT_TOPIC_ORDERS, partitions, (short) 1),
                    new NewTopic(OUTPUT_TOPIC_PRICED_ORDERS, partitions, (short) 1)
            );
            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

            consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC_PRICED_ORDERS));

            // start streams.
            streams.start();

            // produce some input data to the input topics.
            inputPrices.forEach(it -> {
                try {
                    pricesProducer.send(it).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
            inputOrders.forEach(it -> {
                try {
                    ordersProducer.send(it).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });

            // consume and collect streams output
            final List<KeyValue<String, Long>> results = new ArrayList<>();
            Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () -> {
                        ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(500));
                        records.iterator().forEachRemaining(record -> results.add(KeyValue.pair(record.key(), record.value())));

                        return results.size() >= 4;
                    }
            );

            // THEN
            assertThat(results.size()).isEqualTo(4);
            assertThat(results.get(0)).isEqualTo(KeyValue.pair(TEST_KEY, 9L));
            assertThat(results.get(1)).isEqualTo(KeyValue.pair(TEST_KEY, 9L));
            assertThat(results.get(2)).isEqualTo(KeyValue.pair(TEST_KEY, 11L));
            assertThat(results.get(3)).isEqualTo(KeyValue.pair(TEST_KEY, 12L));

            assertThat(store.get(TEST_KEY).value()).isEqualTo(9L);
            assertThat(store.get(TEST_KEY, 60L).value()).isEqualTo(9L);
            assertThat(store.get(TEST_KEY, 50L).value()).isEqualTo(11L);
            assertThat(store.get(TEST_KEY, 35L).value()).isEqualTo(12L);
            assertThat(store.get(TEST_KEY, 30L)).isNull();

            // WHEN 2
            VersionedRecord<Long> deleted = store.delete(TEST_KEY, 48L);

            // THEN 2
            assertThat(deleted).isNotNull();
            assertThat(deleted.timestamp()).isEqualTo(38L);
            assertThat(deleted.value()).isEqualTo(11L);
            assertThat(store.get(TEST_KEY, 47L).value()).isEqualTo(11L);
            assertThat(store.get(TEST_KEY, 48L)).isNull();
            assertThat(store.get(TEST_KEY, 52L)).isNull();
            assertThat(store.get(TEST_KEY, 53L).value()).isEqualTo(9L);
            assertThat(store.get(TEST_KEY).value()).isEqualTo(9L);
        }
    }

    @Test
    public void shouldReturnNullOnDeleteIfNoValueAtPointInTime() throws ExecutionException, InterruptedException, TimeoutException {
        // GIVEN
        final String TEST_KEY = "natto";
        final List<ProducerRecord<String, Long>> inputPrices = Arrays.asList(
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 0L, TEST_KEY, 8L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 4L, TEST_KEY, 10L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 17L, TEST_KEY, 12L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 38L, TEST_KEY, 11L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 53L, TEST_KEY, 9L),
                new ProducerRecord<>(INPUT_TOPIC_PRICES, null, 64L, "any", 99L) // advance stream time
        );
        final List<ProducerRecord<String, String>> inputOrders = Arrays.asList(
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 65L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 60L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 50L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 35L, TEST_KEY, TEST_KEY),
                new ProducerRecord<>(INPUT_TOPIC_ORDERS, null, 30L, TEST_KEY, TEST_KEY)
        );

        // configure and start the processor topology.
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        final Properties props = getStreamsProperties();

        // WHEN
        try (
                final AdminClient adminClient = initAdminClient();
                final KafkaProducer<String, Long> pricesProducer = initProducer(stringSerde, longSerde);
                final KafkaProducer<String, String> ordersProducer = initProducer(stringSerde, stringSerde);
                final KafkaConsumer<String, Long> consumer = initConsumer(stringSerde, longSerde);
                final CqlSession session = initSession();
                final KafkaStreams streams = initStreams(props, session)
        ) {
            // setup input and output topics.
            int partitions = 1;
            Collection<NewTopic> topics = Arrays.asList(
                    new NewTopic(INPUT_TOPIC_PRICES, partitions, (short) 1),
                    new NewTopic(INPUT_TOPIC_ORDERS, partitions, (short) 1),
                    new NewTopic(OUTPUT_TOPIC_PRICED_ORDERS, partitions, (short) 1)
            );
            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

            consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC_PRICED_ORDERS));

            // start streams.
            streams.start();

            // produce some input data to the input topics.
            inputPrices.forEach(it -> {
                try {
                    pricesProducer.send(it).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
            inputOrders.forEach(it -> {
                try {
                    ordersProducer.send(it).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });

            // consume and collect streams output
            final List<KeyValue<String, Long>> results = new ArrayList<>();
            Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () -> {
                        ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(500));
                        records.iterator().forEachRemaining(record -> results.add(KeyValue.pair(record.key(), record.value())));

                        return results.size() >= 4;
                    }
            );

            // THEN
            assertThat(results.size()).isEqualTo(4);
            assertThat(results.get(0)).isEqualTo(KeyValue.pair(TEST_KEY, 9L));
            assertThat(results.get(1)).isEqualTo(KeyValue.pair(TEST_KEY, 9L));
            assertThat(results.get(2)).isEqualTo(KeyValue.pair(TEST_KEY, 11L));
            assertThat(results.get(3)).isEqualTo(KeyValue.pair(TEST_KEY, 12L));

            assertThat(store.get(TEST_KEY).value()).isEqualTo(9L);
            assertThat(store.get(TEST_KEY, 60L).value()).isEqualTo(9L);
            assertThat(store.get(TEST_KEY, 50L).value()).isEqualTo(11L);
            assertThat(store.get(TEST_KEY, 35L).value()).isEqualTo(12L);
            assertThat(store.get(TEST_KEY, 30L)).isNull();

            // WHEN 2
            VersionedRecord<Long> deleted1 = store.delete(TEST_KEY, 16L);
            VersionedRecord<Long> deleted2 = store.delete(TEST_KEY, 33L);
            VersionedRecord<Long> deleted3 = store.delete(TEST_KEY, 48L);
            VersionedRecord<Long> deleted4 = store.delete(TEST_KEY, 49L);

            // THEN 2
            assertThat(deleted1).isNull();
            assertThat(deleted2).isNull();
            assertThat(deleted3).isNotNull();
            assertThat(deleted4).isNull();
        }
    }

    // note: example taken from Kafka Summit London 2023 talk https://www.confluent.io/events/kafka-summit-london-2023/versioned-state-stores-in-kafka-streams/
    @NotNull
    private KafkaStreams initStreams(Properties streamsConfiguration, CqlSession session) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        // prices table
        final KTable<String, Long> prices = builder.table(
                INPUT_TOPIC_PRICES,
                Materialized.<String, Long>as(
                                Stores.persistentVersionedKeyValueStore(
                                        STORE_NAME,
                                        Duration.ofMillis(30)))
                        .withKeySerde(stringSerde)
                        .withValueSerde(longSerde));

        // orders stream
        final KStream<String, String> orders = builder.stream(INPUT_TOPIC_ORDERS);

        // join orders -> prices (versioned)
        orders.join(
                        prices,
                        (meal, price) -> price
                )
                .process(StoreProdivingProcessor::new, Named.as("StoreProdivingProcessor"), STORE_NAME)
                .peek((k, v) -> LOG.info("out => {}::{}", k, v))
                .to(OUTPUT_TOPIC_PRICED_ORDERS, Produced.with(Serdes.String(), Serdes.Long()));

        return new KafkaStreams(builder.build(), streamsConfiguration);
    }

    /**
     * Note: this is a hack required to get access to the underlying state store.
     * To be removed/refactored once interactive queries are supported in a future release of Apache Kafka.
     */
    private class StoreProdivingProcessor implements Processor<String, Long, String, Long> {

        private ProcessorContext<String, Long> context;
        @Override
        public void init(ProcessorContext<String, Long> context) {
            Processor.super.init(context);
            this.context = context;
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(Record<String, Long> record) {
            context.forward(record);
        }

        @Override
        public void close() {
            Processor.super.close();
            store = null;
        }
    }
}
