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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
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

    @Test
    public void shouldAllowToGetRecordsFromVersionedStore() throws ExecutionException, InterruptedException, TimeoutException {
        // given
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

        // when
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
            Thread.sleep(500);
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

            assertThat(results.size()).isEqualTo(4);
            assertThat(results.get(0)).isEqualTo(KeyValue.pair(TEST_KEY, 9L));
            assertThat(results.get(1)).isEqualTo(KeyValue.pair(TEST_KEY, 9L));
            assertThat(results.get(2)).isEqualTo(KeyValue.pair(TEST_KEY, 11L));
            assertThat(results.get(3)).isEqualTo(KeyValue.pair(TEST_KEY, 12L));
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
                .peek((k, v) -> LOG.info("out => {}::{}", k, v))
                .to(OUTPUT_TOPIC_PRICED_ORDERS, Produced.with(Serdes.String(), Serdes.Long()));

        return new KafkaStreams(builder.build(), streamsConfiguration);
    }

}
