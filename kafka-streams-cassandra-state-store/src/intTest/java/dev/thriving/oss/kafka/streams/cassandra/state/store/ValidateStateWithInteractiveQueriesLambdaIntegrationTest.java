/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.thriving.oss.kafka.streams.cassandra.state.store;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.thriving.oss.kafka.streams.cassandra.state.store.utils.IntegrationTestUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStore;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.streams.StoreQueryParameters.fromNameAndType;

/**
 * Demonstrates how to validate an application's expected state through interactive queries.
 * <p>
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class ValidateStateWithInteractiveQueriesLambdaIntegrationTest extends AbstractIntegrationTest {

  private static final String inputTopic = "inputTopic";
  private static final String STORE_NAME_MAX = "max-store";
  private static final String STORE_NAME_MAX_WINDOW = "max-window-store";

  @Test
  public void shouldCalculateMaxClicksPerUser() throws ExecutionException, InterruptedException, TimeoutException {
    // input: A user may be listed multiple times.
    final List<KeyValue<String, Long>> inputUserClicks = Arrays.asList(
            new KeyValue<>("alice", 13L),
            new KeyValue<>("bob", 4L),
            new KeyValue<>("chao", 25L),
            new KeyValue<>("bob", 19L),
            new KeyValue<>("chao", 56L),
            new KeyValue<>("alice", 78L),
            new KeyValue<>("alice", 40L),
            new KeyValue<>("bob", 3L)
    );

    final Map<String, Long> expectedMaxClicksPerUser = new HashMap<String, Long>() {
      {
        put("alice", 78L);
        put("bob", 19L);
        put("chao", 56L);
      }
    };

    // configure and start the processor topology.
    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();
    final Properties props = getStreamsProperties();

    // The commit interval for flushing records to state stores and downstream must be lower than
    // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 2 * 1000);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());

    // when
    try (
            final AdminClient adminClient = initAdminClient();
            final KafkaProducer<String, Long> producer = initProducer(stringSerde, longSerde);
            final CqlSession session = initSession();
            final KafkaStreams streams = initStreams(props, session)
    ) {
      // setup input and output topics.
      Collection<NewTopic> topics = List.of(
              new NewTopic(inputTopic, 6, (short) 1)
      );
      adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

      // start streams.
      streams.start();

      // produce some input data to the input topics.
      inputUserClicks.forEach(it -> {
        try {
          producer.send(new ProducerRecord<>(inputTopic, it.key, it.value)).get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      });

      // then verify the application's output data.
      final ReadOnlyKeyValueStore<String, Long> keyValueStore =
              streams.store(fromNameAndType(STORE_NAME_MAX, QueryableStoreTypes.keyValueStore()));

      final ReadOnlyWindowStore<String, Long> windowStore =
              streams.store(fromNameAndType(STORE_NAME_MAX_WINDOW, QueryableStoreTypes.windowStore()));

      IntegrationTestUtils.assertThatKeyValueStoreContains(keyValueStore, expectedMaxClicksPerUser);
      IntegrationTestUtils.assertThatOldestWindowContains(windowStore, expectedMaxClicksPerUser);
    }
  }

  // note: adapted from https://github.com/confluentinc/kafka-streams-examples/blob/v7.5.0-148/src/test/java/io/confluent/examples/streams/ValidateStateWithInteractiveQueriesLambdaIntegrationTest.java
  @NotNull
  private KafkaStreams initStreams(Properties streamsConfiguration, CqlSession session) {
    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, Long> stream = builder.stream(inputTopic);

    // rolling MAX() aggregation
    final String maxStore = STORE_NAME_MAX;
    stream.groupByKey().aggregate(
            () -> Long.MIN_VALUE,
            (aggKey, value, aggregate) -> Math.max(value, aggregate),
            Materialized.as(maxStore)
    );

    // windowed MAX() aggregation
    final String maxWindowStore = STORE_NAME_MAX_WINDOW;
    stream.groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofMinutes(1L)).grace(Duration.ZERO))
            .aggregate(
                    () -> Long.MIN_VALUE,
                    (aggKey, value, aggregate) -> Math.max(value, aggregate),
                    Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(maxWindowStore).withRetention(Duration.ofMinutes(5L)));

    return new KafkaStreams(builder.build(), streamsConfiguration);
  }
}
