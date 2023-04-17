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
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test that demonstrates how to perform a join between two KStreams.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class StreamToStreamJoinIntegrationTest extends AbstractIntegrationTest {

  private static final String adImpressionsTopic = "adImpressions";
  private static final String adClicksTopic = "adClicks";
  private static final String outputTopic = "output-topic";

  @Test
  public void shouldJoinTwoStreams() throws ExecutionException, InterruptedException, TimeoutException {
    // Input 1: Ad impressions
    final List<KeyValue<String, String>> inputAdImpressions = Arrays.asList(
      new KeyValue<>("car-advertisement", "shown"),
      new KeyValue<>("newspaper-advertisement", "shown"),
      new KeyValue<>("gadget-advertisement", "shown")
    );

    // Input 2: Ad clicks
    final List<KeyValue<String, String>> inputAdClicks = Arrays.asList(
      new KeyValue<>("newspaper-advertisement", "clicked"),
      new KeyValue<>("gadget-advertisement", "clicked"),
      new KeyValue<>("newspaper-advertisement", "clicked")
    );

    final List<KeyValue<String, String>> expectedResults = Arrays.asList(
      new KeyValue<>("car-advertisement", "shown/not-clicked-yet"),
      new KeyValue<>("newspaper-advertisement", "shown/not-clicked-yet"),
      new KeyValue<>("gadget-advertisement", "shown/not-clicked-yet"),
      new KeyValue<>("newspaper-advertisement", "shown/clicked"),
      new KeyValue<>("gadget-advertisement", "shown/clicked"),
      new KeyValue<>("newspaper-advertisement", "shown/clicked")
    );

    // configure and start the processor topology.
    final Serde<String> stringSerde = Serdes.String();
    final Properties props = getStreamsProperties();

    // when
    try (
            final AdminClient adminClient = initAdminClient();
            final KafkaProducer<String, String> producer = initProducer(stringSerde, stringSerde);
            final KafkaConsumer<String, String> consumer = initConsumer(stringSerde, stringSerde);
            final CqlSession session = initSession();
            final KafkaStreams streams = initStreams(props, session)
    ) {
      // setup input and output topics.
      Collection<NewTopic> topics = Arrays.asList(
              new NewTopic(adImpressionsTopic, 6, (short) 1),
              new NewTopic(adClicksTopic, 6, (short) 1),
              new NewTopic(outputTopic, 3, (short) 1)
      );
      adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

      consumer.subscribe(Collections.singletonList(outputTopic));

      // start streams.
      streams.start();

      // produce some input data to the input topics.
      inputAdImpressions.forEach(it -> {
        try {
          producer.send(new ProducerRecord<>(adImpressionsTopic, it.key, it.value)).get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      });
      inputAdClicks.forEach(it -> {
        try {
          producer.send(new ProducerRecord<>(adClicksTopic, it.key, it.value)).get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      });

      // consume and collect streams output
      final List<KeyValue<String, String>> results = new ArrayList<>();
      Unreliables.retryUntilTrue(600, TimeUnit.SECONDS, () -> {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                records.iterator().forEachRemaining(record -> results.add(KeyValue.pair(record.key(), record.value())));

                return results.size() >= expectedResults.size();
              }
      );

      // then verify the application's output data.
      assertThat(results).containsExactlyInAnyOrderElementsOf(expectedResults);
    }
  }

  // note: adapted from https://github.com/confluentinc/kafka-streams-examples/blob/v7.5.0-148/src/test/java/io/confluent/examples/streams/StreamToStreamJoinIntegrationTest.java
  @NotNull
  private KafkaStreams initStreams(Properties streamsConfiguration, CqlSession session) {
    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, String> alerts = builder.stream(adImpressionsTopic);
    final KStream<String, String> incidents = builder.stream(adClicksTopic);

    // In this example, we opt to perform an OUTER JOIN between the two streams.  We picked this
    // join type to show how the Streams API will send further join updates downstream whenever,
    // for the same join key (e.g. "newspaper-advertisement"), we receive an update from either of
    // the two joined streams during the defined join window.
    Duration joinTimeDifference = Duration.ofSeconds(5);
    Duration windowDuration = joinTimeDifference.multipliedBy(2);
    long DEPRECATED_DEFAULT_24_HR_GRACE_PERIOD = 24 * 60 * 60 * 1000L;
    long retentionPeriod = DEPRECATED_DEFAULT_24_HR_GRACE_PERIOD;
    final KStream<String, String> impressionsAndClicks = alerts.outerJoin(
            incidents,
            (impressionValue, clickValue) ->
                    (clickValue == null)? impressionValue + "/not-clicked-yet": impressionValue + "/" + clickValue,
            // KStream-KStream joins are always windowed joins, hence we must provide a join window.
            JoinWindows.of(joinTimeDifference),
            // In this specific example, we don't need to define join serdes explicitly because the key, left value, and
            // right value are all of type String, which matches our default serdes configured for the application.  However,
            // we want to showcase the use of `StreamJoined.with(...)` in case your code needs a different type setup.
            StreamJoined.with(
                    Serdes.String(), /* key */
                    Serdes.String(), /* left value */
                    Serdes.String()  /* right value */
            )
                    .withThisStoreSupplier(
                            CassandraStores.builder(session, "store-this")
                                    .withKeyspace(CASSANDRA_KEYSPACE)
                                    .windowBytesStore(retentionPeriod, -1, windowDuration.toMillis(), true)
                    )
                    .withOtherStoreSupplier(
                            CassandraStores.builder(session, "store-other")
                                    .withKeyspace(CASSANDRA_KEYSPACE)
                                    .windowBytesStore(retentionPeriod, -1, windowDuration.toMillis(), true)
                    )
                    .withLoggingDisabled()
    );

    // Write the results to the output topic.
    impressionsAndClicks.to(outputTopic);

    return new KafkaStreams(builder.build(), streamsConfiguration);
  }
}
