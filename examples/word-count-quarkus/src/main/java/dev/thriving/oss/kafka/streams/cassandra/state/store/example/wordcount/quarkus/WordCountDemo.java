package dev.thriving.oss.kafka.streams.cassandra.state.store.example.wordcount.quarkus;

import com.datastax.oss.quarkus.runtime.api.session.QuarkusCqlSession;
import dev.thriving.oss.kafka.streams.cassandra.state.store.CassandraStores;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.jboss.logging.Logger;

import java.util.Arrays;
import java.util.Locale;

/**
 * ref Kafka Streams Quick Start <a href="https://docs.confluent.io/platform/6.2/streams/quickstart.html">WordCount demo application</a>
 * also <a href="https://github.com/apache/kafka/blob/3.4/streams/examples/src/main/java/org/apache/kafka/streams/examples/wordcount/WordCountDemo.java">WordCountDemo.java</a>
 *
 * @return Kafka Streams Topology
 */
@ApplicationScoped
public class WordCountDemo {

    private static final Logger LOG = Logger.getLogger(WordCountDemo.class);

    public static final String INPUT_TOPIC = "streams-plaintext-input";
    public static final String OUTPUT_TOPIC = "streams-wordcount-output";

    @Inject
    QuarkusCqlSession quarkusCqlSession;

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // Serializers/deserializers (serde) for String and Long types
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        // Construct a `KStream` from the input topic "streams-plaintext-input", where message values
        // represent lines of text (for the sake of this example, we ignore whatever may be stored
        // in the message keys).
        final KStream<String, String> source = builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, stringSerde));

        final KTable<String, Long> wordCounts = source
                .peek((k, v) -> LOG.debugf("in => %s::%s", k, v))
                // Split each text line, by whitespace, into words.  The text lines are the message
                // values, i.e. we can ignore whatever data is in the message keys and thus invoke
                // `flatMapValues` instead of the more generic `flatMap`.
                .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
                // We use `groupBy` to ensure the words are available as message keys
                .groupBy((key, value) -> value)
                // Count the occurrences of each word (message key).
                .count(Materialized.<String, Long>as(
                                CassandraStores.builder(quarkusCqlSession, "word-grouped-count")
                                        .withKeyspace("test")
                                        .withDdlExecutionProfile("ddl")
                                        .withDmlExecutionProfile("dml")
                                        .keyValueStore()
                        )
                        .withLoggingDisabled()
                        .withCachingDisabled()
                        .withKeySerde(stringSerde)
                        .withValueSerde(longSerde));

        wordCounts.toStream()
                .peek((k, v) -> LOG.debugf("out => %s::%s", k, v))
                // Convert the `KTable<String, Long>` into a `KStream<String, Long>` and write to the output topic.
                .to(OUTPUT_TOPIC, Produced.with(stringSerde, longSerde));

        return builder.build();
    }
}
