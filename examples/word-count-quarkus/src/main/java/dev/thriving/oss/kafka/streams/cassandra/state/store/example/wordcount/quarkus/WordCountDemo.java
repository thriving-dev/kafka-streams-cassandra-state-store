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

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        final KStream<String, String> source = builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, stringSerde));

        final KTable<String, Long> counts = source
                .peek((k, v) -> LOG.debugf("in => %s::%s", k, v))
                .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
                .groupBy((key, value) -> value)
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

        // need to override value serde to Long type
        counts.toStream()
                .peek((k, v) -> LOG.debugf("out => %s::%s", k, v))
                .to(OUTPUT_TOPIC, Produced.with(stringSerde, longSerde));

        return builder.build();
    }
}
