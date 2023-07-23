package dev.thriving.oss.kafka.streams.cassandra.state.store;

import com.datastax.oss.driver.api.core.CqlSession;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.jetbrains.annotations.NotNull;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Properties;
import java.util.UUID;

@Testcontainers
public abstract class AbstractIntegrationTest {

    protected static final String CASSANDRA_KEYSPACE = "test";

    @Container
    public RedpandaContainer redpanda = new RedpandaContainer(
            DockerImageName.parse("docker.redpanda.com/vectorized/redpanda:v23.1.1")
    );

    @Container
    public CassandraContainer<?> cassandra = new CassandraContainer<>(
            DockerImageName.parse("cassandra:4.1")
    ).withInitScript("schema.cql");

    @NotNull
    protected Properties getStreamsProperties() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "integration-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, redpanda.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);

        // TODO(#23/#25): needed to have metadata available (`streams.metadataForAllStreamsClients()`)
        //       ref dev.thriving.oss.kafka.streams.cassandra.state.store.CassandraPartitionedReadOnlyKeyValueStore.CassandraPartitionedReadOnlyKeyValueStore
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "test:1234");

        return props;
    }

    @NotNull
    protected AdminClient initAdminClient() {
        return AdminClient.create(
                ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, redpanda.getBootstrapServers())
        );
    }

    @NotNull
    protected <K, V> KafkaProducer<K, V> initProducer(Serde<K> keySerde, Serde<V> valueSerde) {
        return new KafkaProducer<>(
                ImmutableMap.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        redpanda.getBootstrapServers(),
                        ProducerConfig.CLIENT_ID_CONFIG,
                        UUID.randomUUID().toString()
                ),
                keySerde.serializer(),
                valueSerde.serializer()
        );
    }

    @NotNull
    protected <K, V> KafkaConsumer<K, V> initConsumer(Serde<K> keySerde, Serde<V> valueSerde) {
        return new KafkaConsumer<>(
                ImmutableMap.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        redpanda.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG,
                        "tc-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "earliest"
                ),
                keySerde.deserializer(),
                valueSerde.deserializer()
        );
    }

    @NotNull
    protected CqlSession initSession() {
        return CqlSession.builder()
                .withKeyspace(CASSANDRA_KEYSPACE)
                .withLocalDatacenter(cassandra.getLocalDatacenter())
                .addContactPoint(cassandra.getContactPoint())
                .build();
    }

}
