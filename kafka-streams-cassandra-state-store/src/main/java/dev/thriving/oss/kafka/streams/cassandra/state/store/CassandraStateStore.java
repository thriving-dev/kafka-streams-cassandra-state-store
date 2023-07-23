package dev.thriving.oss.kafka.streams.cassandra.state.store;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.thriving.oss.kafka.streams.cassandra.state.store.repo.PartitionedCassandraKeyValueStoreRepository;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.InvalidStateStorePartitionException;
import org.apache.kafka.streams.errors.StreamsNotStartedException;
import org.apache.kafka.streams.errors.UnknownStateStoreException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.internals.DefaultStreamPartitioner;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Objects;
import java.util.function.Function;

import static org.apache.kafka.streams.StoreQueryParameters.fromNameAndType;

/**
 * Implemented by all the libraries custom {@link StateStore} solutions.
 * Further, provides helper methods to get ReadOnly stores for interactive queries.
 */
public interface CassandraStateStore extends StateStore {

    Function<String, String> DEFAULT_TABLE_NAME_FN = storeName -> String.format("%s_kstreams_store", storeName.toLowerCase().replaceAll("[^a-z0-9_]", "_"));

    /**
     * Get an optimised special implementation of {@link ReadOnlyKeyValueStore} for 'partitioned' type CassandraKeyValueStore.
     * The returned object can be used to query the state directly from the underlying Cassandra table.
     * No 'RPC layer' is required since queries for all/individual partitions are executed from this instance, and query
     * results are merged where necessary.
     * <br />
     * The instance has to be created when the streams app is in RUNNING state and can then be re-used!
     * <br />
     * Note: for global CassandraKeyValueStore, use {@link CassandraStateStore#readOnlyGlobalKeyValueStore}!
     *
     * @param streams             the kafka streams instance
     * @param name                the name of the state store
     * @param session             cassandra session to be used by the store (cannot be {@code null})
     * @param keyspace            state store specific keyspace, when null is passed, the CqlSession default applies
     * @param isCountAllEnabled   enable `SELECT COUNT(*)` to be used. . SELECT COUNT(*) requires significant CPU and I/O resources and may be quite slow depending on store size... use with care!!!
     * @param dmlExecutionProfile the driver execution profile to use for DML queries
     * @param keySerde            the key Serde to use (cannot be {@code null})
     * @param valueSerde          the value Serde to use (cannot be {@code null})
     * @param <K>                 key data type
     * @param <V>                 value data type
     * @return an instance of {@link CassandraPartitionedReadOnlyKeyValueStore}
     * @throws StreamsNotStartedException If Streams has not yet been started. Just call {@link KafkaStreams#start()}
     *                                    and then retry this call.
     */
    static <K, V> ReadOnlyKeyValueStore<K, V> readOnlyPartitionedKeyValueStore(KafkaStreams streams,
                                                                               String name,
                                                                               CqlSession session,
                                                                               String keyspace,
                                                                               boolean isCountAllEnabled,
                                                                               String dmlExecutionProfile,
                                                                               Serde<K> keySerde,
                                                                               Serde<V> valueSerde) {
        return readOnlyPartitionedKeyValueStore(
                streams,
                name,
                session,
                keyspace,
                isCountAllEnabled,
                dmlExecutionProfile,
                keySerde,
                valueSerde,
                DEFAULT_TABLE_NAME_FN,
                new DefaultStreamPartitioner<>(keySerde.serializer())
        );
    }

    /**
     * Get an optimised special implementation of {@link ReadOnlyKeyValueStore} for 'partitioned' type CassandraKeyValueStore.
     * The returned object can be used to query the state directly from the underlying Cassandra table.
     * No 'RPC layer' is required since queries for all/individual partitions are executed from this instance, and query
     * results are merged where necessary.
     * <br />
     * The instance has to be created when the streams app is in RUNNING state and can then be re-used!
     * <br />
     * Note: for global CassandraKeyValueStore, use {@link CassandraStateStore#readOnlyGlobalKeyValueStore}!
     *
     * @param streams             the kafka streams instance
     * @param name                the name of the state store
     * @param session             cassandra session to be used by the store (cannot be {@code null})
     * @param keyspace            state store specific keyspace, when null is passed, the CqlSession default applies
     * @param isCountAllEnabled   enable `SELECT COUNT(*)` to be used. . SELECT COUNT(*) requires significant CPU and I/O resources and may be quite slow depending on store size... use with care!!!
     * @param dmlExecutionProfile the driver execution profile to use for DML queries
     * @param keySerde            the key Serde to use (cannot be {@code null})
     * @param valueSerde          the value Serde to use (cannot be {@code null})
     * @param tableNameFn         function to transform 'kstreams store name' -> 'cql table name'. If null, {@link CassandraStateStore#DEFAULT_TABLE_NAME_FN} is used.
     * @param partitioner         The partitioner to determine the partition for a key. If null, DefaultStreamPartitioner is used.
     * @param <K>                 key data type
     * @param <V>                 value data type
     * @return an instance of {@link CassandraPartitionedReadOnlyKeyValueStore}
     * @throws StreamsNotStartedException If Streams has not yet been started. Just call {@link KafkaStreams#start()}
     *                                    and then retry this call.
     */
    static <K, V> ReadOnlyKeyValueStore<K, V> readOnlyPartitionedKeyValueStore(KafkaStreams streams,
                                                                               String name,
                                                                               CqlSession session,
                                                                               String keyspace,
                                                                               boolean isCountAllEnabled,
                                                                               String dmlExecutionProfile,
                                                                               Serde<K> keySerde,
                                                                               Serde<V> valueSerde,
                                                                               Function<String, String> tableNameFn,
                                                                               StreamPartitioner<K, V> partitioner) {
        // validate args
        Objects.requireNonNull(session, "session cannot be null");
        Objects.requireNonNull(name, "name cannot be null");
        Objects.requireNonNull(keySerde, "keySerde cannot be null");
        Objects.requireNonNull(valueSerde, "valueSerde cannot be null");

        // apply defaults
        if (tableNameFn == null) {
            tableNameFn = DEFAULT_TABLE_NAME_FN;
        }
        if (partitioner == null) {
            partitioner = new DefaultStreamPartitioner<>(keySerde.serializer());
        }

        // TODO #23/#25: improve... duplicate code -> when the store is defined
        String resolvedTableName = tableNameFn.apply(name);
        String fullTableName = keyspace != null ? keyspace + "." + resolvedTableName : resolvedTableName;

        return new CassandraPartitionedReadOnlyKeyValueStore<>(
                streams,
                new PartitionedCassandraKeyValueStoreRepository<>(
                        session,
                        fullTableName,
                        false,
                        "",
                        null,
                        dmlExecutionProfile // TODO #23/#25: improve... duplicate code -> when the store is defined
                ),
                isCountAllEnabled,
                keySerde,
                valueSerde,
                partitioner
        );
    }

    /**
     * Get a facade wrapping the local {@link CassandraStateStore}.
     * The returned object can be used to query the 'global' {@link CassandraStateStore} instances.
     * Since the 'globalKeyValueStore' variant is not partitioned, specific StoreQueryParameters are specified to
     * ensure correct results and avoid unnecessary queries to the Cassandra database.
     *
     * @param streams   the kafka streams instance
     * @param storeName the name of the state store
     * @param <K>       the key type
     * @param <V>       the value type
     * @return a facade wrapping the local {@link CassandraStateStore} instances
     * @throws StreamsNotStartedException          If Streams has not yet been started. Just call {@link KafkaStreams#start()}
     *                                             and then retry this call.
     * @throws UnknownStateStoreException          If the specified store name does not exist in the topology.
     * @throws InvalidStateStorePartitionException If the specified partition does not exist.
     * @throws InvalidStateStoreException          If the Streams instance isn't in a queryable state.
     *                                             If the store's type does not match the QueryableStoreType,
     *                                             the Streams instance is not in a queryable state with respect
     *                                             to the parameters, or if the store is not available locally, then
     *                                             an InvalidStateStoreException is thrown upon store access.
     */
    static <K, V> ReadOnlyKeyValueStore<K, V> readOnlyGlobalKeyValueStore(KafkaStreams streams, String storeName) {
        // get the first active task partition for the first streams thread
        final int firstActiveTaskPartition = streams.metadataForLocalThreads()
                .stream().findFirst()
                .orElseThrow(() -> new RuntimeException("no streams threads found"))
                .activeTasks()
                .stream().findFirst()
                .orElseThrow(() -> new RuntimeException("no active task found"))
                .taskId().partition();

        // get a WrappingStoreProvider 'withPartition' -> query only a single store (the first active task)!
        // (WrappingStoreProvider otherwise iterates over all storeProviders for all assigned tasks and repeatedly query Cassandra)
        return streams.store(fromNameAndType(storeName, QueryableStoreTypes.<K, V>keyValueStore())
                .enableStaleStores() // should be unnecessary -> CassandraStateStore should always be used with logging disabled and without standby tasks...
                .withPartition(firstActiveTaskPartition));
    }
}
