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

import java.util.function.Function;

import static org.apache.kafka.streams.StoreQueryParameters.fromNameAndType;

/**
 * Implemented by all the libraries custom {@link StateStore} solutions.
 * Further, provides helper methods to get ReadOnly stores for interactive queries.
 */
public interface CassandraStateStore extends StateStore {

    Function<String, String> DEFAULT_TABLE_NAME_FN = storeName -> String.format("%s_kstreams_store", storeName.toLowerCase().replaceAll("[^a-z0-9_]", "_"));

    /**
     * Get a facade wrapping the local {@link CassandraStateStore}.
     * The returned object can be used to query the {@link CassandraStateStore} instances.
     * <br />
     * Note: for global CassandraKeyValueStore, use {@link CassandraStateStore#readOnlyGlobalKeyValueStore}!
     *
     * @param streams   the kafka streams instance
     * @param storeName the name of the state store
     * @param <X>       the key type
     * @param <Y>       the value type
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
    static <X, Y> ReadOnlyKeyValueStore<X, Y> readOnlyPartitionedKeyValueStore(KafkaStreams streams, String storeName) {
        // get store fromNameAndType (regular way)
        return streams.store(fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
    }

    static <X, Y> ReadOnlyKeyValueStore<X, Y> readOnlyPartitionedKeyValueStore(KafkaStreams streams,
                                                                               String storeName,
                                                                               CqlSession session,
                                                                               String keyspace,
                                                                               boolean isCountAllEnabled,
                                                                               String dmlExecutionProfile,
                                                                               Serde<X> keySerde,
                                                                               Serde<Y> valueSerde) {
        return readOnlyPartitionedKeyValueStore(
                streams,
                storeName,
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

    static <X, Y> ReadOnlyKeyValueStore<X, Y> readOnlyPartitionedKeyValueStore(KafkaStreams streams,
                                                                               String storeName,
                                                                               CqlSession session,
                                                                               String keyspace,
                                                                               boolean isCountAllEnabled,
                                                                               String dmlExecutionProfile,
                                                                               Serde<X> keySerde,
                                                                               Serde<Y> valueSerde,
                                                                               Function<String, String> tableNameFn,
                                                                               StreamPartitioner<X, Y> partitioner) {
        // TODO: improve... duplicate code -> when the store is defined
        String resolvedTableName = tableNameFn.apply(storeName);
        String fullTableName = keyspace != null ? keyspace + "." + resolvedTableName : resolvedTableName;

        return new CassandraReadOnlyKeyValueStore<>(
                streams,
                new PartitionedCassandraKeyValueStoreRepository<>(
                        session,
                        fullTableName,
                        false,
                        "",
                        null,
                        dmlExecutionProfile // TODO: improve... duplicate code -> when the store is defined
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
     * @param <X>       the key type
     * @param <Y>       the value type
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
    static <X, Y> ReadOnlyKeyValueStore<X, Y> readOnlyGlobalKeyValueStore(KafkaStreams streams, String storeName) {
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
        return streams.store(fromNameAndType(storeName, QueryableStoreTypes.<X, Y>keyValueStore())
                .enableStaleStores() // should be unnecessary -> CassandraStateStore should always be used with logging disabled and without standby tasks...
                .withPartition(firstActiveTaskPartition));
    }
}
