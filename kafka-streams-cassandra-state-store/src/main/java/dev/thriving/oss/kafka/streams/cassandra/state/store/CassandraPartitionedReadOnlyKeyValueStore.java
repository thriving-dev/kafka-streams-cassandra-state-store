package dev.thriving.oss.kafka.streams.cassandra.state.store;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import dev.thriving.oss.kafka.streams.cassandra.state.store.repo.CassandraKeyValueStoreRepository;
import dev.thriving.oss.kafka.streams.cassandra.state.store.utils.CompositeKeyValueIterator;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsNotStartedException;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * Optimised, special implementation of {@link ReadOnlyKeyValueStore} for 'partitioned' type CassandraKeyValueStore.
 * Queries the state directly from the underlying Cassandra table.
 * No 'RPC layer' is required since queries for all/individual partitions are executed from this instance, and query
 * results are merged where necessary.
 * <br/>
 * Note: package private, get via
 * {@link CassandraStateStore#readOnlyPartitionedKeyValueStore(KafkaStreams, String, CqlSession, String, boolean, String, Serde, Serde)}
 * or
 * {@link CassandraStateStore#readOnlyPartitionedKeyValueStore(KafkaStreams, String, CqlSession, String, boolean, String, Serde, Serde, Function, StreamPartitioner)}
 * <br/>
 * Note: The instance has to be created when the streams app is in RUNNING state and can then be re-used!
 *
 * @param <K> key data type
 * @param <V> value data type
 */
class CassandraPartitionedReadOnlyKeyValueStore<K, V> implements ReadOnlyKeyValueStore<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraPartitionedReadOnlyKeyValueStore.class);

    private final CassandraKeyValueStoreRepository repo;
    private final boolean isCountAllEnabled;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final StreamPartitioner<K, V> partitioner;
    private final List<Integer> partitions;
    private final List<Integer> reversePartitions;

    /**
     * @param streams           the kafka streams instance
     * @param repo              the CassandraKeyValueStoreRepository instance
     * @param isCountAllEnabled enable `SELECT COUNT(*)` to be used. . SELECT COUNT(*) requires significant CPU and I/O resources and may be quite slow depending on store size... use with care!!!
     * @param keySerde          the key Serde to use (cannot be {@code null})
     * @param valueSerde        the value Serde to use (cannot be {@code null})
     * @param partitioner       The partitioner to determine the partition for a key. If null, DefaultStreamPartitioner is used.
     * @throws StreamsNotStartedException if KafkaStreams has not been started
     * @throws IllegalStateException      if KafkaStreams is not running
     */
    CassandraPartitionedReadOnlyKeyValueStore(KafkaStreams streams,
                                              CassandraKeyValueStoreRepository repo,
                                              boolean isCountAllEnabled,
                                              Serde<K> keySerde,
                                              Serde<V> valueSerde,
                                              StreamPartitioner<K, V> partitioner)
            throws StreamsNotStartedException, IllegalStateException {
        this.repo = repo;
        this.isCountAllEnabled = isCountAllEnabled;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.partitioner = partitioner;

        // validate state
        validateIsRunningOrRebalancing(streams.state());

        // construct complete list of partitions to use for querying Cassandra (+ reverse) across partitions
        // TODO(#23/#25): is there a better way to dynamically determine the number of partitions (streams tasks) that does not require `application.server` streams config to be set?
        int maxPartition = streams.metadataForAllStreamsClients().stream()
                .flatMap(streamsMetadata -> streamsMetadata.topicPartitions().stream())
                .mapToInt(TopicPartition::partition)
                .max().orElseThrow(() -> new RuntimeException("No StreamsClients metadata available to determine no. of partitions -> please provide `application.server` config!!"));
        partitions = IntStream.rangeClosed(0, maxPartition).boxed().toList();
        ArrayList<Integer> reversePartitions = new ArrayList<>(partitions);
        Collections.reverse(reversePartitions);
        this.reversePartitions = ImmutableList.copyOf(reversePartitions);
    }

    // ref: copied + adapted from `org.apache.kafka.streams.KafkaStreams.validateIsRunningOrRebalancing` (at 3.5.0)
    private void validateIsRunningOrRebalancing(KafkaStreams.State state) {
        if (state.hasNotStarted()) {
            throw new StreamsNotStartedException("KafkaStreams has not been started, you can retry after calling start()");
        }
        if (!state.isRunningOrRebalancing()) {
            throw new IllegalStateException("KafkaStreams is not running. State is " + state + ", you can retry once running.");
        }
    }

    @Override
    public V get(K key) {
        int partition = getPartitionForKey(key);
        Bytes keyBytes = serializeKey(key);
        byte[] valueBytes = repo.getByKey(partition, keyBytes);
        return valueSerde.deserializer().deserialize(null, valueBytes);
    }

    // ref: copied + adapted from `org.apache.kafka.streams.processor.internals.StreamsMetadataState.getPartition` (at 3.5.0)
    private int getPartitionForKey(K key) {
        Optional<Set<Integer>> optionalIntegerSet = partitioner.partitions(null, key, null, partitions.size());
        if (!optionalIntegerSet.isPresent()) {
            throw new IllegalArgumentException("The partitions returned by StreamPartitioner#partitions method when used for fetching KeyQueryMetadata for key should be present");
        }
        if (optionalIntegerSet.get().size() != 1) {
            throw new IllegalArgumentException("The partitions returned by StreamPartitioner#partitions method when used for fetching KeyQueryMetadata for key should be a singleton set");
        }
        return optionalIntegerSet.get().iterator().next();
    }

    private Bytes serializeKey(K key) {
        return Bytes.wrap(keySerde.serializer().serialize(null, key));
    }

    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        return queryAllPartitions(
                partition -> repo.getForRange(partition,
                        serializeKey(from),
                        serializeKey(to),
                        true,
                        true),
                true);
    }

    @Override
    public KeyValueIterator<K, V> reverseRange(K from, K to) {
        return queryAllPartitions(
                partition -> repo.getForRange(partition,
                        serializeKey(from),
                        serializeKey(to),
                        false,
                        true),
                false);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return queryAllPartitions(partition -> repo.getAll(partition, true), true);
    }

    @Override
    public KeyValueIterator<K, V> reverseAll() {
        return queryAllPartitions(partition -> repo.getAll(partition, false), false);
    }

    private CompositeKeyValueIterator<K, V> queryAllPartitions(Function<Integer, KeyValueIterator<Bytes, byte[]>> queryFn, boolean forward) {
        List<Integer> list = forward ? partitions : reversePartitions;
        Iterator<KeyValueIterator<Bytes, byte[]>> keyValueIteratorIterator = list.parallelStream()
                .map(queryFn).iterator();
        return new CompositeKeyValueIterator<>(keyValueIteratorIterator,
                keySerde.deserializer(),
                valueSerde.deserializer());
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<K, V> prefixScan(P prefix, PS prefixKeySerializer) {
        final Bytes from = Bytes.wrap(prefixKeySerializer.serialize(null, prefix));
        final Bytes to = Bytes.increment(from);

        return queryAllPartitions(
                partition -> repo.getForRange(partition,
                        from,
                        to,
                        true,
                        false),
                true);
    }

    @Override
    public long approximateNumEntries() {
        if (isCountAllEnabled) {
            return repo.getCount();
        } else {
            LOG.warn("Store count is disabled and always returns '-1', enable via CassandraStores#withCountAllEnabled()");
            return -1;
        }
    }
}
