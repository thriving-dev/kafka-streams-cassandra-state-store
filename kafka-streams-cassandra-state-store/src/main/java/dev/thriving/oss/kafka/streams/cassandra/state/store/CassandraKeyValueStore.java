package dev.thriving.oss.kafka.streams.cassandra.state.store;

import dev.thriving.oss.kafka.streams.cassandra.state.store.repo.CassandraKeyValueStoreRepository;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.StoreQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public class CassandraKeyValueStore extends AbstractCassandraStore implements KeyValueStore<Bytes, byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraKeyValueStore.class);

    private final CassandraKeyValueStoreRepository repo;
    private final boolean isCountAllEnabled;

    public CassandraKeyValueStore(String name, CassandraKeyValueStoreRepository repo, boolean isCountAllEnabled) {
        super(name);
        this.repo = repo;
        this.isCountAllEnabled = isCountAllEnabled;
    }

    @Override
    public <R> QueryResult<R> query(Query<R> query, PositionBound positionBound, QueryConfig config) {
        return StoreQueryUtils.handleBasicQueries(
                query,
                positionBound,
                config,
                this,
                position,
                context
        );
    }

    @Override
    public long approximateNumEntries() {
        if (isCountAllEnabled) {
            return repo.getCount(partition);
        } else {
            LOG.warn("Store count is disabled and always returns '-1', enable via CassandraStores#withCountAllEnabled()");
            return -1;
        }
    }

    @Override
    public void put(Bytes key, byte[] value) {
        LOG.trace("put {}::{}", key, value);
        Objects.requireNonNull(key, "key cannot be null");
        if (value == null) {
            deleteInternal(key);
        } else {
            putInternal(key, value);
        }
    }

    private void putInternal(Bytes key, byte[] value) {
        LOG.trace("putInternal {}::{}", key, value);
        repo.save(partition, key, value);
        StoreQueryUtils.updatePosition(position, context);
    }

    @Override
    public byte[] putIfAbsent(Bytes key, byte[] value) {
        LOG.trace("putIfAbsent {}::{}", key, value);
        Objects.requireNonNull(key, "key cannot be null");
        final byte[] originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    @Override
    public void putAll(List<KeyValue<Bytes, byte[]>> entries) {
        LOG.trace("putAll {}", entries);
        repo.saveBatch(partition, entries);
    }

    @Override
    public byte[] delete(Bytes key) {
        LOG.trace("delete {}", key);
        Objects.requireNonNull(key, "key cannot be null");
        final byte[] originalValue = get(key);
        deleteInternal(key);
        return originalValue;
    }

    private void deleteInternal(Bytes key) {
        LOG.trace("deleteInternal {}", key);
        repo.delete(partition, key);
    }

    @Override
    public byte[] get(Bytes key) {
        LOG.trace("get {}", key);
        Objects.requireNonNull(key, "key cannot be null");
        return repo.getByKey(partition, key);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
        return repo.getForRange(partition, from, to, true, true);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(Bytes from, Bytes to) {
        return repo.getForRange(partition, from, to, false, true);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        return repo.getAll(partition, true);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseAll() {
        return repo.getAll(partition, false);
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(P prefix, PS prefixKeySerializer) {
        final Bytes from = Bytes.wrap(prefixKeySerializer.serialize(null, prefix));
        final Bytes to = Bytes.increment(from);

        return repo.getForRange(partition, from, to, true, false);
    }
}
