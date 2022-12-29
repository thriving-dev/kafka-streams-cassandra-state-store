package dev.thriving.kafka.streams.cassandra.state.store;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.query.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.StoreQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public abstract class AbstractCassandraStore implements KeyValueStore<Bytes, byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCassandraStore.class);

    protected final String name;
    private final CassandraKeyValueStoreRepository repo;
    protected StateStoreContext context;
    protected Position position = Position.emptyPosition();
    private volatile boolean open = false;

    public AbstractCassandraStore(String name, CassandraKeyValueStoreRepository repo) {
        this.name = name;
        this.repo = repo;
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        if (context instanceof StateStoreContext) {
            init((StateStoreContext) context, root);
        } else {
            throw new UnsupportedOperationException(
                    "Use CassandraKeyValueStore#init(StateStoreContext, StateStore) instead."
            );
        }
    }

    @Override
    public void init(StateStoreContext context, StateStore root) {
        this.context = context;

        if (root != null) {
            // register the store
            context.register(
                    root,
                    (RecordBatchingStateRestoreCallback) records -> { }
            );
        }

        open = true;
    }

    @Override
    public void close() {
        this.open = false;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void flush() {
        // do-nothing
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public <R> QueryResult<R> query(Query<R> query, PositionBound positionBound, QueryConfig config) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Position getPosition() {
        return position;
    }

    @Override
    public long approximateNumEntries() {
        // note: SELECT COUNT(*) requires significant CPU and I/O resources and may be quite slow depending on store size...
        throw new UnsupportedOperationException();
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
        repo.save(context.taskId().partition(), key, value);
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
        repo.saveBatch(context.taskId().partition(), entries);
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
        repo.delete(context.taskId().partition(), key);
    }

    @Override
    public byte[] get(Bytes key) {
        LOG.trace("get {}", key);
        Objects.requireNonNull(key, "key cannot be null");
        return repo.getByKey(context.taskId().partition(), key);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
        return repo.range(context.taskId().partition(), from, to);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(Bytes from, Bytes to) {
        return repo.rangeDesc(context.taskId().partition(), from, to);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        return repo.getAll(context.taskId().partition());
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseAll() {
        return repo.getAllDesc(context.taskId().partition());
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(P prefix, PS prefixKeySerializer) {
        return repo.findByPartitionAndKeyPrefix(context.taskId().partition(), prefix.toString());
    }
}
