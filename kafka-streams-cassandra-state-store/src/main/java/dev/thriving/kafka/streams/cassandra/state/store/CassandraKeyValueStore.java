package dev.thriving.kafka.streams.cassandra.state.store;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
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

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class CassandraKeyValueStore implements KeyValueStore<Bytes, byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraKeyValueStore.class);

    private final String name;
    private final String tableName;
    private CqlSession session;
    private volatile boolean open = false;
    private StateStoreContext context;
    private Position position = Position.emptyPosition();
    private PreparedStatement psInsert;
    private PreparedStatement psSelectByPK;
    private PreparedStatement psSelectByPartition;
    private PreparedStatement psSelectByPartitionReversed;
    private PreparedStatement psSelectByPartitionAndKeyFrom;
    private PreparedStatement psSelectByPartitionAndKeyTo;
    private PreparedStatement psSelectByPartitionAndKeyRange;
    private PreparedStatement psSelectByPartitionAndKeyFromReversed;
    private PreparedStatement psSelectByPartitionAndKeyToReversed;
    private PreparedStatement psSelectByPartitionAndKeyRangeReversed;
    private PreparedStatement psDeleteByPK;

    public CassandraKeyValueStore(CqlSession session, String name) {
        this.session = session;
        this.name = name;
        this.tableName = name.replaceAll("[^a-zA-Z0-9_]", "_") + "_changelog";
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

        // TODO: allow to pass store TTL (table default)
        // create table (?+materialized view) if not exists
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(tableName).append("(")
                .append("partition int, ")
                .append("key BLOB, ")
                .append("time timestamp, ")
                .append("value BLOB, ")
                .append("PRIMARY KEY ((partition), key)")
                .append(") ")
                .append("WITH compaction = { 'class' : 'LeveledCompactionStrategy' } ")
                //.append("AND default_time_to_live = 999 ");
                .append(";");
        session.execute(sb.toString());

        // TODO: if TTL, exec `ALTER TABLE xyz WITH default_time_to_live=999`

        // create prepared statements
        psInsert = session.prepare("INSERT INTO " + tableName + " (partition, key, time, value) VALUES (?, ?, ?, ?)");

        psDeleteByPK = session.prepare("DELETE FROM " + tableName + " WHERE partition=? AND key=?");

        psSelectByPK = session.prepare("SELECT value FROM " + tableName + " WHERE partition=? AND key=?");

        psSelectByPartition = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=?");
        psSelectByPartitionReversed = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? ORDER BY key DESC");

        psSelectByPartitionAndKeyFrom = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key>=?");
        psSelectByPartitionAndKeyTo = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key<=?");
        psSelectByPartitionAndKeyRange = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key>=? AND key<=?");
        psSelectByPartitionAndKeyFromReversed = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key>=? ORDER BY key DESC");
        psSelectByPartitionAndKeyToReversed = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key<=? ORDER BY key DESC");
        psSelectByPartitionAndKeyRangeReversed = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key>=? AND key<=? ORDER BY key DESC");

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
        if (!this.session.isClosed()) {
            this.session.close();
        }
        this.open = false;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void put(Bytes key, byte[] value) {
        LOG.debug("put {}::{}", key, value);
        Objects.requireNonNull(key, "key cannot be null");
        if (value == null) {
            deleteInternal(key);
        } else {
            putInternal(key, value);
        }
    }

    private void putInternal(Bytes key, byte[] value) {
        LOG.debug("putInternal {}::{}", key, value);
        BoundStatement prepared = psInsert.bind(context.taskId().partition(), ByteBuffer.wrap(key.get()), Instant.now(), ByteBuffer.wrap(value));
        session.execute(prepared);
        StoreQueryUtils.updatePosition(position, context);
    }

    @Override
    public byte[] putIfAbsent(Bytes key, byte[] value) {
        LOG.debug("putIfAbsent {}::{}", key, value);
        Objects.requireNonNull(key, "key cannot be null");
        final byte[] originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    @Override
    public void putAll(List<KeyValue<Bytes, byte[]>> entries) {
        BatchStatement batch = BatchStatement.newInstance(DefaultBatchType.LOGGED);

        // iterate
        entries.forEach(it -> batch.add(psInsert.bind(context.taskId().partition(), ByteBuffer.wrap(it.key.get()), Instant.now(), ByteBuffer.wrap(it.value))));

        session.execute(batch);
    }

    @Override
    public byte[] delete(Bytes key) {
        LOG.debug("delete {}", key);
        Objects.requireNonNull(key, "key cannot be null");
        final byte[] originalValue = get(key);
        deleteInternal(key);
        return originalValue;
    }

    private void deleteInternal(Bytes key) {
        LOG.debug("deleteInternal {}", key);
        BoundStatement prepared = psDeleteByPK.bind(context.taskId().partition(), ByteBuffer.wrap(key.get()));
        session.execute(prepared);
    }

    @Override
    public byte[] get(Bytes key) {
        LOG.debug("get {}", key);
        Objects.requireNonNull(key, "key cannot be null");
        BoundStatement prepared = psSelectByPK.bind(context.taskId().partition(), ByteBuffer.wrap(key.get()));
        ResultSet rs = session.execute(prepared);
        Row result = rs.one();
        if (result == null) {
            return null;
        } else {
            ByteBuffer byteBuffer = result.getByteBuffer(0);
            return byteBuffer == null ? null : byteBuffer.array();
        }
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
    public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
        if (from == null && to == null) {
            throw new NullPointerException("'from' & 'to' params can not both be NULL");
        } else if (to == null) {
            BoundStatement prepared = psSelectByPartitionAndKeyFrom.bind(context.taskId().partition(), ByteBuffer.wrap(from.get()));
            ResultSet rs = session.execute(prepared);
            return new CassandraKeyValueIterator(rs);
        } else if (from == null) {
            BoundStatement prepared = psSelectByPartitionAndKeyTo.bind(context.taskId().partition(), ByteBuffer.wrap(to.get()));
            ResultSet rs = session.execute(prepared);
            return new CassandraKeyValueIterator(rs);
        } else {
            BoundStatement prepared = psSelectByPartitionAndKeyRange.bind(context.taskId().partition(), ByteBuffer.wrap(from.get()), ByteBuffer.wrap(to.get()));
            ResultSet rs = session.execute(prepared);
            return new CassandraKeyValueIterator(rs);
        }
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(Bytes from, Bytes to) {
        if (from == null && to == null) {
            throw new NullPointerException("'from' & 'to' params can not both be NULL");
        } else if (to == null) {
            BoundStatement prepared = psSelectByPartitionAndKeyFromReversed.bind(context.taskId().partition(), ByteBuffer.wrap(from.get()));
            ResultSet rs = session.execute(prepared);
            return new CassandraKeyValueIterator(rs);
        } else if (from == null) {
            BoundStatement prepared = psSelectByPartitionAndKeyToReversed.bind(context.taskId().partition(), ByteBuffer.wrap(to.get()));
            ResultSet rs = session.execute(prepared);
            return new CassandraKeyValueIterator(rs);
        } else {
            BoundStatement prepared = psSelectByPartitionAndKeyRangeReversed.bind(context.taskId().partition(), ByteBuffer.wrap(from.get()), ByteBuffer.wrap(to.get()));
            ResultSet rs = session.execute(prepared);
            return new CassandraKeyValueIterator(rs);
        }
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        BoundStatement prepared = psSelectByPartition.bind(context.taskId().partition());
        ResultSet rs = session.execute(prepared);
        return new CassandraKeyValueIterator(rs);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseAll() {
        BoundStatement prepared = psSelectByPartitionReversed.bind(context.taskId().partition());
        ResultSet rs = session.execute(prepared);
        return new CassandraKeyValueIterator(rs);
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(P prefix, PS prefixKeySerializer) {
        // would be possible for String keys (cql LIKE '{prefix}%'), but not as generic impl
        // maybe add special child class CassandraStringKeyValueStore
        // only alternative would be to use all() and filter
        throw new UnsupportedOperationException();
    }

    @Override
    public long approximateNumEntries() {
        // note: SELECT COUNT(*) requires significant CPU and I/O resources and may be quite slow depending on store size...
        throw new UnsupportedOperationException();
    }

    private class CassandraKeyValueIterator implements KeyValueIterator<Bytes, byte[]> {
        private final Iterator<Row> iter;

        private CassandraKeyValueIterator(ResultSet rs) {
            this.iter = rs.iterator();
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            Row row = iter.next();
            byte[] key = row.getByteBuffer(0).array();
            byte[] value = row.getByteBuffer(1).array();
            return KeyValue.pair(Bytes.wrap(key), value);
        }

        @Override
        public void close() {
            // do nothing
        }

        @Override
        public Bytes peekNextKey() {
            throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
        }
    }
}
