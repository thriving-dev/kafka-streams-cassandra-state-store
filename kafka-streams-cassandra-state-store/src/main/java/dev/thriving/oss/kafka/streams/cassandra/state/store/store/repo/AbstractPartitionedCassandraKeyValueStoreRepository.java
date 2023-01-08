package dev.thriving.oss.kafka.streams.cassandra.state.store.store.repo;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import dev.thriving.oss.kafka.streams.cassandra.state.store.store.CassandraKeyValueIterator;
import dev.thriving.oss.kafka.streams.cassandra.state.store.store.repo.serde.KeySerde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.function.Function;

public abstract class AbstractPartitionedCassandraKeyValueStoreRepository<K> extends AbstractCassandraKeyValueStoreRepository<K> {

    protected PreparedStatement insert;
    protected PreparedStatement selectByPartitionAndKey;
    protected PreparedStatement deleteByPartitionAndKey;
    protected PreparedStatement selectByPartition;
    protected PreparedStatement selectByPartitionReversed;
    protected PreparedStatement selectByPartitionAndKeyFrom;
    protected PreparedStatement selectByPartitionAndKeyTo;
    protected PreparedStatement selectByPartitionAndKeyRange;
    protected PreparedStatement selectByPartitionAndKeyFromReversed;
    protected PreparedStatement selectByPartitionAndKeyToReversed;
    protected PreparedStatement selectByPartitionAndKeyRangeReversed;

    public AbstractPartitionedCassandraKeyValueStoreRepository(CqlSession session, String tableName, String tableOptions, KeySerde<K> keySerde, Function<Row, Bytes> extractKeyFn) {
        super(session, tableName, tableOptions, keySerde, extractKeyFn);
    }

    protected void initPreparedStatements(String tableName) {
        insert = session.prepare("INSERT INTO " + tableName + " (partition, key, time, value) VALUES (?, ?, ?, ?)");

        deleteByPartitionAndKey = session.prepare("DELETE FROM " + tableName + " WHERE partition=? AND key=?");

        selectByPartitionAndKey = session.prepare("SELECT value FROM " + tableName + " WHERE partition=? AND key=?");

        selectByPartition = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=?");
        selectByPartitionReversed = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? ORDER BY key DESC");

        selectByPartitionAndKeyFrom = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key>=?");
        selectByPartitionAndKeyTo = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key<=?");
        selectByPartitionAndKeyRange = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key>=? AND key<=?");
        selectByPartitionAndKeyFromReversed = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key>=? ORDER BY key DESC");
        selectByPartitionAndKeyToReversed = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key<=? ORDER BY key DESC");
        selectByPartitionAndKeyRangeReversed = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key>=? AND key<=? ORDER BY key DESC");
    }

    @Override
    public byte[] getByKey(int partition, Bytes key) {
        BoundStatement prepared = selectByPartitionAndKey.bind(partition, keySerde.serialize(key));
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
    public void save(int partition, Bytes key, byte[] value) {
        BoundStatement prepared = insert.bind(partition, keySerde.serialize(key), Instant.now(), ByteBuffer.wrap(value));
        session.execute(prepared);
    }

    @Override
    public void saveBatch(int partition, List<KeyValue<Bytes, byte[]>> entries) {
        BatchStatement batch = BatchStatement.newInstance(DefaultBatchType.LOGGED);
        entries.forEach(it -> batch.add(insert.bind(partition, keySerde.serialize(it.key), Instant.now(), ByteBuffer.wrap(it.value))));
        session.execute(batch);
    }

    @Override
    public void delete(int partition, Bytes key) {
        BoundStatement prepared = deleteByPartitionAndKey.bind(partition, keySerde.serialize(key));
        session.execute(prepared);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> getAll(int partition) {
        BoundStatement prepared = selectByPartition.bind(partition);
        ResultSet rs = session.execute(prepared);
        return new CassandraKeyValueIterator(rs.iterator(), extractKeyFn);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> getAllDesc(int partition) {
        BoundStatement prepared = selectByPartitionReversed.bind(partition);
        ResultSet rs = session.execute(prepared);
        return new CassandraKeyValueIterator(rs.iterator(), extractKeyFn);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(int partition, Bytes from, Bytes to) {
        if (from == null && to == null) {
            return getAll(partition);
        } else if (to == null) {
            BoundStatement prepared = selectByPartitionAndKeyFrom.bind(partition, ByteBuffer.wrap(from.get()));
            ResultSet rs = session.execute(prepared);
            return new CassandraKeyValueIterator(rs.iterator(), extractKeyFn);
        } else if (from == null) {
            BoundStatement prepared = selectByPartitionAndKeyTo.bind(partition, ByteBuffer.wrap(to.get()));
            ResultSet rs = session.execute(prepared);
            return new CassandraKeyValueIterator(rs.iterator(), extractKeyFn);
        } else {
            BoundStatement prepared = selectByPartitionAndKeyRange.bind(partition, ByteBuffer.wrap(from.get()), ByteBuffer.wrap(to.get()));
            ResultSet rs = session.execute(prepared);
            return new CassandraKeyValueIterator(rs.iterator(), extractKeyFn);
        }
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> rangeDesc(int partition, Bytes from, Bytes to) {
        if (from == null && to == null) {
            return getAllDesc(partition);
        } else if (to == null) {
            BoundStatement prepared = selectByPartitionAndKeyFromReversed.bind(partition, ByteBuffer.wrap(from.get()));
            ResultSet rs = session.execute(prepared);
            return new CassandraKeyValueIterator(rs.iterator(), extractKeyFn);
        } else if (from == null) {
            BoundStatement prepared = selectByPartitionAndKeyToReversed.bind(partition, ByteBuffer.wrap(to.get()));
            ResultSet rs = session.execute(prepared);
            return new CassandraKeyValueIterator(rs.iterator(), extractKeyFn);
        } else {
            BoundStatement prepared = selectByPartitionAndKeyRangeReversed.bind(partition, ByteBuffer.wrap(from.get()), ByteBuffer.wrap(to.get()));
            ResultSet rs = session.execute(prepared);
            return new CassandraKeyValueIterator(rs.iterator(), extractKeyFn);
        }
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> findByPartitionAndKeyPrefix(int partition, String prefix) {
        // note: supported by PartitionedStringKeyCassandraKeyValueStoreRepository
        throw new UnsupportedOperationException();
    }
}
