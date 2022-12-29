package dev.thriving.kafka.streams.cassandra.state.store;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;

class GlobalBlobKeyCassandraKeyValueStoreRepository extends AbstractCassandraKeyValueStoreRepository<ByteBuffer> {
    private PreparedStatement insert;
    private PreparedStatement deleteByKey;
    private PreparedStatement selectByKey;
    private PreparedStatement selectAll;

    public GlobalBlobKeyCassandraKeyValueStoreRepository(CqlSession session, String tableName, Long defaultTtlSeconds) {
        super(session, tableName, defaultTtlSeconds, KeySerdes.ByteBuffer(), row -> KeySerdes.ByteBuffer().deserialize(row.getByteBuffer(0)));
    }

    @Override
    protected void createTable(String tableName, Long defaultTtlSeconds) {
        StringBuilder sb = new StringBuilder()
                .append("CREATE TABLE IF NOT EXISTS ")
                .append(tableName).append(" (")
                .append("key blob, ")
                .append("time timestamp, ")
                .append("value blob, ")
                .append("PRIMARY KEY (key)")
                .append(") WITH compaction = { 'class' : 'LeveledCompactionStrategy' } ");

        if (defaultTtlSeconds != null && defaultTtlSeconds > 0) {
            sb.append("AND default_time_to_live = ").append(defaultTtlSeconds).append(" ");
        }

        session.execute(sb.toString());
    }

    @Override
    protected void initPreparedStatements(String tableName) {
        insert = session.prepare("INSERT INTO " + tableName + " (key, time, value) VALUES (?, ?, ?)");
        deleteByKey = session.prepare("DELETE FROM " + tableName + " WHERE key=?");
        selectByKey = session.prepare("SELECT value FROM " + tableName + " WHERE key=?");
        selectAll = session.prepare("SELECT key, value FROM " + tableName);
    }

    @Override
    public byte[] getByKey(int partition, Bytes key) {
        BoundStatement prepared = selectByKey.bind(keySerde.serialize(key));
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
        BoundStatement prepared = insert.bind(keySerde.serialize(key), Instant.now(), ByteBuffer.wrap(value));
        session.execute(prepared);
    }

    @Override
    public void saveBatch(int partition, List<KeyValue<Bytes, byte[]>> entries) {
        BatchStatement batch = BatchStatement.newInstance(DefaultBatchType.LOGGED);
        entries.forEach(it -> batch.add(insert.bind(keySerde.serialize(it.key), Instant.now(), ByteBuffer.wrap(it.value))));
        session.execute(batch);
    }

    @Override
    public void delete(int partition, Bytes key) {
        BoundStatement prepared = deleteByKey.bind(keySerde.serialize(key));
        session.execute(prepared);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> getAll(int partition) {
        BoundStatement prepared = selectAll.bind();
        ResultSet rs = session.execute(prepared);
        return new CassandraKeyValueIterator(rs.iterator(), extractKeyFn);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> getAllDesc(int partition) {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(int partition, Bytes from, Bytes to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> rangeDesc(int partition, Bytes from, Bytes to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> findByPartitionAndKeyPrefix(int partition, String prefix) {
        throw new UnsupportedOperationException();
    }
}
