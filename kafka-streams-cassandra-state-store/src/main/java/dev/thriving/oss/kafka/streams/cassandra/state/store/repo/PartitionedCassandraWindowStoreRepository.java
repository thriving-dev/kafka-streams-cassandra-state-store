package dev.thriving.oss.kafka.streams.cassandra.state.store.repo;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import dev.thriving.oss.kafka.streams.cassandra.state.store.CassandraWindowStoreIteratorProvider;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.nio.ByteBuffer;
import java.time.Instant;

public class PartitionedCassandraWindowStoreRepository extends AbstractCassandraKeyValueStoreRepository implements CassandraWindowStoreRepository {

    protected PreparedStatement insert;
    protected PreparedStatement delete;
    protected PreparedStatement select;
    protected PreparedStatement selectByPartitionAndKeyAndStartTimeRangeAsc;

    public PartitionedCassandraWindowStoreRepository(CqlSession session, String tableName, String tableOptions) {
        super(session, tableName, tableOptions);
    }

    @Override
    protected void createTable(String tableName, String tableOptions) {
        PreparedStatement prepare = session.prepare("""
                CREATE TABLE IF NOT EXISTS %s (
                    partition int,
                    start_time bigint,
                    key blob,
                    seq_num bigint,
                    time timestamp,
                    value blob,
                    PRIMARY KEY ((partition, start_time), key, seq_num)
                ) %s
                """.formatted(tableName, tableOptions.isBlank() ? "" : "WITH " + tableOptions));
        session.execute(prepare.bind());
    }

    @Override
    protected void initPreparedStatements(String tableName) {
        insert = session.prepare("INSERT INTO " + tableName + " (partition, start_time, key, seq_num, time, value) VALUES (?, ?, ?, ?, ?, ?)");
        delete = session.prepare("DELETE FROM " + tableName + " WHERE partition=? AND start_time=? AND key=? AND seq_num=?");
        select = session.prepare("SELECT value FROM " + tableName + " WHERE partition=? AND start_time=? AND key=? AND seq_num=?");

        selectByPartitionAndKeyAndStartTimeRangeAsc = session.prepare("SELECT key, value, start_time, time FROM " + tableName + " WHERE partition=? AND start_time>=? AND start_time<=? AND key=? ALLOW FILTERING");
    }

    @Override
    public void save(int partition, long windowStartTimestamp, Bytes key, byte[] value, long seqnum) {
        BoundStatement prepared = insert.bind(partition, windowStartTimestamp, ByteBuffer.wrap(key.get()), seqnum, Instant.now(), ByteBuffer.wrap(value));
        session.execute(prepared);
    }

    @Override
    public void delete(int partition, long windowStartTimestamp, Bytes key, long seqnum) {
        BoundStatement prepared = delete.bind(partition, windowStartTimestamp, key, seqnum);
        session.execute(prepared);
    }

    @Override
    public CassandraWindowStoreIteratorProvider fetch(int partition, Bytes key, long timeFrom, long timeTo, boolean forward, long windowSize) {
        BoundStatement prepared = selectByPartitionAndKeyAndStartTimeRangeAsc.bind(partition, timeFrom, timeTo, ByteBuffer.wrap(key.get()));
        session.execute(prepared);
        ResultSet rs = session.execute(prepared);
        return new CassandraWindowStoreIteratorProvider(rs.iterator(), windowSize);
    }

    @Override
    public CassandraWindowStoreIteratorProvider fetch(int partition, Bytes keyFrom, Bytes keyTo, long timeFrom, long timeTo, boolean forward, long windowSize) {
        // TODO
        return null;
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(int partition, long timeFrom, long timeTo, boolean forward) {
        // TODO
        return null;
    }

    @Override
    public byte[] get(int partition, long windowStartTimestamp, Bytes key, long seqnum) {
        BoundStatement prepared = select.bind(partition, windowStartTimestamp, ByteBuffer.wrap(key.get()), seqnum);
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
    public KeyValueIterator<Windowed<Bytes>, byte[]> getAllWindowsFrom(int partition, long minTime, boolean forward) {
        // TODO
        return null;
    }

    @Override
    public void deleteWindowsOlderThan(int partition, long minLiveTime) {
        // TODO
    }
}
