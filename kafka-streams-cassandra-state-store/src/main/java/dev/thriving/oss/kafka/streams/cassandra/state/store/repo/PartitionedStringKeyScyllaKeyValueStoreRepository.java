package dev.thriving.oss.kafka.streams.cassandra.state.store.repo;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import dev.thriving.oss.kafka.streams.cassandra.state.store.repo.serde.KeySerdes;
import dev.thriving.oss.kafka.streams.cassandra.state.store.CassandraKeyValueIterator;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Arrays;

public class PartitionedStringKeyScyllaKeyValueStoreRepository extends AbstractPartitionedCassandraKeyValueStoreRepository<String> {

    private PreparedStatement selectByPartitionAndKeyPrefix;

    public PartitionedStringKeyScyllaKeyValueStoreRepository(CqlSession session, String tableName, String tableOptions) {
        super(session,
                tableName,
                tableOptions,
                KeySerdes.String(),
                row -> KeySerdes.String().deserialize(row.getString(0)));
    }

    @Override
    protected void createTable(String tableName, String tableOptions) {
        session.execute("""
                CREATE TABLE IF NOT EXISTS %s (
                    partition int,
                    key text,
                    time timestamp,
                    value blob,
                    PRIMARY KEY ((partition), key)
                ) %s
                """.formatted(tableName, tableOptions.isBlank() ? "" : "WITH " + tableOptions));
    }

    @Override
    protected void initPreparedStatements(String tableName) {
        super.initPreparedStatements(tableName);
        selectByPartitionAndKeyPrefix = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key LIKE ?");
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> findByPartitionAndKeyPrefix(int partition, String prefix) {
        BoundStatement prepared = selectByPartitionAndKeyPrefix.bind(partition, prefix + "%");
        ResultSet rs = session.execute(prepared);
        return new CassandraKeyValueIterator(rs.iterator(), extractKeyFn);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(int partition, Bytes from, Bytes to) {
        if (from == null && to == null) {
            return getAll(partition);
        } else if (to == null) {
            BoundStatement prepared = selectByPartitionAndKeyFrom.bind(partition, Arrays.toString(from.get()));
            ResultSet rs = session.execute(prepared);
            return new CassandraKeyValueIterator(rs.iterator(), extractKeyFn);
        } else if (from == null) {
            BoundStatement prepared = selectByPartitionAndKeyTo.bind(partition, Arrays.toString(to.get()));
            ResultSet rs = session.execute(prepared);
            return new CassandraKeyValueIterator(rs.iterator(), extractKeyFn);
        } else {
            BoundStatement prepared = selectByPartitionAndKeyRange.bind(partition, Arrays.toString(from.get()), Arrays.toString(to.get()));
            ResultSet rs = session.execute(prepared);
            return new CassandraKeyValueIterator(rs.iterator(), extractKeyFn);
        }
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> rangeDesc(int partition, Bytes from, Bytes to) {
        if (from == null && to == null) {
            return getAllDesc(partition);
        } else if (to == null) {
            BoundStatement prepared = selectByPartitionAndKeyFromReversed.bind(partition, Arrays.toString(from.get()));
            ResultSet rs = session.execute(prepared);
            return new CassandraKeyValueIterator(rs.iterator(), extractKeyFn);
        } else if (from == null) {
            BoundStatement prepared = selectByPartitionAndKeyToReversed.bind(partition, Arrays.toString(to.get()));
            ResultSet rs = session.execute(prepared);
            return new CassandraKeyValueIterator(rs.iterator(), extractKeyFn);
        } else {
            BoundStatement prepared = selectByPartitionAndKeyRangeReversed.bind(partition, Arrays.toString(from.get()), Arrays.toString(to.get()));
            ResultSet rs = session.execute(prepared);
            return new CassandraKeyValueIterator(rs.iterator(), extractKeyFn);
        }
    }
}
