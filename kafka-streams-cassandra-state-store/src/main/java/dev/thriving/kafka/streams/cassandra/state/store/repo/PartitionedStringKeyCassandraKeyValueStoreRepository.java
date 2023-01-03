package dev.thriving.kafka.streams.cassandra.state.store.repo;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import dev.thriving.kafka.streams.cassandra.state.store.CassandraKeyValueIterator;
import dev.thriving.kafka.streams.cassandra.state.store.serde.KeySerdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

public class PartitionedStringKeyCassandraKeyValueStoreRepository extends AbstractPartitionedCassandraKeyValueStoreRepository<String> {

    private PreparedStatement selectByPartitionAndKeyPrefix;

    public PartitionedStringKeyCassandraKeyValueStoreRepository(CqlSession session, String tableName, String compactionStrategy, Long defaultTtlSeconds) {
        super(session,
                tableName,
                compactionStrategy,
                defaultTtlSeconds,
                KeySerdes.String(),
                row -> KeySerdes.String().deserialize(row.getString(0)));
    }

    @Override
    protected void createTable(String tableName, String compactionStrategy, Long defaultTtlSeconds) {
        session.execute("""
           CREATE TABLE IF NOT EXISTS %s (
               partition int,
               key text,
               time timestamp,
               value blob,
               PRIMARY KEY ((partition), key)
           ) WITH compaction = { 'class' : '%s' }
             AND  default_time_to_live = %d
           """.formatted(tableName, compactionStrategy, defaultTtlSeconds));
    }

    @Override
    protected void initPreparedStatements(String tableName) {
        super.initPreparedStatements(tableName);
        selectByPartitionAndKeyPrefix = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key LIKE ?");
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> findByPartitionAndKeyPrefix(int partition, String prefix) {
        BoundStatement prepared = selectByPartitionAndKeyPrefix.bind(partition, prefix);
        ResultSet rs = session.execute(prepared);
        return new CassandraKeyValueIterator(rs.iterator(), extractKeyFn);
    }
}
