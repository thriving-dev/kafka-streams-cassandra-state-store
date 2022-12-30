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

    public PartitionedStringKeyCassandraKeyValueStoreRepository(CqlSession session, String tableName, Long defaultTtlSeconds) {
        super(session,
                tableName,
                defaultTtlSeconds,
                KeySerdes.String(),
                row -> KeySerdes.String().deserialize(row.getString(0)));
    }

    @Override
    protected void createTable(String tableName, Long defaultTtlSeconds) {
        StringBuilder sb = new StringBuilder()
                .append("CREATE TABLE IF NOT EXISTS ")
                .append(tableName).append(" (")
                .append("partition int, ")
                .append("key text, ")
                .append("time timestamp, ")
                .append("value blob, ")
                .append("PRIMARY KEY ((partition), key)")
                .append(") WITH compaction = { 'class' : 'LeveledCompactionStrategy' } ");

        if (defaultTtlSeconds != null && defaultTtlSeconds > 0) {
            sb.append("AND default_time_to_live = ").append(defaultTtlSeconds).append(" ");
        }

        session.execute(sb.toString());
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
