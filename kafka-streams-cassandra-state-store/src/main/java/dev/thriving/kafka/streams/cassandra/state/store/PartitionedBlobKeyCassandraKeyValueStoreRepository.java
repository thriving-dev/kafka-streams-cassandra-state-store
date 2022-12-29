package dev.thriving.kafka.streams.cassandra.state.store;

import com.datastax.oss.driver.api.core.CqlSession;

import java.nio.ByteBuffer;

class PartitionedBlobKeyCassandraKeyValueStoreRepository extends AbstractPartitionedCassandraKeyValueStoreRepository<ByteBuffer> {

    public PartitionedBlobKeyCassandraKeyValueStoreRepository(CqlSession session, String tableName, Long defaultTtlSeconds) {
        super(session, tableName, defaultTtlSeconds, KeySerdes.ByteBuffer(), row -> KeySerdes.ByteBuffer().deserialize(row.getByteBuffer(0)));
    }

    protected void createTable(String tableName, Long defaultTtlSeconds) {
        StringBuilder sb = new StringBuilder()
                .append("CREATE TABLE IF NOT EXISTS ")
                .append(tableName).append(" (")
                .append("partition int, ")
                .append("key blob, ")
                .append("time timestamp, ")
                .append("value blob, ")
                .append("PRIMARY KEY ((partition), key)")
                .append(") WITH compaction = { 'class' : 'LeveledCompactionStrategy' } ");

        if (defaultTtlSeconds != null && defaultTtlSeconds > 0) {
            sb.append("AND default_time_to_live = ").append(defaultTtlSeconds).append(" ");
        }

        session.execute(sb.toString());
    }
}
