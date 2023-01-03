package dev.thriving.kafka.streams.cassandra.state.store.repo;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.thriving.kafka.streams.cassandra.state.store.serde.KeySerdes;

import java.nio.ByteBuffer;

public class PartitionedBlobKeyCassandraKeyValueStoreRepository extends AbstractPartitionedCassandraKeyValueStoreRepository<ByteBuffer> {

    public PartitionedBlobKeyCassandraKeyValueStoreRepository(CqlSession session, String tableName, String compactionStrategy, Long defaultTtlSeconds) {
        super(session,
                tableName,
                compactionStrategy,
                defaultTtlSeconds,
                KeySerdes.ByteBuffer(),
                row -> KeySerdes.ByteBuffer().deserialize(row.getByteBuffer(0)));
    }

    @Override
    protected void createTable(String tableName, String compactionStrategy, Long defaultTtlSeconds) {
        session.execute("""
           CREATE TABLE IF NOT EXISTS %s (
               partition int,
               key blob,
               time timestamp,
               value blob,
               PRIMARY KEY ((partition), key)
           ) WITH compaction = { 'class' : '%s' }
             AND  default_time_to_live = %d
           """.formatted(tableName, compactionStrategy, defaultTtlSeconds));
    }
}
