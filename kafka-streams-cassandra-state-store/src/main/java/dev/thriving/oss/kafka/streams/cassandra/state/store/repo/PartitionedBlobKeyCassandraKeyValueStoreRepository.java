package dev.thriving.oss.kafka.streams.cassandra.state.store.repo;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import dev.thriving.oss.kafka.streams.cassandra.state.store.repo.serde.KeySerdes;

import java.nio.ByteBuffer;

public class PartitionedBlobKeyCassandraKeyValueStoreRepository extends AbstractPartitionedCassandraKeyValueStoreRepository<ByteBuffer> {

    public PartitionedBlobKeyCassandraKeyValueStoreRepository(CqlSession session, String tableName, String tableOptions) {
        super(session,
                tableName,
                tableOptions,
                KeySerdes.ByteBuffer(),
                row -> KeySerdes.ByteBuffer().deserialize(row.getByteBuffer(0)));
    }

    @Override
    protected void createTable(String tableName, String tableOptions) {
        PreparedStatement prepare = session.prepare("""
           CREATE TABLE IF NOT EXISTS %s (
               partition int,
               key blob,
               time timestamp,
               value blob,
               PRIMARY KEY ((partition), key)
           ) %s
           """.formatted(tableName, tableOptions.isBlank() ? "" : "WITH " + tableOptions));
        session.execute(prepare.bind());
    }
}
