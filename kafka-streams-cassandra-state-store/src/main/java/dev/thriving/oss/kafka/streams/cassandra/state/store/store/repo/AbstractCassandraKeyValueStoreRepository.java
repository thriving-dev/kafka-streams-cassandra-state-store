package dev.thriving.oss.kafka.streams.cassandra.state.store.store.repo;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import dev.thriving.oss.kafka.streams.cassandra.state.store.store.serde.KeySerde;
import org.apache.kafka.common.utils.Bytes;

import java.util.function.Function;

abstract class AbstractCassandraKeyValueStoreRepository<K> implements CassandraKeyValueStoreRepository {
    protected final CqlSession session;
    protected final KeySerde<K> keySerde;
    protected final Function<Row, Bytes> extractKeyFn;

    public AbstractCassandraKeyValueStoreRepository(CqlSession session, String tableName, String compactionStrategy, long defaultTtlSeconds, KeySerde<K> keySerde, Function<Row, Bytes> extractKeyFn) {
        assert session != null : "session cannot be null";
        assert tableName != null && !tableName.isBlank() : "tableName cannot be null or blank";
        assert defaultTtlSeconds >= 0 : "defaultTtlSeconds cannot be null and must be >= 0";
        assert keySerde != null : "keySerde cannot be null";
        assert extractKeyFn != null : "extractKeyFn cannot be null";

        this.session = session;
        this.keySerde = keySerde;
        this.extractKeyFn = extractKeyFn;

        createTable(tableName, compactionStrategy, defaultTtlSeconds);
        initPreparedStatements(tableName);
    }

    protected abstract void createTable(String tableName, String compactionStrategy, Long defaultTtlSeconds);

    protected abstract void initPreparedStatements(String tableName);
}
