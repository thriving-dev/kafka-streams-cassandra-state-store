package dev.thriving.kafka.streams.cassandra.state.store;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.kafka.common.utils.Bytes;

import java.util.function.Function;

abstract class AbstractCassandraKeyValueStoreRepository<K> implements CassandraKeyValueStoreRepository {
    protected final CqlSession session;
    protected final KeySerde<K> keySerde;
    protected final Function<Row, Bytes> extractKeyFn;

    public AbstractCassandraKeyValueStoreRepository(CqlSession session, String tableName, Long defaultTtlSeconds, KeySerde<K> keySerde, Function<Row, Bytes> extractKeyFn) {
        assert session != null : "session cannot be null";
        assert tableName != null && !tableName.isBlank() : "tableName cannot be null or blank";
        assert defaultTtlSeconds != null && defaultTtlSeconds > 0 : "defaultTtlSeconds cannot be null and must be > 0";
        assert keySerde != null : "keySerde cannot be null";
        assert extractKeyFn != null : "extractKeyFn cannot be null";

        this.session = session;
        this.keySerde = keySerde;
        this.extractKeyFn = extractKeyFn;

        createTable(tableName, defaultTtlSeconds);
        initPreparedStatements(tableName);
    }

    protected abstract void createTable(String tableName, Long defaultTtlSeconds);

    protected abstract void initPreparedStatements(String tableName);
}
