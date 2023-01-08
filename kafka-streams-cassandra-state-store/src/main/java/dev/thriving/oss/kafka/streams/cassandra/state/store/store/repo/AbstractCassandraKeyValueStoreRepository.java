package dev.thriving.oss.kafka.streams.cassandra.state.store.store.repo;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import dev.thriving.oss.kafka.streams.cassandra.state.store.store.repo.serde.KeySerde;
import org.apache.kafka.common.utils.Bytes;

import java.util.function.Function;

public abstract class AbstractCassandraKeyValueStoreRepository<K> implements CassandraKeyValueStoreRepository {
    protected final CqlSession session;
    protected final KeySerde<K> keySerde;
    protected final Function<Row, Bytes> extractKeyFn;

    public AbstractCassandraKeyValueStoreRepository(CqlSession session, String tableName, String tableOptions, KeySerde<K> keySerde, Function<Row, Bytes> extractKeyFn) {
        assert session != null : "session cannot be null";
        assert tableName != null && !tableName.isBlank() : "tableName cannot be null or blank";
        assert tableOptions != null : "tableOptions cannot be null";
        assert keySerde != null : "keySerde cannot be null";
        assert extractKeyFn != null : "extractKeyFn cannot be null";

        this.session = session;
        this.keySerde = keySerde;
        this.extractKeyFn = extractKeyFn;

        createTable(tableName, tableOptions);
        initPreparedStatements(tableName);
    }

    protected abstract void createTable(String tableName, String tableOptions);

    protected abstract void initPreparedStatements(String tableName);
}
