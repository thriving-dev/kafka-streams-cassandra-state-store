package dev.thriving.kafka.streams.cassandra.state.store;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.thriving.kafka.streams.cassandra.state.store.repo.CassandraKeyValueStoreRepository;
import dev.thriving.kafka.streams.cassandra.state.store.repo.GlobalBlobKeyCassandraKeyValueStoreRepository;
import dev.thriving.kafka.streams.cassandra.state.store.repo.PartitionedBlobKeyCassandraKeyValueStoreRepository;
import dev.thriving.kafka.streams.cassandra.state.store.repo.PartitionedStringKeyCassandraKeyValueStoreRepository;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.function.Function;

public final class CassandraStores {

    private CassandraStores(String name, CqlSession session) {
        this.name = name;
        this.session = session;
    }

    private final String name;
    private final CqlSession session;
    private String compactionStrategy = "LeveledCompactionStrategy";
    private long defaultTtlSeconds = CassandraKeyValueStoreRepository.NO_TTL;
    private Function<String, String> tableNameFn = storeName -> storeName.replaceAll("[^a-zA-Z0-9_]", "_") + "_kstreams_store";

    public CassandraStores withCompactionStrategy(String compactionStrategy) {
        assert compactionStrategy != null && !compactionStrategy.isBlank() : "compactionStrategy cannot be null or blank";
        this.compactionStrategy = compactionStrategy;
        return this;
    }

    public CassandraStores withDefaultTtlSeconds(long defaultTtlSeconds) {
        assert defaultTtlSeconds >= 0 : "defaultTtlSeconds cannot be null and must be >= 0";
        this.defaultTtlSeconds = defaultTtlSeconds;
        return this;
    }

    public CassandraStores withTableNameFn(Function<String, String> tableNameFn) {
        assert tableNameFn != null : "tableNameFn cannot be null";
        this.tableNameFn = tableNameFn;
        return this;
    }

    public static CassandraStores builder(final CqlSession session, final String name) {
        return new CassandraStores(name, session);
    }

    public KeyValueBytesStoreSupplier cassandraKeyValueStore() {
        return new KeyValueBytesStoreSupplier() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public KeyValueStore<Bytes, byte[]> get() {
                return new CassandraKeyValueStore(name,
                        new PartitionedBlobKeyCassandraKeyValueStoreRepository(
                                session,
                                tableNameFn.apply(name),
                                compactionStrategy,
                                defaultTtlSeconds));
            }

            @Override
            public String metricsScope() {
                return "cassandra";
            }
        };
    }

    public KeyValueBytesStoreSupplier cassandraStringKeyValueStore(final CqlSession session, final String name, final long defaultTtlSeconds) {
        return new KeyValueBytesStoreSupplier() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public KeyValueStore<Bytes, byte[]> get() {
                return new CassandraKeyValueStore(name,
                        new PartitionedStringKeyCassandraKeyValueStoreRepository(
                                session,
                                tableNameFn.apply(name),
                                compactionStrategy,
                                defaultTtlSeconds));
            }

            @Override
            public String metricsScope() {
                return "cassandra";
            }
        };
    }

    public KeyValueBytesStoreSupplier globalCassandraKeyValueStore(final CqlSession session, final String name, final long defaultTtlSeconds) {
        return new KeyValueBytesStoreSupplier() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public KeyValueStore<Bytes, byte[]> get() {
                return new CassandraKeyValueStore(name,
                        new GlobalBlobKeyCassandraKeyValueStoreRepository(
                                session,
                                tableNameFn.apply(name),
                                compactionStrategy,
                                defaultTtlSeconds));
            }

            @Override
            public String metricsScope() {
                return "cassandra";
            }
        };
    }

}
