package dev.thriving.oss.kafka.streams.cassandra.state.store.store;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.thriving.oss.kafka.streams.cassandra.state.store.store.repo.CassandraKeyValueStoreRepository;
import dev.thriving.oss.kafka.streams.cassandra.state.store.store.repo.GlobalBlobKeyCassandraKeyValueStoreRepository;
import dev.thriving.oss.kafka.streams.cassandra.state.store.store.repo.PartitionedBlobKeyCassandraKeyValueStoreRepository;
import dev.thriving.oss.kafka.streams.cassandra.state.store.store.repo.PartitionedStringKeyCassandraKeyValueStoreRepository;
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

    public KeyValueBytesStoreSupplier keyValueStore() {
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

    public KeyValueBytesStoreSupplier stringKeyValueStore() {
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

    public KeyValueBytesStoreSupplier globalKeyValueStore() {
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
