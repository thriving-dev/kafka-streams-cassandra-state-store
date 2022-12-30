package dev.thriving.kafka.streams.cassandra.state.store;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.thriving.kafka.streams.cassandra.state.store.repo.CassandraKeyValueStoreRepository;
import dev.thriving.kafka.streams.cassandra.state.store.repo.GlobalBlobKeyCassandraKeyValueStoreRepository;
import dev.thriving.kafka.streams.cassandra.state.store.repo.PartitionedBlobKeyCassandraKeyValueStoreRepository;
import dev.thriving.kafka.streams.cassandra.state.store.repo.PartitionedStringKeyCassandraKeyValueStoreRepository;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public final class CassandraStores {

    public static KeyValueBytesStoreSupplier cassandraKeyValueStore(final CqlSession session, final String name) {
        return cassandraKeyValueStore(session, name, CassandraKeyValueStoreRepository.NO_TTL);
    }

    public static KeyValueBytesStoreSupplier cassandraKeyValueStore(final CqlSession session, final String name, final long defaultTtlSeconds) {
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
                                getTableName(name),
                                defaultTtlSeconds));
            }

            @Override
            public String metricsScope() {
                return "cassandra";
            }
        };
    }

    public static KeyValueBytesStoreSupplier cassandraStringKeyValueStore(final CqlSession session, final String name) {
        return cassandraStringKeyValueStore(session, name, CassandraKeyValueStoreRepository.NO_TTL);
    }

    public static KeyValueBytesStoreSupplier cassandraStringKeyValueStore(final CqlSession session, final String name, final long defaultTtlSeconds) {
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
                                getTableName(name),
                                defaultTtlSeconds));
            }

            @Override
            public String metricsScope() {
                return "cassandra";
            }
        };
    }

    public static KeyValueBytesStoreSupplier globalCassandraKeyValueStore(final CqlSession session, final String name) {
        return globalCassandraKeyValueStore(session, name, CassandraKeyValueStoreRepository.NO_TTL);
    }

    public static KeyValueBytesStoreSupplier globalCassandraKeyValueStore(final CqlSession session, final String name, final long defaultTtlSeconds) {
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
                                getTableName(name),
                                defaultTtlSeconds));
            }

            @Override
            public String metricsScope() {
                return "cassandra";
            }
        };
    }

    private static String getTableName(String name) {
        return name.replaceAll("[^a-zA-Z0-9_]", "_") + "_kstreams_store";
    }

}
