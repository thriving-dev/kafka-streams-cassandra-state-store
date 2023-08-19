package dev.thriving.oss.kafka.streams.cassandra.state.store;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.thriving.oss.kafka.streams.cassandra.state.store.repo.GlobalCassandraKeyValueStoreRepository;
import dev.thriving.oss.kafka.streams.cassandra.state.store.repo.PartitionedCassandraKeyValueStoreRepository;
import dev.thriving.oss.kafka.streams.cassandra.state.store.repo.PartitionedCassandraVersionedKeyValueStoreRepository;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.*;
import org.apache.kafka.streams.state.internals.VersionedKeyValueToBytesStoreAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

import static dev.thriving.oss.kafka.streams.cassandra.state.store.CassandraStateStore.DEFAULT_TABLE_NAME_FN;

/**
 * Factory for creating cassandra backed state stores in Kafka Streams.
 * <p>
 * When using the high-level DSL, i.e., {@link org.apache.kafka.streams.StreamsBuilder StreamsBuilder}, users create
 * {@link StoreSupplier}s that can be further customized via
 * {@link org.apache.kafka.streams.kstream.Materialized Materialized}.
 * For example, a topic read as {@link org.apache.kafka.streams.kstream.KTable KTable} can be materialized into a
 * cassandra k/v store with custom key/value serdes, with logging and caching disabled:
 * <pre>{@code
 * StreamsBuilder builder = new StreamsBuilder();
 * KTable<Long,String> table = builder.table(
 *   "topicName",
 *   Materialized.<Long,String>as(
 *                  CassandraStores.builder(session, "store-name")
 *                          .partitionedKeyValueStore()
 *               )
 *               .withKeySerde(Serdes.Long())
 *               .withValueSerde(Serdes.String())
 *               .withLoggingDisabled()
 *               .withCachingDisabled());
 * }</pre>
 * When using the Processor API, i.e., {@link org.apache.kafka.streams.Topology Topology}, users create
 * {@link StoreBuilder}s that can be attached to {@link org.apache.kafka.streams.processor.api.Processor Processor}s.
 * For example, you can create a cassandra stringKey value store with custom key/value serdes, logging and caching
 * disabled like:
 * <pre>{@code
 * Topology topology = new Topology();
 *
 * StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(
 *                 CassandraStores.builder(session, "store-name")
 *                         .partitionedKeyValueStore(),
 *                 Serdes.String(),
 *                 Serdes.Long())
 *         .withLoggingDisabled()
 *         .withCachingDisabled();
 *
 * topology.addStateStore(storeBuilder);
 * }</pre>
 */
public final class CassandraStores {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraStores.class);
    private static final String METRICS_SCOPE = "cassandra";

    private final String name;
    private final CqlSession session;
    private String keyspace = null;
    private String tableOptions = """
            compaction = { 'class' : 'LeveledCompactionStrategy' }
            """;

    private Function<String, String> tableNameFn = DEFAULT_TABLE_NAME_FN;
    private boolean isCountAllEnabled = false;
    private boolean createTable = true;
    private String ddlExecutionProfile = null;
    private String dmlExecutionProfile = null;

    private CassandraStores(String name, CqlSession session) {
        this.name = name;
        this.session = session;
    }

    /**
     * Create a builder with cassandra @{@link CqlSession} and store name provided as mandatory parameters.
     * <p>
     * This builder allows customizing optional configuration via 'wither' methods:
     * - {@link #withKeyspace(String)}
     * - {@link #withTableOptions(String)}
     * - {@link #withTableNameFn(Function)}
     * <p>
     * With the builder configured, you can create different implementation of {@link KeyValueBytesStoreSupplier} via:
     * - {@link #partitionedKeyValueStore()}
     * - {@link #globalKeyValueStore()}
     * <p>
     * <b>!Important: Always disable logging + caching (by default kafka streams is buffering writes)
     * via {@link StoreSupplier} {@link StoreBuilder}.</b>
     * For this, always apply to the respective storeSupplier / storeBuilder:
     * <pre>{@code
     *   .withLoggingDisabled()
     *   .withCachingDisabled()
     * }</pre>
     * See {@link CassandraStores} class level JavaDoc for full example.
     * <p>
     * The store supplier ultimately created via this builder can be passed into a {@link StoreBuilder}
     * or {@link org.apache.kafka.streams.kstream.Materialized}.
     *
     * @param session cassandra session to be used by the store (cannot be {@code null})
     * @param name    name of the store (cannot be {@code null})
     * @return an instance of {@link CassandraStores} that can be used to build a {@link KeyValueBytesStoreSupplier}
     */
    public static CassandraStores builder(final CqlSession session, final String name) {
        Objects.requireNonNull(session, "session cannot be null");
        Objects.requireNonNull(name, "name cannot be null");
        return new CassandraStores(name, session);
    }

    /**
     * The keyspace for the state store to operate in.
     * <p>
     * By default, the CqlSession session-keyspace is used.
     *
     * @param keyspace state store specific keyspace (cannot be {@code null})
     * @return itself
     */
    public CassandraStores withKeyspace(String keyspace) {
        assert keyspace != null && !keyspace.isBlank() : "keyspace cannot be null or blank";
        this.keyspace = keyspace;
        return this;
    }

    /**
     * A CQL table has a number of options that can be set at creation.
     * <p>
     * Please omit `WITH ` prefix.
     * Multiple options can be added using `AND`, e.g. 'table_option1 AND table_option2'.
     * <p>
     * Recommended compaction strategy is 'LeveledCompactionStrategy' which is applied by default.
     * -> Do not forget to add when overwriting table options.
     * <p>
     * Please refer to table options of your cassandra cluster.
     * - <a href="https://cassandra.apache.org/doc/latest/cassandra/cql/ddl.html#create-table-options">Cassandra 4</a>
     * - <a href="https://docs.scylladb.com/stable/cql/ddl.html#table-options">ScyllaDB</a>
     * <p>
     * Please note this config will only apply upon initial table creation. ('ALTER TABLE' is not yet supported).
     * <p>
     * Default: "compaction = { 'class' : 'LeveledCompactionStrategy' }"
     *
     * @param tableOptions cql table options which will be added to CREATE TABLE statement (cannot be {@code null})
     * @return itself
     */
    public CassandraStores withTableOptions(String tableOptions) {
        assert tableOptions != null : "tableOptions cannot be null";
        // remove 'WITH', tableOptions will be prefixed automatically if not blank
        this.tableOptions = tableOptions.replace("WITH", "").trim();
        return this;
    }

    /**
     * Customize how the state store cassandra table is named, based on the kstreams store name.
     * <p>
     * Please note changing the store name for a pre-existing store will result in a new empty table to be created.
     * <p>
     * Default: `${normalisedStoreName}_kstreams_store` - normalise := lowercase, replaces all [^a-z0-9_] with '_'
     * e.g. ("TEXT3.word-count2") -> "text3_word_count2_kstreams_store"
     *
     * @param tableNameFn function to transform 'kstreams store name' -> 'cql table name' (cannot be {@code null})
     * @return itself
     */
    public CassandraStores withTableNameFn(Function<String, String> tableNameFn) {
        assert tableNameFn != null : "tableNameFn cannot be null";
        this.tableNameFn = tableNameFn;
        return this;
    }

    /**
     * Enable (opt-in) the CassandraKeyValueStore to use `SELECT COUNT(*)` when
     * {@link ReadOnlyKeyValueStore#approximateNumEntries() approximateNumEntries} is invoked.
     * <p>
     * Cassandra/CQL does not support getting approximate counts. Exact row count using `SELECT COUNT(*)` requires significant
     * CPU and I/O resources and may be quite slow depending on store size... use with care!!!
     * <p>
     * Disabled by default.
     *
     * @return itself
     */
    public CassandraStores withCountAllEnabled() {
        LOG.warn("Cassandra/CQL does not support getting approximate counts. SELECT COUNT(*) requires significant CPU and I/O resources and may be quite slow depending on store size... use with care!!!");
        this.isCountAllEnabled = true;
        return this;
    }

    /**
     * Disable (opt-out) automatic table creation during store initialization.
     * <p>
     * Enabled by default.
     *
     * @return itself
     */
    public CassandraStores withCreateTableDisabled() {
        this.createTable = false;
        return this;
    }

    /**
     * Set the execution profile to be used by the driver for all DDL (Data Definition Language) queries.
     * <p>
     * Note: Only applies if table creation ({@link CassandraStores#withCreateTableDisabled()}) is enabled (default).
     * If no profile is set - DDL queries are executed with consistency `ALL`.
     * When using a custom profile, it is recommended to also set consistency=ALL
     * (reason: avoid issues with concurrent schema updates)
     * <p>
     * Reference: <a href="https://docs.datastax.com/en/developer/java-driver/4.15/manual/core/configuration/#execution-profiles">...</a>
     * <p>
     * Must be a non-blank String.
     * Set to `null` to disable (basic applies).
     * <p>
     * Default: `null`
     *
     * @param ddlExecutionProfile the driver execution profile to use for DDL queries
     * @return itself
     */
    public CassandraStores withDdlExecutionProfile(String ddlExecutionProfile) {
        assert ddlExecutionProfile == null || !ddlExecutionProfile.isBlank() : "ddlExecutionProfile cannot be blank";
        this.ddlExecutionProfile = ddlExecutionProfile;
        return this;
    }

    /**
     * Set the execution profile to be used by the driver for all DML (Data Manipulation Language) queries.
     * <p>
     * Reference: <a href="https://docs.datastax.com/en/developer/java-driver/4.15/manual/core/configuration/#execution-profiles">...</a>
     * <p>
     * Must be a non-blank String.
     * Set to `null` to disable (basic applies).
     * <p>
     * Default: `null`
     *
     * @param dmlExecutionProfile the driver execution profile to use for DML queries
     * @return itself
     */
    public CassandraStores withDmlExecutionProfile(String dmlExecutionProfile) {
        assert dmlExecutionProfile == null || !dmlExecutionProfile.isBlank() : "dmlExecutionProfile cannot be blank";
        this.dmlExecutionProfile = dmlExecutionProfile;
        return this;
    }


    /**
     * Creates a persistent {@link KeyValueBytesStoreSupplier}.
     * <p>
     * The key value store is persisted in a cassandra table, partitioned by the store context task partition.
     * Therefore, all CRUD operations against this store always are by stream task partition.
     * <p>
     * The store supports Cassandra 3.11, Cassandra 4, ScyllaDB.
     * <p>
     * Supported operations:
     * - put
     * - putIfAbsent
     * - putAll
     * - delete
     * - get
     * - range
     * - reverseRange
     * - all
     * - reverseAll
     * - query
     * Supported with opt-in:
     * - approximateNumEntries
     * Not supported:
     * - prefixScan
     *
     * @return an instance of a {@link KeyValueBytesStoreSupplier} that can be used
     *         to build a persistent key-value store
     */
    public KeyValueBytesStoreSupplier partitionedKeyValueStore() {
        return new KeyValueBytesStoreSupplier() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public KeyValueStore<Bytes, byte[]> get() {
                return new CassandraKeyValueStore(name,
                        new PartitionedCassandraKeyValueStoreRepository<>(
                                session,
                                resolveTableName(),
                                createTable,
                                tableOptions,
                                ddlExecutionProfile,
                                dmlExecutionProfile),
                        isCountAllEnabled);
            }

            @Override
            public String metricsScope() {
                return METRICS_SCOPE;
            }
        };
    }

    /**
     * Creates a persistent {@link KeyValueBytesStoreSupplier}.
     * <p>
     * The key value store is persisted in a cassandra table, having the 'key' as sole PRIMARY KEY.
     * Therefore, all CRUD operations against this store always are "global", partitioned by the key itself.
     * Due to the nature of cassandra tables having a single PK (no clustering key), this store supports only a limited number of operations.
     * <p>
     * This global store should not be used and confused with a Kafka Streams Global Store!
     * It has to be used as a non-global (regular!) streams KeyValue state store - allows to read ({@link org.apache.kafka.streams.state.ReadOnlyKeyValueStore#get(Object)}) entries from any streams context (streams task/thread).
     * context (streams task).
     * <p>
     * Tip: This store type can be useful when exposing state store access via REST API. Each running instance of your app can serve all requests without the need to proxy the request to the right instance having the task (kafka partition) assigned for the key in question.
     * <p>
     * The store supports Cassandra 3.11, Cassandra 4, ScyllaDB.
     * <p>
     * Supported operations:
     * - put
     * - putIfAbsent
     * - putAll
     * - delete
     * - get
     * - all
     * - query
     * Supported with opt-in:
     * - approximateNumEntries
     * Not supported:
     * - reverseAll
     * - range
     * - reverseRange
     * - prefixScan
     *
     * @return an instance of a {@link KeyValueBytesStoreSupplier} that can be used
     *         to build a persistent key-value store
     */
    public KeyValueBytesStoreSupplier globalKeyValueStore() {
        return new KeyValueBytesStoreSupplier() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public KeyValueStore<Bytes, byte[]> get() {
                return new CassandraKeyValueStore(name,
                        new GlobalCassandraKeyValueStoreRepository(
                                session,
                                resolveTableName(),
                                createTable,
                                tableOptions,
                                ddlExecutionProfile,
                                dmlExecutionProfile),
                        isCountAllEnabled);
            }

            @Override
            public String metricsScope() {
                return METRICS_SCOPE;
            }
        };
    }

    /**
     * Creates a persistent {@link VersionedBytesStoreSupplier}.
     * <p>
     * The versioned key value store is persisted in a cassandra table, partitioned by the store context task partition.
     * Therefore, all CRUD operations against this store always are by stream task partition.
     * <p>
     * The store supports Cassandra 3.11, Cassandra 4, ScyllaDB.
     * <p>
     * Supported operations:
     * - put
     * - delete
     * - get (asOfTimestamp)
     * - get (latest)
     *
     * @param historyRetention length of time that old record versions are available for query
     *                         (cannot be negative). If a timestamp bound provided to
     *                         {@link VersionedKeyValueStore#get(Object, long)} is older than this
     *                         specified history retention, then the get operation will not return data.
     *                         This parameter also determines the "grace period" after which
     *                         out-of-order writes will no longer be accepted.
     * @return an instance of a {@link VersionedBytesStoreSupplier} that can be used
     *         to build a persistent versioned key-value store
     */
    public VersionedBytesStoreSupplier partitionedVersionedKeyValueStore(Duration historyRetention) {
        return new VersionedBytesStoreSupplier() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public KeyValueStore<Bytes, byte[]> get() {
                return new VersionedKeyValueToBytesStoreAdapter(
                        new CassandraVersionedKeyValueStore(
                                name,
                                new PartitionedCassandraVersionedKeyValueStoreRepository<>(
                                        session,
                                        resolveTableName(),
                                        createTable,
                                        tableOptions,
                                        ddlExecutionProfile,
                                        dmlExecutionProfile),
                                historyRetention.toMillis()
                        )
                );
            }

            @Override
            public String metricsScope() {
                return METRICS_SCOPE;
            }

            @Override
            public long historyRetentionMs() {
                return historyRetention.toMillis();
            }
        };
    }

    private String resolveTableName() {
        String resolvedTableName = tableNameFn.apply(name);
        return keyspace != null ? keyspace + "." + resolvedTableName : resolvedTableName;
    }

}
