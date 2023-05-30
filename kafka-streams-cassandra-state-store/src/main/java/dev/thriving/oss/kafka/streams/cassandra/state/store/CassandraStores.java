package dev.thriving.oss.kafka.streams.cassandra.state.store;

import com.datastax.oss.driver.api.core.CqlSession;
import dev.thriving.oss.kafka.streams.cassandra.state.store.repo.GlobalCassandraKeyValueStoreRepository;
import dev.thriving.oss.kafka.streams.cassandra.state.store.repo.PartitionedCassandraKeyValueStoreRepository;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Function;

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
 *                          .keyValueStore()
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
 *                         .keyValueStore(),
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

    private final String name;
    private final CqlSession session;
    private String keyspace = null;
    private String tableOptions = """
            compaction = { 'class' : 'LeveledCompactionStrategy' }
            """;

    private Function<String, String> tableNameFn = storeName -> String.format("%s_kstreams_store", storeName.toLowerCase().replaceAll("[^a-z0-9_]", "_"));
    private boolean isCountAllEnabled = false;

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
     * - {@link #keyValueStore()}
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
     * @param name name of the store (cannot be {@code null})
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
     *   e.g. ("TEXT3.word-count2") -> "text3_word_count2_kstreams_store"
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
     * Enable/disable the CassandraKeyValueStore to use `SELECT COUNT(*)` when
     * {@link ReadOnlyKeyValueStore#approximateNumEntries() approximateNumEntries} is invoked.
     * <p>
     * Cassandra/CQL does not support getting approximate counts. Exact row count using `SELECT COUNT(*)` requires significant
     * CPU and I/O resources and may be quite slow depending on store size... use with care!!!
     * <p>
     * Default: false
     *
     * @param enabled enable/disable using cql count for `approximateNumEntries`
     * @return itself
     */
    public CassandraStores withCountAllEnabled(boolean enabled) {
        LOG.warn("Cassandra/CQL does not support getting approximate counts. SELECT COUNT(*) requires significant CPU and I/O resources and may be quite slow depending on store size... use with care!!!");
        this.isCountAllEnabled = enabled;
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
     *      * to build a persistent key-value store
     */
    public KeyValueBytesStoreSupplier keyValueStore() {
        return new KeyValueBytesStoreSupplier() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public KeyValueStore<Bytes, byte[]> get() {
                return new CassandraKeyValueStore(name,
                        new PartitionedCassandraKeyValueStoreRepository(
                                session,
                                resolveTableName(),
                                tableOptions),
                        isCountAllEnabled);
            }

            @Override
            public String metricsScope() {
                return "cassandra";
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
     *      * to build a persistent key-value store
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
                                tableOptions),
                        isCountAllEnabled);
            }

            @Override
            public String metricsScope() {
                return "cassandra";
            }
        };
    }

    private String resolveTableName() {
        String resolvedTableName = tableNameFn.apply(name);
        return keyspace != null ? keyspace + "." + resolvedTableName : resolvedTableName;
    }

}
