package dev.thriving.oss.kafka.streams.cassandra.state.store.repo;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.kafka.streams.state.VersionedKeyValueStore.PUT_RETURN_CODE_VALID_TO_UNDEFINED;

public class GlobalCassandraVersionedKeyValueStoreRepository<K>
        extends AbstractCassandraStateStoreRepository<K>
        implements CassandraVersionedKeyValueStoreRepository {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalCassandraVersionedKeyValueStoreRepository.class);

    private PreparedStatement selectByPartitionAndKeyAndValidTo;
    private PreparedStatement selectByPartitionAndKeyAndValidToGTE;
    private PreparedStatement cleanup;
    private PreparedStatement insert;

    public GlobalCassandraVersionedKeyValueStoreRepository(CqlSession session,
                                                           String tableName,
                                                           boolean createTable,
                                                           String tableOptions,
                                                           String ddlExecutionProfile,
                                                           String dmlExecutionProfile) {
        super(session, tableName, createTable, tableOptions, ddlExecutionProfile, dmlExecutionProfile);
    }

    @Override
    protected String buildCreateTableQuery(String tableName, String tableOptions) {
        return """
                CREATE TABLE IF NOT EXISTS %s (
                    key blob,
                    validFrom timestamp,
                    validTo timestamp,
                    time timestamp,
                    value blob,
                    PRIMARY KEY ((key), validTo)
                ) %s
                """.formatted(tableName, tableOptions.isBlank() ? "" : "WITH " + tableOptions);
    }

    @Override
    protected void initPreparedStatements(String tableName) {
        selectByPartitionAndKeyAndValidTo = session.prepare("SELECT value, time, validFrom, validTo FROM " + tableName + " WHERE key=? AND validTo=?");
        selectByPartitionAndKeyAndValidToGTE = session.prepare("SELECT value, time, validFrom, validTo FROM " + tableName + " WHERE key=? AND validTo>? ORDER BY validTo ASC LIMIT 1");
        cleanup = session.prepare("DELETE FROM " + tableName + " WHERE key=? AND validTo<? AND validTo>=0");
        insert = session.prepare("INSERT INTO " + tableName + " (key, validFrom, validTo, time, value) VALUES (?, ?, ?, ?, ?)");
    }

    @Override
    public VersionedEntry get(int partition, Bytes key) {
        BoundStatement stmt = selectByPartitionAndKeyAndValidTo.bind(
                ByteBuffer.wrap(key.get()),
                Instant.ofEpochMilli(PUT_RETURN_CODE_VALID_TO_UNDEFINED));
        stmt = stmt.setExecutionProfileName(ddlExecutionProfile);
        ResultSet rs = session.execute(stmt);
        return toVersionedEntry(rs.one());
    }

    @Override
    public VersionedEntry get(int partition, Bytes key, Instant asOfTimestamp) {
        BoundStatement stmt = selectByPartitionAndKeyAndValidToGTE.bind(
                ByteBuffer.wrap(key.get()),
                asOfTimestamp);
        stmt = stmt.setExecutionProfileName(ddlExecutionProfile);
        ResultSet rs = session.execute(stmt);
        return toVersionedEntry(rs.one());
    }

    // TODO: remember to document that cleanup only happens upon write operations for the actual key
    @Override
    public void cleanup(int partition, Bytes key, Instant observedStreamTime) {
        BoundStatement stmt = cleanup.bind(
                ByteBuffer.wrap(key.get()),
                observedStreamTime);
        stmt = stmt.setExecutionProfileName(ddlExecutionProfile);
        session.execute(stmt);
    }

    @Override
    public void saveInBatch(int partition, Bytes key, List<VersionedEntry> entries) {
        Objects.requireNonNull(entries);
        assert entries.size() >= 1 : "the list of entries to persist must always be >= 1";

        // check if >1 entry
        if (entries.size() == 1) {
            VersionedEntry entry = entries.get(0);
            BoundStatement stmt = insert.bind(
                    ByteBuffer.wrap(key.get()),
                    entry.validFrom(),
                    entry.validTo(),
                    entry.timestamp(),
                    toByteBuffer(entry.value()));
            stmt = stmt.setExecutionProfileName(ddlExecutionProfile);
            session.execute(stmt);
        } else {
            List<BatchableStatement<?>> inserts = new ArrayList<>();
            entries.forEach(it -> {
                inserts.add(
                        insert.bind(
                                ByteBuffer.wrap(key.get()),
                                it.validFrom(),
                                it.validTo(),
                                Instant.now(),
                                toByteBuffer(it.value()))
                );
            });
            BatchStatement batch = BatchStatement.newInstance(DefaultBatchType.LOGGED);
            batch = batch.addAll(inserts);
            if (dmlExecutionProfile != null) {
                batch = batch.setExecutionProfileName(ddlExecutionProfile);
            }
            session.execute(batch);
        }
    }

    private static ByteBuffer toByteBuffer(byte[] value) {
        if (value == null) {
            return null;
        }
        return ByteBuffer.wrap(value);
    }

    private static VersionedEntry toVersionedEntry(Row result) {
        if (result == null) {
            return null;
        } else {
            ByteBuffer byteBuffer = result.getByteBuffer(0);
            return new VersionedEntry(
                    byteBuffer == null ? null : byteBuffer.array(),
                    result.getInstant(1),
                    result.getInstant(2),
                    result.getInstant(3)
            );
        }
    }
}
