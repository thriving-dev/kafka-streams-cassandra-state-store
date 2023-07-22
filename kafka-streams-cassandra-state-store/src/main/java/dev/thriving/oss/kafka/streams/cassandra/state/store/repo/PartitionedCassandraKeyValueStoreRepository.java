package dev.thriving.oss.kafka.streams.cassandra.state.store.repo;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import dev.thriving.oss.kafka.streams.cassandra.state.store.CassandraKeyValueIterator;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class PartitionedCassandraKeyValueStoreRepository<K> extends AbstractCassandraKeyValueStoreRepository<K> implements CassandraKeyValueStoreRepository {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionedCassandraKeyValueStoreRepository.class);

    private PreparedStatement insert;
    private PreparedStatement selectByPartitionAndKey;
    private PreparedStatement deleteByPartitionAndKey;
    private PreparedStatement selectByPartition;
    private PreparedStatement selectByPartitionReversed;
    private PreparedStatement selectCountAll;
    private PreparedStatement selectCountByPartition;
    private PreparedStatement selectByPartitionAndKeyFrom;
    private PreparedStatement selectByPartitionAndKeyTo;
    private PreparedStatement selectByPartitionAndKeyToInclusive;
    private PreparedStatement selectByPartitionAndKeyRange;
    private PreparedStatement selectByPartitionAndKeyRangeToInclusive;
    private PreparedStatement selectByPartitionAndKeyFromReversed;
    private PreparedStatement selectByPartitionAndKeyToReversed;
    private PreparedStatement selectByPartitionAndKeyToInclusiveReversed;
    private PreparedStatement selectByPartitionAndKeyRangeReversed;
    private PreparedStatement selectByPartitionAndKeyRangeToInclusiveReversed;

    public PartitionedCassandraKeyValueStoreRepository(CqlSession session,
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
                    partition int,
                    key blob,
                    time timestamp,
                    value blob,
                    PRIMARY KEY ((partition), key)
                ) %s
                """.formatted(tableName, tableOptions.isBlank() ? "" : "WITH " + tableOptions);
    }

    protected void initPreparedStatements(String tableName) {
        insert = session.prepare("INSERT INTO " + tableName + " (partition, key, time, value) VALUES (?, ?, ?, ?)");

        deleteByPartitionAndKey = session.prepare("DELETE FROM " + tableName + " WHERE partition=? AND key=?");

        selectByPartitionAndKey = session.prepare("SELECT value FROM " + tableName + " WHERE partition=? AND key=?");

        selectByPartition = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=?");
        selectByPartitionReversed = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? ORDER BY key DESC");
        selectCountAll = session.prepare("SELECT COUNT(*) FROM " + tableName);
        selectCountByPartition = session.prepare("SELECT COUNT(*) FROM " + tableName + " WHERE partition=?");

        selectByPartitionAndKeyFrom = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key>=?");
        selectByPartitionAndKeyTo = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key<?");
        selectByPartitionAndKeyToInclusive = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key<=?");
        selectByPartitionAndKeyRange = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key>=? AND key<?");
        selectByPartitionAndKeyRangeToInclusive = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key>=? AND key<=?");
        selectByPartitionAndKeyFromReversed = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key>=? ORDER BY key DESC");
        selectByPartitionAndKeyToReversed = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key<? ORDER BY key DESC");
        selectByPartitionAndKeyToInclusiveReversed = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key<=? ORDER BY key DESC");
        selectByPartitionAndKeyRangeReversed = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key>=? AND key<? ORDER BY key DESC");
        selectByPartitionAndKeyRangeToInclusiveReversed = session.prepare("SELECT key, value FROM " + tableName + " WHERE partition=? AND key>=? AND key<=? ORDER BY key DESC");
    }

    @Override
    public byte[] getByKey(int partition, Bytes key) {
        BoundStatement stmt = selectByPartitionAndKey.bind(partition, ByteBuffer.wrap(key.get()));
        stmt = stmt.setExecutionProfileName(ddlExecutionProfile);
        ResultSet rs = session.execute(stmt);
        Row result = rs.one();
        if (result == null) {
            return null;
        } else {
            ByteBuffer byteBuffer = result.getByteBuffer(0);
            return byteBuffer == null ? null : byteBuffer.array();
        }
    }

    @Override
    public void save(int partition, Bytes key, byte[] value) {
        BoundStatement stmt = insert.bind(partition, ByteBuffer.wrap(key.get()), Instant.now(), ByteBuffer.wrap(value));
        stmt = stmt.setExecutionProfileName(ddlExecutionProfile);
        session.execute(stmt);
    }

    @Override
    public void saveBatch(int partition, List<KeyValue<Bytes, byte[]>> entries) {
        List<BatchableStatement<?>> inserts = new ArrayList<>();
        entries.forEach(it -> {
            inserts.add(insert.bind(ByteBuffer.wrap(it.key.get()), Instant.now(), ByteBuffer.wrap(it.value)));
        });
        BatchStatement batch = BatchStatement.newInstance(DefaultBatchType.LOGGED);
        batch.addAll(inserts);
        if (dmlExecutionProfile != null) {
            batch = batch.setExecutionProfileName(ddlExecutionProfile);
        }
        session.execute(batch);
    }

    @Override
    public void delete(int partition, Bytes key) {
        BoundStatement stmt = deleteByPartitionAndKey.bind(partition, ByteBuffer.wrap(key.get()));
        stmt = stmt.setExecutionProfileName(ddlExecutionProfile);
        session.execute(stmt);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> getAll(int partition, boolean forward) {
        PreparedStatement statement = forward ? selectByPartition : selectByPartitionReversed;
        BoundStatement stmt = statement.bind(partition);
        stmt = stmt.setExecutionProfileName(ddlExecutionProfile);
        ResultSet rs = session.execute(stmt);
        return new CassandraKeyValueIterator(rs.iterator());
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> getForRange(int partition, Bytes from, Bytes to, boolean forward, boolean toInclusive) {
        BoundStatement stmt;
        if (from == null && to == null) {
            return getAll(partition, forward);
        } else if (to == null) {
            PreparedStatement statement = forward ? selectByPartitionAndKeyFrom : selectByPartitionAndKeyFromReversed;
            stmt = statement.bind(partition, ByteBuffer.wrap(from.get()));
        } else if (from == null) {
            PreparedStatement statement = forward ?
                    (toInclusive ? selectByPartitionAndKeyToInclusive : selectByPartitionAndKeyTo) :
                    (toInclusive ? selectByPartitionAndKeyToInclusiveReversed : selectByPartitionAndKeyToReversed);
            stmt = statement.bind(partition, ByteBuffer.wrap(to.get()));
        } else if (from.compareTo(to) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. " +
                    "This may be due to range arguments set in the wrong order, " +
                    "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                    "Note that the built-in numerical serdes do not follow this for negative numbers");
            return CassandraKeyValueIterator.emptyIterator();
        } else {
            PreparedStatement statement = forward ?
                    (toInclusive ? selectByPartitionAndKeyRangeToInclusive : selectByPartitionAndKeyRange) :
                    (toInclusive ? selectByPartitionAndKeyRangeToInclusiveReversed : selectByPartitionAndKeyRangeReversed);
            stmt = statement.bind(partition, ByteBuffer.wrap(from.get()), ByteBuffer.wrap(to.get()));
        }
        stmt = stmt.setExecutionProfileName(ddlExecutionProfile);
        ResultSet rs = session.execute(stmt);
        return new CassandraKeyValueIterator(rs.iterator());
    }

    @Override
    public long getCount() {
        BoundStatement stmt = selectCountAll.bind();
        stmt = stmt.setExecutionProfileName(ddlExecutionProfile);
        return executeSelectCount(stmt);
    }

    @Override
    public long getCount(int partition) {
        BoundStatement stmt = selectCountByPartition.bind(partition);
        stmt = stmt.setExecutionProfileName(ddlExecutionProfile);
        return executeSelectCount(stmt);
    }

}
