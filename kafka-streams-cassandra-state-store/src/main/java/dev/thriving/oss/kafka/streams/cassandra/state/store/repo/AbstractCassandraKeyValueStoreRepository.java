package dev.thriving.oss.kafka.streams.cassandra.state.store.repo;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public abstract class AbstractCassandraKeyValueStoreRepository<K> implements CassandraKeyValueStoreRepository {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCassandraKeyValueStoreRepository.class);

    protected final CqlSession session;

    public AbstractCassandraKeyValueStoreRepository(CqlSession session, String tableName, String tableOptions) {
        assert session != null : "session cannot be null";
        assert tableName != null && !tableName.isBlank() : "tableName cannot be null or blank";
        assert tableOptions != null : "tableOptions cannot be null";

        this.session = session;

        createTable(tableName, tableOptions);
        initPreparedStatements(tableName);
    }

    protected abstract void createTable(String tableName, String tableOptions);

    protected abstract void initPreparedStatements(String tableName);

    protected long executeSelectCount(BoundStatement prepared, ResultSet execute) {
        try {
            prepared = prepared.setTimeout(Duration.ofSeconds(5)); // TODO: should this be configurable? higher/lower?
            ResultSet rs = execute;
            Row result = rs.one();
            if (result == null) {
                LOG.error("`SELECT COUNT(*)` did not return any results, this should never happen.");
                return -1;
            }
            return result.getLong(0);
        } catch (DriverTimeoutException ex) {
            LOG.error("`SELECT COUNT(*)` query timed out. Your table is too large, using `store.approximateNumEntries()` is discouraged!");
            return -1;
        }
    }
}
