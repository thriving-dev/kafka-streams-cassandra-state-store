package dev.thriving.oss.kafka.streams.cassandra.state.store.repo;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractCassandraKeyValueStoreRepository<K> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCassandraKeyValueStoreRepository.class);

    protected final CqlSession session;
    protected final String ddlExecutionProfile;
    protected final String dmlExecutionProfile;

    public AbstractCassandraKeyValueStoreRepository(CqlSession session,
                                                    String tableName,
                                                    boolean createTable,
                                                    String tableOptions,
                                                    String ddlExecutionProfile,
                                                    String dmlExecutionProfile) {
        assert session != null : "session cannot be null";
        assert tableName != null && !tableName.isBlank() : "tableName cannot be null or blank";
        assert tableOptions != null : "tableOptions cannot be null";
        assert ddlExecutionProfile == null || !ddlExecutionProfile.isBlank() : "ddlExecutionProfile cannot be blank";
        assert dmlExecutionProfile == null || !dmlExecutionProfile.isBlank() : "dmlExecutionProfile cannot be blank";

        this.session = session;
        this.ddlExecutionProfile = ddlExecutionProfile;
        this.dmlExecutionProfile = dmlExecutionProfile;

        createTable(tableName, tableOptions, createTable);
        initPreparedStatements(tableName);
    }

    private void createTable(String tableName, String tableOptions, boolean createTable) {
        String query = buildCreateTableQuery(tableName, tableOptions);
        if (createTable) {
            BoundStatement boundStatement = session.prepare(query).bind();
            if (ddlExecutionProfile != null) {
                boundStatement = boundStatement.setExecutionProfileName(ddlExecutionProfile);
            } else {
                LOG.debug("No `ddlExecutionProfile` has been configured, exec CREATE TABLE query with consistency level `ALL`");
                boundStatement = boundStatement.setConsistencyLevel(ConsistencyLevel.ALL);
            }

            LOG.debug("Auto-creating state store table with:\n{}", query);
            session.execute(boundStatement);
        } else {
            LOG.info("Automatic creation of table is disabled, ensure to manually create the state store table with:\n{}", query);
        }
    }

    protected abstract String buildCreateTableQuery(String tableName, String tableOptions);

    protected abstract void initPreparedStatements(String tableName);

    protected long executeSelectCount(BoundStatement prepared) {
        try {
            ResultSet rs = session.execute(prepared);
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
