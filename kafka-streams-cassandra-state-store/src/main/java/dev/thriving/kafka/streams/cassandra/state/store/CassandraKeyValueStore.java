package dev.thriving.kafka.streams.cassandra.state.store;

import com.datastax.oss.driver.api.core.CqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraKeyValueStore extends AbstractCassandraStore {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraKeyValueStore.class);


    public CassandraKeyValueStore(CqlSession session, String name) {
        super(name,
                new PartitionedBlobKeyCassandraKeyValueStoreRepository(
                        session,
                        name.replaceAll("[^a-zA-Z0-9_]", "_") + "_kstreams_store",
                        CassandraKeyValueStoreRepository.NO_TTL));
    }

}
