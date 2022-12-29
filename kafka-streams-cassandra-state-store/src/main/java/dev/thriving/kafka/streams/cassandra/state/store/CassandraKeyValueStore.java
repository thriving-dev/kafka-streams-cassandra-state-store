package dev.thriving.kafka.streams.cassandra.state.store;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.internals.StoreQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

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
