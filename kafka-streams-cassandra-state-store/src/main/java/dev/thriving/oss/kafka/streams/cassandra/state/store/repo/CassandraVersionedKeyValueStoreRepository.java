package dev.thriving.oss.kafka.streams.cassandra.state.store.repo;

import org.apache.kafka.common.utils.Bytes;

import java.time.Instant;
import java.util.List;

public interface CassandraVersionedKeyValueStoreRepository {

    VersionedEntry get(int partition, Bytes key);
    VersionedEntry get(int partition, Bytes key, Instant asOfTimestamp);
    void cleanup(int partition, Bytes key, Instant observedStreamTime);

    void saveInBatch(int partition, Bytes key, List<VersionedEntry> entries);
}
