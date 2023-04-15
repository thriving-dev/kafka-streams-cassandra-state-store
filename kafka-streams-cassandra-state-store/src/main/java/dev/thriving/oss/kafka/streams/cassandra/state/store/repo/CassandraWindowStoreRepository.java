package dev.thriving.oss.kafka.streams.cassandra.state.store.repo;

import dev.thriving.oss.kafka.streams.cassandra.state.store.CassandraWindowStoreIteratorProvider;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;

public interface CassandraWindowStoreRepository {
    void save(int partition, long windowStartTimestamp, Bytes key, byte[] value, long seqnum);

    void delete(int partition, long windowStartTimestamp, Bytes key, long seqnum);

    CassandraWindowStoreIteratorProvider fetch(int partition, Bytes key, long timeFrom, long timeTo, boolean forward, long windowSize);
    CassandraWindowStoreIteratorProvider fetch(int partition, Bytes keyFrom, Bytes keyTo, long timeFrom, long timeTo, boolean forward, long windowSize);

    KeyValueIterator<Windowed<Bytes>,byte[]> fetchAll(int partition, long timeFrom, long timeTo, boolean forward);

    byte[] get(int partition, long windowStartTimestamp, Bytes key, long seqnum);

    KeyValueIterator<Windowed<Bytes>,byte[]> getAllWindowsFrom(int partition, long minTime, boolean forward);

    void deleteWindowsOlderThan(int partition, long minLiveTime);
}
