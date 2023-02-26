package dev.thriving.oss.kafka.streams.cassandra.state.store.repo;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.List;

public interface CassandraKeyValueStoreRepository {

    byte[] getByKey(int partition, Bytes key);
    void save(int partition, Bytes key, byte[] value);
    void saveBatch(int partition, List<KeyValue<Bytes, byte[]>> entries);
    void delete(int partition, Bytes key);
    KeyValueIterator<Bytes, byte[]> getAll(int partition, boolean forward);
    KeyValueIterator<Bytes, byte[]> getForRange(int partition, Bytes from, Bytes to, boolean forward, boolean toInclusive);

    long getCount(int partition);

}
