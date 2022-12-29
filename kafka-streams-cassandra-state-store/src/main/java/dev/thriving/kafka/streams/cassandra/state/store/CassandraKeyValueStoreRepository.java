package dev.thriving.kafka.streams.cassandra.state.store;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.List;

interface CassandraKeyValueStoreRepository {

    long NO_TTL = 0;

    byte[] getByKey(int partition, Bytes key);
    void save(int partition, Bytes key, byte[] value);
    void saveBatch(int partition, List<KeyValue<Bytes, byte[]>> entries);
    void delete(int partition, Bytes key);
    KeyValueIterator<Bytes, byte[]> getAll(int partition);
    KeyValueIterator<Bytes, byte[]> getAllDesc(int partition);
    KeyValueIterator<Bytes, byte[]> range(int partition, Bytes from, Bytes to);
    KeyValueIterator<Bytes, byte[]> rangeDesc(int partition, Bytes from, Bytes to);
    KeyValueIterator<Bytes, byte[]> findByPartitionAndKeyPrefix(int partition, String prefix);
}
