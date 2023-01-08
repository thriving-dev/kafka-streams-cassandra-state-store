package dev.thriving.oss.kafka.streams.cassandra.state.store.repo.serde;

import org.apache.kafka.common.utils.Bytes;

public interface KeySerde<T> {

    T serialize(Bytes data);
    Bytes deserialize(T data);

}
