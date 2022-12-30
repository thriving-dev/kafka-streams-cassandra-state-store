package dev.thriving.kafka.streams.cassandra.state.store.serde;

import org.apache.kafka.common.utils.Bytes;

public interface KeySerde<T> {

    T serialize(Bytes data);
    Bytes deserialize(T data);

}
