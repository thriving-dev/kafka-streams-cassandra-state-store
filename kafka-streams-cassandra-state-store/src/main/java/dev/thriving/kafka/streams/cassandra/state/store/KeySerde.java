package dev.thriving.kafka.streams.cassandra.state.store;

import org.apache.kafka.common.utils.Bytes;

interface KeySerde<T> {

    T serialize(Bytes data);
    Bytes deserialize(T data);

}
