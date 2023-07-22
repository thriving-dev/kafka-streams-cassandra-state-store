package dev.thriving.oss.kafka.streams.cassandra.state.store.utils;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class CompositeKeyValueIterator<K, V> implements KeyValueIterator<K, V> {

    private final Iterator<KeyValueIterator<Bytes, byte[]>> keyValueIteratorIterator;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    private KeyValueIterator<Bytes, byte[]> current;

    public CompositeKeyValueIterator(Iterator<KeyValueIterator<Bytes, byte[]>> keyValueIteratorIterator,
                                     Deserializer<K> keyDeserializer,
                                     Deserializer<V> valueDeserializer) {
        this.keyValueIteratorIterator = keyValueIteratorIterator;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    @Override
    public void close() {
        if (current != null) {
            current.close();
            current = null;
        }
    }

    @Override
    public K peekNextKey() {
        throw new UnsupportedOperationException("peekNextKey not supported");
    }

    @Override
    public boolean hasNext() {
        while ((current == null || !current.hasNext()) && keyValueIteratorIterator.hasNext()) {
            close();
            current = keyValueIteratorIterator.next();
        }
        return current != null && current.hasNext();
    }


    @Override
    public KeyValue<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        KeyValue<Bytes, byte[]> next = current.next();
        return KeyValue.pair(
                keyDeserializer.deserialize(null, next.key.get()),
                valueDeserializer.deserialize(null, next.value)
        );
    }

}
