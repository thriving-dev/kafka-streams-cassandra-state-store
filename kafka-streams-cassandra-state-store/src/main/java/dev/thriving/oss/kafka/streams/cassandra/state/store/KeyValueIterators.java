package dev.thriving.oss.kafka.streams.cassandra.state.store;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.NoSuchElementException;

class KeyValueIterators {

    private static class EmptyKeyValueIterator<K, V> implements KeyValueIterator<K, V> {

        @Override
        public void close() {
        }

        @Override
        public K peekNextKey() {
            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public KeyValue<K, V> next() {
            throw new NoSuchElementException();
        }

    }

    private static class EmptyWindowStoreIterator<V> extends KeyValueIterators.EmptyKeyValueIterator<Long, V>
            implements WindowStoreIterator<V> {
    }

    private static final KeyValueIterator EMPTY_ITERATOR = new KeyValueIterators.EmptyKeyValueIterator();
    private static final WindowStoreIterator EMPTY_WINDOW_STORE_ITERATOR = new KeyValueIterators.EmptyWindowStoreIterator();


    @SuppressWarnings("unchecked")
    static <K, V> KeyValueIterator<K, V> emptyIterator() {
        return (KeyValueIterator<K, V>) EMPTY_ITERATOR;
    }

    @SuppressWarnings("unchecked")
    static <V> WindowStoreIterator<V> emptyWindowStoreIterator() {
        return (WindowStoreIterator<V>) EMPTY_WINDOW_STORE_ITERATOR;
    }
}
