package io.confluent.developer.store;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class CassandraKeyValueBytesStoreSupplier implements KeyValueBytesStoreSupplier {

        private final CqlSession session;
        private final String name;

        public CassandraKeyValueBytesStoreSupplier(final CqlSession session, final String name) {
            this.session = session;
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public KeyValueStore<Bytes, byte[]> get() {
            return new CassandraKeyValueStore(session, name);
        }

        @Override
        public String metricsScope() {
            return "cassandra";
        }
}
