package dev.thriving.oss.kafka.streams.cassandra.state.store.repo.serde;

import org.apache.kafka.common.utils.Bytes;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public abstract class KeySerdes {

    static private final class StringKeySerde implements KeySerde<String> {
        @Override
        public String serialize(Bytes data) {
            return data.toString();
        }

        @Override
        public Bytes deserialize(String data) {
            return Bytes.wrap(data.getBytes(StandardCharsets.UTF_8));
        }
    }

    static private final class ByteBufferKeySerde implements KeySerde<ByteBuffer> {
        @Override
        public ByteBuffer serialize(Bytes data) {
            return ByteBuffer.wrap(data.get());
        }

        @Override
        public Bytes deserialize(ByteBuffer data) {
            return Bytes.wrap(data.array());
        }
    }

    /**
     * A serde for {@code String} type.
     */
    public static KeySerde<String> String() {
        return new StringKeySerde();
    }

    /**
     * A serde for {@code ByteBuffer} type.
     */
    public static KeySerde<ByteBuffer> ByteBuffer() {
        return new ByteBufferKeySerde();
    }
}
