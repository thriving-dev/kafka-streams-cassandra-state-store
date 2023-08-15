package dev.thriving.oss.kafka.streams.cassandra.state.store.repo;

import java.time.Instant;

public record VersionedEntry(
        byte[] value,
        Instant timestamp,
        Instant validFrom,
        Instant validTo
) {
}
