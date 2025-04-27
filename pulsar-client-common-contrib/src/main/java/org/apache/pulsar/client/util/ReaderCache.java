package org.apache.pulsar.client.util;

import com.github.benmanes.caffeine.cache.LoadingCache;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;

/**
 * ReaderCache is a utility class that provides a cache for storing and retrieving reader instances
 * for a given partition topic and offset.
 */
public class ReaderCache<T> {
    private static final int MAX_CACHE_SIZE = 10;
    private static final Duration EXPIRE_AFTER_ACCESS = Duration.ofMinutes(5);

    private final PulsarClient pulsarClient;
    private final OffsetToMessageIdCache offsetToMessageIdCache;
    private final Map<String/*partitioned topic name or non-partition topic name*/,
            LoadingCache<Long/*the next message offset*/, Reader<T>>>
            readerCacheMap = new ConcurrentHashMap<>();
    private final Schema<T> scheme;

    public ReaderCache(PulsarClient pulsarClient, OffsetToMessageIdCache offsetToMessageIdCache, Schema<T> schema) {
        this.pulsarClient = Objects.requireNonNull(pulsarClient, "PulsarClient must not be null");
        this.offsetToMessageIdCache = Objects.requireNonNull(offsetToMessageIdCache, "OffsetToMessageIdCache must not be null");
        this.scheme = Objects.requireNonNull(schema, "Schema must not be null");
    }

    private LoadingCache<Long, Reader<T>> createCache(String partitionTopic) {
        return com.github.benmanes.caffeine.cache.Caffeine.newBuilder()
                .maximumSize(MAX_CACHE_SIZE)
                .expireAfterAccess(EXPIRE_AFTER_ACCESS)
                .recordStats()
                .build(key -> loadReader(partitionTopic, key));
    }

    private Reader<T> loadReader(String partitionTopic, Long offset) throws PulsarClientException {
        return pulsarClient.newReader(scheme)
                .startMessageId(offsetToMessageIdCache.getMessageIdByOffset(partitionTopic, offset))
                .topic(partitionTopic)
                .create();
    }

    public synchronized Reader<T> getReaderByOffset(String partitionTopic, long offset) {
        LoadingCache<Long, Reader<T>> cache = readerCacheMap.computeIfAbsent(
                partitionTopic,
                this::createCache
        );
        Reader<T> reader = cache.get(offset);
        // One reader can only be used by one thread at a time.
        cache.invalidate(offset);
        return reader;
    }

    // put reader back after use, so that it can be reused
    public synchronized void putReaderByOffset(String partitionTopic, long offset, Reader<T> reader) {
        LoadingCache<Long, Reader<T>> cache = readerCacheMap.computeIfAbsent(
                partitionTopic,
                this::createCache
        );
        cache.put(offset, reader);
    }
}
