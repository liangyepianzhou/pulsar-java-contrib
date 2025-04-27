package org.apache.pulsar.client.util;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;

public class OffsetToMessageIdCache {
    private static final int MAX_CACHE_SIZE = 1000;
    private static final Duration EXPIRE_AFTER_ACCESS = Duration.ofMinutes(5);

    private final PulsarAdmin pulsarAdmin;
    private final Map<String, LoadingCache<Long, MessageId>> caffeineCacheMap = new ConcurrentHashMap<>();

    public OffsetToMessageIdCache(PulsarAdmin pulsarAdmin) {
        this.pulsarAdmin = Objects.requireNonNull(pulsarAdmin, "PulsarAdmin must not be null");
    }

    private LoadingCache<Long, MessageId> createCache(String partitionTopic) {
        return Caffeine.newBuilder()
                .maximumSize(MAX_CACHE_SIZE)
                .expireAfterAccess(EXPIRE_AFTER_ACCESS)
                .recordStats()
                .build(key -> loadMessageId(partitionTopic, key));
    }

    private MessageId loadMessageId(String partitionTopic, Long offset) {
//        try {
//            // todo: waiting for https://github.com/apache/pulsar/pull/24220
//            return  pulsarAdmin.topics().getMessageIDByOffsetAndPartitionID(partitionTopic, offset);
//        } catch (PulsarAdminException e) {
//            throw new CompletionException("Failed to load message ID", e);
//        }
        return null;
    }

    public MessageId getMessageIdByOffset(String partitionTopic, long offset) {
        LoadingCache<Long, MessageId> cache = caffeineCacheMap.computeIfAbsent(
                partitionTopic,
                this::createCache
        );
        return cache.get(offset);
    }

    public void putMessageIdByOffset(String partitionTopic, long offset, MessageId messageId) {
        LoadingCache<Long, MessageId> caffeineCache = caffeineCacheMap.computeIfAbsent(partitionTopic,
                k -> createCache(partitionTopic));
        caffeineCache.put(offset, messageId);
    }

    public void cleanup() {
        for (LoadingCache<Long, MessageId> cache : caffeineCacheMap.values()) {
            cache.cleanUp();
        }
    }
}
