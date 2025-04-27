package org.apache.pulsar.client.api.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.PulsarPullConsumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.util.OffsetToMessageIdCache;
import org.apache.pulsar.client.util.OffsetToMessageIdCacheProvider;
import org.apache.pulsar.client.util.ReaderCache;
import org.apache.pulsar.client.util.ReaderCacheProvider;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;

public class PulsarPullConsumerImpl<T> implements PulsarPullConsumer<T> {
    private final String topic;
    private final String subscription;
    private final String brokerCluster;
    private final Schema<T> schema;
    private static final String PARTITION_SPLICER = "-partition-";
    private int partitionNum;

    Map<String, Consumer<?>> consumerMap;
    private final OffsetToMessageIdCache offsetToMessageIdCache;
    private final ReaderCache<T> readerCache;

    private final PulsarAdmin pulsarAdmin;
    private final PulsarClient pulsarClient;

    public PulsarPullConsumerImpl(String topic, String subscription, Schema<T> schema, PulsarClient client,
                                  PulsarAdmin admin, String brokerCluster) {
        this.topic = topic;
        this.subscription = subscription;
        this.brokerCluster = brokerCluster;
        this.schema = schema;
        this.consumerMap = new ConcurrentHashMap<>();
        this.pulsarClient = client;
        this.pulsarAdmin = admin;
        this.offsetToMessageIdCache = OffsetToMessageIdCacheProvider.getOffsetToMessageIdCache(admin, brokerCluster);
        this.readerCache = ReaderCacheProvider.getReaderCache(brokerCluster, schema, client, offsetToMessageIdCache);
    }

    @Override
    public void start() throws PulsarClientException, PulsarAdminException {
        discoverPartition();
    }

    private void discoverPartition() throws PulsarClientException, PulsarAdminException {
        PartitionedTopicMetadata partitionedTopicMetadata = pulsarAdmin.topics().getPartitionedTopicMetadata(topic);
        this.partitionNum = partitionedTopicMetadata.partitions;
        if (partitionNum == 0) {
            Consumer<?> consumer = pulsarClient.newConsumer(schema)
                    .topic(topic)
                    .subscriptionName(subscription)
                    .subscribe();
            consumerMap.put(topic, consumer);
            return;
        }

        for (int i = 0; i < partitionNum; i++) {
            String partitionTopic = getPartitionTopicName(topic, i);
            Consumer<?> consumer = pulsarClient.newConsumer(schema)
                    .topic(partitionTopic)
                    .subscriptionName(subscription)
                    .subscribe();
            consumerMap.put(partitionTopic, consumer);
        }
    }

    @Override
    public List<Message<?>> pull(long offset, int partition, int maxNum, int maxSize,
                                 int timeout, TimeUnit timeUnit)
            throws PulsarClientException {
        List<Message<?>> messages = new ArrayList<>();
        int totalSize = 0;
        String partitionTopic = getPartitionTopicName(topic, partition);
        Reader<T> reader = readerCache.getReaderByOffset(partitionTopic, offset);

        Message<?> lastMessage = null;
        long startTime = System.nanoTime();

        while (messages.size() < maxNum && totalSize < maxSize) {
            long elapsed = System.nanoTime() - startTime;
            long remainingNanos = Math.max(0, TimeUnit.NANOSECONDS.convert(timeout, timeUnit) - elapsed);

            Message<?> message = reader.readNext(Math.toIntExact(TimeUnit.MILLISECONDS.convert(remainingNanos, TimeUnit.NANOSECONDS)),
                    TimeUnit.MILLISECONDS);
            if (message == null) break;
            messages.add(message);
            lastMessage = message;
            totalSize += message.getData().length;
        }
        if (lastMessage != null) {
            readerCache.putReaderByOffset(partitionTopic, lastMessage.getIndex().get() + 1, reader);
        }
        return messages;
    }

    @Override
    public void ack(long offset, int partition) throws PulsarClientException {
        String partitionTopic = getPartitionTopicName(topic, partition);
        Consumer<?> consumer = consumerMap.get(partitionTopic);
        MessageId messageId = offsetToMessageIdCache.getMessageIdByOffset(partitionTopic, offset);
        consumer.acknowledgeCumulative(messageId);
    }

    @Override
    public long searchOffset(String topic, int partition, long timestamp) throws PulsarAdminException {
        String partitionTopic = getPartitionTopicName(topic, partition);
        MessageIdAdv messageId = (MessageIdAdv) pulsarAdmin.topics().getMessageIdByTimestamp(partitionTopic, timestamp);
        List<Message<byte[]>> messages =  pulsarAdmin.topics()
                .getMessagesById(partitionTopic, messageId.getLedgerId(), messageId.getEntryId());
        if (messages == null || messages.isEmpty()) {
            throw new IllegalArgumentException("The message is not found");
        }
        Message<byte[]> message = messages.get(messages.size() - 1);
        if (message.getIndex().isEmpty()) {
            throw new IllegalArgumentException("The message index is empty");
        }
        offsetToMessageIdCache.putMessageIdByOffset(partitionTopic, message.getIndex().get(), message.getMessageId());
        return message.getIndex().get();
    }

    @Override
    public long getConsumeStats(String topic, Integer partition, String group) throws PulsarAdminException {
        String partitionTopic = getPartitionTopicName(topic, partition);

        // Get partition stats using proper partition identifier
        String messageIdString = pulsarAdmin.topics().getPartitionedInternalStats(topic)
                .partitions.get(partitionTopic)
                .cursors
                .get(subscription)
                .markDeletePosition;

        // Handle potential format errors
        if (!messageIdString.contains(":")) {
            throw new PulsarAdminException("Invalid message ID format: " + messageIdString);
        }

        String[] ids = messageIdString.split(":");
        try {
            long ledgerId = Long.parseLong(ids[0]);
            long entryId = Long.parseLong(ids[1]);

            // Use partition-specific topic to retrieve message
            List<Message<byte[]>> messages = pulsarAdmin.topics().getMessagesById(partitionTopic, ledgerId, entryId);
            if (messages == null || messages.isEmpty()) {
                throw new PulsarAdminException("Message not found for offset: " + messageIdString);
            }
            Message<?> message = messages.get(messages.size() - 1);
            if (message.getIndex().isEmpty()) {
                throw new PulsarAdminException("Message index is empty for offset: " + messageIdString);
            }
            offsetToMessageIdCache.putMessageIdByOffset(partitionTopic, message.getIndex().get(), message.getMessageId());
            return message.getIndex().get();
        } catch (NumberFormatException e) {
            throw new PulsarAdminException("Invalid message ID components: " + messageIdString, e);
        }
    }

    // Add resource cleanup method
    @Override
    public void close() throws IOException {
        // Close all consumers
        for (Consumer<?> consumer : consumerMap.values()) {
            consumer.close();
        }

        offsetToMessageIdCache.cleanup();
    }

    private String getPartitionTopicName(String topic, int partition) {
        if (partitionNum > 0 && partition < 0) {
            throw new IllegalArgumentException("Partition index must be non-negative for partitioned topics");
        } else if (partition >= partitionNum) {
            throw new IllegalArgumentException("Partition index out of bounds: " + partition +
                    " for topic " + topic + " with " + partitionNum + " partitions");
        }
        return partition >= 0 ? topic + PARTITION_SPLICER + partition : topic;
    }
}
