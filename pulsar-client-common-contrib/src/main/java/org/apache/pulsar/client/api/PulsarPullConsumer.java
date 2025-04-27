package org.apache.pulsar.client.api;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.admin.PulsarAdminException;

public interface PulsarPullConsumer<T> {
    /**
     * Initializes the consumer and establishes connection to brokers.
     *
     * @throws PulsarClientException if client setup fails
     * @throws PulsarAdminException if admin operations fail
     */
    void start() throws PulsarClientException, PulsarAdminException;

    /**
     * Pulls messages from specified partition starting from the given offset (inclusive).
     *
     * @param offset     the starting offset to consume from (inclusive)
     * @param partition  the target partition index, -1 means non-partitioned topic
     * @param maxNum     maximum number of messages to return
     * @param maxSize    maximum total size (bytes) of messages to return
     * @param timeout    maximum wait time per message
     * @param timeUnit   timeout unit
     * @return list of messages starting from the specified offset
     * @throws PulsarClientException if read operations fail
     * @throws PulsarAdminException  if offset mapping fails
     */
    List<Message<?>> pull(long offset, int partition, int maxNum, int maxSize,
                          int timeout, TimeUnit timeUnit)
            throws PulsarClientException, PulsarAdminException;

    /**
     * Acknowledges consumption of all messages up to and including the specified offset.
     *
     * @param offset    the offset to acknowledge (inclusive)
     * @param partition the target partition index, -1 means non-partitioned topic
     * @throws PulsarClientException if acknowledgment fails
     */
    void ack(long offset, int partition) throws PulsarClientException;

    /**
     * Finds the latest message offset before or at the specified timestamp.
     *
     * @param topic       target topic name
     * @param partition the target partition index, -1 means non-partitioned topic
     * @param timestamp   target timestamp (milliseconds since epoch)
     * @return the closest offset where message timestamp ≤ specified timestamp
     * @throws PulsarAdminException if message lookup fails
     */
    long searchOffset(String topic, int partition, long timestamp) throws PulsarAdminException;

    /**
     * Retrieves consumption statistics for a consumer group.
     *
     * @param topic      target topic name
     * @param partition the target partition index, -1 means non-partitioned topic
     * @param group      consumer group name
     * @return the current consumed offset for the group
     * @throws PulsarAdminException if statistics retrieval fails
     */
    long getConsumeStats(String topic, Integer partition, String group) throws PulsarAdminException;

    /**
     * Releases all resources and closes connections.
     * Implements AutoCloseable for try-with-resources support.
     *
     * @throws IOException if graceful shutdown fails
     */
    void close() throws IOException;
}
