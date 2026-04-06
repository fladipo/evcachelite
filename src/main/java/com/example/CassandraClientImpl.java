package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A concrete implementation of the {@link CassandraClient} interface used to
 * simulate high-performance data retrieval from a persistent store.
 * 
 * <p>
 * This class models real-world database communication by adding simulated
 * network latency and managing thread-level interruptions.
 * </p>
 * 
 * @param <K> the type of keys processed by this client
 * @param <V> the type of values retrieved from the store, may be null
 */
public class CassandraClientImpl<K, V> implements CassandraClient<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(CassandraClientImpl.class);

    /**
     * Fetches a value from the simulated Cassandra store.
     * 
     * <p>
     * <b>Implementation Behavior:</b>
     * This method introduces a 150ms delay to simulate network round trip time,
     * allowing for a more realistic analysis of the {@code EVCacheManager} read-through
     * caching logic.
     * </p>
     * 
     * <p>
     * <b>On Type Safety:</b>
     * Since I am simulating a generic backend fetch, I cast String to the generic
     * type V. Since the compiler cannot verify the safety of this cast at compile
     * time due to Java's Type Erasure, I use {@code @SuppressWarnings("unchecked")} 
     * to acknowledge the cast while keeping build logs clean.
     * </p>
     * 
     * @param key the unique identifier to fetch from the store, not null
     * @return a simulated data string for the given key or {@code null} if not found
     */
    @Override
    @SuppressWarnings("unchecked")
    public V fetch(K key) {
        // Simulate some network latency to mirror a real cluster fetch
        try {
            Thread.sleep(150);
        } catch (InterruptedException e) {
            // Restore the interrupted status to follow JVM concurrency best practices.
            // This prevents exception swallowing by resetting the interruption flag
            // so that callers know the thread was asked to stop.
            Thread.currentThread().interrupt();
        }

        logger.info("Cassandra fetching fresh data for key: {}", key);
        return (V) ("feranmi-loves-natural-hair for key : " + key);
    }
}
