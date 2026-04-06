package com.example;

/**
 * Interface for a Cassandra data access client
 * 
 * <p>
 * This interface decouples the {@code EVCacheManager} from the implementation
 * of the persistent storage layer.
 * </p>
 * 
 * <p>
 * <b>Note on Mocking:</b> An interface is used here intentionally because
 * Mockito creates a Proxy class at runtime that implements it and
 * during unit testing, a mock implementation is used to simulate backend
 * behaviors and failures without requiring a live cluster connection.
 * </p>
 * 
 * @param <K> the type of keys maintained by this client
 * @param <V> the type of values maintained by this client
 */
public interface CassandraClient<K, V> {
    /**
     * Retrieves the value associated with the specified key from the Cassandra store
     * 
     * <p>
     * In a production environment, this method typically would point to a
     * {@code CassandraClientImpl} that would manage
     * cluster connections and session handling.
     * </p>
     * 
     * @param key the key whose value is meant to be returned
     * @return the value associated with the specified key, or {@code null} if the
     *         store has no mapping for the key
     */
    V fetch(K key);
}
