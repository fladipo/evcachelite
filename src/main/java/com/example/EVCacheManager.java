package com.example;

import java.util.function.Supplier;

import javax.naming.ServiceUnavailableException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * The central orchestrator for caching, resilience, and observability.
 * 
 * <p>
 * This class implements a <b>Read-Through (Lazy Loading)</b> strategy,
 * decoupling the core business logic from fault tolerance patterns. 
 * It ensures that the persistent storage layer is protected by a Circuit Breaker 
 * and that all backend calls are created for real-time monitoring.
 * </p>
 * 
 * @param <K> The type of keys used for lookup.
 * @param <V> The type of values being managed.
 */
public class EVCacheManager<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(EVCacheManager.class);

    private final EVCacheLite<K, V> cache;
    private final CassandraClient<K, V> cassandraClient;
    private final CircuitBreaker circuitBreaker;
    private final Timer backendTimer;

    /**
     * Sets up the manager with sharded storage and resilience registries.
     */
    public EVCacheManager(EVCacheLite<K, V> cache, CassandraClient<K, V> cassandraClient,
            CircuitBreakerRegistry cbRegistry, MeterRegistry registry) {
        this.cache = cache;
        this.cassandraClient = cassandraClient;
        this.circuitBreaker = cbRegistry.circuitBreaker("Cassandra");

        this.backendTimer = Timer.builder("EVCacheManager.backend.fetch")
                .description("Latency of Cassandra backend fetches on cache misses")
                .tag("dependency", "cassandra")
                .publishPercentiles(0.95, 0.999)
                .register(registry);

        logger.info("EVCacheManager initialized with Read-Through caching and Circuit Breaker protection.");
    }

    /**
     * Executes a Read-Through strategy to retrieve data.
     * 
     * <p>
     * <b>Lifecycle:</b>
     * 1. Check local sharded cache (O(1) hit path).
     * 2. On miss, attempt fetch from Cassandra.
     * 3. If successful, backfill the cache to ensure the next request is a hit.
     * </p>
     * 
     * @param key the unique identifier.
     * @return The value from cache (Hit) or Cassandra (Miss). Returns null if not
     *         in DB.
     * @throws ServiceUnavailableException If the Circuit Breaker is OPEN, failing
     *                                     fast to protect the backend.
     */
    public V get(K key) throws ServiceUnavailableException {
        if (key == null)
            return null;

        // This is the hot path, sharded cache lookup
        V value = cache.get(key);
        if (value != null) {
            return value;
        }

        // On a cache miss, get from Cassandra protected by the circuit breaker
        Supplier<V> backendSupplier = () -> cassandraClient.fetch(key);
        Supplier<V> decoratedSupplier = CircuitBreaker.decorateSupplier(circuitBreaker, backendSupplier);

        try {
            // Use recordCallable instead of record because our fallback logic throws a
            // checked exception
            return backendTimer.recordCallable(() -> {
                try {
                    V backendValue = decoratedSupplier.get();
                    // Backfill the cache so that future requests avoid the database
                    if (backendValue != null) {
                        cache.put(key, backendValue);
                    }

                    return backendValue;
                } catch (Exception e) {
                    return handleBackendFailure(key, e);
                }
            });
        } catch (ServiceUnavailableException e) {
            // Re-throw custom exception so it bubbles up to the caller/test
            throw e;
        } catch (Exception e) {
            logger.error("Unexpected execution error for key [{}]: ", key, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Analyzes backend failures to determine the best fallback or escalation.
     * 
     * @throws ServiceUnavailableException If the failure is due to a tripped
     *                                     Circuit Breaker.
     */
    private V handleBackendFailure(K key, Exception e) throws ServiceUnavailableException {
        // Identify if the failure is a Trip or a Miss.
        if (e instanceof CallNotPermittedException) {
            logger.warn("Circuit Breaker OPEN for key {}. Failing fast to protect Cassandra.", key);
            throw new ServiceUnavailableException("Circuit is OPEN - backend is currently unavailable.");
        }

        // Log the actual backend error (Timeout, Connection, etc.)
        logger.error("Backend fetch failed for key {}. Fallback: null. Error: {}", key, e.getMessage());

        // Standard cache-miss behavior: return null so the caller can handle the empty
        // result
        return null;
    }
}