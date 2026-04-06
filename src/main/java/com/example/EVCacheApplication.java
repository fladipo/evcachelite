package com.example;

import javax.naming.ServiceUnavailableException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * The entry point for the EVCacheLite engine simulation.
 * 
 * <p>
 * This application sets up the entire data platfrom, including sharded storage,
 * consistent hashing, and circuit breaker resilience, to demonstrate a
 * real-world Read-Through Caching workflow.
 * </p>
 * 
 * <p>
 * <b>Operational Flow:</b>
 * 1. Initializes a 150-capacity {@link EVCacheLite} storage layer.
 * 2. Connects a simulated {@link CassandraClient} backend.
 * 3. Orchestrates traffic through the {@link EVCacheManager}.
 * 4. Verifies cache backfill and hit/miss logic via Micrometer metrics.
 * </p>
 */
public class EVCacheApplication {
    private static final Logger logger = LoggerFactory.getLogger(EVCacheApplication.class);

    // Execute simulation
    public static void main(String[] args) throws ServiceUnavailableException {
        logger.info("-----Starting EVCacheLite Engine-----");

        // 1. Set up infrastructure (observability and resilience)
        MeterRegistry registry = new SimpleMeterRegistry();
        CircuitBreakerRegistry cBreakerRegistry = CircuitBreakerRegistry.ofDefaults();

        // 2. Initialize storage with 4-stripe sharded cache with lock stripping
        EVCacheLite<String, String> cache = new EVCacheLite<>(150, registry);

        // 3. Initialize Cassandra Client, the database
        CassandraClient<String, String> cassandra = new CassandraClientImpl<>();

        // 4. Initialize the orchestrator, the manager
        EVCacheManager<String, String> manager = new EVCacheManager<>(cache, cassandra, cBreakerRegistry, registry);

        // 5. Run simple example here
        logger.info("Requesting user:feranmi123");

        /*
         * Execution 1: Cache Miss
         * This triggers: Manager -> Cache Miss -> CircuitBreaker -> Cassandra Fetch ->
         * Cache Backfill
         */
        String cacheMissVal = manager.get("feranmi123");
        logger.info("First call is a cache miss, value is {}", cacheMissVal);

        /*
         * Execution 2: Cache Hit
         * This triggers: Manager -> Cache Hit. The database is never touched.
         */
        String cacheHitVal = manager.get("feranmi123");
        logger.info("Second call is a cache hit, value is {}", cacheHitVal);

        // 6. Verify orchestration logic from telemetry
        double fetchCount = registry.find("EVCacheManager.backend.fetch").timer().count();
        logger.info("Total Backend Fetches: {}", fetchCount);

        logger.info("-----EVCacheLite Engine Shutdown Cleanly-----");
    }
}
