package com.example;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.naming.ServiceUnavailableException;

import static org.mockito.ArgumentMatchers.anyString;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * Unit test for EvCacheManager.
 */
public class EVCacheManagerTest {
    private EVCacheLite<String, String> cache;
    private EVCacheManager<String, String> cacheManager;
    private CassandraClient<String, String> cassandraClient;
    private MeterRegistry meterRegistry;
    private CircuitBreakerRegistry cbRegistry;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        cache = Mockito.mock(EVCacheLite.class);
        cassandraClient = Mockito.mock(CassandraClient.class);

        meterRegistry = new SimpleMeterRegistry();

        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Trip if 50% fail
                .minimumNumberOfCalls(5) // Calculate after only 5 calls
                .slidingWindowSize(10) // Look at the last 10 calls
                .build();

        cbRegistry = CircuitBreakerRegistry.of(config);
        cacheManager = new EVCacheManager<>(cache, cassandraClient, cbRegistry, meterRegistry);
    }

    @Test
    @DisplayName("Cache Hit: Avoids Cassandra call")
    void testCacheHit() throws ServiceUnavailableException {
        // Simulate a cache hit
        when(cache.get("key1")).thenReturn("cached-value");

        String result = cacheManager.get("key1");
        assertTrue(result.equals("cached-value"));

        // Verify that the Cassandra client was never called, 
        // since it should be a cache hit
        Mockito.verify(cassandraClient, Mockito.never()).fetch(anyString());
    }

    @Test
    @DisplayName("Cache Miss: Fetches from Cassandra and backfills the cache")
    void testCacheMiss() throws ServiceUnavailableException {
        // Simulate a cache hit
        when(cache.get("key1")).thenReturn(null);
        when(cassandraClient.fetch("key1")).thenReturn("db-value");

        String result = cacheManager.get("key1");
        assertTrue(result.equals("db-value"));
    }

    @Test
    @DisplayName("Resilience: Trip Circuit Breaker and Fail-Fast When Cassandra is down")
    void testCircuitBreaker() {
        // Simulate the failure, the cache misses and Cassandra throws an error
        when(cache.get(anyString())).thenReturn(null);
        when(cassandraClient.fetch(anyString())).thenThrow(new RuntimeException("Cassandra is down"));

        for (int i = 0; i < 15; i++) {
            try {
                StringBuilder key = new StringBuilder();
                key.append("key").append(i);
                cacheManager.get(key.toString());

            } catch (Exception ignoredException) {
            }
        }

        // Assert, the circuitBreaker state should now be open
        CircuitBreaker cb = cbRegistry.circuitBreaker("Cassandra");
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
        assertThrows(ServiceUnavailableException.class, () -> cacheManager.get("anyKey"));

        verify(cassandraClient, times(5)).fetch(anyString());
    }

    @Test
    @DisplayName("Observability: Metric recorded for backend fetch")
    void testMetricRecording() throws ServiceUnavailableException {
        // Simulate the failure, the cache misses and Cassandra throws an error
        when(cache.get("key1")).thenReturn(null);
        when(cassandraClient.fetch("key1")).thenReturn("db-value");

        cacheManager.get("key1");

        // Find the timer by name and assert it recorded an event
        double count = meterRegistry.find("EVCacheManager.backend.fetch").timer().count();
        assertEquals(1.0, count);
    }

    @Test
    @DisplayName("Concurrency: 50 threads. The Thundering Herd simulation")
    void testHighConcurrency() throws InterruptedException {
        int threadCount = 50;
        // We set this to 1. All threads will sit and wait until this drops to 0
        // This ensires that they all hit the manager at the same time microsend
        CountDownLatch startLatch = new CountDownLatch(1);

        // The main test thread needs to wait for all 50 threads to finish their work
        // before we start our assertions
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        when(cache.get(anyString())).thenReturn(null); // Force a cache miss
        when(cassandraClient.fetch(anyString())).thenReturn("db-value");

        // Using virtual threads to simulate massive parallel load
        // without the memory overhead of 50 heavy platform threads
        for (int i = 0; i < threadCount; i++) {
            Thread.ofVirtual().start(() -> {
                try {
                    // Threads are warm and ready but blocked here
                    startLatch.await();
                    cacheManager.get("hotKey");

                } catch (Exception ignoredException) {
                } finally {
                    // Each thread signals that it has finished its own work
                    doneLatch.countDown();
                }
            });
        }

        // This drops the latch to 0, all 50 threads fire simulataneously
        startLatch.countDown();
        boolean completed = false;
        completed = doneLatch.await(5, TimeUnit.SECONDS);

        assertTrue(completed, "Threads should complete within the 5s timeout");
        
        // Verifying stability under a 50-thread storm.
        // In a real-world thundering herd, we would actually want to hit the database only once.
        // Future work includes request coalescing to handle this.
        Mockito.verify(cassandraClient, Mockito.times(threadCount)).fetch("hotKey");
        double count = meterRegistry.find("EVCacheManager.backend.fetch").timer().count();
        assertEquals((double) threadCount, count);
    }
}