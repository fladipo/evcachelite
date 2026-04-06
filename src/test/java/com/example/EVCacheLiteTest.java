package com.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static java.time.Duration.ofMillis;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

public class EVCacheLiteTest {
  private EVCacheLite<String, String> cache;
  private MeterRegistry meterRegistry;
  private final int CAPACITY = 3;

  @BeforeEach
  public void setUp() {
    meterRegistry = new SimpleMeterRegistry();
    cache = new EVCacheLite<>(CAPACITY, 1, meterRegistry);
  }

  @Test
  @DisplayName("LRU Accuracy: Should evict the least recently used item")
  public void testLRUEviction() {
    // Fill the cache to capacity
    cache.put("monday", "braids");
    cache.put("tuesday", "twists");
    cache.put("wednesday", "afro");

    // access monday to move to the most recently used
    cache.get("monday");
    // triggers the removal of tuesday
    cache.put("friday", "deep condition");

    // verify
    assertNull(cache.get("tuesday"), "tuesday should have been evicted from the cache.");
    assertNotNull(cache.get("wednesday"), "wednesday should still exist.");
    assertNotNull(cache.get("friday"), "friday should still exist.");
    assertNotNull(cache.get("monday"), "monday should still exist.");
  }

  @Test
  @DisplayName("Memory Hygiene: The map and list must stay in sync")
  public void testMemoryLeaks() {
    // Hammer the cache with about 10 entries into a capacity of 3
    StringBuilder sb = new StringBuilder(); 
    String key = null;
    String value = null;
    for (int i = 0; i < 10; i++) {
      sb.setLength(0);
      key = sb.append("key").append(i).toString();

      sb.setLength(0);
      value = sb.append("value").append(i).toString();
      cache.put(key, value);
    }

    int cacheSize = 0;
    for (int i = 0; i < 10; i++) {
      sb.setLength(0);      
      key = sb.append("key").append(i).toString();
      if (cache.get(key) != null) {
        cacheSize++;
      }
    }

    // Check the size of the map and confirm the size remained at the capacity
    assertEquals(CAPACITY, cacheSize, "The cache must strictly enforce capacity to prevent OOM");
  }

  @Test()
  @DisplayName("Concurrency: Concurrent Writes to different shards should not block")
  public void testShardLocking() throws InterruptedException {
    EVCacheLite<String, String> multiShardCache = new EVCacheLite<>(150, meterRegistry);
    assertTimeoutPreemptively(ofMillis(5000), () -> {
      // Spawn 2 lightweight threads. Unlike traditional threads, these are
      // lightweight and incredibly fast to start, meaning they
      // hit the put method almost instantly at the exact same time.
      Thread t1 = Thread.ofVirtual().start(() -> {
        for (int i = 0; i < 1000; i++) {
          multiShardCache.put("A" + i, "val");
        }
      });

      Thread t2 = Thread.ofVirtual().start(() -> {
        for (int i = 0; i < 1000; i++) {
          multiShardCache.put("Z" + i, "val");
        }
      });

      try {
        // Causes the current thread to wait until the target thread dies, unless the
        // current thread is interrupted.
        t1.join();
        t2.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt(); // Restore the flag
        throw new RuntimeException("Test interrupted during concurrency check", e);
      }

    }, "Deadlock detected: Shard locking took longer than 5 seconds!");
  }

  @Test
  @DisplayName("Observability: Validate count misses and hits correctly")
  public void testMetrics() {
    cache.put("hit", "texture");

    cache.get("hit");
    cache.get("miss");
    cache.get("miss");

    // Verify shard metrics
    double hits = meterRegistry.get("EVCacheLite.gets").tag("result", "hit").counter().count();
    double misses = meterRegistry.get("EVCacheLite.gets").tag("result", "miss").counter().count();
    assertEquals(1.0, hits);
    assertEquals(2.0, misses);
  }
}
