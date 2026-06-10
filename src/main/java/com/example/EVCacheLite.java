package com.example;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * A high-performance, sharded LRU (Least Recently Used) cache
 * 
 * <p>
 * This implementation uses Lock Stripping across multiple {@link CacheShard}
 * instances to minimize lock contention
 * and maximize parallel throughput. By sharding the keyspace, the system
 * ensures that heavy write pressure on one
 * shard does not block concurrent access to the remaining shards.
 * </p>
 * 
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of mapped values
 */
public class EVCacheLite<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(EVCacheLite.class);

    /**
     * A doubly-linked list node representing a cache entry.
     * 
     * <p>
     * <b>Design Note:</b> This class is marked as static to prevent a hidden
     * reference to the outer class
     * {@code EVCacheLite} instance. Decoupling these two classes ensures that
     * individual nodes do not prevent the garbage collection of the
     * parent cache insatnce, effectively eliminating a common source of memory
     * leaks.
     * </p>
     * 
     * @param <K> the type of keys maintained by this cache.
     * @param <V> the type of mapped values.
     */
    private static class Node<K, V> {
        K key;
        V value;
        Node<K, V> next;
        Node<K, V> prev;

        /**
         * Constructs a new Node.
         * 
         * @param key the unique identifier.
         * @param value the value associated with the key.
         */
        Node(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    /**
     * An independent storage shard within the parent cache.
     * EVCacheLite is the router while CacheShard is the storage.
     * 
     * <p>
     * Each shard manages its own lifecycle, including LRU eviction, metrics, and
     * concurrency control through a {@link ReentrantLock}.
     * </p>
     * 
     * @param <K> the type of keys maintained by this cacheShard.
     * @param <V> the type of mapped values.
     */
    private static class CacheShard<K, V> {
        // Internal state marked final where possible to ensure visibility across
        // threads after construction
        final Map<K, Node<K, V>> map;
        final ReentrantLock shardLock = new ReentrantLock();
        static final Logger logger = LoggerFactory.getLogger(CacheShard.class);
        final int capacity;

        // Sentinel (Dummy Nodes) eliminate null checks in the LRU hot paths
        final Node<K, V> leftDummy;
        final Node<K, V> rightDummy;

        final String shardId;

        // Metrics for real-time operational monitoring
        final Counter hitCounter;
        final Counter missCounter;
        final Counter upSertCounter;
        final Counter evictionCounter;
        final Timer getTimer;
        final Timer putTimer;

        /**
         * Initializes a cache shard with capacity and dedicated metrics.
         * 
         * @param capacity the maximum number of entries before eviction.
         * @param shardId  the identifier used for metric tagging.
         * @param registry the Micrometer registry for telemetry.
         */
        CacheShard(int capacity, String shardId, MeterRegistry registry) {
            this.capacity = capacity;
            this.shardId = shardId;
            logger.info("[Shard {}] Initialized with capacity: {}", shardId, capacity);
            this.map = new ConcurrentHashMap<>(capacity);

            // Sentinel nodes point to the LRU (left) and MRU(right) ends of the list
            this.rightDummy = new Node<>(null, null);
            this.leftDummy = new Node<>(null, null);
            this.rightDummy.prev = this.leftDummy;
            this.leftDummy.next = this.rightDummy;

            // Initialize counters with shard specific tags for granular Datadog dashboards
            this.hitCounter = registry.counter("EVCacheLite.gets", "shard", shardId, "result", "hit");
            this.missCounter = registry.counter("EVCacheLite.gets", "shard", shardId, "result", "miss");
            this.upSertCounter = registry.counter("EVCacheLite.puts", "shard", shardId, "result", "upsert");
            this.evictionCounter = registry.counter("EVCacheLite.puts", "shard", shardId, "result", "eviction");

            // Timer captures p99/p99.9 latency to identify performance regressions
            this.getTimer = Timer.builder("EVCacheLite.get.latency").tag("shard", shardId)
                    .publishPercentiles(0.99, 0.999).register(registry);
            this.putTimer = Timer.builder("EVCacheLite.put.latency").tag("shard", shardId)
                    .publishPercentiles(0.99, 0.999).register(registry);
        }

        /**
         * Retrieves an entry from the shard.
         * 
         * <p>
         * <b>Concurrency Design:</b> This method acquires a writeLock since a cache hit
         * requires modifying the doubly-linked list to move the accessed node to the
         * Most Recently Used (right) position.
         * This assures the atomicity of the LRU state.
         * </p>
         * 
         * @param key the entry to look up.
         * @return the value associated with the key, or null on a miss.
         */
        public V get(K key) {
            // Recording the duration as early as possible increases metric accuracy
            return getTimer.record(() -> {
                if (key == null)
                    return null;

                // Without sharding our cache, this lock would create a global lock here
                // resulting in an absolute bottleneck, making the cache thread-safe but not scalable.
                shardLock.lock();
                Node<K, V> node = null;
                try {
                    if (map.containsKey(key)) {
                        this.hitCounter.increment();
                        logger.debug("[Shard {}] Cache Hit for key: {}", shardId, key);
                        node = map.get(key);
                        removeNodeInternal(node);
                        insertAtRightInternal(node);
                    } else {
                        this.missCounter.increment();
                        logger.debug("[Shard {}] Cache Miss for key: {}", shardId, key);
                        return null;
                    }
                } catch (Exception e) {
                    logger.error("[Shard {}] Critical failure during GET for key: {}", shardId, key, e);
                    throw e;
                } finally {
                    shardLock.unlock();
                }

                return node.value;
            });
        }

        /**
         * Upserts an entry and manages the shard's capacity.
         * 
         * <p>
         * If capacity is hit, the LRU entry at {@code leftDummy.next} is evicted to
         * make room for the new entry, preventing memory exhaustion.
         * </p>
         * 
         * @param key the key to store.
         * @param value the data associated with the key.
         * @return true if the method succeeded and false if failure occurred.
         */
        public boolean put(K key, V value) {
            // Why use a write lock immediately? Reading first from the map then acquiring a
            // write lock creates a race condition where another thread
            // could have evicted/modified the data in between.
            // Not having a read first here is a tradeoff. Consolidating into a write lock
            // is safer for atomicity but it does sacrifice read concurrency and creates
            // contention.
            return putTimer.record(() -> {
                shardLock.lock();
                try {
                    if (map.containsKey(key)) {
                        this.upSertCounter.increment();
                        logger.debug("[Shard {}] Cache Update for key: {}", shardId, key);
                        Node<K, V> currNode = map.get(key);
                        currNode.value = value;
                        removeNodeInternal(currNode);
                        insertAtRightInternal(currNode);
                        return true;
                    }

                    if (map.size() >= capacity) {
                        this.evictionCounter.increment();
                        Node<K, V> nodeToRemove = leftDummy.next;
                        logger.info("[Shard {}] evicting least recently used key: {}", shardId, nodeToRemove.key);
                        removeNodeInternal(nodeToRemove);
                        // Clean up the map as well to prevent memory leaks
                        map.remove(nodeToRemove.key);
                    }

                    Node<K, V> newNode = new Node<>(key, value);
                    map.put(key, newNode);
                    insertAtRightInternal(newNode);

                    return true;
                } catch (Exception e) {
                    logger.error("[Shard {}] Critical failure during PUT for key: {}", shardId, key, e);
                    throw e;
                } finally {
                    shardLock.unlock();
                }
            });
        }

        /**
         * Removes a node from the doubly-linked list.
         * 
         * @implNote Performs memory hygiene by nulling out pointers after removal
         *           to assist with defensive programming and GC efficiency.
         */
        private Node<K, V> removeNodeInternal(Node<K, V> node) {
            if (node != null) {
                Node<K, V> next = node.next;
                Node<K, V> prev = node.prev;
                prev.next = next;
                next.prev = prev;

                // Faster memory hygiene and defensive programming
                node.next = null;
                node.prev = null;
            }

            return node;
        }

        /**
         * Inserts a node at the Most Recently used (right) position
         */
        private void insertAtRightInternal(Node<K, V> node) {
            if (node != null) {
                Node<K, V> prev = rightDummy.prev;
                prev.next = node;
                rightDummy.prev = node;
                node.next = rightDummy;
                node.prev = prev;
            }
        }
    }

    // EVCacheLite is a sharded, in-memory cache utilizing consistent hashing.
    // Design Note:
    // - Decouples routing (HashRing) from storage (CacheShard).
    // - Remains agnostic to the observability stack through MeterRegistry (write
    // once, export anywhere).
    // - Optimizes for high concurrency through lock-stripping across shards.
    private final HashRing ring;
    private final int capacity;
    private final int DEFAULT_VIRTUAL_NODES = 150;
    // Why List.of? Its immutable and can never be updated elsewhere during runtime.
    private static final List<String> INTERNAL_SERVERS = List.of("cache-prod-001", "cache-prod-002", "cache-prod-003",
            "cache-prod-004");
    private final Map<String, CacheShard<K, V>> shards;

    /**
     * Flexible constructor allowing for custom shard counts (at most 4 in simulation).
     * Initializes the HashRing and distributes capacity across shards.
     */
    public EVCacheLite(int capacity, int numShards, MeterRegistry registry) {
        this.capacity = capacity;
        this.ring = new HashRing(DEFAULT_VIRTUAL_NODES);
        this.shards = new ConcurrentHashMap<>(capacity);
        int shardCapacity = this.capacity / numShards;

        logger.info("Initializing EVCacheLite with {} shards. Total capacity: {}. Shard capacity: {}", numShards,
                capacity, shardCapacity);
        for (int i = 0; i < numShards; i++) {
            String shardName = INTERNAL_SERVERS.get(i);
            String shardId = new StringBuilder().append(shardName).toString();
            this.shards.put(shardName, new CacheShard<>(shardCapacity, shardId, registry));
            // add to our HashRing
            ring.addServer(shardName);
        }
    }

    /**
     * Default constructor for standard production clusters.
     */
    public EVCacheLite(int capacity, MeterRegistry registry) {
        this(capacity, INTERNAL_SERVERS.size(), registry);
    }

    /**
     * Converts generic keys into stable strings for hashing.
     * 
     * <p>
     * If key is a custom object, we rely on the user's implementation, otherwise if
     * string contains '@', it is possible it is a default Object.toString(). Since this could be
     * tied to the object's memory address, it is unstable and can lead to cache leaks where the same key maps
     * to different shards across restarts.
     * </p>
     */
    private String keyExtractor(K key) {
        if (key == null) {
            throw new IllegalArgumentException("EVCacheLite: Null keys are not allowed.");
        }

        String stableKey = String.valueOf(key);

        if (stableKey.contains("@") && stableKey.contains(key.getClass().getName())) {
            logger.warn("Unstable key detected for class: {}.", key.getClass().getName());
        }

        return stableKey;
    }

    /**
     * Retrieves value from the routed shard
     * 
     * <p>
     * <b>Concurrency note:</b> We avoid double-locking here. The HashRing manages
     * its own reentrant read locks, and CacheShard handles its own internal state.
     * Adding a lock here would just increase CPU overhead and potential deadlock
     * risk.
     * </p>
     * 
     * @param key the entry to look up.
     * @return the value associated with the key, or null on a miss.
     */
    public V get(K key) {
        if (key == null || ring == null)
            return null;

        // There is a tiny window where we have a race condition
        // 1. Thread A calls getServer and gets "server-001".
        // 2. Thread B (an admin/autoscaler) calls removeServer("server-001"). The
        // writeLock is acquired, the server is removed, and the writeLock is released.
        // 3. Thread A continues and calls shards.get("server-001").
        // In a real system, we might rety once to get the new owner as the previous one
        // has been removed.
        String serverId = ring.getServer(keyExtractor(key));
        if (serverId == null) {
            logger.error("Routing failure: Could not find shard for key: {}", key);
            return null;
        }

        // We have successfully created lock stripping
        // With our 4 shards, 4 threads can perform put/get in parallel as long as they
        // hit different shards.
        CacheShard<K, V> shard = shards.get(serverId);
        // If null we should log as a high-priority alert
        if (shard == null) {
            logger.error("Routing failure: Could not find shard for key: {}", key);
            return null;
        }

        return shard.get(key);
    }

    /**
     * Places value into a specific shard based on the consistent hashRing.
     * 
     * <p>
     * <b>Replication Note:</b> The current implementation is 1 shard per server.
     * Future iterations should handle re-mapping
     * if a shard is decommissioned during a put. Also, to ensure high
     * availabilty, there could be an async background job
     * that replicates this put to the next N virtual nodes in the ring.
     * </p>
     * 
     * @param key the key to store.
     * @param value the data associated with the key.
     * @return true if the method succeeded and false if it did not.
     */
    public boolean put(K key, V value) {
        if (key == null || ring == null)
            return false;

        String serverId = ring.getServer(keyExtractor(key));
        if (serverId == null) {
            logger.error("Routing failure: Could not find shard for key: {}", key);
            return false;
        }

        CacheShard<K, V> shard = shards.get(serverId);
        if (shard == null) {
            logger.error("Routing failure: Shard mismatch for key {}", key);
            return false;
        }

        return shard.put(key, value);
    }
}