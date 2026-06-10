package com.example;

import java.util.TreeMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.codec.digest.MurmurHash3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A consistent hashing implementation for deterministic data routing.
 * 
 * <p>
 * <b>Why TreeMap?</b> A {@link TreeMap} is used because it inherently maintains
 * sorted order through an underlying Red-Black Tree. This allows us to use
 * {@code ceilingEntry()} to find the owning shard in O(log n) time. 
 * While a HashMap is O(1), it suffers from performance spikes during rebalancing.
 * </p>
 * 
 * <p>
 * <b>Thread Safety:</b> Rule of Thumb — Ownership equals Responsibility. Since
 * this class owns the mutable TreeMap, it is responsible for protecting its
 * integrity.
 * We use a {@link ReentrantReadWriteLock} to maximize parallelism, allowing
 * concurrent reads while protecting the ring's state during shard scaling.
 * </p>
 */
public class HashRing {
    private final TreeMap<Long, String> ring = new TreeMap<>();
    private final int numOfVirtualNodes;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private static final Logger logger = LoggerFactory.getLogger(HashRing.class);

    /**
     * Initializes the routing ring.
     * 
     * @param numOfVirtualNodes The number of virtual nodes per shard.
     *                          Higher numbers ensure more uniform distribution.
     */
    public HashRing(int numOfVirtualNodes) {
        this.numOfVirtualNodes = numOfVirtualNodes;
        logger.info("HashRing initialized with {} virtual nodes per physical shard.", numOfVirtualNodes);
    }

    /**
     * Maps a key to a 32-bit unsigned hash space (0 to 2^32-1).
     * 
     * <p>
     * <b>Why MurmurHash3?</b> It's a non-cryptographic hash optimized for speed
     * and uniform distribution leading to low collision rates. Unlike MD5, it
     * avoids expensive cryptographic computations.
     * </p>
     * 
     * @param key the string key to be hashed.
     * @return A 64-bit long representing the unsigned 32-bit hash value.
     */
    private long hash(String key) {
        // x86 variant is optimized for low-latency hashing
        int hash = MurmurHash3.hash32x86(key.getBytes());

        // Handle signed bit to ensure we stay in the positive ring space
        return (long) hash & 0xFFFFFFFFL;
    }

    /**
     * Adds a physical server to the ring by creating virtual nodes.
     * 
     * <p>
     * We use a {@link StringBuilder} with {@code setLength(0)}
     * to reuse the same buffer across all virtual nodes, minimizing object
     * allocation and GC pressure during ring expansion.
     * </p>
     * 
     * @param serverName the physical shard identifier.
     * @return true if the server was successfully added.
     */
    public boolean addServer(String serverName) {
        if (serverName == null || serverName.isEmpty())
            return false;

        StringBuilder sb = new StringBuilder();
        lock.writeLock().lock();
        try {
            for (int i = 0; i < numOfVirtualNodes; i++) {
                // Buffer reuse: setLength(0) is pure profit for small hashing keys.
                // We keep the same underlying array instead of allocating 150+ new ones.
                sb.setLength(0);
                String updatedName = sb.append(serverName).append(":").append(i).toString();
                ring.put(hash(updatedName), serverName);
            }

            logger.info("Successfully added physical shard [{}] to the ring.", serverName);
            return true;
        } catch (Exception e) {
            logger.error("Failed to add server [{}] to HashRing", serverName, e);
            return false;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes a server and all its virtual nodes from the ring.
     * 
     * <p>
     * <b>Defensive Programming:</b> Prep work happens outside the lock.
     * We only hold the write lock for the absolute minimum time required to
     * update the shared TreeMap.
     * </p>
     * 
     * @param serverName the physical shard identifier to remove
     * @return true if at least one of the virtual nodes was removed
     */
    public boolean removeServer(String serverName) {
        if (serverName == null || serverName.isEmpty())
            return false;

        boolean removedAtLeastOne = false;

        // You must loop and remove the exact same number of virtual nodes too
        StringBuilder sb = new StringBuilder();
        lock.writeLock().lock();
        try {
            for (int i = 0; i < numOfVirtualNodes; i++) {
                sb.setLength(0);
                String updatedName = sb.append(serverName).append(":").append(i).toString();

                if (ring.remove(hash(updatedName)) != null) {
                    if (!removedAtLeastOne)
                        removedAtLeastOne = true;
                }
            }

            if (removedAtLeastOne) {
                logger.info("Successfully removed physical shard [{}] from the ring.", serverName);
            } else {
                logger.warn("Attempted to remove shard [{}] but it was not found on the ring.", serverName);
            }

            return removedAtLeastOne;

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Finds the owning shard for a given key.
     * 
     * <p>
     * <b>Virtual-Thread Friendly:</b>
     * Explicit locks allow OS carrier threads
     * to unmount when blocked, maximizing CPU efficiency. We perform the hash
     * math outside the lock to keep the protected critical section as fast as
     * possible.
     * </p>
     * 
     * @param key the key to route.
     * @return The name of the server responsible for this key, or null if key or ring is
     *         empty.
     */
    public String getServer(String key) {
        if (key == null || key.isEmpty()) {
            return null;
        }

        // Why isnt this in our readlock?
        // It does not care if 1000 threads are calling it at the same time. The hash
        // function is idempotent.
        // You allow threads to do the heavy math outside of our lock. You want the
        // logic inside the lock to be as simple as possible allowing us to maximize
        // parallelism.
        long keyHash = hash(key);

        // Unlike synchronized, ReentrantReadWriteLock is virtual-thread friendly. If a
        // thread is blocked waiting for the lock, it will unmount from the carrier
        // thread, allowing the CPU to work on other requests. This is why we chose 
        // explicit locks over intrinsic monitors.
        lock.readLock().lock();
        try {
            // Why check if the ring is empty here? Ring couldve changed outside of our
            // lock.
            if (ring.isEmpty()) {
                logger.warn("Routing request for key [{}] failed: HashRing is empty.", key);
                return null;
            }

            // Now move clockwise until we hit the owning server
            // ceilingEntry gives us the first entry >= keyHash in O(logn) time
            Map.Entry<Long, String> entry = ring.ceilingEntry(keyHash);
            String server = null;

            // If the entry is null, it means the keyHash is larger than any server hash.
            // On a circle, that means we loop back to the very first server
            if (entry == null) {
                server = ring.firstEntry().getValue();
                logger.debug("Key [{}] (hash: {}) wrapped around to first shard: {}", key, keyHash, server);
            } else {
                server = entry.getValue();
                logger.debug("Key [{}] (hash: {}) routed to shard: {}", key, keyHash, server);
            }
            return server;

        } finally {
            lock.readLock().unlock();
        }
    }
}
