# EVCacheLite: High-Scale Data Platform Engine

**EVCacheLite** is a distributed, thread-safe, and resilient "Stowage" engine. It aims to solve one of the hardest problems in distributed systems, maintaining a high-throughput state without global contention.

---

### Goals

1.  **Distributed State:** Implement a **Consistent Hash Ring** with 150 virtual nodes to minimize Hot Shards.
2.  **Concurrency:** Leverage **Java 21 Virtual Threads** to handle thousands of concurrent requests without carrier thread pinning or thread pool exhaustion.
3.  **High Efficiency:** A thread-safe **LRU Eviction Policy** using `ConcurrentHashMap` and `ReentrantLock` for O(1) performance.
4.  **Resilience:** Integrated **Resilience4j Circuit Breakers** to prevent Thundering Herds from taking down Cassandra/DB clusters.

I used Lock Striping with ReentrantLock to provide Strong Consistency for the LRU state. I chose this over a lock-free approach because maintaining the integrity of the Doubly Linked List pointers during eviction is more critical than the minor latency hit of a write-lock, especially since I minimized contention by sharding the cache into segments.

---

### Core Architecture & Design Choices

#### 1. Why Instance over Static?

In a high throughput system, static state can hinder performance. By using instance-based scoping, I allow the data platform to manage multiple isolated cache objects (separate caches for Login vs. Metadata). This eliminates a global lock contention across the JVM. Inner classes (like `Node` and `CacheShard`) are explicitly marked static to decouple them from the parent EVCacheLite instance, preventing hidden pointers from creating memory leaks and ensuring the Garbage Collector can reclaim resources efficiently.

#### 2. ReentrantLock (RL) vs. Synchronized

I chose **RL** because 99% of Data Platform traffic is `get()` (Reads) and only 1% is `put()` (Writes).

- **Parallelism:** A standard `synchronized` block represents a line while a RL is like a library where a 100 people can read simultaneously. We only clear the room when someone writes, for example.
- **Virtual-Thread Friendly:** In Java 21, `synchronized` causes thread pinning (blocking the OS carrier thread) while RL allows virtual threads to unmount while waiting. This ends up freeing the CPU to work on other tasks.

#### 3. Lock Stripping & Atomicity

To achieve high availability, I implemented Lock Stripping. Instead of one global lock, the cache is segmented into 4 independent shards.

- **Atomicity:** I acquire the `writeLock` at the start of the `get()` logic. This ensures that the `removeNodeInternal` and `insertAtRightInternal` methods are a single atomic unit, preventing race conditions where another thread could evict a key mid-operation.
- **Reentrancy:** By using a Reentrant lock, our internal helper methods simply increment the hold-count rather than recalculating lock state, keeping the hot path incredibly fast.

---

### Distributed Routing: The Hash Ring

Traditional `hash(key) % N` is problematic at scale. If you add 1 node to a 10-node cluster, 90% of your data moves, causing a 0% hit rate and leading to a database thundering herd.

**Solution: Consistent Hashing**

- **The Theory:** I map both servers and keys onto a fixed circular 32-bit unsigned hash space (0 to 2^32-1). To find a key's owner, I move clockwise on the ring until I hit the first server.
- **Virtual Nodes:** I map each physical shard to 150 virtual nodes. This divides the load into uniform chunks, minimizing the chance of a single shard becoming a bottleneck.
- **TreeMap vs. HashMap:** I use a `TreeMap` for the ring because it maintains a sorted Red-Black tree. This allows us to use `ceilingEntry()` to find the owning shard in O(logn) time, which is more predictable than the periodic rebalancing spikes of a HashMap.

---

### Technical Specs & Observability

- **MurmurHash3:** Used because it’s a non-cryptographic hash optimized for speed and uniform distribution.
- **Observability:** Integrated Micrometer to track p99 latency, hit/miss/upsert ratios and eviction rates.

---

### Performance Proof

Using **CountDownLatch** simulations, my test suite proves that under a "Thundering Herd" of 50 simultaneous virtual threads:

1.  The **Circuit Breaker** trips at exactly the configured threshold to save the backend.
2.  The **Shard Locks** prevent deadlocks while allowing parallel reads.
3.  **Total Backend Fetches** remain deterministic, proving the Read-Through/Backfill logic is solid.

---

### Future Roadmap and Scalability

- **Request Coalescing:** I can ensure that if 100 threads miss on the same key, only the first thread performs the expensive backend fetch while the others await the same result.
- **Async Replication:** To ensure high availabilty, a background replication job could mirror every `put()` to the next N virtual nodes in the hash ring. This prevents data loss if the primary shard fails, allowing the ring to route to a replica.
- **Self-Healing:** Add automated re-mapping logic to handle node removal. If a shard becomes unresponsive, the system can detect the failure and trigger a failover mechanism to route requests to the next available healthy node on the ring.
- **TTLs:** Implement time-based expiration to remove stale entries and ensure data freshness.
