A distributed key-value store that runs across multiple nodes.

    Data storage is durable, i.e., the system does not lose data if a single node fails. Use replication for fault tolerance.
    The key-value store is to be built optimizing for read throughput. Accessing data concurrently from multiple replicas of the data is used to improve performance.
    Operations on the store are atomic. i.e., either the operation should succeed completely or fail altogether without any side effects. The Two-Phase Commit protocol is used to ensure atomic operations.

Multiple clients communicate with a single coordinating server in a given messaging format (KVMessage) using a client library (KVClient). The coordinator contains a write-through set-associative cache (KVCache), and it uses the cache to serve GET requests without going to the (slave) key-value servers it coordinates. The slave key-value servers are contacted for a GET only upon a cache miss on the coordinator. The coordinator uses the TPCMaster library to forward client requests for PUT and DEL to multiple key-value servers (KVServer) using TPCMessage and follow the 2PC protocol for atomic PUT and DEL operations across multiple key-value servers.
========================

A distributed key-value store system (Java and XML)
