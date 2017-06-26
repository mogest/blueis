# blueis

A redis-compatible disk-based database.

On memory-constrained systems, you might want a store with the simplicity of redis, but without keeping the whole
store loaded in memory.  blueis stores data in a sqlite3 database.  This makes it much, much slower than redis.

blueis is wildly incomplete, still in development.
