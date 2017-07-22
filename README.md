# blueis

[![Build Status](https://travis-ci.org/mogest/blueis.svg?branch=master)](https://travis-ci.org/mogest/blueis)

A redis-compatible disk-based database.

On memory-constrained systems, you might want a store with the simplicity of redis, but without keeping the whole
store loaded in memory.  blueis stores data in a sqlite3 database.  This makes it much, much slower than redis.

Note that blocking commands (BLPOP, BRPOP) do not guarantee delivery in the order they were called, unlike Redis.

blueis is wildly incomplete, still in development.

## Building it

You'll need Rust installed.

    cargo build

## Running it

If you want it to accept connections from other computers on the typical redis port:

    blueis 0.0.0.0:6379 /path/to/database.sqlite3

If the database doesn't exist, it'll create it.

## Supported commands

### List commands

 * BLPOP
 * BRPOP
 * LINDEX
 * LLEN
 * LPOP
 * LPUSH
 * LPUSHX
 * LRANGE
 * LSET
 * LTRIM
 * RPOP
 * RPOPLPUSH
 * RPUSH
 * RPUSHX

### Other commands

 * MONITOR
 * QUIT
