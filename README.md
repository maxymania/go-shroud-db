# go-shroud-db
**Unorthodox Key Value store (and less).**

[![GoDoc](https://godoc.org/github.com/maxymania/go-shroud-db/kvi?status.svg)](https://godoc.org/github.com/maxymania/go-shroud-db/kvi) Interfaces.

[![GoDoc](https://godoc.org/github.com/maxymania/go-shroud-db/batchr?status.svg)](https://godoc.org/github.com/maxymania/go-shroud-db/batchr) Backend (uses [badger](https://github.com/dgraph-io/badger))

- Shroud is a key-value store with an emphasis on asynchronous writes.
- Shroud seperates interface and implementation to enable modularity.
- Shroud is designed for applications, that insert, but don't update records.

Features:

- Read-After-Write consistent: As soon as `SetEntry()` returns, the inserted Entry is readable with `GetValue()` and `HasValue()`.
- Eventual Durability: `SetEntry()` returns as early as possible, whilst the Entry is persisted in the Background.
- Low write latency: Because the data is persisted in background, `SetEntry()` usually takes 10-20ms<sup>1</sup>, depending on write concurrency to return.

<sup>1</sup>Currently the backend uses `db.Batch(...)` instead of `db.Update(...)` on the transaction bypass cache. This hurts latency at single-threaded writes,
but improves performance at multi-threaded workloads.

How it works:

The backend is based on [badger](https://github.com/dgraph-io/badger) and [go-unstable/bbolt](https://github.com/maxymania/go-unstable/tree/master/bbolt).
Badger serves as the persistent datastore, and Bolt serves as the transaction bypass cache. Bolt is initialized with `NoSync` and the `DB_WriteSharedMmap`-flag,
this ensures low commit latency and high write throughput, at the expense of Durability. Writing Entries into a badger-database will cause a certain latency
until the data is visible to other readers <sup>1</sup>. Shroud is designed to work around this issue <sup>2</sup>. When a new Entry is inserted with
`SetEntry()`, the entry is written into the transaction bypass cache, rendering it instantly observable (and thus, readable). Then it is submitted to a worker
thread writing the Entry to the persistent datastore. Once the Entry is committed, it is evicted from the transaction bypass cache, as it can now be retrieved
from the persistent datastore.

<sup>1</sup>As Badger is a transactional datastore, updates and inserts become visible, only after the transaction is fully committed and durable.<br/>
<sup>2</sup>This issue is use-case specific. For most applications, transactional semantics are desired, which is: New Data is observable after it is durable.
