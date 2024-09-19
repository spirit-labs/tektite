# Overview of the storage engine
# Overview
The storage engine is backed by an LSM based key-value store built on top of object storage services like S3, GCS, etc.

# Memtables
Like traditional disk-based LSM engines, memtables serve as the entrypoint for incoming write operations.
Tektite's memtable has a maximum memory size controlled by a region based memory allocator (the `arena`).
The arena allocates a fixed size byte array and tracks entries via offsets in the byte array.

Data within the memtable is maintained in sorted order via a skiplist.
Skiplists have many desirable properties over traditional sorted structures in the context of databases.
One such is that they lend themselves more easily to concurrent access.
The skiplist in tektite's memtable is based on the one from BadgerDB.

Data is written to the memtable in batches. Keys in these batches are maintained by a [linked hash map](../mem/linked_kvmap.go).
This linked hash map structure assumes that keys are suffixed by an 8 byte version number, which is truncated so that
it only stores the latest version of the key.

# Processor Store
The [ProcessorStore](../proc/storage.go) is the layer that interacts with memtables, object storage, and the level manager.
Each processor embeds a `ProcessorStore`, and upon initialization kicks off a goroutine that watches a channel for memtables that need to be flushed.
When flushing, the processor will build an SSTable, push it to object storage, and register it with the level manager.
A successful flush will trigger a set of callbacks. 
Also, memtables are flushed on a configurable schedule, which by default is 30 seconds, so that they don't stay in memory too long to avoid data loss.

Each processor holds a reference to a processor manager which maintains the cluster version, which is a monotonically increasing integer.
Similarly, each processor maintains their own current version and last processed version.
After the processor builds and pushes the SSTable to object storage, the processor manager sends the processor's version, along with the cluster version it holds to the version manager.
The version manager also maintains versions and when it receives versions it validates them and updates and broadcasts new versions accordingly.
