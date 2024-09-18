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
