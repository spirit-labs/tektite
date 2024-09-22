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

# Level Manager
After successfully pushing the SSTable to object storage, it is added to the table cache, and then registered with the Level Manager.
It is first registered at the first level, `L0` as a [RegistrationBatch](../levels/structs.go) which includes metadata like the cluster version, processor id, min/max version, key start/end, etc.
The level manager will do some initial validation of the cluster version to guard against network partitions, then replicate the registration command.
As part of the registration process, the level manager first applies deregistrations, then registrations, but when writing new data, the registration batch won't have any deregistrations, but if running as part of a compaction job, it might.

When applying registrations, it first creates a [TableEntry](../levels/structs.go) with metadata from the registration batch, ie
```go
tabEntry := &TableEntry{
SSTableID:        registration.TableID,
RangeStart:       registration.KeyStart,
RangeEnd:         registration.KeyEnd,
MinVersion:       registration.MinVersion,
MaxVersion:       registration.MaxVersion,
DeleteRatio:      registration.DeleteRatio,
AddedTime:        registration.AddedTime,
NumEntries:       registration.NumEntries,
Size:             registration.TableSize,
NumPrefixDeletes: registration.NumPrefixDeletes,
}
```
This will be used to determine the appropriate level in the LSM.
A new SSTable will be registered in `L0`, but a compaction job might create entries in higher levels.
The level manager uses segments to organize table entries in the various levels. A segment is a slice of table entries.
Since `L0` can have overlapping table key ranges, but higher levels cannot, there is only one segment in `L0`.
The level manager will grow the `L0` segment as needed.

If the table entry is destined for a higher level, it will find the appropriate segment to add it to.
If the new table were to exceed the max number of table entries, it will create a new one, possibly splitting the current one into, or merging it with the next one.

Afterwards, the master record is updated.

