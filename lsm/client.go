package lsm

import (
	"time"
)

type Client interface {
	QueryTablesInRange(keyStart []byte, keyEnd []byte) (OverlappingTables, error)

	ApplyChanges(registrationBatch RegistrationBatch) (bool, error)

	PollForJob() (*CompactionJob, error)

	RegisterDeadVersionRange(versionRange VersionRange) error

	StoreLastFlushedVersion(version int64) error

	LoadLastFlushedVersion() (int64, error)

	GetStats() (Stats, error)

	RegisterSlabRetention(slabID int, retention time.Duration) error

	UnregisterSlabRetention(slabID int) error

	GetSlabRetention(slabID int) (time.Duration, error)

	Start() error

	Stop() error
}

type ClientFactory interface {
	CreateLevelManagerClient() Client
}
