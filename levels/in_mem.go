package levels

import "time"

type InMemClient struct {
	LevelManager *LevelManager
}

func (c *InMemClient) StoreLastFlushedVersion(int64) error {
	return nil
}

func (c *InMemClient) LoadLastFlushedVersion() (int64, error) {
	return -1, nil
}

func (c *InMemClient) QueryTablesInRange(keyStart []byte, keyEnd []byte) (OverlappingTables, error) {
	return c.LevelManager.QueryTablesInRange(keyStart, keyEnd)
}

func (c *InMemClient) RegisterL0Tables(registrationBatch RegistrationBatch) error {
	ch := make(chan error, 1)
	c.LevelManager.RegisterL0Tables(registrationBatch, func(err error) {
		ch <- err
	})
	return <-ch
}

func (c *InMemClient) ApplyChanges(registrationBatch RegistrationBatch) error {
	return c.LevelManager.ApplyChanges(registrationBatch, false, -1)
}

func (c *InMemClient) RegisterDeadVersionRange(versionRange VersionRange, clusterName string, clusterVersion int) error {
	return c.LevelManager.RegisterDeadVersionRange(versionRange, clusterName, clusterVersion, false, -1)
}

func (c *InMemClient) PollForJob() (*CompactionJob, error) {
	type pollRes struct {
		job *CompactionJob
		err error
	}
	ch := make(chan pollRes, 1)
	c.LevelManager.pollForJob(-1, func(job *CompactionJob, err error) {
		ch <- pollRes{job, err}
	})
	res := <-ch
	return res.job, res.err
}

func (c *InMemClient) GetStats() (Stats, error) {
	return c.LevelManager.GetStats(), nil
}

func (c *InMemClient) RegisterSlabRetention(slabID int, retention time.Duration) error {
	return c.LevelManager.RegisterSlabRetention(slabID, retention, false, -1)
}

func (c *InMemClient) UnregisterSlabRetention(slabID int) error {
	return c.LevelManager.UnregisterSlabRetention(slabID, false, -1)
}

func (c *InMemClient) GetSlabRetention(slabID int) (time.Duration, error) {
	return c.LevelManager.GetSlabRetention(slabID)
}

func (c *InMemClient) Start() error {
	return nil
}

func (c *InMemClient) Stop() error {
	return nil
}
