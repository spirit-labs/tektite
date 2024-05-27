package levels

import "github.com/spirit-labs/tektite/retention"

type InMemClient struct {
	LevelManager *LevelManager
}

func (c *InMemClient) StoreLastFlushedVersion(int64) error {
	return nil
}

func (c *InMemClient) LoadLastFlushedVersion() (int64, error) {
	return -1, nil
}

func (c *InMemClient) GetTableIDsForRange(keyStart []byte, keyEnd []byte) (OverlappingTableIDs, uint64, []VersionRange, error) {
	return c.LevelManager.GetTableIDsForRange(keyStart, keyEnd)
}

func (c *InMemClient) GetPrefixRetentions() ([]retention.PrefixRetention, error) {
	return c.LevelManager.GetPrefixRetentions()
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

func (c *InMemClient) RegisterPrefixRetentions(prefixRetentions []retention.PrefixRetention) error {
	return c.LevelManager.RegisterPrefixRetentions(prefixRetentions, false, -1)
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

func (c *InMemClient) Start() error {
	return nil
}

func (c *InMemClient) Stop() error {
	return nil
}
