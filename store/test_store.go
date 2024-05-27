package store

import (
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/levels"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/retention"
	"github.com/spirit-labs/tektite/tabcache"
)

func TestStore() *Store {
	cloudStore := dev.NewInMemStore(0)
	cfg := conf.Config{}
	cfg.ApplyDefaults()
	lmClient := &levels.InMemClient{}
	bi := testCommandBatchIngestor{}
	tabCache, err := tabcache.NewTableCache(cloudStore, &cfg)
	if err != nil {
		panic(err)
	}
	levelManager := levels.NewLevelManager(&cfg, cloudStore, tabCache, bi.ingest, false, false, false)
	lmClient.LevelManager = levelManager
	bi.lm = levelManager
	if err := levelManager.Start(true); err != nil {
		panic(err)
	}
	err = levelManager.Activate()
	if err != nil {
		panic(err)
	}
	tableCache, err := tabcache.NewTableCache(cloudStore, &cfg)
	if err != nil {
		panic(err)
	}
	prefixRetentionsService := retention.NewPrefixRetentionsService(lmClient, &cfg)
	if err := prefixRetentionsService.Start(); err != nil {
		panic(err)
	}
	store := NewStore(cloudStore, lmClient, tableCache, prefixRetentionsService, cfg)
	return store
}

type testCommandBatchIngestor struct {
	lm *levels.LevelManager
}

func (tcbi *testCommandBatchIngestor) ingest(buff []byte, complFunc func(error)) {
	regBatch := levels.RegistrationBatch{}
	regBatch.Deserialize(buff, 1)
	go func() {
		err := tcbi.lm.ApplyChanges(regBatch, false, 0)
		complFunc(err)
	}()
}
