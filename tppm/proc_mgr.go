//go:build !main

package tppm

import (
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/expr"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/opers"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/tabcache"
	"time"
)

func TestProcessorManager(cfg *conf.Config) (proc.Manager, opers.StreamManager, clustmgr.StateManager) {
	clustMgr := clustmgr.NewLocalStateManager(cfg.ProcessorCount + 1)
	streamMgr := opers.NewStreamManager(nil, &testSlabRetentions{},
		&expr.ExpressionFactory{}, cfg, true)
	if err := streamMgr.Start(); err != nil {
		panic(err)
	}
	objStoreClient := dev.NewInMemStore(0)
	levelMgrClient := proc.NewLevelManagerLocalClient(cfg)
	tableCache, err := tabcache.NewTableCache(objStoreClient, cfg)
	if err != nil {
		panic(err)
	}
	pm := proc.NewProcessorManagerWithFailure(clustMgr, streamMgr, cfg, nil, func(processorID int) proc.BatchHandler {
		return streamMgr
	}, nil, &testIngestNotifier{}, objStoreClient, levelMgrClient, tableCache, false)

	return pm, streamMgr, clustMgr
}

type testSlabRetentions struct {
}

func (t testSlabRetentions) RegisterSlabRetention(slabID int, retention time.Duration) error {
	return nil
}

func (t testSlabRetentions) UnregisterSlabRetention(slabID int) error {
	return nil
}

type testIngestNotifier struct {
}

func (t *testIngestNotifier) StopIngest() error {
	return nil
}

func (t *testIngestNotifier) StartIngest(int) error {
	return nil
}
