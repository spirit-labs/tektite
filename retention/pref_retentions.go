package retention

import (
	"bytes"
	"fmt"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/errors"
	log "github.com/spirit-labs/tektite/logger"
	"sync"
	"sync/atomic"
	"time"
)

type PrefixRetentionsService struct {
	cfg                     *conf.Config
	levelMgrClient          retentionsProvider
	prefixes                atomic.Pointer[[]PrefixRetention]
	versionPendingPrefixMap map[string]pendingPrefix
	currWriteVersion        int64
	prevFlushedVersion      int64
	loadTimer               *common.TimerHandle
	lock                    sync.Mutex
	started                 bool
	needsVersionCorrection  bool
}

type retentionsProvider interface {
	GetPrefixRetentions() ([]PrefixRetention, error)
	RegisterPrefixRetentions(prefixes []PrefixRetention) error
}

type pendingPrefix struct {
	prefixRetention PrefixRetention
	version         int64
}

func NewPrefixRetentionsService(levelMgrClient retentionsProvider, cfg *conf.Config) *PrefixRetentionsService {
	return &PrefixRetentionsService{
		levelMgrClient:          levelMgrClient,
		versionPendingPrefixMap: map[string]pendingPrefix{},
		currWriteVersion:        -1,
		prevFlushedVersion:      -1,
		cfg:                     cfg,
	}
}

func (d *PrefixRetentionsService) Start() error {
	// We load without lock held as SetVersions can come in while we are retrying load
	if err := d.loadPrefixRetentionsWithRetry(); err != nil {
		return err
	}
	d.lock.Lock()
	defer d.lock.Unlock()
	d.started = true
	d.scheduleLoadPrefixRetentionsTimer(true)
	return nil
}

func (d *PrefixRetentionsService) Stop() error {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.loadTimer != nil {
		d.loadTimer.Stop()
	}
	d.started = false
	return nil
}

func (d *PrefixRetentionsService) getPrefixRetentions() []PrefixRetention {
	return *d.prefixes.Load()
}

func (d *PrefixRetentionsService) GetMatchingPrefixRetentions(rangeStart []byte, rangeEnd []byte, zeroTimeOnly bool) []PrefixRetention {
	prefixRetentions := d.getPrefixRetentions()
	var matching []PrefixRetention
	for _, prefixRetention := range prefixRetentions {
		if zeroTimeOnly && prefixRetention.Retention != 0 {
			continue
		}
		if DoesPrefixApplyToTable(prefixRetention.Prefix, rangeStart, rangeEnd) {
			matching = append(matching, prefixRetention)
		}
	}
	return matching
}

func (d *PrefixRetentionsService) AddPrefixRetention(prefixRetention PrefixRetention) {
	// doesn't get called frequently so ok to use mutex
	d.lock.Lock()
	defer d.lock.Unlock()
	prefixRetentions := d.getPrefixRetentions()
	prefixRetentions = append(prefixRetentions, prefixRetention)
	d.prefixes.Store(&prefixRetentions)
	var versiontoUse int64
	if d.currWriteVersion == -1 {
		// We haven't got a version broadcast yet, we will correct this when the first one arrives
		versiontoUse = -1
		d.needsVersionCorrection = true
	} else {
		// we could theoretically have current write version and current write version + 1 in progress
		versiontoUse = d.currWriteVersion + 1
	}
	d.versionPendingPrefixMap[string(prefixRetention.Prefix)] = pendingPrefix{
		prefixRetention: prefixRetention,
		version:         versiontoUse,
	}
}

func (d *PrefixRetentionsService) SetVersions(writeVersion int64, flushedVersion int64) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.currWriteVersion = writeVersion
	if d.needsVersionCorrection {
		// We had prefix(es) added before we got first version, we can now set the correct version on them
		for pref, pendingPref := range d.versionPendingPrefixMap {
			if pendingPref.version == -1 {
				pendingPref.version = writeVersion
				d.versionPendingPrefixMap[pref] = pendingPref
			}
		}
		d.needsVersionCorrection = false
	}
	d.versionFlushed(flushedVersion)
}

func (d *PrefixRetentionsService) HasPendingPrefixesToRegister() bool {
	d.lock.Lock()
	defer d.lock.Unlock()
	return len(d.versionPendingPrefixMap) > 0
}

// VersionFlushed - We register prefix retentions only after all data from before the point when the prefix retention
// was logically deleted has been flushed. This is because, if we registered the prefix with the level manager
// straightaway there might be un-flushed data with that prefix in the local store. Then the level manager might delete
// the prefix after it has determined there is no flushed data for the prefix, then the PrefixRetentions service might
// reload prefix retentions from the level manager, and then queries would be able to see the un-flushed deleted data.
func (d *PrefixRetentionsService) versionFlushed(flushedVersion int64) {
	if flushedVersion == d.prevFlushedVersion {
		return
	}
	if flushedVersion < d.prevFlushedVersion {
		panic(fmt.Sprintf("flushedVersion went back:%d previous:%d", flushedVersion, d.prevFlushedVersion))
	}
	var deletedPrefixes []PrefixRetention
	for _, pendingPrefix := range d.versionPendingPrefixMap {
		if flushedVersion >= pendingPrefix.version {
			deletedPrefixes = append(deletedPrefixes, pendingPrefix.prefixRetention)
		}
	}
	if len(deletedPrefixes) > 0 {
		if err := d.levelMgrClient.RegisterPrefixRetentions(deletedPrefixes); err != nil {
			// If we fail to register, e.g. due to an unavailability error, then it's ok, it will be retried the next
			// time a version is flushed. If the node crashes before registering that is ok too, when the stream
			// is redeployed on startup, it will register again. If it registers again, and it has already been
			// registered that is ok too. The prefix will soon be removed on the server, when there is no data for it.
			msg := fmt.Sprintf("failed to register prefix retentions: %v", err)
			if common.IsUnavailableError(err) {
				log.Warn(msg)
			} else {
				log.Error(msg)
			}
			return
		}
	}
	// remove the prefixes
	for _, prefixRetention := range deletedPrefixes {
		delete(d.versionPendingPrefixMap, string(prefixRetention.Prefix))
	}
	d.prevFlushedVersion = flushedVersion
}

func (d *PrefixRetentionsService) scheduleLoadPrefixRetentionsTimer(first bool) {
	d.loadTimer = common.ScheduleTimer(d.cfg.PrefixRetentionRefreshInterval, first, func() {
		d.lock.Lock()
		defer d.lock.Unlock()
		if !d.started {
			return
		}
		if err := d.loadPrefixRetentions(); err != nil {
			log.Errorf("failed to load prefix retentions %v", err)
			return
		}
		d.scheduleLoadPrefixRetentionsTimer(false)
	})
}

func (d *PrefixRetentionsService) loadPrefixRetentionsWithRetry() error {
	for {
		err := d.loadPrefixRetentionsWithLock()
		if err == nil {
			return nil
		}
		var terr errors.TektiteError
		if errors.As(err, &terr) {
			if terr.Code == errors.Unavailable || terr.Code == errors.LevelManagerNotLeaderNode {
				log.Debugf("failed to load prefix retentions, will retry %v", err)
				time.Sleep(d.cfg.LevelManagerRetryDelay)
				continue
			}
		}
		return err
	}
}

func (d *PrefixRetentionsService) loadPrefixRetentionsWithLock() error {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.loadPrefixRetentions()
}

func (d *PrefixRetentionsService) loadPrefixRetentions() error {
	prefixRetentions, err := d.levelMgrClient.GetPrefixRetentions()
	if err != nil {
		return err
	}
	// we don't overwrite any prefixes pending to be registered
	newPrefixes := make([]PrefixRetention, 0, len(prefixRetentions))
	for _, prefixRetention := range prefixRetentions {
		_, pending := d.versionPendingPrefixMap[string(prefixRetention.Prefix)]
		if !pending {
			newPrefixes = append(newPrefixes, prefixRetention)
		}
	}
	for _, pending := range d.versionPendingPrefixMap {
		newPrefixes = append(newPrefixes, pending.prefixRetention)
	}
	d.prefixes.Store(&newPrefixes)
	return nil
}

func DoesPrefixApplyToTable(prefix []byte, rangeStart []byte, rangeEnd []byte) bool {
	lp := len(prefix)
	ls := len(rangeStart)
	min := lp
	if ls < lp {
		min = ls
	}
	if bytes.Compare(prefix[:min], rangeStart[:min]) < 0 {
		// prefix is smaller than beginning of range
		return false
	}
	le := len(rangeEnd)
	min = lp
	if le < lp {
		min = le
	}
	if bytes.Compare(prefix[:min], rangeEnd[:min]) > 0 {
		// prefix is smaller than beginning of range
		return false
	}
	return true
}

type PrefixRetention struct {
	Prefix    []byte
	Retention uint64
}

func (pr *PrefixRetention) Serialize(bytes []byte) []byte {
	bytes = encoding.AppendUint32ToBufferLE(bytes, uint32(len(pr.Prefix)))
	bytes = append(bytes, pr.Prefix...)
	bytes = encoding.AppendUint64ToBufferLE(bytes, pr.Retention)
	return bytes
}

func (pr *PrefixRetention) Deserialize(bytes []byte, offset int) int {
	var lp uint32
	lp, offset = encoding.ReadUint32FromBufferLE(bytes, offset)
	pr.Prefix = common.CopyByteSlice(bytes[offset : offset+int(lp)])
	offset += int(lp)
	var ret uint64
	ret, offset = encoding.ReadUint64FromBufferLE(bytes, offset)
	pr.Retention = ret
	return offset
}

func SerializePrefixRetentions(bytes []byte, prefixRetentions []PrefixRetention) []byte {
	bytes = encoding.AppendUint32ToBufferLE(bytes, uint32(len(prefixRetentions)))
	for _, pr := range prefixRetentions {
		bytes = pr.Serialize(bytes)
	}
	return bytes
}

func DeserializePrefixRetentions(bytes []byte, offset int) ([]PrefixRetention, int) {
	var nr uint32
	nr, offset = encoding.ReadUint32FromBufferLE(bytes, offset)
	inr := int(nr)
	prefixRetentions := make([]PrefixRetention, inr)
	for i := 0; i < inr; i++ {
		offset = prefixRetentions[i].Deserialize(bytes, offset)
	}
	return prefixRetentions, offset
}
