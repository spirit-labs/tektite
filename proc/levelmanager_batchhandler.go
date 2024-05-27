package proc

import (
	"encoding/binary"
	"github.com/spirit-labs/tektite/encoding"
	"github.com/spirit-labs/tektite/errors"
	"github.com/spirit-labs/tektite/levels"
	"github.com/spirit-labs/tektite/mem"
	"github.com/spirit-labs/tektite/retention"
)

type levelManagerBatchHandler struct {
	levelManagerService *levels.LevelManagerService
}

func NewLevelManagerBatchHandler(mapperService *levels.LevelManagerService) BatchHandler {
	return &levelManagerBatchHandler{levelManagerService: mapperService}
}

func (m *levelManagerBatchHandler) HandleProcessBatch(_ Processor,
	processBatch *ProcessBatch, reprocess bool) (bool, *mem.Batch, []*ProcessBatch, error) {
	if processBatch.Barrier {
		return true, nil, nil, nil
	}
	levelManager, err := m.levelManagerService.GetMapper()
	if err != nil {
		return false, nil, nil, err
	}
	if levelManager == nil {
		return false, nil, nil, errors.New("cannot process levelManager batch, no levelManager on node")
	}
	processBatch.CheckDeserializeEvBatch(levels.CommandSchema)
	batch := processBatch.EvBatch
	bytes := batch.GetBytesColumn(0).Get(0)
	// Decode the command
	command := bytes[0]
	switch command {
	case levels.ApplyChangesCommand:
		regBatch := levels.RegistrationBatch{}
		regBatch.Deserialize(bytes, 1)
		return true, nil, nil, levelManager.ApplyChanges(regBatch, reprocess, processBatch.ReplSeq)
	case levels.RegisterPrefixRetentionsCommand:
		prefixRetentions, _ := retention.DeserializePrefixRetentions(bytes, 1)
		return true, nil, nil, levelManager.RegisterPrefixRetentions(prefixRetentions, reprocess, processBatch.ReplSeq)
	case levels.RegisterDeadVersionRangeCommand:
		versionRange := levels.VersionRange{}
		off := versionRange.Deserialize(bytes, 1)
		var clusterName string
		clusterName, off = encoding.ReadStringFromBufferLE(bytes, off)
		var clusterVersion uint64
		clusterVersion, _ = encoding.ReadUint64FromBufferLE(bytes, off)
		return true, nil, nil, levelManager.RegisterDeadVersionRange(versionRange, clusterName, int(clusterVersion),
			reprocess, processBatch.ReplSeq)
	case levels.StoreLastFlushedVersionCommand:
		lastFlushedVersion := int64(binary.LittleEndian.Uint64(bytes[1:]))
		return true, nil, nil, levelManager.StoreLastFlushedVersion(lastFlushedVersion, reprocess, processBatch.ReplSeq)
	default:
		panic("unknown command")
	}
}
