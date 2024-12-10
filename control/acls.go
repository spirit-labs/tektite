package control

import (
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/acls"
	"github.com/spirit-labs/tektite/asl/encoding"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/parthash"
	"github.com/spirit-labs/tektite/queryutils"
	"github.com/spirit-labs/tektite/sst"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type AclManager struct {
	lock          sync.RWMutex
	started       bool
	loaded        bool
	stopping      atomic.Bool
	tableGetter   sst.TableGetter
	kvWriter      kvWriter
	querier       queryutils.Querier
	aclEntries    map[int64]acls.AclEntry
	partHash      []byte
	entrySequence int64
}

type kvWriter func([]common.KV) error

const (
	aclDataVersion       uint16 = 1
	wildcardResourceName        = "*"
)

func NewAclManager(tableGetter sst.TableGetter, kvWriter kvWriter, querier queryutils.Querier) (*AclManager, error) {
	partHash, err := parthash.CreateHash([]byte("acls"))
	if err != nil {
		return nil, err
	}
	return &AclManager{
		partHash:    partHash,
		tableGetter: tableGetter,
		kvWriter:    kvWriter,
		querier:     querier,
		aclEntries:  map[int64]acls.AclEntry{},
	}, nil
}

func (m *AclManager) Start() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.started {
		return nil
	}
	m.started = true
	return nil
}

func (m *AclManager) Stop() error {
	m.stopping.Store(true)
	m.lock.Lock()
	defer m.lock.Unlock()
	if !m.started {
		return nil
	}
	m.started = false
	return nil
}

func (a *AclManager) Authorise(principal string, resourceType acls.ResourceType, resourceName string,
	operation acls.Operation, clientIP string) (bool, error) {
	// List acls with match
	entries, err := a.ListAcls(resourceType, resourceName, acls.ResourcePatternTypeMatch, principal, clientIP,
		operation, acls.PermissionAny)
	if err != nil {
		return false, err
	}
	if len(entries) == 0 {
		// No ACLs means deny
		return false, nil
	}
	for _, entry := range entries {
		if entry.Permission == acls.PermissionDeny {
			// Deny overrides allow
			return false, nil
		}
	}
	return true, nil
}

func (a *AclManager) CreateAcls(aclEntries []acls.AclEntry) error {
	a.lock.Lock()
	defer a.lock.Unlock()
	if !a.started {
		return errors.New("AclManager is not started")
	}
	if !a.loaded {
		if err := a.load(); err != nil {
			return err
		}
	}
	// First, weed out any duplicates
	aclEntries = a.removeDuplicates(aclEntries)
	entriesToAdd := map[int64]acls.AclEntry{}
	var kvs []common.KV
	for _, aclEntry := range aclEntries {
		if err := validateCreatedAcl(aclEntry); err != nil {
			return err
		}
		seq := a.entrySequence
		a.entrySequence++
		key := a.createAclKey(seq)
		// Encode a version number before the data
		value := binary.BigEndian.AppendUint16(nil, aclDataVersion)
		value = aclEntry.Serialize(value)
		value = common.AppendValueMetadata(value)
		kvs = append(kvs, common.KV{Key: key, Value: value})
		log.Infof("ck: %v", key)
		entriesToAdd[seq] = aclEntry
	}
	if err := a.kvWriter(kvs); err != nil {
		return err
	}
	for seq, entry := range entriesToAdd {
		a.aclEntries[seq] = entry
	}
	return nil
}

func (a *AclManager) removeDuplicates(aclEntries []acls.AclEntry) []acls.AclEntry {
	uniqueMap := make(map[acls.AclEntry]struct{})
	var dedupped1 []acls.AclEntry
	// First dedup from incoming
	for _, entry := range aclEntries {
		if _, exists := uniqueMap[entry]; !exists {
			uniqueMap[entry] = struct{}{}
			dedupped1 = append(dedupped1, entry)
		}
	}
	var dedupped2 []acls.AclEntry
	for _, entry := range dedupped1 {
		// check for dup already created
		exists := false
		for _, aclEntry := range a.aclEntries {
			if entry == aclEntry {
				exists = true
				break
			}
		}
		if !exists {
			dedupped2 = append(dedupped2, entry)
		}
	}
	return dedupped2
}

func validateCreatedAcl(entry acls.AclEntry) error {
	// Some ACL fields values are only used in filters - never in actual stored acls
	if entry.Permission != acls.PermissionAllow && entry.Permission != acls.PermissionDeny {
		return errors.Errorf("ACL entry has invalid permission: %d", entry.Permission)
	}
	if entry.Operation == acls.OperationAny || entry.Operation == acls.OperationUnknown {
		return errors.Errorf("ACL entry has invalid operation: %d", entry.Operation)
	}
	if entry.Host != "*" {
		ip := net.ParseIP(entry.Host)
		if ip == nil {
			return errors.Errorf("ACL entry has invalid host: %s - must be valid IP address", entry.Host)
		}
	}
	if !strings.HasPrefix(entry.Principal, "User:") {
		return errors.Errorf("ACL principals must start with 'User:': %s", entry.Principal)
	}
	if entry.ResourcePatternType != acls.ResourcePatternTypeLiteral && entry.ResourcePatternType != acls.ResourcePatternTypePrefixed {
		return errors.Errorf("ACL resource pattern type must be literal or prefixed: %d", entry.ResourcePatternType)
	}
	if strings.TrimSpace(entry.ResourceName) == "" {
		return errors.Errorf("ACL entry resource name cannot be blank: %s", entry.ResourceName)
	}
	if entry.ResourceType == acls.ResourceTypeAny || entry.ResourceType == acls.ResourceTypeUnknown {
		return errors.Errorf("ACL entry resource type cannot be any or unknown: %d", entry.ResourceType)
	}
	return nil
}

func (a *AclManager) createAclKey(seq int64) []byte {
	key := common.ByteSliceCopy(a.partHash)
	key = encoding.KeyEncodeInt(key, seq)
	return encoding.EncodeVersion(key, 0)
}

func (a *AclManager) DeleteAcls(resourceType acls.ResourceType, resourceNameFilter string,
	patternTypeFilter acls.ResourcePatternType, principal string, host string, operation acls.Operation,
	permission acls.Permission) error {
	a.lock.Lock()
	defer a.lock.Unlock()
	if !a.started {
		return errors.New("AclManager is not started")
	}
	if !a.loaded {
		if err := a.load(); err != nil {
			return err
		}
	}
	var kvs []common.KV
	var seqsToRemove []int64
	for seq, existing := range a.aclEntries {
		if matches(resourceType, resourceNameFilter, patternTypeFilter, principal, host, operation, permission,
			existing) {
			key := a.createAclKey(seq)
			log.Infof("dk: %v", key)
			kvs = append(kvs, common.KV{Key: key, Value: nil})
			seqsToRemove = append(seqsToRemove, seq)
		}
	}
	if len(kvs) == 0 {
		// Not deleting anything is not an error
		return nil
	}
	if err := a.kvWriter(kvs); err != nil {
		return err
	}
	for _, seq := range seqsToRemove {
		delete(a.aclEntries, seq)
	}
	return nil
}

func (a *AclManager) ListAcls(resourceType acls.ResourceType, resourceNameFilter string,
	patternTypeFilter acls.ResourcePatternType, principal string, host string, operation acls.Operation,
	permission acls.Permission) ([]acls.AclEntry, error) {
	a.lock.RLock()
	defer a.lock.RUnlock()
	if !a.started {
		return nil, errors.New("AclManager is not started")
	}
	if !a.loaded {
		a.lock.RUnlock()
		if err := a.loadWithLock(); err != nil {
			return nil, err
		}
		a.lock.RLock()
	}
	var aclEntries []acls.AclEntry
	for _, entry := range a.aclEntries {
		if matches(resourceType, resourceNameFilter, patternTypeFilter, principal, host, operation, permission, entry) {
			aclEntries = append(aclEntries, entry)
		}
	}
	return aclEntries, nil
}

func (a *AclManager) AclCount() int {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return len(a.aclEntries)
}

func matches(resourceType acls.ResourceType, resourceNameFilter string,
	patternTypeFilter acls.ResourcePatternType, principal string, host string, operation acls.Operation,
	permission acls.Permission, entry acls.AclEntry) bool {
	if !matchesResource(resourceType, resourceNameFilter, patternTypeFilter, entry) {
		return false
	}
	if principal != "" && principal != entry.Principal {
		return false
	}
	if host != "" && entry.Host != "*" && host != entry.Host {
		return false
	}
	if operation != acls.OperationAny && entry.Operation != acls.OperationAll && operation != entry.Operation {
		return false
	}
	if permission != acls.PermissionAny && permission != entry.Permission {
		return false
	}
	return true
}

func matchesResource(resourceType acls.ResourceType, resourceName string,
	patternType acls.ResourcePatternType, entry acls.AclEntry) bool {
	if resourceType != acls.ResourceTypeAny && resourceType != entry.ResourceType {
		return false
	}
	switch patternType {
	case acls.ResourcePatternTypeAny:
		return resourceName == "" || entry.ResourceName == resourceName
	case acls.ResourcePatternTypeMatch:
		if entry.ResourcePatternType == acls.ResourcePatternTypeLiteral {
			return resourceName == entry.ResourceName || entry.ResourceName == wildcardResourceName
		} else if entry.ResourcePatternType == acls.ResourcePatternTypePrefixed {
			return strings.HasPrefix(resourceName, entry.ResourceName)
		} else {
			panic("invalid stored resource pattern type")
		}
	case acls.ResourcePatternTypeLiteral, acls.ResourcePatternTypePrefixed:
		return entry.ResourcePatternType == patternType && resourceName == entry.ResourceName
	default:
		panic("unsupported pattern type")
	}
}

func (a *AclManager) loadWithLock() error {
	a.lock.Lock()
	defer a.lock.Unlock()
	return a.load()
}

func (m *AclManager) load() error {
	aclEntries, err := m.loadAclsWithRetry()
	if err != nil {
		return err
	}
	m.aclEntries = aclEntries
	m.loaded = true
	return nil
}

func (m *AclManager) loadAclsWithRetry() (map[int64]acls.AclEntry, error) {
	for {
		infos, err := m.loadAcls0()
		if err == nil {
			return infos, nil
		}
		if m.stopping.Load() {
			return nil, errors.New("acl manager is stopping")
		}
		if common.IsUnavailableError(err) {
			log.Warnf("Unable to load acls due to unavailability, will retry after delay: %v", err)
			time.Sleep(unavailabilityRetryDelay)
		}
	}
}

func (m *AclManager) loadAcls0() (map[int64]acls.AclEntry, error) {
	keyEnd := common.IncBigEndianBytes(m.partHash)
	mi, err := queryutils.CreateIteratorForKeyRange(m.partHash, keyEnd, m.querier, m.tableGetter)
	if err != nil {
		return nil, err
	}
	if mi == nil {
		m.entrySequence = 0
		return nil, nil
	}
	defer mi.Close()
	allEntries := map[int64]acls.AclEntry{}
	lastSequence := int64(-1)
	for {
		ok, kv, err := mi.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		var entry acls.AclEntry
		dataVersion := binary.BigEndian.Uint16(kv.Value)
		if dataVersion != aclDataVersion {
			return nil, errors.Errorf("invalid acl data version %d", dataVersion)
		}
		entry.Deserialize(kv.Value, 2)
		lastSequence, _ = encoding.KeyDecodeInt(kv.Key, 16)
		allEntries[lastSequence] = entry
	}
	log.Infof("loaded %d acl entries", len(allEntries))
	m.entrySequence = lastSequence + 1
	return allEntries, nil
}
