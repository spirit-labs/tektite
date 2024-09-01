package cluster

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/arista"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/objstore"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

/*
StateMachine is a distributed state machine that persists its state using object storage. Clients can concurrently
update the state machine through their own instances of this struct. It serves as a foundation for various distributed
concurrency primitives, such as group membership, distributed locks, distributed sequences, and more.

The state managed by StateMachine is serialized as JSON bytes and stored in object storage. The state can be of any type
that json.Marshal can accept as a pointer.

StateMachine uses conditional writes via the PutIfNotExists method of the object store. This method atomically stores an
object only if no existing object with the same key is present.

Each StateMachine instance maintains an internal sequence. When updating the state, it attempts to store a key composed
of a prefix and a sequence number. If the key already exists, it indicates that another StateMachine instance has
updated the state based on the previous state. In this scenario, the latest state is loaded, and the update is retried
until successful. The sequence number is stored as MaxInt64 - sequence, ensuring that keys are ordered from newest to
oldest lexicographically. This ordering makes it faster to retrieve the most recent key during initialization, as it
avoids iterating through numerous keys.

Upon startup, each instance lists objects with the given prefix and uses the smallest key as the starting state,
representing the most recent update.

If an instance hasn’t updated the state for a while, and other instances have made many updates, its sequence number
might fall significantly behind. In this case, the instance may experience multiple failed PutIfNotExists attempts
before succeeding. To prevent instances from lagging too much, no-op updates that don’t change the state but update
the sequence can be made periodically by settings `AutoUpdate` to `true` on the options and providing an `AutoUpdateInterval`.

StateMachine does not delete old keys. Deleting keys is challenging because members might lag behind due to network
issues or other factors. Members retry updates based on their current sequence value up to a time limit before
discarding that value and reinitializing upon reconnection. This approach prevents members from relying on outdated
sequences for an extended period. It's crucial not to delete keys associated with in-use sequences, as doing so could
result in lost state.

For key management, it's recommended to store the keys in a dedicated bucket with an expiration policy set to an
appropriate duration (e.g., 7 days), longer than the maximum retry time for the state machine.

If the object store becomes unavailable, or if all StateMachine instances are shut down for a period longer than the
expiration time, all state will be lost.

Optionally, StateMachine can be configured to write the latest state to a dedicated key for each member as a backup.
This is done by specifying the LatestStateBucketName in the options. The bucket should have no expiration policy. In the
rare event that the most recent state machine key is lost, the key can be restored from this backup to the state machine bucket.
*/
type StateMachine[T any] struct {
	lock           sync.Mutex
	stateBucket    string
	stateKeyPrefix string
	keyBucket      string
	objStoreClient objstore.Client
	memberID       string
	opts           StateMachineOpts
	started        bool
	stopping       atomic.Bool
	updateTimer    *time.Timer
	lastUpdateTime int64
	nextSequence   int
	state          T
	reinitCount    int64
	retryCount     int64
}

const DefaultMaxTimeBeforeReinitialise = 30 * time.Millisecond
const DefaultObjStoreCallTimeout = 5 * time.Second
const DefaultAvailabilityRetryInterval = 5 * time.Second
const DefaultAutoUpdateInterval = 5 * time.Second

type StateMachineOpts struct {
	AutoUpdate                bool
	AutoUpdateInterval        time.Duration
	MaxTimeBeforeReinitialise time.Duration
	ObjStoreCallTimeout       time.Duration
	AvailabilityRetryInterval time.Duration
	LatestStateBucketName     string
}

func NewStateMachine[T any](stateBucket string, stateKeyPrefix string, objStoreClient objstore.Client,
	opts StateMachineOpts) *StateMachine[T] {
	if opts.MaxTimeBeforeReinitialise == 0 {
		opts.MaxTimeBeforeReinitialise = DefaultMaxTimeBeforeReinitialise
	}
	if opts.ObjStoreCallTimeout == 0 {
		opts.ObjStoreCallTimeout = DefaultObjStoreCallTimeout
	}
	if opts.AvailabilityRetryInterval == 0 {
		opts.AvailabilityRetryInterval = DefaultAvailabilityRetryInterval
	}
	if opts.AutoUpdate && opts.AutoUpdateInterval == 0 {
		opts.AutoUpdateInterval = DefaultAutoUpdateInterval
	}
	memberID := ""
	if opts.LatestStateBucketName != "" {
		memberID = uuid.New().String()
	}
	sm := &StateMachine[T]{
		stateBucket:    stateBucket,
		stateKeyPrefix: stateKeyPrefix,
		objStoreClient: objStoreClient,
		memberID:       memberID,
		opts:           opts,
		nextSequence:   -1,
		lastUpdateTime: -1,
	}
	return sm
}

func (s *StateMachine[T]) Start() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.started {
		return
	}
	if s.opts.AutoUpdate {
		s.scheduleUpdate()
	}
	s.started = true
}

func (s *StateMachine[T]) Stop() {
	s.stopping.Store(true)
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.started {
		return
	}
	if s.opts.AutoUpdate {
		s.updateTimer.Stop()
	}
	s.started = false
}

func (s *StateMachine[T]) NextSequence() int {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.nextSequence
}

func (s *StateMachine[T]) scheduleUpdate() {
	s.updateTimer = time.AfterFunc(s.opts.AutoUpdateInterval, func() {
		s.lock.Lock()
		defer s.lock.Unlock()
		if !s.started {
			return
		}
		if int64(arista.NanoTime())-s.lastUpdateTime >= int64(s.opts.AutoUpdateInterval) {
			// If user hasn't updated, then we run a no-op update - this ensures our current sequence number remains
			// up to date with latest state.
			if _, err := s.update(func(state T) (T, error) {
				return state, nil
			}); err != nil {
				log.Errorf("failed to update: %v", err)
			}
		}
		s.scheduleUpdate()
	})
}

func (s *StateMachine[T]) createKey(sequence int) string {
	// Note that later keys have a smaller key - this allows us to init more quickly as we just load the first key
	key := fmt.Sprintf("%s-%09d", s.stateKeyPrefix, math.MaxInt64-sequence)
	return key
}

func (s *StateMachine[T]) extractSequenceFromKey(key string) (int, error) {
	i, err := strconv.Atoi(key[len(s.stateKeyPrefix)+1:])
	if err != nil {
		return 0, err
	}
	return math.MaxInt64 - i, nil
}

// Update updates the state based on the previous state. The update function provides the operation to update the state
// based on the previous state, returning the new state which will be stored. The function returns when the new state
// has been committed to object storage, or an error occurs.
func (s *StateMachine[T]) Update(updateFunc func(state T) (T, error)) (T, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	state, err := s.update(updateFunc)
	s.lastUpdateTime = int64(arista.NanoTime())
	return state, err
}

func (s *StateMachine[T]) update(updateFunc func(state T) (T, error)) (T, error) {
	var tZero T
outer:
	for {
		if s.nextSequence == -1 {
			// Initialise sequence by loading newest one from object store
			if err := s.init(); err != nil {
				return tZero, err
			}
		}
		seq := s.nextSequence
		state := s.state
		start := arista.NanoTime()
		for {
			if s.stopping.Load() {
				return tZero, errors.New("state machine is stopping")
			}
			// Try and update state
			updated, newState, err := s.innerUpdate(seq, state, updateFunc)
			if err != nil {
				if common.IsUnavailableError(err) {
					// Update failed because object store is unavailable or call timed out, sleep before retrying
					time.Sleep(s.opts.AvailabilityRetryInterval)
					if time.Duration(arista.NanoTime()-start) >= s.opts.MaxTimeBeforeReinitialise {
						// We've reached the limit of retrying with the current sequence, set it to -1 and continue
						// outer loop so sequence gets re-initialised from the object store. This prevents us holding
						// on to sequences that are too old.
						s.nextSequence = -1
						atomic.AddInt64(&s.reinitCount, 1)
						continue outer
					}
					// Retry with the current sequence value
					atomic.AddInt64(&s.retryCount, 1)
					continue
				}
				return tZero, err
			}
			if updated {
				// We updated ok
				s.nextSequence = seq + 1
				s.state = newState
				return s.state, nil
			}
			// We failed to update due to our sequence being old (another member must have updated since we loaded)
			// increment sequence and retry inner loop
			state = newState
			seq++
		}
	}
}

func (s *StateMachine[T]) innerUpdate(seq int, state T, updateFunc func(state T) (T, error)) (bool, T, error) {
	var tZero T
	var err error
	state, err = updateFunc(state)
	if err != nil {
		return false, tZero, err
	}
	buff, err := json.Marshal(&state)
	if err != nil {
		return false, tZero, err
	}
	newKey := s.createKey(seq)
	put, err := objstore.PutIfNotExistsWithTimeout(s.objStoreClient, s.stateBucket, newKey, buff, s.opts.ObjStoreCallTimeout)
	if err != nil {
		return false, tZero, err
	}
	if put {
		if s.opts.LatestStateBucketName != "" {
			// If latest state bucket name is provided we also store the latest state in a key, without versioning.
			// This can be used as a backup to restore state if sequenced keys are lost, e.g. due to bucket expiration
			// deleting them
			latestStateKey := fmt.Sprintf("%s-latest-state-%s", s.stateKeyPrefix, s.memberID)
			if err := objstore.PutWithTimeout(s.objStoreClient, s.opts.LatestStateBucketName, latestStateKey, buff, s.opts.ObjStoreCallTimeout); err != nil {
				return false, tZero, err
			}
		}
		return true, state, nil
	}
	// key exists already - load the state
	buffRead, err := objstore.GetWithTimeout(s.objStoreClient, s.stateBucket, newKey, s.opts.ObjStoreCallTimeout)
	if err != nil {
		return false, tZero, err
	}
	if buffRead == nil {
		return false, tZero, errors.Errorf("cannot find key %v", newKey)
	}
	var newState T
	if err := json.Unmarshal(buffRead, &newState); err != nil {
		return false, tZero, err
	}
	state = newState
	return false, state, nil
}

func (s *StateMachine[T]) init() error {
	for {
		if s.stopping.Load() {
			return errors.New("state machine is stopping")
		}
		if err := s.initInner(); err != nil {
			if common.IsUnavailableError(err) {
				// Retry
				time.Sleep(s.opts.AvailabilityRetryInterval)
				continue
			}
		} else {
			return nil
		}
	}
}

func (s *StateMachine[T]) initInner() error {
	// We store keys in reverse order, so the latest key will be the first one returned - we only need to list 1 key
	existingInfos, err := objstore.ListObjectsWithPrefixWithTimeout(s.objStoreClient, s.stateBucket, s.stateKeyPrefix,
		1, s.opts.ObjStoreCallTimeout)
	if err != nil {
		return err
	}
	if len(existingInfos) > 1 {
		panic("too many keys returned")
	}
	if len(existingInfos) == 1 {
		lastInfo := existingInfos[0]
		lastSeq, err := s.extractSequenceFromKey(lastInfo.Key)
		if err != nil {
			return err
		}
		buff, err := objstore.GetWithTimeout(s.objStoreClient, s.stateBucket, lastInfo.Key, s.opts.ObjStoreCallTimeout)
		if err != nil {
			return err
		}
		if buff == nil {
			return errors.Errorf("cannot find key %s on init", lastInfo.Key)
		}
		var state T
		if err := json.Unmarshal(buff, &state); err != nil {
			return err
		}
		s.nextSequence = lastSeq + 1
		s.state = state
	} else {
		s.nextSequence = 0
		var zeroT T
		s.state = zeroT
	}
	return nil
}

func (s *StateMachine[T]) GetState() (T, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.state, nil
}
