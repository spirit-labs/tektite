package cluster

import (
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestClusteredData(t *testing.T) {
	objStore := dev.NewInMemStore(0)

	cd1 := NewClusteredData("statebucket", "stateprefix",
		"databucket", "dataprefix", objStore, ClusteredDataOpts{})

	data, err := cd1.AcquireData()
	require.NoError(t, err)
	require.Nil(t, data)

	sData := "hello"

	ok, err := cd1.StoreData([]byte(sData))
	require.NoError(t, err)
	require.True(t, ok)

	cd2 := NewClusteredData("statebucket", "stateprefix",
		"databucket", "dataprefix", objStore, ClusteredDataOpts{})

	data, err = cd2.AcquireData()
	require.NoError(t, err)
	require.NotNil(t, data)
	require.Equal(t, sData, string(data))

	sData += "clustered"

	ok, err = cd2.StoreData([]byte(sData))
	require.NoError(t, err)
	require.True(t, ok)

	data, err = cd2.AcquireData()
	require.NoError(t, err)
	require.NotNil(t, data)
	require.Equal(t, sData, string(data))

	// Now try and store on previous clustered data

	sData2 := "armadillos"

	ok, err = cd1.StoreData([]byte(sData2))
	require.NoError(t, err)
	// must fail as cd2 has advanced the epoch
	require.False(t, ok)

	cd3 := NewClusteredData("statebucket", "stateprefix",
		"databucket", "dataprefix", objStore, ClusteredDataOpts{})

	data, err = cd3.AcquireData()
	require.NoError(t, err)
	require.NotNil(t, data)
	require.Equal(t, sData, string(data))
}

func TestClusteredDataReadyState(t *testing.T) {
	objStore := dev.NewInMemStore(0)

	cd := NewClusteredData("statebucket", "stateprefix",
		"databucket", "dataprefix", objStore, ClusteredDataOpts{})

	// not loaded
	ok, err := cd.StoreData([]byte("foo"))
	require.Error(t, err)
	require.Equal(t, "not loaded", err.Error())
	require.False(t, ok)

	_, err = cd.AcquireData()
	require.NoError(t, err)

	// Can load more than once
	_, err = cd.AcquireData()
	require.NoError(t, err)

	cd.Stop()

	_, err = cd.AcquireData()
	require.Error(t, err)
	require.Equal(t, "stopped", err.Error())

	ok, err = cd.StoreData([]byte("foo"))
	require.Error(t, err)
	require.Equal(t, "not loaded", err.Error())
	require.False(t, ok)
}

// TestClusteredDataConcurrency - this test concurrently invokes LoadState and StoreState on multiple ClusteredState
// instances and validates that no state is lost
func TestClusteredDataConcurrency(t *testing.T) {
	objStore := dev.NewInMemStore(0)
	concurrency := 10
	updatesPerRunner := 100

	var runners []runner
	for i := 0; i < concurrency; i++ {
		runners = append(runners, runner{
			objStore:    objStore,
			runnerIndex: i,
			numUpdates:  updatesPerRunner,
			completeCh:  make(chan error, 1),
		})
	}
	for _, runner := range runners {
		r := runner
		go func() {
			r.Run()
		}()
	}
	for _, runner := range runners {
		err := <-runner.completeCh
		require.NoError(t, err)
	}
	// Check final state
	cd := NewClusteredData("statebucket", "stateprefix",
		"databucket", "dataprefix", objStore, ClusteredDataOpts{})
	data, err := cd.AcquireData()
	require.NoError(t, err)

	m := deserializeMap(data)
	require.Equal(t, concurrency, len(m))
	for i := 0; i < concurrency; i++ {
		require.Equal(t, m[i], updatesPerRunner)
	}
}

type runner struct {
	objStore    objstore.Client
	runnerIndex int
	numUpdates  int
	completeCh  chan error
}

func (r *runner) Run() {
	r.completeCh <- r.run()
}

func (r *runner) run() error {
	for i := 0; i < r.numUpdates; i++ {
		for {
			cd := NewClusteredData("statebucket", "stateprefix",
				"databucket", "dataprefix", r.objStore, ClusteredDataOpts{})
			data, err := cd.AcquireData()
			if err != nil {
				return err
			}
			if data == nil {
				data = make([]byte, 8) // zero length map
			}
			// deserialize map
			m := deserializeMap(data)
			prev := m[r.runnerIndex]
			if i > 0 {
				if prev < i {
					// lost data
					return errors.Errorf("runner %d expected at least previous data %d found %d", r.runnerIndex, i, prev)
				}
			}
			// update map
			m[r.runnerIndex] = i + 1
			// serialize map
			buff := serializeMap(m)
			ok, err := cd.StoreData(buff)
			if err != nil {
				return err
			}
			if ok {
				break
			}
			// failed to store, epoch change. retry
		}
	}
	return nil
}

func deserializeMap(data []byte) map[int]int {
	numEntries := int(binary.BigEndian.Uint64(data))
	offset := 8
	m := make(map[int]int, numEntries)
	for i := 0; i < numEntries; i++ {
		k := int(binary.BigEndian.Uint64(data[offset:]))
		offset += 8
		v := int(binary.BigEndian.Uint64(data[offset:]))
		offset += 8
		m[k] = v
	}
	return m
}

func serializeMap(m map[int]int) []byte {
	buff := binary.BigEndian.AppendUint64(nil, uint64(len(m)))
	for k, v := range m {
		buff = binary.BigEndian.AppendUint64(buff, uint64(k))
		buff = binary.BigEndian.AppendUint64(buff, uint64(v))
	}
	return buff
}
