package iteration

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/encoding"
)

func NewStaticIterator(entries []common.KV) *StaticIterator {
	return &StaticIterator{kvs: entries}
}

type StaticIterator struct {
	kvs                []common.KV
	pos                int
	hasValidOverride   bool
	validOverRideValue bool
}

func (s *StaticIterator) SetValidOverride(valid bool) {
	s.hasValidOverride = true
	s.validOverRideValue = valid
}

func (s *StaticIterator) UnsetValidOverride() {
	s.hasValidOverride = false
}

func (s *StaticIterator) AddKVAsString(k string, v string) {
	s.kvs = append(s.kvs, common.KV{
		Key:   []byte(k),
		Value: []byte(v),
	})
}

func (s *StaticIterator) AddKVAsStringWithVersion(k string, v string, ver uint64) {
	s.kvs = append(s.kvs, common.KV{
		Key:   encoding.EncodeVersion([]byte(k), ver),
		Value: []byte(v),
	})
}

func (s *StaticIterator) AddKV(k []byte, v []byte) {
	s.kvs = append(s.kvs, common.KV{
		Key:   k,
		Value: v,
	})
}

func (s *StaticIterator) Current() common.KV {
	if s.pos == -1 {
		return common.KV{}
	}
	return s.kvs[s.pos]
}

func (s *StaticIterator) Next() error {
	s.pos++
	if s.pos == len(s.kvs) {
		s.pos = -1
	}
	return nil
}

func (s *StaticIterator) IsValid() (bool, error) {
	if s.hasValidOverride {
		return s.validOverRideValue, nil
	}
	if len(s.kvs) == 0 {
		return false, nil
	}
	return s.pos != -1, nil
}

func (s *StaticIterator) Close() {
}
