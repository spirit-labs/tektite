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

func (s *StaticIterator) Reset() {
	s.pos = 0
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

func (s *StaticIterator) Next() (bool, common.KV, error) {
	if s.pos >= len(s.kvs) {
		s.pos = -1
	}
	if (s.hasValidOverride && !s.validOverRideValue) || s.pos == -1 {
		return false, common.KV{}, nil
	}
	v := s.kvs[s.pos]
	s.pos++
	return true, v, nil
}

func (s *StaticIterator) Current() common.KV {
	if s.pos <= 0 {
		return common.KV{}
	}
	return s.kvs[s.pos-1]
}

func (s *StaticIterator) Close() {
}
