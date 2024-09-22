package common

func NewKvSliceIterator(kvs []KV) *KvSliceIterator {
	return &KvSliceIterator{kvs: kvs, lkvs: len(kvs)}
}

type KvSliceIterator struct {
	lkvs int
	kvs  []KV
	pos  int
}

func (s *KvSliceIterator) Next() (bool, KV, error) {
	if s.pos >= s.lkvs {
		s.pos = -1
	}
	if s.pos == -1 {
		return false, KV{}, nil
	}
	result := s.kvs[s.pos]
	s.pos++
	return true, result, nil
}

func (s *KvSliceIterator) Current() KV {
	if s.pos <= 0 {
		return KV{}
	}
	return s.kvs[s.pos-1]
}

func (s *KvSliceIterator) Close() {
}
