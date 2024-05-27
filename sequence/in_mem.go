package sequence

import "sync"

func NewInMemSequenceManager() Manager {
	return &inMemSequenceManager{sequences: map[string]int{}}
}

type inMemSequenceManager struct {
	mu        sync.Mutex
	sequences map[string]int
}

func (s *inMemSequenceManager) GetNextID(sequenceName string, _ int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.sequences[sequenceName]
	s.sequences[sequenceName] = id + 1
	return id, nil
}
