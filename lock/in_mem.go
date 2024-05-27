package lock

import "sync"

func NewInMemLockManager() Manager {
	return &inMemLockManager{locks: map[string]struct{}{}}
}

type inMemLockManager struct {
	mu    sync.Mutex
	locks map[string]struct{}
}

func (l *inMemLockManager) GetLock(name string) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.locks[name]
	if !ok {
		l.locks[name] = struct{}{}
		return true, nil
	}
	return false, nil
}

func (l *inMemLockManager) ReleaseLock(name string) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.locks[name]
	if !ok {
		return false, nil
	}
	delete(l.locks, name)
	return true, nil
}
