// Copyright 2024 The Tektite Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
