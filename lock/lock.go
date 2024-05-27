package lock

type Manager interface {
	GetLock(prefix string) (bool, error)
	ReleaseLock(prefix string) (bool, error)
}
