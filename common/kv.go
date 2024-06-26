package common

type KV struct {
	Key   []byte
	Value []byte
}

type KVV struct {
	Key     []byte
	Value   []byte
	Version uint64
}
