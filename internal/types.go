package types

// Entry represents a key-value pair stored in WAL and SSTable entries.
type Entry struct {
	Key   []byte
	Value []byte
}
