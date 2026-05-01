package utils

// IsTombstone returns true if the value is the deletion sentinel.
func IsTombstone(value []byte) bool {
	return len(value) == 1 && value[0] == 0x00
}
