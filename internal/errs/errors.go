package errs

import "errors"

var (
	ErrKeyNotFound = errors.New("Key not found")
	ErrKeyDeleted  = errors.New("Key is deleted")
)
