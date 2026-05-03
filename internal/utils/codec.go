package utils

import (
	"encoding/binary"
	"fmt"
)

// Length-Prefixed Codec for encoding key-value entries.
//
// Wire format:
// ┌──────────┬───────┬──────────┬───────┐
// │ key_len  │  key  │ val_len  │ value  │
// │ (varint) │ bytes │ (varint) │ bytes  │
// ├──────────┼───────┼──────────┼───────┤
// │   0x05   │hello  │   0x06   │world  │
// └──────────┴───────┴──────────┴───────┘

func EncodeEntry(buf []byte, key, value []byte) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(key)))
	buf = append(buf, key...)
	buf = binary.AppendUvarint(buf, uint64(len(value)))
	buf = append(buf, value...)
	return buf
}

func DecodeEntry(data []byte) (key, value []byte, n int, err error) {
	keyLen, i := binary.Uvarint(data)
	if i <= 0 {
		return nil, nil, 0, fmt.Errorf("invalid key length varint (n=%d)", i)
	}

	keyEnd := i + int(keyLen)
	if keyEnd > len(data) {
		return nil, nil, 0, fmt.Errorf("key data truncated: need %d bytes, have %d", keyLen, len(data)-i)
	}
	key = data[i:keyEnd]

	valueLen, j := binary.Uvarint(data[keyEnd:])
	if j <= 0 {
		return nil, nil, 0, fmt.Errorf("invalid value length varint (n=%d)", j)
	}

	valueStart := keyEnd + j
	valueEnd := valueStart + int(valueLen)
	if valueEnd > len(data) {
		return nil, nil, 0, fmt.Errorf("value data truncated: need %d bytes, have %d", valueLen, len(data)-valueStart)
	}
	value = data[valueStart:valueEnd]

	return key, value, valueEnd, nil
}
