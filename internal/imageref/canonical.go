// Package imageref validates public image identifiers and canonical projections.
// It owns no image, file or process resources.
package imageref

import (
	"bytes"
	"encoding/json"
	"io"
)

func ValidSHA(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
func ValidID(s string) bool {
	if len(s) == 0 || len(s) > 256 {
		return false
	}
	for _, c := range s {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '.' && c != '_' && c != '-' {
			return false
		}
	}
	return true
}
func CanonicalStrings(ss []string, required bool) bool {
	if ss == nil || required && len(ss) == 0 {
		return false
	}
	for i, s := range ss {
		if !ValidID(s) || (i > 0 && ss[i-1] >= s) {
			return false
		}
	}
	return true
}

// decodeCanonical rejects missing zero fields, duplicate keys, unknown fields,
// alternate encodings and trailing bytes. A strict decoder alone accepts missing
// required decisions and duplicate object keys, which silently reset ownership.
// origin: hot-update S6 plan §2.2 / T47
func DecodeCanonical(data []byte, target any) bool {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if dec.Decode(target) != nil || dec.Decode(new(any)) != io.EOF {
		return false
	}
	encoded, err := json.Marshal(target)
	if err != nil || !bytes.Equal(data, encoded) {
		return false
	}
	return true
}
