package sealedstate

import "errors"

// Close releases only descriptors created for this cut. Resources also names
// borrowed business sockets/replays; closing that table would interrupt the
// owner when an export is replaced or an exec attempt is abandoned.
// The cut owner serializes replacement and Close, as it does resource export.
// origin: hot-update S5 plan §5.3 / T51
func (s *Snapshot) Close() error {
	if s == nil {
		return nil
	}
	owned := s.Owned
	s.Owned = nil
	var result error
	for _, file := range owned {
		if file == nil {
			continue
		}
		if err := file.Close(); err != nil {
			// File errors contain names supplied by callers. Keep them out of
			// diagnostics, even when a malformed snapshot contains a secret name.
			result = errors.Join(result, ErrInvalid)
		}
	}
	return result
}

// Match handoff's ASCII resource/file grammar before any allocation or Take.
// Do not echo an invalid name: private state callers can supply secret strings.
// origin: hot-update S5 plan §5.1
func validStateName(name string, limit int) bool {
	if len(name) == 0 || len(name) > limit {
		return false
	}
	for _, c := range name {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' {
			continue
		}
		return false
	}
	return true
}
