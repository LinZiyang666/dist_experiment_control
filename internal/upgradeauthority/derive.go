package upgradeauthority

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Derive binds a reusable SID to the existing creation identity, without a new
// schema column. createdAt is Unix nanoseconds; its eight-byte big-endian value
// and each other field have an eight-byte big-endian length prefix. Keeping all
// four boundaries prevents two differently split identities sharing a digest.
// origin: hot-update S8 plan §6.2 / P08
func Derive(sid, owner string, createdAt int64, salt []byte) (string, error) {
	if !creationText(sid, 32) || !creationText(owner, 256) || createdAt <= 0 || len(salt) != 16 {
		return "", creationUnavailable()
	}
	var timestamp [8]byte
	binary.BigEndian.PutUint64(timestamp[:], uint64(createdAt))
	h := sha256.New()
	for _, field := range [][]byte{[]byte(sid), []byte(owner), timestamp[:], salt} {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(field)))
		_, _ = h.Write(size[:])
		_, _ = h.Write(field)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// SQL literals travel through JSON in clustered writers. Rejecting NUL and
// invalid UTF-8 here prevents that transport from changing the compared value;
// printable punctuation remains valid local material and is parameter-bound.
func creationText(value string, limit int) bool {
	return value != "" && len(value) <= limit && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

func creationUnavailable() error {
	return &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "session creation identity is unavailable"}
}

// creationSalt follows internal/auth/pin.go's PHC format and fixed Argon2id
// parameters. Its parser is private to auth; this local parser neither verifies
// a guessed PIN nor performs an expensive Argon2 calculation inside a writer.
// Reject incomplete material before it can become an authority proof. Errors
// deliberately omit parser/driver text, which can contain the stored hash.
// origin: hot-update S8 plan §6.2 / P08
func creationSalt(encoded string) ([]byte, error) {
	if !creationText(encoded, 512) {
		return nil, creationUnavailable()
	}
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" || !phcNumber(parts[2], "v=", 19) {
		return nil, creationUnavailable()
	}
	params := strings.Split(parts[3], ",")
	if len(params) != 3 || !phcNumber(params[0], "m=", 65536) ||
		!phcNumber(params[1], "t=", 3) || !phcNumber(params[2], "p=", 2) {
		return nil, creationUnavailable()
	}
	// A canonical raw encoding excludes ignored newlines and trailing bits.
	// The hash bytes validate PHC integrity but are never part of incarnation:
	// re-encoding a hash with the same creation salt must preserve the digest.
	salt, saltErr := base64.RawStdEncoding.Strict().DecodeString(parts[4])
	hash, hashErr := base64.RawStdEncoding.Strict().DecodeString(parts[5])
	if saltErr != nil || hashErr != nil || len(salt) != 16 || len(hash) != 32 ||
		base64.RawStdEncoding.EncodeToString(salt) != parts[4] ||
		base64.RawStdEncoding.EncodeToString(hash) != parts[5] {
		return nil, creationUnavailable()
	}
	return salt, nil
}

func phcNumber(text, prefix string, want uint64) bool {
	value, ok := strings.CutPrefix(text, prefix)
	if !ok {
		return false
	}
	number, err := strconv.ParseUint(value, 10, 32)
	return err == nil && number == want
}
