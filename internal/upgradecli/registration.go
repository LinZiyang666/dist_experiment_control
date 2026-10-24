package upgradecli

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// registerRetrofit consumes the installer's inert deployment declaration on
// the explicit first capable cold start. The installer cannot run tether to
// derive a domain: the Go owner performs the canonical parent-inode derivation
// and durable O_EXCL publication through RegisterDomain instead. Subsequent
// boots revalidate the declaration against that immutable registration.
// origin: S8 internal review remaining-orchestration F5 (R5) / hot-update S8 plan §5.2 LQ1
func registerRetrofit(role BootRole) error {
	path := filepath.Join(role.ControlDir, "installation.json")
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return registrationCorrupt()
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil || !controlledRegistration(info, role.Want.UID) || info.Size() <= 0 || info.Size() > 1<<20 {
		return registrationCorrupt()
	}
	body, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return registrationCorrupt()
	}
	tokens := json.NewDecoder(bytes.NewReader(body))
	if registrationValue(tokens, 0) != nil {
		return registrationCorrupt()
	}
	var reg upgradelock.Registration
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&reg) != nil || decoder.Decode(new(any)) != io.EOF || reg.ControlDir != role.ControlDir {
		return registrationCorrupt()
	}
	// Match every configured field, never fill a missing node or UID from the
	// declaration. A copied declaration cannot register another daemon's key.
	found := false
	for _, p := range reg.Participants {
		key := p.Key
		key.DomainID = ""
		if key == role.Want {
			found = true
		}
	}
	if !found {
		return registrationCorrupt()
	}
	if _, err := upgradelock.RegisterDomain(reg); err != nil {
		return err
	}
	key, registered, ok, err := upgradeartifact.RegisteredKey(role.ControlDir, role.Want)
	if err != nil || !ok {
		if err != nil {
			return err
		}
		return registrationCorrupt()
	}
	for _, dir := range []string{filepath.Join(role.ControlDir, "artifacts"), filepath.Dir(role.Journal(key, registered))} {
		if err := controlledDirectory(dir, role.Want.UID); err != nil {
			return err
		}
	}
	return nil
}

func registrationCorrupt() error {
	return &upgradetx.Error{Code: upgradetx.CodeStateCorrupt, Detail: "installation registration is invalid"}
}

func controlledRegistration(info os.FileInfo, uid uint32) bool {
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && info.Mode().IsRegular() && info.Mode().Perm()&0022 == 0 && (st.Uid == 0 || st.Uid == uid) && st.Nlink == 1
}

// Each component journal parent is prepared before Open; checking every
// existing ancestor prevents MkdirAll from following a declaration alias.
func controlledDirectory(path string, uid uint32) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return registrationCorrupt()
	}
	var missing []string
	for current := path; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			missing = append(missing, current)
			continue
		}
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return registrationCorrupt()
		}
		resolved, err := filepath.EvalSymlinks(current)
		if err != nil || resolved != current {
			return registrationCorrupt()
		}
		break
	}
	for i := len(missing) - 1; i >= 0; i-- {
		if err := os.Mkdir(missing[i], 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return registrationCorrupt()
		}
		// A later journal fsync cannot make an unsynced ancestor durable.
		parent, err := os.Open(filepath.Dir(missing[i]))
		if err != nil {
			return registrationCorrupt()
		}
		syncErr, closeErr := parent.Sync(), parent.Close()
		if syncErr != nil || closeErr != nil {
			return registrationCorrupt()
		}
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0022 != 0 {
		return registrationCorrupt()
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != 0 && st.Uid != uid {
		return registrationCorrupt()
	}
	return nil
}

// Duplicate keys are ambiguous configuration, even when encoding/json would
// choose the last one. Check nested participant keys before typed decoding.
func registrationValue(decoder *json.Decoder, depth int) error {
	if depth > 32 {
		return registrationCorrupt()
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	seen := map[string]bool{}
	count := 0
	for decoder.More() {
		count++
		if count > 4096 {
			return registrationCorrupt()
		}
		if delimiter == '{' {
			token, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := token.(string)
			if !ok || seen[key] || key != strings.ToLower(key) {
				return registrationCorrupt()
			}
			seen[key] = true
		}
		if err := registrationValue(decoder, depth+1); err != nil {
			return err
		}
	}
	_, err = decoder.Token()
	return err
}
