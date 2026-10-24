//go:build linux

package installfs

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrapriv"
	"golang.org/x/sys/unix"
)

// Open pins a trusted composition root before any installed record is read.
// origin: hot-update S9 plan §10.1–§10.2 / T83
func Open(prefix string, owner uint32) (*Files, error) {
	fd, err := unix.Open(prefix, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	return &Files{os.NewFile(uintptr(fd), "infra-installation"), prefix, owner}, nil
}

// Close releases the namespace only after its composition owner has joined users.
func (f *Files) Close() error { return f.root.Close() }

// Each ancestor is pinned and checked before the next openat. A root-owned
// basename inside a service-writable directory is not a trusted root record.
func (f *Files) Directory(name string, create bool, mode uint32, gid uint32) (*os.File, error) {
	if !filepath.IsAbs(name) || filepath.Clean(name) != name {
		return nil, infrapriv.ErrPermission
	}
	fd, err := unix.FcntlInt(f.root.Fd(), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(strings.TrimPrefix(name, "/"), "/")
	for i, part := range parts {
		if part == "" {
			continue
		}
		next, err := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if errors.Is(err, unix.ENOENT) && create {
			perm := uint32(0755)
			group := uint32(0)
			if i == len(parts)-1 {
				perm, group = mode, gid
			}
			if f.owner != 0 && group == 0 {
				group = uint32(os.Getegid())
			}
			if err = unix.Mkdirat(fd, part, perm); err != nil {
				return nil, errors.Join(err, unix.Close(fd))
			}
			next, err = unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
			if err == nil {
				err = unix.Fchown(next, int(f.owner), int(group))
			}
			if err == nil {
				err = unix.Fchmod(next, perm)
			}
			if err == nil {
				err = unix.Fsync(fd)
			}
		}
		err = errors.Join(err, unix.Close(fd))
		if err != nil {
			if next >= 0 {
				err = errors.Join(err, unix.Close(next))
			}
			return nil, err
		}
		var stat unix.Stat_t
		if unix.Fstat(next, &stat) != nil || stat.Uid != f.owner || stat.Mode&0022 != 0 {
			return nil, errors.Join(infrapriv.ErrPermission, unix.Close(next))
		}
		fd = next
	}
	return os.NewFile(uintptr(fd), "infra-directory"), nil
}

// Open rejects links and mutable ancestors before lending an immutable record FD.
func (f *Files) Open(name string, mode uint32) (*os.File, error) {
	dir, err := f.Directory(filepath.Dir(name), false, 0, 0)
	if err != nil {
		return nil, err
	}
	fd, err := unix.Openat(int(dir.Fd()), filepath.Base(name), unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	err = errors.Join(err, dir.Close())
	if err != nil {
		if fd >= 0 {
			err = errors.Join(err, unix.Close(fd))
		}
		return nil, err
	}
	var stat unix.Stat_t
	if unix.Fstat(fd, &stat) != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 || stat.Uid != f.owner || stat.Mode&0022 != 0 || (mode != 0 && stat.Mode&07777 != mode) {
		return nil, errors.Join(infrapriv.ErrPermission, unix.Close(fd))
	}
	return os.NewFile(uintptr(fd), "infra-record"), nil
}

// Read bounds allocation before an installed record reaches a decoder.
func (f *Files) Read(name string, limit int64) ([]byte, error) {
	file, err := f.Open(name, 0)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	err = errors.Join(err, file.Close())
	if err != nil || int64(len(data)) > limit {
		return nil, errors.Join(c.ErrIncomplete, err)
	}
	return data, nil
}

// Decode rejects alternate JSON spellings of the same authority record.
func (f *Files) Decode(name string, value any) error {
	data, err := f.Read(name, c.MaxCensusBytes)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(value) != nil {
		return c.ErrIncomplete
	}
	canonical, err := json.Marshal(value)
	if err != nil || !bytes.Equal(data, canonical) {
		return c.ErrIncomplete
	}
	return nil
}

// Replace fsyncs bytes and metadata before rename and then fsyncs the pinned
// parent. A partial write never becomes a policy, selector or enabled marker.
func (f *Files) Replace(name string, data []byte, mode uint32, gid uint32) (resultErr error) {
	dir, err := f.Directory(filepath.Dir(name), false, 0, 0)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, dir.Close()) }()
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	temp := ".infra-" + hex.EncodeToString(token[:])
	fd, err := unix.Openat(int(dir.Fd()), temp, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, mode)
	if err != nil {
		return err
	}
	defer func() {
		// A successful rename already consumed the temporary name. Other cleanup
		// failures must remain visible so a caller cannot claim a clean publication.
		if err := unix.Unlinkat(int(dir.Fd()), temp, 0); err != nil && !errors.Is(err, unix.ENOENT) {
			resultErr = errors.Join(resultErr, err)
		}
	}()
	file := os.NewFile(uintptr(fd), "infra-record-write")
	err = file.Chown(int(f.owner), int(gid))
	if err == nil {
		err = file.Chmod(os.FileMode(mode))
	}
	if err == nil {
		_, err = file.Write(data)
	}
	if err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		return err
	}
	if err := unix.Renameat(int(dir.Fd()), temp, int(dir.Fd()), filepath.Base(name)); err != nil {
		return err
	}
	return dir.Sync()
}

// Encode uses the same durable replacement for every installed selector or policy.
func (f *Files) Encode(name string, value any, mode uint32, gid uint32) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return f.Replace(name, data, mode, gid)
}

// Lock keeps competing installed-state mutations on one checked flock inode.
func (f *Files) Lock(name string) (func(), error) {
	dir, err := f.Directory(filepath.Dir(name), false, 0, 0)
	if err != nil {
		return nil, err
	}
	fd, err := unix.Openat(int(dir.Fd()), filepath.Base(name), unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	err = errors.Join(err, dir.Close())
	if err != nil {
		if fd >= 0 {
			err = errors.Join(err, unix.Close(fd))
		}
		return nil, err
	}
	var stat unix.Stat_t
	if unix.Fstat(fd, &stat) != nil || stat.Uid != f.owner || stat.Mode&07777 != 0600 || stat.Nlink != 1 {
		return nil, errors.Join(infrapriv.ErrPermission, unix.Close(fd))
	}
	if unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB) != nil {
		return nil, errors.Join(infrapriv.ErrBusy, unix.Close(fd))
	}
	return func() {
		unix.Close(fd) //nolint:errcheck // The frozen no-result unlock callback releases a flock only after all writes were synced.
	}, nil
}

// ServiceDirectory pins a service-owned store without relaxing the root-record ancestor checks.
func (f *Files) ServiceDirectory(path string, uid uint32) (*os.File, error) {
	fd, err := unix.Openat2(int(f.root.Fd()), strings.TrimPrefix(path, "/"), &unix.OpenHow{Flags: unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC, Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS})
	if err != nil {
		return nil, c.ErrUnsupported
	}
	var stat unix.Stat_t
	if unix.Fstat(fd, &stat) != nil || (stat.Uid != uid && stat.Uid != f.owner) || stat.Mode&0002 != 0 {
		return nil, errors.Join(infrapriv.ErrPermission, unix.Close(fd))
	}
	return os.NewFile(uintptr(fd), "infra-service-directory"), nil
}

// Duplicate retains the caller-supplied open file description with CLOEXEC custody.
// origin: hot-update S9 plan §10.2 / T83 — retain the supplied OFD rather than reopening its path.
func Duplicate(input *os.File) (*os.File, error) {
	fd, err := unix.FcntlInt(input.Fd(), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), "infra-stage-input"), nil
}
