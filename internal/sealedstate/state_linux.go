//go:build linux

package sealedstate

import (
	"encoding/binary"
	"os"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"golang.org/x/sys/unix"
)

const (
	stateSeals       = unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	stateHeaderBytes = 16
	stateMagic       = "TETHSS5\x00"
	stateKind        = "sealed-state-v1"
)

// New owns one immutable memfd; the resource table only borrows it for Prepare.
// A fixed envelope lets Read validate this layer's version and exact size before
// the domain codec sees any bytes. The 32 MiB cap includes this envelope.
// origin: hot-update S5 plan §5.1 / §6.6
func New(id string, data []byte) (*Snapshot, error) {
	if !validStateName(id, 256) {
		return nil, ErrInvalid
	}
	if len(data) > MaxStateBytes-stateHeaderBytes {
		return nil, ErrCapacity
	}
	fd, err := unix.MemfdCreate("tether-sealed-state", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return nil, ErrInvalid
	}
	file := os.NewFile(uintptr(fd), "tether-sealed-state")
	complete := false
	defer func() {
		if !complete {
			_ = file.Close()
		}
	}()
	var header [stateHeaderBytes]byte
	copy(header[:8], stateMagic)
	binary.BigEndian.PutUint32(header[8:12], Version)
	binary.BigEndian.PutUint32(header[12:16], uint32(len(data)))
	if _, err := file.WriteAt(header[:], 0); err != nil {
		return nil, ErrInvalid
	}
	if _, err := file.WriteAt(data, stateHeaderBytes); err != nil {
		return nil, ErrInvalid
	}
	// Every seal matters: WRITE blocks overwrite; GROW/SHRINK fix bounds;
	// SEAL fixes the seal configuration for every subsequent owner.
	// origin: hot-update S5 plan §5.1 / TestSealedStateRejectsUnsealedDescriptor
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, stateSeals); err != nil {
		return nil, ErrInvalid
	}
	if _, err := validateStateFile(file); err != nil {
		return nil, err
	}
	complete = true
	return &Snapshot{
		Resources: []handoff.Resource{{ID: id, Kind: stateKind, Files: map[string]*os.File{"state": file}}},
		Owned:     []*os.File{file},
	}, nil
}

// Read transfers exactly one state descriptor. On any failure after TakeFile,
// it returns that descriptor with the error so the caller can salvage/re-dispatch
// or close it. It never silently destroys a taken resource on import refusal.
// Pread (via ReadAt) leaves the OFD offset shared with Prepared unchanged.
// origin: hot-update S5 plan §5.1 / §5.3
func Read(src rawsocket.FileSource, id, name string, limit int64) ([]byte, *os.File, error) {
	if src == nil || !validStateName(id, 256) || !validStateName(name, 64) || limit < 0 {
		return nil, nil, ErrInvalid
	}
	if limit > MaxStateBytes {
		return nil, nil, ErrCapacity
	}
	file, err := src.TakeFile(id, name)
	if err != nil || file == nil {
		return nil, file, ErrInvalid
	}
	size, err := validateStateFile(file)
	if err != nil {
		return nil, file, err
	}
	var header [stateHeaderBytes]byte
	if _, err := file.ReadAt(header[:], 0); err != nil {
		return nil, file, ErrInvalid
	}
	// Refuse mismatched versions, truncation and trailing bytes before allocation.
	// A sealed descriptor alone does not prove it contains our private format.
	// origin: hot-update S5 plan §5.1
	if string(header[:8]) != stateMagic || binary.BigEndian.Uint32(header[8:12]) != Version ||
		int64(binary.BigEndian.Uint32(header[12:16])) != size-stateHeaderBytes {
		return nil, file, ErrInvalid
	}
	length := size - stateHeaderBytes
	if length > limit {
		return nil, file, ErrCapacity
	}
	data := make([]byte, int(length))
	if _, err := file.ReadAt(data, stateHeaderBytes); err != nil {
		return nil, file, ErrInvalid
	}
	return data, file, nil
}

// validateStateFile only inspects immutable facts and never repairs flags.
// RawConn keeps descriptor access pinned without File.Fd's O_NONBLOCK side
// effects. Seals exclude disk files; anonymous regular-file type and UID keep
// foreign descriptors out of this same-process cut.
// origin: hot-update S5 plan §5.1
func validateStateFile(file *os.File) (int64, error) {
	if file == nil {
		return 0, ErrInvalid
	}
	raw, err := file.SyscallConn()
	if err != nil {
		return 0, ErrInvalid
	}
	var size int64
	var checkErr error
	err = raw.Control(func(fd uintptr) {
		var info unix.Stat_t
		if err := unix.Fstat(int(fd), &info); err != nil {
			checkErr = ErrInvalid
			return
		}
		seals, sealErr := unix.FcntlInt(fd, unix.F_GET_SEALS, 0)
		flags, flagErr := unix.FcntlInt(fd, unix.F_GETFL, 0)
		fdFlags, fdErr := unix.FcntlInt(fd, unix.F_GETFD, 0)
		if sealErr != nil || flagErr != nil || fdErr != nil || seals&stateSeals != stateSeals ||
			info.Mode&unix.S_IFMT != unix.S_IFREG || info.Uid != uint32(os.Geteuid()) || info.Nlink != 0 ||
			flags&unix.O_ACCMODE == unix.O_WRONLY || flags&unix.O_APPEND != 0 || fdFlags&unix.FD_CLOEXEC == 0 ||
			info.Size < stateHeaderBytes {
			checkErr = ErrInvalid
			return
		}
		if info.Size > MaxStateBytes {
			checkErr = ErrCapacity
			return
		}
		size = info.Size
	})
	if err != nil {
		return 0, ErrInvalid
	}
	return size, checkErr
}
