package workload

import (
	"fmt"
	"os"
	"os/exec"

	"golang.org/x/sys/unix"
)

// Stdio owns explicit parent and child pipe ends. Using files for every Cmd
// stream prevents os/exec from hiding a copier or a pipe closer in Cmd.Wait;
// the runtime can retain these exact parent ends across an image switch.
type Stdio struct {
	Stdin  *os.File
	Stdout *os.File
	Stderr *os.File

	childIn  *os.File
	childOut *os.File
	childErr *os.File
}

// NewStdio allocates all three pairs or closes every partial allocation. The
// caller must Close eventually, including when Start fails or is abandoned.
func NewStdio() (*Stdio, error) {
	s := &Stdio{}
	var err error
	if s.childIn, s.Stdin, err = os.Pipe(); err == nil {
		if s.Stdout, s.childOut, err = os.Pipe(); err == nil {
			s.Stderr, s.childErr, err = os.Pipe()
		}
	}
	if err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// Bind must precede Start and spawnexec.Prepare so its helper inherits the
// same child ends. Stdio has one lifecycle owner; Bind/Close are not concurrent.
func (s *Stdio) Bind(cmd *exec.Cmd) {
	cmd.Stdin, cmd.Stdout, cmd.Stderr = s.childIn, s.childOut, s.childErr
}

// CloseChild drops parent copies of the slave ends immediately after Start.
// Keeping an output writer here would prevent the parent from observing EOF.
func (s *Stdio) CloseChild() {
	for _, f := range []*os.File{s.childIn, s.childOut, s.childErr} {
		if f != nil {
			_ = f.Close()
		}
	}
}

func (s *Stdio) Close() {
	s.CloseChild()
	for _, f := range []*os.File{s.Stdin, s.Stdout, s.Stderr} {
		if f != nil {
			_ = f.Close()
		}
	}
}

// validateStreamFile inspects the borrowed descriptor without File.Fd, which
// can change its blocking mode. Deadline support alone does not prove the
// descriptor permits the intended direction (both pipe ends are pollable).
// CLOEXEC and O_NONBLOCK are required before ownership transfer: a blocking
// syscall cannot be interrupted by this actor's temporary freeze deadline.
func validateStreamFile(file *os.File, write bool) error {
	raw, err := file.SyscallConn()
	if err != nil {
		return err
	}
	var checkErr error
	err = raw.Control(func(fd uintptr) {
		var info unix.Stat_t
		if err := unix.Fstat(int(fd), &info); err != nil {
			checkErr = err
			return
		}
		flags, e1 := unix.FcntlInt(fd, unix.F_GETFL, 0)
		fdFlags, e2 := unix.FcntlInt(fd, unix.F_GETFD, 0)
		access := flags & unix.O_ACCMODE
		if e1 != nil || e2 != nil || flags&unix.O_NONBLOCK == 0 || fdFlags&unix.FD_CLOEXEC == 0 ||
			(write && access == unix.O_RDONLY) || (!write && access == unix.O_WRONLY) {
			checkErr = fmt.Errorf("workload: stream descriptor has incompatible access or flags")
		}
		if write && info.Mode&unix.S_IFMT != unix.S_IFIFO {
			checkErr = fmt.Errorf("workload: input requires a pipe; other endpoints need domain-specific EOF")
		}
	})
	if err != nil {
		return err
	}
	return checkErr
}

// CheckStreamFile is the read-only stream-descriptor predicate an importer
// runs on a taken file before building an actor on it: non-blocking, CLOEXEC,
// readable (write=false) or a writable pipe (write=true). It never changes
// the descriptor; RestoreOutput/RestoreInput apply the same predicate.
func CheckStreamFile(file *os.File, write bool) error {
	if file == nil {
		return fmt.Errorf("workload: missing stream file")
	}
	return validateStreamFile(file, write)
}
