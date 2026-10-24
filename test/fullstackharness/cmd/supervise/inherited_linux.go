//go:build linux

package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

type inheritedChild struct {
	ticks, state string
}

type inheritedWaitFailure struct{ error }

func (inheritedWaitFailure) SupervisorExitCode() int { return 125 }

// A shell exec can leave its pipe-backed here-document writer as our child.
// That wait duty precedes the workload: the shared supervisor cannot wait for a
// sibling, and its outer census correctly fails if we abandon the duty at exit.
// Only the initial child set is maintained here. The shared FIX-ORPHAN helper
// retains every target/descendant wait, native status and escape census.
// origin: S10 internal review FIX-SUPERVISE F1
func watchInheritedChildren() (func() error, error) {
	pending, err := inheritedChildren()
	if err != nil {
		return nil, err
	}
	if len(pending) == 0 {
		return func() error { return nil }, nil
	}
	stop, done := make(chan struct{}), make(chan struct{})
	var fault error
	go func() {
		defer close(done)
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			if err := reapInheritedChildren(pending); err != nil {
				fault = err
				return
			}
			select {
			case <-stop:
				return
			case <-tick.C:
			}
		}
	}()
	return func() error {
		close(stop)
		<-done
		fault = errors.Join(fault, reapInheritedChildren(pending))
		if len(pending) == 0 {
			return fault
		}
		// These are live inherited survivors, not ordinary zombie maintenance.
		// Keep their exact census red even after emergency cleanup succeeds.
		// The shared target waiter has already joined, so remaining direct
		// children (including subsequent adoption) belong to this initial tree.
		return inheritedWaitFailure{errors.Join(errors.New("supervise: inherited descendants survived workload"), fault, cleanInheritedChildren())}
	}, nil
}

func inheritedChildren() (map[int]inheritedChild, error) {
	threads, err := os.ReadDir("/proc/self/task")
	if err != nil {
		return nil, err
	}
	children := make(map[int]inheritedChild)
	for _, thread := range threads {
		body, err := os.ReadFile("/proc/self/task/" + thread.Name() + "/children")
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ESRCH) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, token := range strings.Fields(string(body)) {
			pid, err := strconv.Atoi(token)
			if err != nil {
				return nil, err
			}
			child, err := inheritedChildState(pid)
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ESRCH) {
				continue
			}
			if err != nil {
				return nil, err
			}
			children[pid] = child
		}
	}
	return children, nil
}

func inheritedChildState(pid int) (inheritedChild, error) {
	body, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return inheritedChild{}, err
	}
	end := strings.LastIndexByte(string(body), ')')
	if end < 0 {
		return inheritedChild{}, errors.New("supervise: malformed inherited identity")
	}
	fields := strings.Fields(string(body)[end+1:])
	if len(fields) < 20 {
		return inheritedChild{}, errors.New("supervise: incomplete inherited identity")
	}
	return inheritedChild{ticks: fields[19], state: fields[0]}, nil
}

func reapInheritedChildren(pending map[int]inheritedChild) error {
	for pid, original := range pending {
		current, err := inheritedChildState(pid)
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ESRCH) || (err == nil && current.ticks != original.ticks) {
			delete(pending, pid)
			continue
		}
		if err != nil {
			return err
		}
		if current.state != "Z" {
			continue
		}
		reaped, err := unix.Wait4(pid, nil, unix.WNOHANG, nil)
		if reaped == pid || errors.Is(err, unix.ECHILD) {
			delete(pending, pid)
		} else if err != nil && !errors.Is(err, unix.EINTR) {
			return err
		}
	}
	return nil
}

func cleanInheritedChildren() error {
	var census error
	recorded := make(map[string]bool)
	deadline := time.Now().Add(10 * time.Second)
	for {
		children, err := inheritedChildren()
		if err != nil {
			return errors.Join(census, err)
		}
		if len(children) == 0 {
			return census
		}
		for pid, child := range children {
			key := strconv.Itoa(pid) + ":" + child.ticks
			if !recorded[key] {
				census = errors.Join(census, fmt.Errorf("inherited descendant pid=%d start_ticks=%s state=%s", pid, child.ticks, child.state))
				recorded[key] = true
			}
			fd, err := unix.PidfdOpen(pid, 0)
			if errors.Is(err, unix.ESRCH) {
				continue
			}
			if err != nil {
				return errors.Join(census, err)
			}
			current, readErr := inheritedChildState(pid)
			if readErr == nil && current.ticks == child.ticks {
				err = unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0)
			}
			closeErr := unix.Close(fd)
			if readErr != nil && !errors.Is(readErr, os.ErrNotExist) && !errors.Is(readErr, unix.ESRCH) {
				return errors.Join(census, readErr, closeErr)
			}
			if err != nil && !errors.Is(err, unix.ESRCH) {
				return errors.Join(census, err, closeErr)
			}
			if closeErr != nil {
				return errors.Join(census, closeErr)
			}
		}
		if err := reapInheritedChildren(children); err != nil {
			return errors.Join(census, err)
		}
		if time.Now().After(deadline) {
			return errors.Join(census, errors.New("supervise: inherited tree did not retire"))
		}
		time.Sleep(10 * time.Millisecond) // Poll exact killed/adopted identities until every wait is joined.
	}
}
