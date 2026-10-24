//go:build !linux

package observationexec

import (
	"errors"
	"sync/atomic"
)

func Register(int, int, [32]byte, *atomic.Bool) error {
	return errors.New("observation exec: unsupported platform")
}
func Descriptors() ([]int, error)       { return nil, nil }
func Environment(env []string) []string { return env }

func Validate(int, int, [32]byte) error { return errors.New("observation exec: unsupported platform") }
