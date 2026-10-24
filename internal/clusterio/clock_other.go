//go:build !linux

package clusterio

import "errors"

func bootNow() (int64, error) { return 0, errors.New("clusterio: image unsupported on this platform") }
