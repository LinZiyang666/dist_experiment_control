package cluster

import (
	"os"

	"github.com/LinZiyang666/tether/internal/cluster/storelock"
)

// The leaf owns this responsibility; the facade preserves the cluster API.
// origin: hot-update S6 plan §9.3 / CLUSTER-SLIM

const DataDirLockFile = storelock.DataDirLockFile

var ErrDataDirLockHeld = storelock.ErrDataDirLockHeld
var ErrDataDirLockUnusable = storelock.ErrDataDirLockUnusable

type DataDirLock = storelock.DataDirLock

func DataDirLockPath(dataDir string) string             { return storelock.DataDirLockPath(dataDir) }
func AcquireDataDirLock(dataDir string) (func(), error) { return storelock.AcquireDataDirLock(dataDir) }
func AcquireImageDataDirLock(dataDir string) (*DataDirLock, error) {
	return storelock.AcquireImageDataDirLock(dataDir)
}
func AdoptDataDirLock(dataDir string, f *os.File) (*DataDirLock, error) {
	return storelock.AdoptDataDirLock(dataDir, f)
}
func requireImageDataDirLock(dataDir string) error { return storelock.RequireImageDataDirLock(dataDir) }
