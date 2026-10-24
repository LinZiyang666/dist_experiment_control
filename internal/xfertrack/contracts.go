// Package xfertrack exposes its frozen transfer contracts here.
// Implementation constructors belong to the leaf owner; aliases keep config
// ports identical without a reverse dependency from xfercontract.
// origin: hot-update S7 plan §8.1 / P00
package xfertrack

import "github.com/LinZiyang666/tether/internal/xfercontract"

type Config = xfercontract.TrackerConfig
type Bindings = xfercontract.Bindings
type Manager = xfercontract.TrackerManager
type Frozen = xfercontract.TrackerFrozen
type Factory = xfercontract.TrackerFactory
type ImportFactory = xfercontract.TrackerImportFactory
