// Package transferpath exposes its frozen transfer contracts here.
// Implementation constructors belong to the leaf owner; aliases keep config
// ports identical without a reverse dependency from xfercontract.
// origin: hot-update S7 plan §8.1 / P00
package transferpath

import (
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/xfercontract/xferport"
)

type FileSource = xferport.FileSource
type AtomicReceiver = xferport.AtomicPort[controlbudget.Deadline, handoff.Resource]
type ImportFactory = xferport.AtomicImportFactory[controlbudget.Deadline, handoff.Resource]
