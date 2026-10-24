// Package xferimage exposes its frozen transfer contracts here.
// Implementation constructors belong to the leaf owner; aliases keep config
// ports identical without a reverse dependency from xfercontract.
// origin: hot-update S7 plan §8.1 / P00
package xferimage

import "github.com/LinZiyang666/tether/internal/xfercontract"

type WorkerEncoder = xfercontract.WorkerEncoder
type WorkerDecoder = xfercontract.WorkerDecoder
type TrackerEncoder = xfercontract.TrackerEncoder
type TrackerDecoder = xfercontract.TrackerDecoder
