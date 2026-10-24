// Package xferfinal exposes its frozen transfer contracts here.
// Implementation constructors belong to the leaf owner; aliases keep config
// ports identical without a reverse dependency from xfercontract.
// origin: hot-update S7 plan §8.1 / P00
package xferfinal

import "github.com/LinZiyang666/tether/internal/xfercontract"

type Config = xfercontract.FinalConfig
type Finalizer = xfercontract.FinalizerPort
type Factory = xfercontract.FinalizerFactory
