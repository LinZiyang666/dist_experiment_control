// Package xferclient exposes its frozen transfer contracts here.
// Implementation constructors belong to the leaf owner; aliases keep config
// ports identical without a reverse dependency from xfercontract.
// origin: hot-update S7 plan §8.1 / P00
package xferclient

import "github.com/LinZiyang666/tether/internal/xfercontract"

type Config = xfercontract.ClientConfig
type Invocation = xfercontract.Invocation
type Factory = xfercontract.ClientFactory
type LegacyConfig = xfercontract.LegacyConfig
type LegacyInvocation = xfercontract.LegacyInvocation
type LegacyFactory = xfercontract.LegacyFactory
