// Package xferauth exposes its frozen transfer contracts here.
// Implementation constructors belong to the leaf owner; aliases keep config
// ports identical without a reverse dependency from xfercontract.
// origin: hot-update S7 plan §8.1 / P00
package xferauth

import "github.com/LinZiyang666/tether/internal/xfercontract"

type Config = xfercontract.AuthConfig
type SettlementVerifier = xfercontract.SettlementVerifier
type Factory = xfercontract.AuthFactory
