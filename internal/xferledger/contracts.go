// Package xferledger exposes its frozen transfer contracts here.
// Implementation constructors belong to the leaf owner; aliases keep config
// ports identical without a reverse dependency from xfercontract.
// origin: hot-update S7 plan §8.1 / P00
package xferledger

import "github.com/LinZiyang666/tether/internal/xfercontract"

type Config = xfercontract.LedgerConfig
type RowV1 = xfercontract.LedgerRow
type Store = xfercontract.LedgerPort
type Factory = xfercontract.LedgerFactory
