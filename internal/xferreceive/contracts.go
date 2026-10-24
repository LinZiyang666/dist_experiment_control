// Package xferreceive exposes its frozen transfer contracts here.
// Implementation constructors belong to the leaf owner; aliases keep config
// ports identical without a reverse dependency from xfercontract.
// origin: hot-update S7 plan §8.1 / P00
package xferreceive

import "github.com/LinZiyang666/tether/internal/xfercontract"

type Config = xfercontract.ReceiveConfig
type Receiver = xfercontract.Receiver
type Factory = xfercontract.ReceiverFactory
