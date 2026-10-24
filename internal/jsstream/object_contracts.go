// Package jsstream exposes its frozen transfer contracts here.
// Implementation constructors belong to the leaf owner; aliases keep config
// ports identical without a reverse dependency from xfercontract.
// origin: hot-update S7 plan §8.1 / P00
package jsstream

import (
	"io"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

type Writer = xfercontract.ObjectWriter
type WriterFactory = xfercontract.WriterFactory
type ResumeReader = xfercontract.ObjectReader
type ReaderFactory = xfercontract.ReaderFactory

// PutLegacyOnce is the only SDK Put primitive. Keeping ObjectInfo here avoids
// adding SDK result objects to the neutral contract's public-interface imports.
type LegacyPutFactory func(jetstream.JetStream, string, string, io.Reader, controlbudget.Deadline) (*jetstream.ObjectInfo, error)
