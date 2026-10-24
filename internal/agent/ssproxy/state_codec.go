package ssproxy

import (
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/ssstate"
)

// Keep the runtime and fuzz entry points on the shared private image codec.
// origin: hot-update S5 plan §5.1 / §12.1
func encodeState(records []sealedstate.Record) ([]byte, error) { return ssstate.Encode(records) }
func decodeState(data []byte) ([]sealedstate.Record, error)    { return ssstate.Decode(data) }
