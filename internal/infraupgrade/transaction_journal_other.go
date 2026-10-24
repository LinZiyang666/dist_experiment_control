//go:build !linux

package infraupgrade

import (
	"os"

	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// Other platforms cannot supply the Linux journal's pathname and file-kind fence.
// origin: hot-update S9 plan §10.2 / T83
func openTransactionJournal(string) (*os.File, error) { return nil, c.ErrUnsupported }
