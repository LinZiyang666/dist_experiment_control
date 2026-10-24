//go:build !linux

package infraroot

import (
	"os"

	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// origin: hot-update S9 plan §10.2 / T83 — unsupported hosts cannot enable the management socket.
func linkInstalledSocket(*os.File) error { return c.ErrUnsupported }
