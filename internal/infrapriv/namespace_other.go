//go:build !linux

package infrapriv

import "os"
import c "github.com/LinZiyang666/tether/internal/infracontract"

func NewPinnedClient(*os.File, uint32, uint32) (*Client, error) { return nil, c.ErrUnsupported }
func NewPinnedWorkerLimit(*os.File) (*WorkerLimit, error)       { return nil, c.ErrUnsupported }
