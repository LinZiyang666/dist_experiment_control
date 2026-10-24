//go:build !linux

package infraroot

import c "github.com/LinZiyang666/tether/internal/infracontract"

func openInstalled(bool) (*Root, error) { return nil, c.ErrUnsupported }
