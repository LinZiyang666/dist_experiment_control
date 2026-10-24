//go:build !linux

package infrapriv

import (
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"os"
)

func InitializeCredentialStore(*os.File, uint32, uint32) error { return c.ErrUnsupported }
func ReadCredentialStore(*os.File, CredentialKind, uint32, uint32) (*Credential, error) {
	return nil, c.ErrUnsupported
}
