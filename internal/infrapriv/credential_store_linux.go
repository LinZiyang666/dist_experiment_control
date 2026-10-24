//go:build linux

package infrapriv

import "os"

// InitializeCredentialStore is the dirfd form used by trusted bootstrap
// composition and hermetic namespace fixtures. The directory must already be
// pinned by that owner; it never discovers an account or follows a seed path.
// origin: S9 internal review rv-infra F01
func InitializeCredentialStore(dir *os.File, owner, gid uint32) error {
	if dir == nil {
		return ErrCredential
	}
	return bootstrapCredentialsAt(dir, owner, gid)
}

func ReadCredentialStore(dir *os.File, kind CredentialKind, owner, gid uint32) (*Credential, error) {
	row, err := credentialRow(kind)
	if err != nil || dir == nil {
		return nil, ErrCredential
	}
	data, err := readOwnedFile(dir, row.Name, owner, gid, row.Mode, 58)
	if err != nil {
		return nil, err
	}
	return decodeCredential(data)
}
