//go:build !linux

package handoff

import "os"

// Prepared is unavailable outside Linux. Ordinary startup still succeeds;
// callers must refuse live image replacement on unsupported platforms.
type Prepared struct{}

func Prepare(State, []Resource) (*Prepared, error) { return nil, ErrUnsupported }

func (*Prepared) Exec(string, []string, []string) error { return ErrUnsupported }

func (*Prepared) Close() error { return nil }

func (*Restored) Repack(...Resource) (*Prepared, error) { return nil, ErrUnsupported }

func Restore() (*Restored, error) {
	_, present := os.LookupEnv(environmentKey)
	if !present {
		return nil, nil
	}
	if err := os.Unsetenv(environmentKey); err != nil {
		return nil, err
	}
	return nil, ErrUnsupported
}

// origin: hot-update S6 plan §4.1 / T27
func ValidateResourceOwners([]Resource) error { return ErrUnsupported }

func ResourceFileIdentity(*os.File) (string, error) { return "", ErrUnsupported }

func ValidateListenerFile(*os.File, uint64, uint64, uint64, int, int, bool) error {
	return ErrUnsupported
}
