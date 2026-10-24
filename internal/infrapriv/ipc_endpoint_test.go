//go:build linux && amd64

// Copy into internal/infrapriv. Reads installer text only; never executes it.
package infrapriv

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// origin: S9 internal review rv-infra F03
func TestInstalledManagementSocketMatchesClient(t *testing.T) {
	body, err := os.ReadFile("../../scripts/install.sh")
	if err != nil {
		t.Fatal(err)
	}
	var endpoint string
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "ListenStream=$INFRA_RUN_DIR/") {
			endpoint = strings.ReplaceAll(strings.TrimPrefix(line, "ListenStream="), "$INFRA_RUN_DIR", filepath.Dir(DefaultSocketPath))
		}
	}
	if endpoint == "" {
		t.Fatal("installer socket not found")
	}
	if endpoint != DefaultSocketPath {
		t.Fatalf("installer listens at %s but NewClient dials %s", endpoint, DefaultSocketPath)
	}
}
