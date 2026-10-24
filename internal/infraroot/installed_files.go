package infraroot

import (
	"github.com/LinZiyang666/tether/internal/infraroot/installfs"
)

const stateDirectory = "/var/lib/tether-infra"
const credentialDirectory = "/etc/tether/infra/credentials"
const runtimeDirectory = "/run/tether-infra"

type installedFiles = installfs.Files

func openInstalledFiles(prefix string, owner uint32) (*installedFiles, error) {
	return installfs.Open(prefix, owner)
}
