//go:build linux

package infraroot

import (
	"errors"
	"io"
	"os"
	"os/user"
	"strconv"
	"strings"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"github.com/LinZiyang666/tether/internal/infralaunch"
	"github.com/LinZiyang666/tether/internal/infrapriv"
	"github.com/LinZiyang666/tether/internal/ingressswitch"
)

func openInstalled(bootstrap bool) (_ *Root, resultErr error) {
	if err := supported(); err != nil {
		return nil, err
	}
	if os.Geteuid() != 0 {
		return nil, infrapriv.ErrPermission
	}
	files, err := openInstalledFiles("/", 0)
	if err != nil {
		return nil, err
	}
	failed := true
	defer func() {
		if failed {
			resultErr = errors.Join(resultErr, files.Close())
		}
	}()
	account, err := user.Lookup("tether")
	if err != nil {
		return nil, infrapriv.ErrNotEnabled
	}
	uid, err := strconv.ParseUint(account.Uid, 10, 32)
	if err != nil || uid == 0 {
		return nil, infrapriv.ErrPermission
	}
	gid, err := strconv.ParseUint(account.Gid, 10, 32)
	if err != nil || gid == 0 {
		return nil, infrapriv.ErrPermission
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return nil, c.ErrUnsupported
	}
	manager, err := infrahost.NewSystemdManager()
	if err != nil {
		return nil, err
	}
	ingress, err := ingressswitch.NewLinux()
	if err != nil {
		return nil, err
	}
	i := &installation{files: files, manager: manager, ingress: ingress, uid: uint32(uid), gid: uint32(gid), boot: strings.TrimSpace(string(boot)), census: infrahost.ReadCensus, process: infrahost.ReadProcess, before: installedProcesses}
	i.initial = i.initialState
	i.mainProcess = installedMainProcess
	var root *Root
	if bootstrap {
		// No policy or journal is opened on this path until explicit Bootstrap.
		root = &Root{stop: make(chan struct{}), initialize: i.bootstrap, owned: []io.Closer{files}}
	} else {
		root, err = i.worker()
		if err != nil {
			return nil, err
		}
		root.owned = append(root.owned, files)
	}
	failed = false
	return root, nil
}

func (i *installation) worker() (root *Root, resultErr error) {
	defer func() {
		if resultErr != nil && root != nil {
			resultErr = errors.Join(resultErr, root.Close())
			root = nil
		}
	}()
	enabled, err := i.files.Read(stateDirectory+"/enabled", 128)
	if err != nil || string(enabled) != "tether-infra-enabled-v1\n" {
		return nil, infrapriv.ErrNotEnabled
	}
	policy, err := i.policy()
	if err != nil {
		return nil, err
	}
	var record installedRecord
	if err := i.files.Decode(stateDirectory+"/installed.json", &record); err != nil {
		return nil, err
	}
	if record.Version != 1 || record.Purpose != "tether-infra-installed-v1" || record.Scope.Domain != policy.Domain {
		return nil, c.ErrIncomplete
	}
	records := &installedRecords{installation: i, record: record, policy: policy}
	storageRoot, err := i.files.Directory(stateDirectory+"/acme", false, 0, 0)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, storageRoot.Close()) }()
	initial, err := i.files.Directory("/root/.local/share/caddy", false, 0, 0)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, initial.Close()) }()
	deps := Dependencies{Policy: policy, JournalPath: i.files.Path(stateDirectory), Scope: record.Scope,
		Source: records, Facts: records, Manager: i.manager, UnitState: records, UnitPolicies: record.Units,
		Ingress: i.ingress, Changes: installedChanges{records}, Evidence: installedEvidence{records}, Files: records,
		Storage:         infrahost.ACMEConfig{Root: storageRoot, Initial: initial, UID: int(i.uid), GID: int(i.gid), Exit: records.writersExited},
		StorageRevision: records.storageRevision, ResolvePolicy: records.resolvePolicy, PersistPolicy: records.persistPolicy,
		Launch: infralaunch.Bindings{Authorize: records.selection, StoreCensus: records.storeCensus, Process: records.observe, Inspect: records.Inspect},
	}
	root, err = New(deps)
	if err != nil {
		return nil, errors.Join(err, records.Close())
	}
	records.root = root
	root.owned = append(root.owned, records)
	return root, nil
}

// Only fixed managed cgroups are enumerated, including all descendants in the
// full census later. This snapshot seeds Before; it never substitutes MainPID
// for complete socket/writer responsibility at an effect boundary.
func installedProcesses(d c.Deadline) ([]c.Process, error) {
	var result []c.Process
	for _, group := range []string{"/system.slice/nats-server.service", "/system.slice/caddy.service"} {
		data, err := os.ReadFile("/sys/fs/cgroup" + group + "/cgroup.procs")
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, c.ErrIncomplete
		}
		for _, text := range strings.Fields(string(data)) {
			pid, err := strconv.ParseInt(text, 10, 32)
			if err != nil {
				return nil, c.ErrIncomplete
			}
			process, err := infrahost.ReadProcess(int32(pid), d)
			if err != nil || process.Cgroup != group {
				return nil, c.ErrIncomplete
			}
			result = append(result, process)
		}
	}
	if len(result) == 0 {
		return nil, c.ErrIncomplete
	}
	return result, nil
}
