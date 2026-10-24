package upgradecli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/agentupgrade"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/brokerupgrade"
	"github.com/LinZiyang666/tether/internal/brokerupgradeport"
	"github.com/LinZiyang666/tether/internal/spawnsafe"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradestage"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// BootRole is one daemon's configured identity, known after its flags/config
// are parsed but before any daemon state is created. Want carries every Key
// field except DomainID; ControlDir is the role's registered control
// directory; Journal maps the registered Key to its unique store path.
// origin: hot-update S8 plan §3.1 / §5.2 / §11.2 P19
type BootRole struct {
	Want       upgradetx.Key
	ControlDir string
	Journal    func(upgradetx.Key, upgradelock.Registration) string
}

// BootPlan is the canonical boot decision. Registered=false means the
// component has no S8 registration: it runs exactly as before and advertises
// no transaction capability. Exec=true means the running bytes are not the
// durable effective selector and the process must become Effective.
type BootPlan struct {
	Key                     upgradetx.Key
	Registered              bool
	Journal, Guard          string
	Effective, EffectiveSHA string
	Exec                    bool
	// Canonical is the registration's canonical entry: where a broker's
	// candidate is staged (describe). origin: S8 BROKER-EXPORTER R-11
	Canonical string
}

// BootDepthEnv bounds the selector to one same-PID exec. A forged value can
// only make boot refuse; it can never redirect the selector to a path.
// origin: hot-update S8 plan §5.2 ("private boot token 有深度上限")
const BootDepthEnv = "TETHER_UPGRADE_BOOT_SELECTED"

// AgentBootRole is the agent's role: the per-user registered control
// directory and the readonly journal path from §3.1.
func AgentBootRole(home, sid, nid string, uid uint32) BootRole {
	control := filepath.Join(home, "agents", sid, "upgrades")
	return BootRole{
		Want:       upgradetx.Key{Component: upgradetx.ComponentAgent, SID: sid, ConfiguredNID: nid, ConfigPath: filepath.Join(home, "agent", sid, "agent.yaml"), UID: uid},
		ControlDir: control,
		Journal: func(key upgradetx.Key, _ upgradelock.Registration) string {
			return filepath.Join(control, key.DomainID, key.ConfiguredNID, "upgrade-state.json")
		},
	}
}

// BrokerBootRole is the broker's role: the data directory owns both the
// registered control directory and the single canonical journal (§3.1).
func BrokerBootRole(dataDir, configPath, nodeID string, uid uint32) BootRole {
	return BootRole{
		Want:       upgradetx.Key{Component: upgradetx.ComponentBroker, ConfiguredNID: nodeID, ConfigPath: configPath, UID: uid},
		ControlDir: filepath.Join(dataDir, "upgrade-control"),
		Journal: func(upgradetx.Key, upgradelock.Registration) string {
			return filepath.Join(dataDir, "upgrade-state.json")
		},
	}
}

// UserHome is the tether home of the component's UID. For this process's own
// UID it is the caller's resolved home (which honors TETHER_HOME); for another
// UID — root staging a user's agent — it is that account's ~/.tether. It never
// reads the grant file's paths.
func UserHome(uid uint32, own string) (string, error) {
	if uint64(uid) == uint64(os.Geteuid()) {
		return own, nil
	}
	account, err := user.LookupId(strconv.FormatUint(uint64(uid), 10))
	if err != nil || !filepath.IsAbs(account.HomeDir) {
		return "", rejected("component account home is unknown")
	}
	return filepath.Join(account.HomeDir, ".tether"), nil
}

// BrokerDataDir is the directory that owns the broker store: the cluster data
// directory when configured, otherwise the directory of a filesystem DB path
// (a file: URI is reduced to its path). An in-memory or relative store has no
// stable data directory and therefore no S8 registration.
func BrokerDataDir(clusterDataDir, dbPath string) string {
	if clusterDataDir != "" {
		return filepath.Clean(clusterDataDir)
	}
	path := strings.TrimPrefix(dbPath, "file:")
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	if path == "" || path == ":memory:" || !filepath.IsAbs(path) {
		return ""
	}
	return filepath.Dir(filepath.Clean(path))
}

// PlanBoot finds the registration and journal and decides the cold-boot
// image. The journal is opened read-only through its CAS guard under this
// process's boot identity; a journal that exists but holds no accepted
// transaction leaves the canonical entry effective (a Prepare never moves the
// selector). Any unreadable or pending selector refuses boot.
// origin: hot-update S8 plan §5.2 / IL13 / DB09 / N108
func PlanBoot(role BootRole) (BootPlan, error) {
	if role.Journal == nil {
		return BootPlan{}, rejected("boot role has no journal")
	}
	// origin: S8 internal review remaining-orchestration F5 (R5): the explicit
	// retrofit declaration must not silently boot as an unregistered component.
	if err := registerRetrofit(role); err != nil {
		return BootPlan{}, err
	}
	key, reg, registered, err := upgradeartifact.RegisteredKey(role.ControlDir, role.Want)
	if err != nil || !registered {
		return BootPlan{}, err
	}
	running, owner, err := bootProcess()
	if err != nil {
		return BootPlan{}, err
	}
	plan := BootPlan{Key: key, Registered: true, Journal: role.Journal(key, reg), Guard: filepath.Join(reg.ControlDir, key.DomainID+".guard"), Canonical: reg.CanonicalPath}
	var view *upgradetx.View
	if _, statErr := os.Lstat(plan.Journal); statErr == nil {
		store, err := upgradestore.Open(upgradestore.Options{Path: plan.Journal, GuardPath: plan.Guard, Key: key, Owner: owner})
		if err != nil {
			return BootPlan{}, err
		}
		loaded, err := store.Load(key, "")
		var unknown *upgradetx.Error
		switch {
		case err == nil:
			view = &loaded
		case errors.As(err, &unknown) && unknown.Code == upgradetx.CodeGrantUnknown:
		default:
			return BootPlan{}, err
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return BootPlan{}, &upgradetx.Error{Code: upgradetx.CodeStateCorrupt, Detail: "upgrade journal cannot be inspected"}
	}
	plan.Effective, plan.EffectiveSHA, plan.Exec, err = upgradeartifact.SelectEffective(reg.CanonicalPath, key, view, running)
	if err != nil {
		return BootPlan{}, err
	}
	return plan, nil
}

// CanonicalBoot runs a registered component's durable effective selector
// before any daemon state exists (agent and serve). The caller skips it for an
// inherited same-PID image: the cut owner, not the selector, decides that
// boot. An unregistered component runs exactly as before. The two log lines
// are the operator's (and the cold-boot test's) evidence of which image ran.
// origin: hot-update S8 plan §5.2 / §11.2 P19 (canonical boot)
// The returned plan names the registered Key and its unique journal so the
// daemon can bind its store without searching for another one.
func CanonicalBoot(role BootRole, logger *slog.Logger) (BootPlan, error) {
	plan, err := SelectBootImage(role, func(path string, env []string) error {
		logger.Info("upgrade boot: exec selected image", "path", path, "pid", os.Getpid())
		return syscall.Exec(path, os.Args, env)
	})
	if err == nil && plan.Registered {
		logger.Info("upgrade boot: running selected image", "effective_sha", plan.EffectiveSHA, "pid", os.Getpid())
	}
	return plan, err
}

// RegisteredJournal resolves a registered component's Key and unique journal
// WITHOUT running the selector. An inherited same-PID image belongs to its cut
// owner, which still needs the very journal that accepted the cut to verify
// the carried adjunct; re-running the selector there could exec away from it.
// origin: hot-update S8 plan §3.1 / §5.2; S8 FIX-DESCRIBE (P19 request R3)
func RegisteredJournal(role BootRole) (BootPlan, error) {
	if role.Journal == nil {
		return BootPlan{}, rejected("boot role has no journal")
	}
	// origin: S8 internal review remaining-orchestration F5 (R5): the explicit
	// retrofit declaration must not silently boot as an unregistered component.
	if err := registerRetrofit(role); err != nil {
		return BootPlan{}, err
	}
	key, reg, registered, err := upgradeartifact.RegisteredKey(role.ControlDir, role.Want)
	if err != nil || !registered {
		return BootPlan{}, err
	}
	return BootPlan{Key: key, Registered: true, Journal: role.Journal(key, reg), Guard: filepath.Join(reg.ControlDir, key.DomainID+".guard"), Canonical: reg.CanonicalPath}, nil
}

// AgentBoot is the agent daemon's boot decision: the canonical selector for a
// cold start, the bare registration for an inherited same-PID image.
func AgentBoot(role BootRole, inherited bool, logger *slog.Logger) (BootPlan, error) {
	if inherited {
		return RegisteredJournal(role)
	}
	return CanonicalBoot(role, logger)
}

// BrokerBoot is serve's boot decision. A broker with an absolute config path
// and a stable data directory runs its registered selector on a cold start
// and resolves the bare registration for an inherited image; a config check
// or a broker without that identity runs unregistered, exactly as before.
// origin: hot-update S8 plan §5.2 / §11.2 P19; S8 FIX-DESCRIBE (P19 request R4)
func BrokerBoot(clusterDataDir, dbPath, configPath string, inherited, configCheck bool, logger *slog.Logger) (BootPlan, error) {
	dataDir := BrokerDataDir(clusterDataDir, dbPath)
	if configCheck || dataDir == "" || !filepath.IsAbs(configPath) {
		return BootPlan{}, nil
	}
	role := BrokerBootRole(dataDir, filepath.Clean(configPath), "", uint32(os.Geteuid()))
	if inherited {
		return RegisteredJournal(role)
	}
	return CanonicalBoot(role, logger)
}

// BrokerDependencies are the S8 ports of a registered broker: its Key, its
// unique journal opened under this process's owner, the broker runtime
// adapter (which refuses every cut step until the running Broker attaches its
// port) and the completion registry — the imported cut's own registry for a
// restored image that carried an S8 adjunct, an unwired one otherwise. The
// carried adjunct is verified against this journal before the import takes
// any descriptor. An unregistered broker gets none.
// origin: hot-update S8 plan §2.2 / §3.1 / §5.1; S8 FIX-DESCRIBE (P19 request R4); S8 BROKER-EXPORTER R-11
func BrokerDependencies(plan BootPlan, boot *brokerimage.BootImage) (*brokerupgrade.Dependencies, error) {
	if !plan.Registered {
		return nil, nil
	}
	owner, err := brokerOwner()
	if err != nil {
		return nil, err
	}
	store, err := upgradestore.Open(upgradestore.Options{Path: plan.Journal, GuardPath: plan.Guard, Key: plan.Key, Owner: owner, Transition: upgradetx.Transition, ParentReservation: func(parent, slot string) (int64, error) {
		return brokerupgradeport.ParentDeadline(plan.Key, owner, parent, slot)
	}})
	if err != nil {
		return nil, err
	}
	// The running image cannot change under a live process: an exec replaces
	// the whole process (and builds these ports again), and Linux refuses to
	// open a running text for writing (ETXTBSY). Its digest is read once, not
	// once per preflight and sample inside one five-second Start budget.
	// origin: hot-update S8 plan §5.2 / §6.1; S8 BROKER-EXPORTER R-11 (real-cut finding)
	var runningOnce struct {
		sync.Mutex
		sha string
	}
	running := func() (string, error) {
		runningOnce.Lock()
		defer runningOnce.Unlock()
		if runningOnce.sha != "" {
			return runningOnce.sha, nil
		}
		sha, err := upgradeartifact.FileSHA("/proc/self/exe")
		if err == nil {
			runningOnce.sha = sha
		}
		return sha, err
	}
	host := brokerupgrade.RuntimeHost{Key: plan.Key, Journal: store, Owner: brokerOwner, RunningSHA: running,
		Source: upgradestage.SourceReader, Reader: upgradestage.ImageReader, Canonical: plan.Canonical, Candidate: upgradestage.BrokerCandidate}
	deps := &brokerupgrade.Dependencies{Key: plan.Key, Store: store, Registry: brokerupgrade.UnwiredRegistry()}
	if boot != nil {
		if state, _ := boot.UpgradeCut(); state != nil {
			sha, err := running()
			if err == nil {
				err = brokerupgradeport.VerifyInherited(store, plan.Key, *state, sha)
			}
			if err != nil {
				return nil, err
			}
			host.Inherited, deps.Imported = state, state.Attempt
			deps.Lease = func() upgradelock.Lease { _, lease := boot.UpgradeCut(); return lease }
		}
	}
	deps.Stage = func(ctx context.Context, g upgradetx.Grant) (upgradeartifact.Receipt, error) {
		return upgradestage.PublishBroker(ctx, plan.Key, plan.Canonical, filepath.Dir(plan.Guard), store, g, &StagePort{ControlDir: filepath.Dir(plan.Guard), Probe: func(path string) (string, agentimage.Capabilities, error) {
			return agentimage.ProbeCapabilities(path, spawnsafe.RunVersionProbe)
		}})
	}
	deps.Runtime = brokerupgrade.NewRuntime(host)
	if host.Inherited != nil {
		if deps.Registry, err = brokerupgradeport.CutRegistry(deps.Runtime, store, *host.Inherited); err != nil {
			return nil, err
		}
	}
	return deps, nil
}

// brokerOwner is this broker process's S8 owner identity. Same-PID exec keeps
// boot, PID and kernel start tick, so the instance derived from them survives
// an image cut, while any restart yields a new owner: an issuer restart
// invalidates every unstarted grant (§3.1).
func brokerOwner() (upgradetx.Owner, error) {
	owner, err := ProcessOwner(os.Getpid())
	if err != nil {
		return owner, err
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("tether-broker-upgrade-owner\x00%s\x00%d\x00%d", owner.BootID, owner.PID, owner.StartTicks)))
	owner.InstanceID = hex.EncodeToString(sum[:16])
	return owner, nil
}

// AgentPorts are the S8 ports of a registered agent daemon: its registered
// Key and the owner-late opener of its unique journal. The owner is the
// lineage instance agent.New mints or inherits, so only the agent can call
// Open (the agent's openAndImportUpgrade). The journal's directory lives inside the
// registered control directory and is created owner-only on first use; the
// store itself still refuses a corrupt or foreign journal. An unregistered
// agent gets no ports and advertises no transaction capability.
// origin: hot-update S8 plan §3.1 / §5.2; S8 FIX-DESCRIBE (P19 request R3)
func AgentPorts(plan BootPlan) *agentupgrade.Dependencies {
	if !plan.Registered {
		return nil
	}
	return &agentupgrade.Dependencies{Key: plan.Key, Open: func(owner upgradetx.Owner) (upgradetx.Store, error) {
		if err := os.MkdirAll(filepath.Dir(plan.Journal), 0o700); err != nil {
			return nil, &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "upgrade journal directory is unavailable"}
		}
		store, err := agentupgrade.OpenJournal(plan.Journal, plan.Guard, plan.Key, owner, 0)
		if err != nil {
			return nil, err
		}
		return store, nil
	}}
}

// SelectBootImage runs PlanBoot under the one-exec depth bound. After an exec
// into the selected image the running SHA must equal the selection; if it
// does not, boot refuses rather than looping between images.
func SelectBootImage(role BootRole, exec func(path string, env []string) error) (BootPlan, error) {
	depth := os.Getenv(BootDepthEnv)
	_ = os.Unsetenv(BootDepthEnv) // never inherited by daemon children
	plan, err := PlanBoot(role)
	if err != nil || !plan.Exec {
		return plan, err
	}
	if depth != "" {
		return BootPlan{}, &upgradetx.Error{Code: upgradetx.CodeStateCorrupt, Detail: "selected image is not running after its boot exec"}
	}
	if exec == nil {
		return BootPlan{}, rejected("selected image needs an exec port")
	}
	env := append(os.Environ(), BootDepthEnv+"="+plan.EffectiveSHA)
	return plan, exec(plan.Effective, env)
}
