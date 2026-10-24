package brokerruntime

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/brokercontrol"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/xferbridge"
	"github.com/nats-io/nats.go"
)

// GuardReloadRole preserves the caller's reader choice (including ACTIVE rollback) while
// checking the original admission at the actual irreversible exec latch, at reversible
// preparation, and once more as the final role/lease sample that follows preparation and
// immediately precedes the real close entry; a refusal returns through the original cut.
// validate(stopped) reads the CURRENT role on every call.
// origin: hot-update S6 plan §5 / internal review rv-cluster-7 / S6 rv-final F1; S8 §12.1 (P15 move)
func GuardReloadRole[T any](image *brokerimage.StoreRun[T], shutdown brokerimage.StoreShutdown, validate func(stopped bool) error) (*brokerimage.StoreRun[T], brokerimage.StoreShutdown) {
	if image != nil {
		guarded := *image
		// Admission must refuse before transfer, listener or store fencing. The
		// later samples still catch expiry or ownership changes during the cut.
		// origin: S6 internal review SINGLEACK F2 / lead decision D-C / plan §7.1
		if freeze := image.Freeze; freeze != nil {
			guarded.Freeze = func(cfg brokerimage.Config) (brokerimage.Cut, error) {
				if err := validate(false); err != nil {
					return nil, err
				}
				return freeze(cfg)
			}
		}
		latch := image.WithExecLatch
		if latch != nil {
			guarded.WithExecLatch = func(exec func() error) error {
				return latch(func() error {
					if err := validate(true); err != nil {
						return err
					}
					return exec()
				})
			}
		}
		image = &guarded
	}
	prepare := shutdown.Prepare
	shutdown.Prepare = func(deadline controlbudget.Deadline) error {
		if err := validate(false); err != nil {
			return err
		}
		if prepare != nil {
			return prepare(deadline)
		}
		return nil
	}
	shutdown.AfterPrepare = func() error { return validate(false) }
	return image, shutdown
}

// NodeShutdown builds the store shutdown for a reload of the current Node: prepare runs the
// supplied role-checked preparation, close stops the Node. Single mode (nil node) has neither.
func NodeShutdown(node func() *cluster.Node, prepare func(*cluster.Node, controlbudget.Deadline) error) brokerimage.StoreShutdown {
	return brokerimage.StoreShutdown{Prepare: func(d controlbudget.Deadline) error {
		if n := node(); n != nil {
			return prepare(n, d)
		}
		return nil
	}, Close: func() error {
		if n := node(); n != nil {
			return n.Shutdown()
		}
		return nil
	}}
}

// PrepareNodeShutdown re-validates the cut's admitted role, the reload topology and an empty
// membership census on the CURRENT Node immediately before the replicated store prepares to stop.
// runErr is the owning Run's cancellation.
// origin: hot-update S6 plan §1.2 / §4.1 / rv-cluster-2; S8 §12.1 (P15 move)
func PrepareNodeShutdown(node *cluster.Node, role storage.RoleReceipt, runErr error, deadline controlbudget.Deadline) error {
	if runErr != nil {
		return runErr
	}
	if err := cluster.ValidateImageRole(node, role); err != nil {
		return err
	}
	if err := brokerimage.CheckReloadRole(node); err != nil {
		return err
	}
	ops, err := cluster.NonTerminalOperations(node.RODB())
	if err != nil {
		return err
	}
	if len(ops) != 0 {
		return errors.New("broker: cluster operations changed during cut")
	}
	return cluster.PrepareImageShutdown(node, deadline)
}

// ProductionImageConfig is the replicated store's image-capable production configuration. paths
// returns the CURRENT DB path and data dir when a stamp is read (Run canonicalises them first).
func ProductionImageConfig(self, raftBind string, logger *slog.Logger, paths func() (string, string), caCert, leafCert, leafKey string) cluster.ProductionConfig {
	dbPath, dataDir := paths()
	return cluster.ProductionConfig{LocalID: self, DataDir: dataDir, DBPath: dbPath,
		RaftBind: raftBind, Logger: logger, CACertPath: caCert, LeafCertPath: leafCert, LeafKeyPath: leafKey,
		ImageOpenCurrent: storage.OpenCurrent, ImageReadOnly: storage.OpenReadOnly,
		ImageRegistry: storage.DefaultDriverRegistry(), ImageStamp: func(db *sql.DB) (storage.StoreStamp, error) {
			identity, err := storage.CanonicalStoreIdentity(paths())
			if err != nil {
				return storage.StoreStamp{}, err
			}
			return storage.ReadStoreStamp(db, identity)
		}}
}

// PreflightImages freshly probes both actual readers against today's schema and the sealed
// boot configuration. Rollback binds its target to the previous SHA carried by the validated
// incoming image (expectedPrevious); it cannot accept an arbitrary file replaced at that pathname.
// origin: hot-update S6 plan §4.2 / T49; S8 §12.1 (P15 move)
func PreflightImages(args []string, stamp storage.StoreStamp, transfers bool, candidatePath, previousPath, expectedPrevious string, deadline controlbudget.Deadline) (candidate, previous, configDigest string, err error) {
	config, err := brokerimage.BootConfigBytes(args)
	if err != nil {
		return "", "", "", err
	}
	domains := []string{brokerimage.Domain, "flow-v1"}
	if transfers {
		domains = append(domains, xferbridge.Domain)
	}
	cand, prev, err := brokerimage.Preflight(brokerimage.PreflightRequest{CandidatePath: candidatePath,
		PreviousPath: previousPath, Config: config, AllowedArgs: args, Stamp: stamp,
		RequiredDomains: domains, Deadline: deadline})
	if err != nil {
		return "", "", "", err
	}
	if expectedPrevious != "" && (cand.SHA256 != expectedPrevious || prev.SHA256 != expectedPrevious) {
		return "", "", "", brokerimage.ErrImage
	}
	digest := sha256.Sum256(config)
	return cand.SHA256, prev.SHA256, hex.EncodeToString(digest[:]), nil
}

// DrainArmed joins the DB-free reply sender (admin and NATS), rather than assuming a timer
// proved delivery of the armed reply.
func DrainArmed(admin *adminsock.Server, nc *nats.Conn, deadline controlbudget.Deadline) error {
	if admin != nil {
		if err := adminsock.JoinImageReplies(admin, deadline); err != nil {
			return err
		}
	}
	left, err := brokercontrol.BrokerAuthRemaining(deadline)
	if err != nil {
		return err
	}
	return nc.FlushTimeout(left)
}

// LocalMaintenance selects the local (single / sole-voter) maintenance lease a restored image
// carried. Remote holders' leases stay in the imported history; only a "local-" holder is this
// process's own admission.
func LocalMaintenance(leases []brokermaint.LocalLease) brokermaint.LocalLease {
	var local brokermaint.LocalLease
	for _, lease := range leases {
		if strings.HasPrefix(lease.Record.Holder, "local-") {
			local = lease
		}
	}
	return local
}

// ImageIdentity returns a fresh cut instance and this host's boot ID. A cluster broker's
// instance is its node ID (the identity peers already route by).
func ImageIdentity(node *cluster.Node) (instance, boot string, err error) {
	instance, err = brokercontrol.MaintenanceNonce()
	if err != nil {
		return "", "", err
	}
	if node != nil {
		instance = node.SelfID()
	}
	bootID, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", "", err
	}
	return instance, strings.TrimSpace(string(bootID)), nil
}

// ImageMode classifies the store a cut exports: single, sole voter or quorum.
func ImageMode(clusterMode bool, node *cluster.Node) (brokerimage.Mode, error) {
	if !clusterMode {
		return brokerimage.ModeSingle, nil
	}
	voters, err := node.NumVoters()
	if err != nil {
		return 0, err
	}
	if voters == 1 {
		return brokerimage.ModeSoleVoter, nil
	}
	return brokerimage.ModeQuorum, nil
}

// A missing boot status has no generation to bind into planned maintenance.
// Returning zero keeps the canonical record validator responsible for refusal.
// origin: S6 lead decision D-C / plan §7.1
func ImageGeneration(status *proto.BrokerImageStatus) uint64 {
	if status == nil {
		return 0
	}
	return status.Generation
}

// JoinDone waits for one closed-handler/loop completion channel within the cut deadline.
func JoinDone(d controlbudget.Deadline, done <-chan struct{}) error {
	return brokerimage.JoinStoreUsers(d, func() (bool, <-chan struct{}) {
		select {
		case <-done:
			return true, nil
		default:
			return false, done
		}
	})
}

// UnsubscribeJoined stops future dispatch on every subscription except the initiating remote
// reload's reply owner (skip): it owns its reply until Armed, is DB-free after admission and is
// joined separately by StopTransport. Each returned channel closes when its handler has joined.
func UnsubscribeJoined(subs []*nats.Subscription, skip string) ([]<-chan struct{}, error) {
	var joins []<-chan struct{}
	var result error
	for _, sub := range subs {
		if sub.Subject == skip {
			continue
		}
		done := make(chan struct{})
		sub.SetClosedHandler(func(string) { close(done) })
		joins = append(joins, done)
		result = errors.Join(result, sub.Unsubscribe())
	}
	return joins, result
}

// LiveSubscriptions keeps only the still-valid reply owners, so a later reversible cut never
// unsubscribes stale handles. origin: S6 internal review rv-final F2
func LiveSubscriptions(subs []*nats.Subscription) []*nats.Subscription {
	live := subs[:0]
	for _, sub := range subs {
		if sub.IsValid() {
			live = append(live, sub)
		}
	}
	return live
}

// StopTransport flushes within the cut budget and joins every NATS control callback. Only after
// it succeeds may the caller forget its connection pointer.
func StopTransport(nc *nats.Conn, deadline controlbudget.Deadline) error {
	left, err := brokercontrol.BrokerAuthRemaining(deadline)
	if err != nil {
		return err
	}
	if err := nc.FlushTimeout(left); err != nil {
		return err
	}
	return brokercontrol.JoinNATSControl(nc)
}

// Connect dials the broker's NATS transport within the cut budget, through the private-inbox
// probe whenever the broker has its own public key.
func Connect(url, pub string, opts []nats.Option, deadline controlbudget.Deadline) (*nats.Conn, error) {
	left, err := brokercontrol.BrokerAuthRemaining(deadline)
	if err != nil {
		return nil, err
	}
	opts = append(opts, nats.Timeout(left))
	if pub == "" {
		return nats.Connect(url, opts...)
	}
	nc, _, err := natsinbox.Connect(url, pub, opts)
	return nc, err
}

// WaitSoleVoter awaits the sole voter's election before exposing its inherited server to
// reconnects: premature home_catching_up DENY spends another backoff after the store is ready.
// origin: hot-update S6 plan §5 / T37
func WaitSoleVoter(n *cluster.Node, d controlbudget.Deadline) error {
	cfg, err := n.RaftConfiguration()
	if err != nil {
		return err
	}
	voters, self := 0, false
	for _, server := range cfg {
		if server.Voter {
			voters++
			self = server.NodeID == n.SelfID()
		}
	}
	if voters != 1 || !self {
		return nil
	}
	for {
		if n.IsLeader() {
			return n.VerifyLeaderRead(func(*sql.DB) error { return nil })
		}
		left, err := brokercontrol.BrokerAuthRemaining(d)
		if err != nil {
			return err
		}
		time.Sleep(min(left, 10*time.Millisecond))
	}
}

// WakeAgents: rebuilt subscriptions missed beats published in the cut. Ask the actual
// agents to send their current heartbeat; do not synthesize ONLINE from a
// retained row or extend the liveness clock. Publication is nonblocking and
// the normal joined heartbeat handlers still own every resulting DB write.
// origin: hot-update S6 plan §8.1 / T50 / EXITS-2-3
func WakeAgents(db *sql.DB, nc *nats.Conn) error {
	nodes, err := node.List(db)
	if err != nil {
		return err
	}
	for _, agent := range nodes {
		if err := nc.PublishRequest(proto.SubjCmdForwarded(agent.SID, agent.NID, proto.ClaimProbeVerb), nc.NewInbox(), nil); err != nil {
			return err
		}
	}
	return nil
}
