package xferserve

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/xferauth"
	"github.com/LinZiyang666/tether/internal/xferbucket"
	"github.com/LinZiyang666/tether/internal/xfercapacity"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercontrol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

// Host is the broker's current backend as the composition sees it. Every
// getter resolves today's handle: an image cut or session rebuild never leaves
// a port holding a pre-cut connection or ledger.
type Host struct {
	// ClusterDataDir and DBPath locate the durable continuation state beside
	// the broker's own durable state (StateBase).
	ClusterDataDir, DBPath string
	BrokerID               string
	Conn                   func() *nats.Conn
	Tracker                func() x.TrackerManager
	Settlement             func() SettlementView
	Ledger                 func() x.LedgerPort
	Rows                   Rows
	// Census is the bucket owner's own live-responsibility census; without it
	// no reservation shrink can ever be proved (xferbucket.NewWithCensus).
	Census xferbucket.Census
	Limits x.StorageLimits
	// Router wraps the local coordinator for a cluster (leader selection);
	// nil in single mode.
	Router            func(x.BucketPort) x.BucketPort
	Remote            x.OwnerResolver
	Peers             func(x.Deadline) ([]x.Origin, error)
	Leader            func() bool
	Probe             func(x.Origin, x.Deadline) (x.ProbeResult, error)
	Replicas          int
	Logger            *slog.Logger
	AuthenticateAgent func(string, string, string) error
}

// Assembly is the composed configuration plus the link the broker ingress uses
// to deliver grants and receipts to the original agent.
type Assembly struct {
	Directory *Directory
	Owners    Owners
	Reconnect *settlementReconnect
	Host      Host
	Config    xfercontrol.AssemblyConfig
	Link      *Link
	Local     x.BucketPort
	// Root and Generation identify this origin's config writer for
	// RetireConfigWriter; its successor (rollback or restored image) adopts it.
	Root       string
	Generation uint64
	base       string
	temporary  bool
}

// Release runs after the graph's owners joined: it records the config
// writer's retirement, and removes an in-memory broker's private state root.
func (a Assembly) Release() error {
	d, err := controlbudget.After(5 * time.Second)
	if err == nil {
		err = a.RetireConfigWriter(d)
	}
	if a.Reconnect != nil {
		err = errors.Join(err, a.Reconnect.Stop(d))
	}
	if a.temporary {
		err = errors.Join(err, os.RemoveAll(a.base))
	}
	return err
}

// Assemble builds every production port. It creates no goroutine and
// subscribes nothing; the sleeping tracker activates only in the caller's Bind.
// origin: hot-update S7 plan §5 / §6 / §8.2 / P22
func Assemble(h Host, js jetstream.JetStream, d x.Deadline) (Assembly, error) {
	if js == nil || h.Conn == nil || h.Tracker == nil || h.Ledger == nil || h.Rows == nil {
		return Assembly{}, errors.New(x.CodeOwnerRecovering + ": incomplete broker host")
	}
	if _, err := controlbudget.Remaining(d); err != nil {
		return Assembly{}, err
	}
	root, temporary, err := StateBase(h.ClusterDataDir, h.DBPath)
	if err != nil {
		return Assembly{}, err
	}
	cleanup := func(err error) (Assembly, error) {
		if temporary {
			err = errors.Join(err, os.RemoveAll(root))
		}
		return Assembly{}, err
	}
	identity, stranded, err := LoadIdentity(root, h.BrokerID)
	if err != nil {
		return cleanup(err)
	}
	if h.Logger != nil {
		for _, root := range stranded {
			h.Logger.Warn("broker: continuation state of a previous boot is not adopted; its rows keep their objects until an operator retires them", "root", root)
		}
	}
	boot := identity.Origin.BootID
	base := x.Base{Clock: func() (string, int64, error) {
		now, err := controlbudget.Now()
		return boot, now, err
	}, UTC: time.Now, Capacity: xfercapacity.New("xfer-broker-")}
	permits, err := xferauth.NewPermitAuthority(filepath.Join(identity.Root, "permits"), identity.Origin, identity.Issuer, time.Now)
	if err != nil {
		return cleanup(err)
	}
	// One origin keeps a single bucket-config writer lineage: a graph that is
	// reinstalled after a cut's join (rollback, or the restored process of the
	// same boot) is that writer's successor, never a second generation one.
	previous, generation, err := loadConfigWriter(identity.Root)
	if err != nil {
		return cleanup(err)
	}
	generation++
	authority, err := xferbucket.NewAuthority(x.ConfigWriterProof{BrokerID: identity.Origin.BrokerID, InstanceID: identity.Origin.InstanceID,
		BootID: boot, Generation: generation, Nonce: nuid.Next()}, previous)
	if err != nil {
		return cleanup(err)
	}
	limits := h.Limits
	if nc := h.Conn(); nc != nil && limits.MaxPayload == 0 {
		limits.MaxPayload, limits.MaxMsgSize = nc.MaxPayload(), nc.MaxPayload()
	}
	local, err := xferbucket.NewWithCensus(xferbucket.Config{Base: base, JS: js, Authority: authority, Limits: limits}, h.Census)
	if err != nil {
		return cleanup(err)
	}
	var directory *Directory
	if h.Remote != nil {
		directory, err = openDirectory(js, h.Replicas, d, identity.Root)
		if err != nil {
			return cleanup(err)
		}
		pub, pubErr := identity.Issuer.PublicKey()
		if pubErr != nil {
			return cleanup(pubErr)
		}
		if err = pinDirectory(identity.Root, directory); err != nil {
			return cleanup(err)
		}
		if err = directory.register(identity.Origin, pub, d); err != nil {
			return cleanup(err)
		}
		local = clusterCoordinator(h, directory, identity.Origin, base, xferbucket.Config{Base: base, JS: js, Limits: limits})
	}
	buckets := x.BucketPort(local)
	if h.Router != nil {
		buckets = h.Router(local)
	}
	retirement := &RetirementJournal{Root: filepath.Join(identity.Root, "retirement")}
	capsules := &CapsuleStore{Root: filepath.Join(identity.Root, "capsules"), issuer: identity.Issuer}
	link := &Link{Capsules: capsules, Retirement: retirement, JS: js, AuthenticateAgent: h.AuthenticateAgent, Conn: h.Conn, Rows: h.Rows, Origin: identity.Origin, Issuer: identity.Issuer, Status: func(r x.StatusRequest, d x.Deadline) (x.Status, error) {
		tracker := h.Tracker()
		if tracker == nil {
			return x.Status{}, errors.New(x.CodeOwnerRecovering)
		}
		return tracker.Status(r, d)
	}}
	owners := Owners{Directory: directory, Capsules: capsules, Rows: h.Rows, PreviousRoots: stranded, Origin: identity.Origin, Ledger: h.Ledger, Settlement: h.Settlement, Remote: h.Remote, Peers: h.Peers,
		Retirement: retirement, Permits: permits, PermitRoot: filepath.Join(identity.Root, "permits"), Tracker: h.Tracker, UTC: base.UTC,
		CourierAbsent: func(id x.Identity, origin x.Origin, d x.Deadline) (bool, error) {
			return retirement.courierAbsent(link, id, origin, d)
		}}
	if directory != nil {
		owners.Peers = func(d x.Deadline) ([]x.Origin, error) { return clusterRoster(h.Conn(), d) }
		link.ClusterProbe = func(d x.Deadline) error { return probeRoster(h, owners.Peers, d) }
	}
	pub, err := identity.Issuer.PublicKey()
	if err != nil {
		return cleanup(err)
	}
	issuers, err := xferauth.NewIssuerVerifier([]string{pub})
	if err != nil {
		return cleanup(err)
	}
	verifier, err := xferauth.New(xferauth.Config{Base: base, Issuers: issuers, Owners: owners})
	if err != nil {
		return cleanup(err)
	}
	reconnect := &settlementReconnect{local: verifier, owners: owners, directory: directory, issuer: pub, conn: h.Conn}
	cfg := xfercontrol.AssemblyConfig{Base: base, Origin: identity.Origin, Root: filepath.Join(identity.Root, "ledger"),
		FallbackRoot: filepath.Join(identity.Root, "outbox"), JS: js, Permits: GuardPermits{Directory: directory, PermitAuthority: permits, Owners: owners}, Transport: link, Buckets: buckets,
		Receiver: Receiver{Rows: h.Rows}, Objects: Objects{JS: func() jetstream.JetStream { return js }, Rows: h.Rows, Link: link, Retirement: retirement},
		Owners: owners, SessionOwners: owners}
	return Assembly{Directory: directory, Owners: owners, Reconnect: reconnect, Host: h, Config: cfg, Link: link, Local: local, Root: identity.Root, Generation: generation, base: root, temporary: temporary}, nil
}

// RetainsState reports whether a previous incarnation left durable
// continuation authority beside the broker's durable state. Until the default
// graph re-adopts it, a session removal must not cascade over responsibility
// it cannot enumerate; with none (or an in-memory broker) there is nothing to
// protect and the pre-S7 cascade runs.
// origin: hot-update S7 plan §6.4 / LQ1 — S7 production activation
func RetainsState(clusterDataDir, dbPath string) bool {
	var base string
	switch {
	case clusterDataDir != "":
		base = filepath.Join(clusterDataDir, "xfer-continuation")
	case dbPath != "" && dbPath != ":memory:":
		base = filepath.Join(filepath.Dir(dbPath), "xfer-continuation")
	default:
		return false
	}
	_, err := os.Stat(filepath.Join(base, "identity.json"))
	return !os.IsNotExist(err) // an unreadable root is not proof of absence
}

// StateBase picks the durable continuation directory beside the broker's
// existing durable state (the legacy cap0 ledger uses the same rule). An
// in-memory broker gets a private temporary directory that the caller removes
// at graph stop; it holds nothing a restart could resume.
func StateBase(clusterDataDir, dbPath string) (base string, temporary bool, err error) {
	switch {
	case clusterDataDir != "":
		return filepath.Join(clusterDataDir, "xfer-continuation"), false, nil
	case dbPath != "" && dbPath != ":memory:":
		return filepath.Join(filepath.Dir(dbPath), "xfer-continuation"), false, nil
	}
	base, err = os.MkdirTemp("", "tether-xfer-continuation-")
	return base, true, err
}
