package p10_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/agentupgrade"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/nats-io/nats.go"
	"golang.org/x/sys/unix"
)

// These are in-process policy/candidate fixtures, not same-PID exec evidence.
// Register the complete sibling roster before starting business, then let
// agent.New bind its real runtime and owner-late journal. No runtime, source
// image, completion receipt or successful exec is substituted by the fixture.
// origin: hot-update S8 plan §3.1 / §5.1 / §7.2; S8 internal review R2/R5/R6 P10
type upgradeFixture struct {
	url, pub, fp, exe, control, archiveSHA string
	nc                                     *nats.Conn
	server                                 *httptest.Server
	requests                               atomic.Int64
	keys                                   map[string]upgradetx.Key
}

func newUpgradeFixture(t *testing.T, content []byte, nids ...string) *upgradeFixture {
	t.Helper()
	return newUpgradeFixtureWithDisk(t, content, nil, nids...)
}

func newUpgradeFixtureWithDisk(t *testing.T, content []byte, initialize func(*upgradeFixture), nids ...string) *upgradeFixture {
	t.Helper()
	f := &upgradeFixture{url: startNATS(t), keys: make(map[string]upgradetx.Key)}
	db := openDB(t)
	f.pub, f.fp = freshUserPub(t)
	// The shared legacy seed has a placeholder PIN hash with no creation
	// salt. S8 authority requires an actual creation identity from the writer.
	phc, err := auth.HashPIN("p10-upgrade-fixture")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Create(db, "lab", "lab", f.fp, phc, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	archive, sum := makeTarball(t, content)
	f.archiveSHA = sum
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		f.requests.Add(1)
		_, _ = w.Write(archive)
	}))
	t.Cleanup(f.server.Close)
	root := t.TempDir()
	f.exe, f.control = filepath.Join(root, "tether"), filepath.Join(root, "control")
	if err := os.WriteFile(f.exe, []byte("OLD"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(f.control, 0o700); err != nil {
		t.Fatal(err)
	}
	// Legacy recovery evidence is a cold fixture premise. Never retrofit a
	// marker or previous image after business has begun (S8 plan §9.4).
	if initialize != nil {
		initialize(f)
	}
	if len(nids) == 0 {
		nids = []string{"lab-1"}
	}
	var participants []upgradelock.Participant
	for _, nid := range nids {
		key := upgradetx.Key{Component: upgradetx.ComponentAgent, SID: "lab", ConfiguredNID: nid, UID: uint32(os.Geteuid())}
		participants = append(participants, upgradelock.Participant{Key: key, ServiceID: "p10-agent-" + nid})
	}
	domain, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: f.exe, ControlDir: f.control,
		DeploymentID: "p10-upgrade", Participants: participants})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(startBroker(t, f.url, db, []string{f.server.URL}))
	for _, participant := range participants {
		key := participant.Key
		key.DomainID = domain
		f.keys[key.ConfiguredNID] = key
		deps := &agent.UpgradeDependencies{Key: key, Open: func(owner upgradetx.Owner) (upgradetx.Store, error) {
			return agentupgrade.OpenJournal(filepath.Join(f.control, key.ConfiguredNID+".json"),
				filepath.Join(f.control, domain+".guard"), key, owner, 0)
		}}
		a, err := agent.New(agent.Config{NATSURL: f.url, SID: key.SID, NID: key.ConfiguredNID,
			Logger: silentLog(), HeartbeatInterval: 100 * time.Millisecond, RegisterTimeout: 2 * time.Second,
			Upgrade: deps, UpgradeURLAllowlist: []string{"https://only-this.example.com/"},
			UpgradeNoExit: true, UpgradeExecutablePath: f.exe})
		if err != nil {
			t.Fatal(err)
		}
		if agent.UpgradeEndpoint(a) == nil {
			t.Fatal("registered agent did not bind its production transaction endpoint")
		}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- a.Run(ctx) }()
		t.Cleanup(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("agent did not stop cleanly: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Error("agent did not exit")
			}
		})
	}
	f.nc, err = nats.Connect(f.url, nats.CustomInboxPrefix(auth.InboxPrefixFor(f.pub)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.nc.Close)
	// A successful target description proves the latest register advertised
	// upgrade-tx and the real forwarded subscriber can answer. A sleep or a
	// cached ONLINE row cannot prove either of those readiness conditions.
	for _, nid := range nids {
		var last error
		testharness.WaitFor(t, 5*time.Second, 10*time.Millisecond, func() bool {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			_, last = upgradecli.DescribeNode(ctx, f.session(), nid)
			return last == nil
		})
		if last != nil {
			t.Fatalf("registered target never described itself: %v", last)
		}
	}
	return f
}

func (f *upgradeFixture) session() *upgradecli.NodeSession {
	return upgradecli.NewNodeSession("lab", f.pub, f.fp,
		func(ctx context.Context, subject string, body []byte) ([]byte, error) {
			msg, err := f.nc.RequestWithContext(ctx, subject, body)
			if err != nil {
				return nil, err
			}
			return msg.Data, nil
		}, func() {})
}

func (f *upgradeFixture) request() proto.UpgradeReq {
	return proto.UpgradeReq{URL: f.server.URL + "/tether.tar.gz", SHA256: f.archiveSHA, ProtoVersion: proto.ProtoVersion}
}

func probeUpgradeCandidate(path string) (string, agentimage.Capabilities, error) {
	return agentimage.ProbeCapabilities(path, testharness.RunProcess)
}

// Capture existence, bytes, mode and inode: an equal-content rename is still
// a forbidden install effect, and a missing prev/marker must remain absent.
// origin: hot-update S8 plan §3.3 / §5.2 / §7.2
type upgradeDiskFile struct {
	info os.FileInfo
	body []byte
}

func captureUpgradeDisk(t *testing.T, f *upgradeFixture) map[string]upgradeDiskFile {
	t.Helper()
	files := make(map[string]upgradeDiskFile)
	for _, path := range []string{f.exe, f.exe + ".prev", filepath.Join(filepath.Dir(f.exe), ".tether-upgrade.json")} {
		info, err := os.Stat(path)
		if os.IsNotExist(err) {
			files[path] = upgradeDiskFile{}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		files[path] = upgradeDiskFile{info: info, body: body}
	}
	return files
}

func assertUpgradeDiskUnchanged(t *testing.T, before map[string]upgradeDiskFile) {
	t.Helper()
	for path, old := range before {
		info, err := os.Stat(path)
		if old.info == nil && os.IsNotExist(err) {
			continue
		}
		if err != nil || old.info == nil {
			t.Errorf("refusal changed file existence at %s: %v", path, err)
			continue
		}
		body, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(old.body, body) || info.Mode() != old.info.Mode() || !os.SameFile(info, old.info) {
			t.Errorf("refusal changed binary, prev or marker at %s: %v", path, err)
		}
	}
}

func assertUpgradeRefusal(t *testing.T, resp proto.UpgradeResp, code, detail string, rc int) {
	t.Helper()
	if resp.OK || resp.NewVersion != "" || resp.Code != code || resp.Error != detail ||
		upgradecli.ExitCode(&upgradetx.Error{Code: resp.Code, Detail: resp.Error}) != rc {
		t.Fatalf("expected %s/%s (rc %d), got %+v", code, detail, rc, resp)
	}
}

func assertNoUpgradeDownload(t *testing.T, f *upgradeFixture, before int64) {
	t.Helper()
	if got := f.requests.Load(); got != before {
		t.Fatalf("unauthorized request reached the archive server: requests %d -> %d", before, got)
	}
	if _, err := os.Stat(filepath.Join(f.control, "artifacts")); !os.IsNotExist(err) {
		t.Fatalf("a legacy request published artifacts: %v", err)
	}
}

// The old per-Agent install mutex/flock race is now a domain reservation
// race. Keep the winner's lease until BOTH contenders have answered: closing
// it on receipt of the first result would let a missing guard pass serially.
// origin: hot-update S8 plan §5.1
func assertUpgradeReservationContention(t *testing.T, f *upgradeFixture, nids []string) {
	t.Helper()
	type result struct {
		lease upgradelock.Lease
		err   error
	}
	start := make(chan struct{})
	results := make(chan result, len(nids))
	for _, nid := range nids {
		key := f.keys[nid]
		go func() {
			<-start
			lease, err := upgradelock.Acquire(t.Context(), key, "")
			results <- result{lease, err}
		}()
	}
	close(start)
	var acquired, deferred int
	for range nids {
		r := <-results
		if r.lease != nil {
			t.Cleanup(func() {
				if err := r.lease.Close(); err != nil {
					t.Errorf("close reservation: %v", err)
				}
			})
		}
		switch {
		case r.err == nil && r.lease != nil:
			acquired++
		case r.lease == nil && upgradecli.ExitCode(r.err) == 75:
			deferred++
		default:
			t.Errorf("unexpected reservation result: %v", r.err)
		}
	}
	if acquired != 1 || deferred != 1 {
		t.Fatalf("domain contenders: got %d acquired / %d deferred, want 1 / 1", acquired, deferred)
	}
	// The process-local owner table alone could produce 1/1 even with the
	// kernel lock removed. A separate open-file description is the witness
	// that another process would also be excluded for the whole reservation.
	contender, err := os.OpenFile(filepath.Join(f.control, f.keys[nids[0]].DomainID+".lease"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = contender.Close() }()
	if err := unix.Flock(int(contender.Fd()), unix.LOCK_EX|unix.LOCK_NB); !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
		t.Fatalf("independent file description acquired the held domain reservation: %v", err)
	}
}

func exerciseUpgradeConcurrentRefusals(t *testing.T, f *upgradeFixture, nids []string) {
	t.Helper()
	before := captureUpgradeDisk(t, f)
	for _, nid := range nids {
		assertUpgradeRefusal(t, upgradeRequest(t, f.nc, "lab", f.pub, nid, f.request()),
			"try_again", "legacy_quiescence_unproved", 75)
	}
	body, err := json.Marshal(proto.UpgradeForwardedReq{URL: f.request().URL, SHA256: f.archiveSHA, ActorFP: f.fp})
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		resp proto.UpgradeForwardedResp
		err  error
	}
	start := make(chan struct{})
	results := make(chan result, len(nids))
	for _, nid := range nids {
		go func() {
			<-start
			msg, err := f.nc.Request(proto.SubjCmdForwarded("lab", nid, "upgrade"), body, 10*time.Second)
			var resp proto.UpgradeForwardedResp
			if err == nil {
				err = json.Unmarshal(msg.Data, &resp)
			}
			results <- result{resp, err}
		}()
	}
	close(start)
	// Join both callers before asserting so a first failure cannot abandon
	// the other request while test cleanup closes its connection.
	var replies []result
	for range nids {
		replies = append(replies, <-results)
	}
	for _, r := range replies {
		if r.err != nil {
			t.Fatal(r.err)
		}
		if r.resp.BinaryReplaced {
			t.Fatalf("concurrent grantless request replaced binary: %+v", r.resp)
		}
		assertUpgradeRefusal(t, proto.UpgradeResp{OK: r.resp.OK, Code: r.resp.Code, Error: r.resp.Error, NewVersion: r.resp.NewVersion},
			"upgrade_deferred", "a target-issued upgrade transaction is required", 75)
	}
	assertNoUpgradeDownload(t, f, 0)
	assertUpgradeDiskUnchanged(t, before)
}

func writeUpgradePending(t *testing.T, f *upgradeFixture, deadline time.Time) {
	t.Helper()
	// This is legacy recovery evidence, never a substitute for a target-issued
	// grant. Expiring it cannot authorize Start or erase unresolved history.
	pending := fmt.Sprintf(`{"state":"pending","prev_sha":"aa","new_sha":"bb",`+
		`"prev_version":"v0.0.1","new_version":"v0.0.2","deadline":%q,"boot_count":0,"boot_budget":3}`,
		deadline.UTC().Format(time.RFC3339))
	if err := os.WriteFile(filepath.Join(filepath.Dir(f.exe), ".tether-upgrade.json"), []byte(pending), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.exe+".prev", []byte("KNOWN-GOOD-PREVIOUS"), 0o755); err != nil {
		t.Fatal(err)
	}
}
