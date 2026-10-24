package clusterharness

import (
	"bufio"
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

const storeFixtureMode = "_TETHER_S6_STORE_FIXTURE"

// The component receiver runs before testing parses argv after same-PID exec.
// It exercises real storage/Node/handoff implementations. Product checkpoints
// remain separate: a component receiver cannot certify Broker.Run or auth ready.
// origin: hot-update S6 plan §12.1 P26 / testing-standards §0.5–§0.6
func init() {
	if mode := os.Getenv(storeFixtureMode); mode != "" {
		if runtime.GOOS != "linux" {
			os.Exit(1)
		}
		if mode == "bolt" {
			fmt.Println("BOLT_SECOND_OPEN_ENTER")
			b, err := raftboltdb.NewBoltStore(os.Args[len(os.Args)-1])
			if err == nil {
				err = b.Close()
			}
			if err != nil {
				os.Exit(1)
			}
			fmt.Println("BOLT_SECOND_OPEN_DONE")
			os.Exit(0)
		}
		if mode != "owner" {
			os.Exit(1)
		}
		os.Exit(runStoreFixtureOwner())
	}
}

// BrokerStoreEvidence is a public, secret-free projection. ACKs are read from
// the database; the caller's separate ACK ledger supplies their expected set.
// BeforeReplay is captured inside ImageOpenCurrent, before NewForImage creates
// Raft. Never substitute the post-replay rows for this independent durability cut.
// origin: hot-update S6 plan §3.3 / T13 / §14
type BrokerStoreEvidence struct {
	Claim              string             `json:"claim"`
	TestOrArm          string             `json:"test_or_arm"`
	Command            string             `json:"command"`
	NativeRC           int                `json:"native_rc"`
	Verdict            string             `json:"verdict"`
	SourceSHA          string             `json:"source_sha"`
	SourceDigest       string             `json:"source_digest"`
	ArtifactSHA        string             `json:"artifact_sha"`
	PreviousSHA        string             `json:"previous_sha"`
	RunningSHA         string             `json:"running_sha256"`
	ArtifactKind       string             `json:"artifact_kind"`
	PID                int                `json:"pid"`
	StartTicks         uint64             `json:"startticks"`
	Generation         uint64             `json:"generation"`
	Topology           string             `json:"topology"`
	NodeID             string             `json:"node_id"`
	Leader             bool               `json:"leader"`
	Address            string             `json:"address"`
	WallSeconds        float64            `json:"wall_seconds"`
	Store              storage.StoreStamp `json:"store"`
	Seal               storage.StoreSeal  `json:"seal"`
	Drivers            [2]uint32          `json:"driver_counts"`
	ACKs               map[string]string  `json:"ack_ids"`
	Effects            map[string]int     `json:"effects"`
	BeforeReplay       map[string]string  `json:"pre_replay_ack"`
	BeforeReplayCursor uint64             `json:"pre_replay_cursor"`
	ReplayGateCount    uint32             `json:"replay_gate_count"`
	SecondRestoreEmpty bool               `json:"second_restore_empty"`
	NodeCloseSeq       uint64             `json:"node_close_seq"`
	ExecSeq            uint64             `json:"exec_seq"`
	Events             []BrokerStoreEvent `json:"phase_intersections"`
	OracleInputs       map[string]string  `json:"oracle_inputs"`
	Counters           map[string]uint64  `json:"counters"`
	MutationDigest     string             `json:"mutation_digest"`
	StdoutPath         string             `json:"stdout_path"`
	StderrPath         string             `json:"stderr_path"`
	LogMapping         map[string]string  `json:"log_mapping"`
	JS                 *BrokerJSWitness   `json:"js_witness,omitempty"`
}

type BrokerStoreEvent struct {
	Name   string `json:"name"`
	Seq    uint64 `json:"seq"`
	BootNS int64  `json:"boot_ns"`
}

type BrokerJSWitness struct {
	ServerID       string `json:"server_id"`
	NUID           string `json:"nuid"`
	Chunks         uint32 `json:"chunks"`
	Bytes          uint64 `json:"bytes"`
	BytesSHA256    string `json:"bytes_sha256"`
	ConfigSHA256   string `json:"config_sha256"`
	ConsumerAck    uint64 `json:"consumer_ack"`
	ConsumerStream uint64 `json:"consumer_stream"`
	AuditSHA256    string `json:"audit_sha256"`
}

type storeFixtureRequest struct {
	Operation        string              `json:"operation"`
	Config           *storeFixtureConfig `json:"config,omitempty"`
	Peers            []raft.Server       `json:"peers,omitempty"`
	ID               string              `json:"id,omitempty"`
	Value            string              `json:"value,omitempty"`
	Fault            *BrokerImageFault   `json:"fault,omitempty"`
	Artifact         string              `json:"artifact,omitempty"`
	PreviousArtifact string              `json:"previous_artifact,omitempty"`
}

type storeFixtureConfig struct {
	Root, Topology, NodeID, CAPath, CertPath, KeyPath, Candidate, Previous string
}

type storeFixtureReply struct {
	Evidence BrokerStoreEvidence `json:"evidence"`
	Error    string              `json:"error,omitempty"`
}

type storeFixtureProcess struct {
	cmd    *exec.Cmd
	in     io.WriteCloser
	out    <-chan storeFixtureReply
	done   <-chan struct{}
	read   <-chan struct{}
	err    error // published by closing done
	logs   *os.File
	config storeFixtureConfig
}

type brokerStoreFixture struct {
	mu          sync.Mutex
	root        string
	processes   []*storeFixtureProcess
	selected    int
	closed      bool
	sourceSHA   string
	sourceTree  string
	previousSHA string
	started     time.Time
	faultSHA    string
	nats        *server.Server
	nc          *nats.Conn
	js          nats.JetStreamContext
	objects     nats.ObjectStore
	stalled     net.Conn
}

// StartBrokerImageFixture starts one or three private real mTLS/SQLite owners.
// All subprocesses are copies of this test binary, with exact init dispatch;
// supplied product artifacts are copied into custody and never run from PATH.
// Supported checkpoints: before-reload, write/<ID>, retry/<ID>, barrier,
// store-closed, component-exec, component-exec-error, normal-stop, preflight,
// js-witness and node/<ID>. Product reload-control-ready is deliberately refused
// until the production Run assembly exists; callers must report that dependency.
// origin: hot-update S6 plan §2.1 / §12.1 P26
func StartBrokerImageFixture(t *testing.T, cfg BrokerImageFixtureConfig) (*BrokerImageFixture, error) {
	t.Helper()
	if runtime.GOOS != "linux" {
		return nil, handoff.ErrUnsupported
	}
	if cfg.Topology != "single" && cfg.Topology != "sole-voter" && cfg.Topology != "quorum" {
		return nil, errors.New("broker fixture: topology must be single, sole-voter or quorum")
	}
	root := testharness.GuardProcessDirectory(t, t.TempDir())
	f := &brokerStoreFixture{root: root, started: time.Now()}
	wrapped := &BrokerImageFixture{api: f}
	t.Cleanup(func() {
		if err := wrapped.Close(); err != nil {
			t.Error(err)
		}
	})
	_, file, _, _ := runtime.Caller(0)
	repo := filepath.Dir(filepath.Dir(filepath.Dir(file)))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "git", "rev-parse", "HEAD")
	command.Dir = repo
	sha, err := testharness.Output(command)
	if err != nil {
		return wrapped, err
	}
	f.sourceSHA = strings.TrimSpace(string(sha))
	f.sourceTree, err = storeFixtureSourceDigest(repo)
	if err != nil {
		return wrapped, err
	}
	exe, err := os.Executable()
	if err != nil {
		return wrapped, err
	}
	helper := filepath.Join(root, "store-owner")
	if err := storeFixtureCopy(exe, helper, nil); err != nil {
		return wrapped, err
	}
	candidate, previous := filepath.Join(root, "candidate"), filepath.Join(root, "previous")
	for _, input := range []struct {
		source, target string
		suffix         []byte
	}{
		{exe, previous, nil}, {exe, candidate, []byte("\nS6 component candidate\n")},
	} {
		if err := storeFixtureCopy(input.source, input.target, input.suffix); err != nil {
			return wrapped, err
		}
	}
	f.previousSHA, err = storeFixtureHash(previous)
	if err != nil {
		return wrapped, err
	}
	// Product artifacts have separate custody paths. They do not replace the
	// component receiver and cannot accidentally satisfy its exec observations.
	for _, input := range []struct{ source, name string }{{cfg.CandidatePath, "product-candidate"}, {cfg.PreviousPath, "product-previous"}, {cfg.FaultArtifactPath, "fault-artifact"}} {
		if input.source == "" {
			continue
		}
		if !filepath.IsAbs(input.source) || strings.Contains(input.source, "/.local/bin/") || strings.Contains(input.source, "/.tether/") {
			return wrapped, errors.New("broker fixture: artifact requires an absolute non-production path")
		}
		if err := storeFixtureCopy(input.source, filepath.Join(root, input.name), nil); err != nil {
			return wrapped, err
		}
	}
	if cfg.FaultArtifactPath != "" {
		f.faultSHA, err = storeFixtureHash(filepath.Join(root, "fault-artifact"))
		if err != nil {
			return wrapped, err
		}
	}
	ca := NewRouteCA(t, "s6-store")
	caPath := filepath.Join(root, "route-ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Cert.Raw}), 0600); err != nil {
		return wrapped, err
	}
	count := 1
	if cfg.Topology == "quorum" {
		count = 3
	}
	var peers []raft.Server
	for i := 0; i < count; i++ {
		dir := filepath.Join(root, fmt.Sprintf("node-%d", i))
		if err := os.Mkdir(dir, 0700); err != nil {
			return wrapped, err
		}
		leaf := ca.Leaf(t, fmt.Sprintf("node-%d", i))
		// Standard client verification needs a loopback SAN. Keep the generated
		// key and cluster CA; no InsecureSkipVerify exception enters the harness.
		template, err := x509.ParseCertificate(leaf.Certificate[0])
		if err != nil {
			return wrapped, err
		}
		template.IPAddresses = []net.IP{net.IPv4(127, 0, 0, 1)}
		publicKey, err := brokerFixturePublicKey(leaf.PrivateKey)
		if err != nil {
			return wrapped, err
		}
		der, err := x509.CreateCertificate(rand.Reader, template, ca.Cert, publicKey, ca.Key)
		if err != nil {
			return wrapped, err
		}
		leaf.Certificate[0] = der
		certPath, keyPath := filepath.Join(dir, "route-cert.pem"), filepath.Join(dir, "route-key.pem")
		key, err := x509.MarshalPKCS8PrivateKey(leaf.PrivateKey)
		if err != nil {
			return wrapped, err
		}
		for path, block := range map[string]*pem.Block{certPath: {Type: "CERTIFICATE", Bytes: leaf.Certificate[0]}, keyPath: {Type: "PRIVATE KEY", Bytes: key}} {
			if err := os.WriteFile(path, pem.EncodeToMemory(block), 0600); err != nil {
				return wrapped, err
			}
		}
		p, err := startStoreFixtureProcess(helper, root, storeFixtureConfig{Root: dir, Topology: cfg.Topology, NodeID: fmt.Sprintf("node-%d", i), CAPath: caPath, CertPath: certPath, KeyPath: keyPath, Candidate: candidate, Previous: previous})
		if p != nil {
			f.processes = append(f.processes, p)
		}
		if err != nil {
			return wrapped, err
		}
		row, err := p.call(storeFixtureRequest{Operation: "prepare", Config: &p.config})
		if err != nil {
			return wrapped, err
		}
		peers = append(peers, raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(p.config.NodeID), Address: raft.ServerAddress(row.Address)})
	}
	for _, p := range f.processes {
		if _, err := p.call(storeFixtureRequest{Operation: "start", Peers: peers}); err != nil {
			return wrapped, err
		}
	}
	if cfg.Topology != "single" {
		deadline := time.Now().Add(15 * time.Second)
		found := false
		for time.Now().Before(deadline) {
			for i, p := range f.processes {
				row, err := p.call(storeFixtureRequest{Operation: "status"})
				if err != nil {
					return wrapped, err
				}
				if row.Leader {
					f.selected, found = i, true
					break
				}
			}
			if found {
				break
			}
			time.Sleep(20 * time.Millisecond) // Poll actual Raft election; production timers are unchanged.
		}
		if !found {
			return wrapped, errors.New("broker fixture: production-timer leader election expired")
		}
		if _, err := f.processes[f.selected].call(storeFixtureRequest{Operation: "barrier"}); err != nil {
			return wrapped, err
		}
	}
	return wrapped, nil
}

func (f *BrokerImageFixture) Checkpoint(label string) (BrokerImageReceipt, error) {
	if f == nil || f.api == nil {
		return BrokerImageReceipt{}, errors.New("broker fixture: missing owner")
	}
	return f.api.Checkpoint(label)
}
func (f *BrokerImageFixture) ArmFault(fault BrokerImageFault) error {
	if f == nil || f.api == nil {
		return errors.New("broker fixture: missing owner")
	}
	return f.api.ArmFault(fault)
}
func (f *BrokerImageFixture) Close() error {
	if f == nil || f.api == nil {
		return nil
	}
	return f.api.Close()
}

func (f *brokerStoreFixture) Checkpoint(label string) (BrokerImageReceipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || len(f.processes) == 0 {
		return BrokerImageReceipt{}, errors.New("broker fixture: owner closed")
	}
	request := storeFixtureRequest{Operation: label}
	switch {
	case label == "before-reload" || label == "js-witness" || label == "snapshot-stall" || label == "snapshot-reject-stale":
		request.Operation = "status"
	case label == "reload-control-ready":
		return BrokerImageReceipt{}, errors.New("NOT_RUN pending P11/P12/P22: production ImageRun assembly and restored Run activation are not installed; component exec cannot certify Broker.Run")
	case strings.HasPrefix(label, "node/"):
		id := strings.TrimPrefix(label, "node/")
		found := false
		for i, p := range f.processes {
			if p.config.NodeID == id {
				f.selected, found = i, true
			}
		}
		if !found {
			return BrokerImageReceipt{}, errors.New("broker fixture: unknown node")
		}
		request.Operation = "status"
	case strings.HasPrefix(label, "write/") || strings.HasPrefix(label, "retry/"):
		_, request.ID, _ = strings.Cut(label, "/")
		request.Operation, request.Value = "write", storeFixtureDigest([]byte("ack:"+request.ID))
	case label == "preflight":
		request.Artifact = filepath.Join(f.root, "product-candidate")
		request.PreviousArtifact = filepath.Join(f.root, "product-previous")
	}
	if label == "js-witness" && f.js == nil {
		if err := f.startJS(); err != nil {
			return BrokerImageReceipt{}, err
		}
	}
	row, err := f.processes[f.selected].call(request)
	if err != nil {
		return BrokerImageReceipt{}, err
	}
	if label == "snapshot-stall" {
		if err := f.stallSnapshot(&row); err != nil {
			return BrokerImageReceipt{}, err
		}
	}
	if label == "snapshot-reject-stale" {
		if err := f.rejectStaleSnapshot(&row); err != nil {
			return BrokerImageReceipt{}, err
		}
	}
	if label == "store-closed" && f.stalled != nil {
		// Only set this oracle deadline after the product reports joined/closed.
		// It cannot rescue product staging before PrepareImageShutdown returns.
		if err := f.stalled.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			return BrokerImageReceipt{}, err
		}
		_, err := io.Copy(io.Discard, f.stalled)
		if err != nil {
			return BrokerImageReceipt{}, errors.New("broker fixture: stalled snapshot peer remained open after close receipt")
		}
		row.Counters["snapshot_peer_closed"] = 1
		_ = f.stalled.Close()
		f.stalled = nil
	}
	if f.js != nil {
		row.JS, err = f.jsWitness()
		if err != nil {
			return BrokerImageReceipt{}, err
		}
	}
	row.Claim, row.TestOrArm, row.Command = label, "P26/store-process", "fixture checkpoint "+label
	row.Verdict, row.SourceSHA, row.SourceDigest, row.PreviousSHA = "OBSERVED", f.sourceSHA, f.sourceTree, f.previousSHA
	row.ArtifactKind, row.ArtifactSHA, row.WallSeconds = "component-exec", row.RunningSHA, time.Since(f.started).Seconds()
	row.StdoutPath, row.StderrPath = "receipts.jsonl", f.processes[f.selected].config.NodeID+"/stderr.log"
	row.LogMapping = map[string]string{"owner_stderr": row.StderrPath, "checkpoint": row.StdoutPath}
	if row.OracleInputs == nil {
		row.OracleInputs = make(map[string]string)
	}
	row.OracleInputs["source_digest"] = f.sourceTree
	// Receipt paths are relative to the private archive root, while device/inode
	// remain the independently measured physical identity.
	for _, stamp := range []*storage.StoreStamp{&row.Store, &row.Seal.Stamp} {
		stamp.Identity.Path, _ = filepath.Rel(f.root, stamp.Identity.Path)
		stamp.Identity.DataDir, _ = filepath.Rel(f.root, stamp.Identity.DataDir)
		stamp.Identity.LockPath, _ = filepath.Rel(f.root, stamp.Identity.LockPath)
	}
	body, err := json.Marshal(row)
	if err != nil {
		return BrokerImageReceipt{}, err
	}
	archive, err := os.OpenFile(filepath.Join(f.root, "receipts.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return BrokerImageReceipt{}, err
	}
	_, writeErr := archive.Write(append(append([]byte(nil), body...), '\n'))
	if err := errors.Join(writeErr, archive.Close()); err != nil {
		return BrokerImageReceipt{}, err
	}
	return BrokerImageReceipt{Label: label, JSON: body, Digest: storeFixtureDigest(body)}, nil
}

func (f *brokerStoreFixture) ArmFault(fault BrokerImageFault) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || len(f.processes) == 0 || fault.Ordinal != 1 || !fault.Once {
		return errors.New("broker fixture: fault requires a live owner and one explicit occurrence")
	}
	if fault.Point == "after-take" || fault.Point == "active" {
		if f.faultSHA == "" {
			return errors.New("broker fixture: missing SHA-bound fault artifact")
		}
		return errors.New("NOT_RUN pending P11/P12/P22: fault artifact requires production image activation")
	}
	if fault.Point == "js-recreate" {
		if f.objects == nil {
			return errors.New("broker fixture: JS witness not initialized")
		}
		body, err := f.objects.GetBytes("original")
		if err != nil {
			return err
		}
		if err := f.objects.Delete("original"); err != nil {
			return err
		}
		_, err = f.objects.PutBytes("original", body)
		return err
	}
	_, err := f.processes[f.selected].call(storeFixtureRequest{Operation: "fault", Fault: &fault})
	return err
}

func (f *brokerStoreFixture) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	defer func() {
		if f.stalled != nil {
			_ = f.stalled.Close()
		}
	}()
	var errs []error
	for _, p := range f.processes {
		select {
		case <-p.done:
		default:
			_, err := p.call(storeFixtureRequest{Operation: "stop"})
			errs = append(errs, err)
		}
		_ = p.in.Close()
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			_ = p.cmd.Process.Kill()
			<-p.done
			errs = append(errs, errors.New("broker fixture: normal stop did not join"))
		}
		<-p.read
		errs = append(errs, p.err, p.logs.Close())
	}
	if f.nc != nil {
		f.nc.Close()
	}
	if f.nats != nil {
		f.nats.Shutdown()
		f.nats.WaitForShutdown()
	}
	return errors.Join(errs...)
}

func startStoreFixtureProcess(path, home string, cfg storeFixtureConfig) (*storeFixtureProcess, error) {
	p := &storeFixtureProcess{config: cfg, cmd: exec.Command(path, "-test.run=^TestBrokerStoreFixtureReceiver$", "-test.count=1")}
	p.cmd.Dir, p.cmd.Env = cfg.Root, storeFixtureEnvironment(home, "owner")
	var err error
	p.logs, err = os.OpenFile(filepath.Join(cfg.Root, "stderr.log"), os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	p.cmd.Stderr = p.logs
	p.in, err = p.cmd.StdinPipe()
	if err != nil {
		_ = p.logs.Close()
		return nil, err
	}
	output, writer, err := os.Pipe()
	if err != nil {
		_ = p.in.Close()
		_ = p.logs.Close()
		return nil, err
	}
	p.cmd.Stdout = writer
	waitProcess, err := testharness.StartProcess(p.cmd)
	if err != nil {
		_ = output.Close()
		_ = writer.Close()
		_ = p.in.Close()
		_ = p.logs.Close()
		return nil, err
	}
	_ = writer.Close()
	done, read := make(chan struct{}), make(chan struct{})
	replies := make(chan storeFixtureReply, 1)
	p.done, p.read, p.out = done, read, replies
	go func() { p.err = waitProcess(); close(done) }()
	go func() {
		defer func() { _ = output.Close() }()
		defer close(read)
		defer close(replies)
		decoder := json.NewDecoder(output)
		for {
			var reply storeFixtureReply
			if decoder.Decode(&reply) != nil {
				return
			}
			select {
			case replies <- reply:
			default:
				select {
				case replies <- reply:
				case <-done:
					return
				}
			}
		}
	}()
	return p, nil
}

func (p *storeFixtureProcess) call(request storeFixtureRequest) (BrokerStoreEvidence, error) {
	if err := json.NewEncoder(p.in).Encode(request); err != nil {
		return BrokerStoreEvidence{}, err
	}
	select {
	case reply, ok := <-p.out:
		if !ok {
			return BrokerStoreEvidence{}, errors.New("broker fixture: child exited without receipt")
		}
		if reply.Error != "" {
			return reply.Evidence, errors.New(reply.Error)
		}
		return reply.Evidence, nil
	case <-time.After(15 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
		<-p.read
		return BrokerStoreEvidence{}, errors.New("broker fixture: checkpoint timeout; child killed and waited")
	}
}

func storeFixtureEnvironment(home, mode string) []string {
	var env []string
	for _, row := range handoff.CleanEnvironment(os.Environ()) {
		name, _, _ := strings.Cut(row, "=")
		if name != "HOME" && name != "GORACE" && !strings.HasPrefix(name, "TETHER_") && !strings.HasPrefix(name, "_TETHER_") {
			env = append(env, row)
		}
	}
	// Race's exit-only diagnostic sleep is not a product timer. Four pure probe
	// children plus their reaper would consume the 5s admission budget solely
	// after successful work, as in the existing cli_e2e/flow exec harnesses.
	return append(env, "HOME="+home, "TETHER_HOME="+home, "GORACE="+strings.TrimSpace(os.Getenv("GORACE")+" atexit_sleep_ms=0"), storeFixtureMode+"="+mode)
}

func storeFixtureCopy(source, target string, suffix []byte) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	info, err := in.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("broker fixture: artifact is not a regular file")
	}
	out, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	_, suffixErr := out.Write(suffix)
	return errors.Join(copyErr, suffixErr, out.Close())
}
func storeFixtureHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	_, err = io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil)), err
}
func storeFixtureDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
func storeFixtureSourceDigest(root string) (string, error) {
	paths := []string{"go.mod", "go.sum"}
	for _, dir := range []string{"cmd", "internal", "test/clusterharness"} {
		if err := filepath.WalkDir(filepath.Join(root, dir), func(path string, e os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !e.IsDir() && strings.HasSuffix(path, ".go") {
				relative, err := filepath.Rel(root, path)
				if err != nil {
					return err
				}
				paths = append(paths, relative)
			}
			return nil
		}); err != nil {
			return "", err
		}
	}
	sort.Strings(paths)
	h := sha256.New()
	for _, path := range paths {
		body, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			return "", err
		}
		_, _ = fmt.Fprintf(h, "%d:%s:%d:", len(path), path, len(body))
		_, _ = h.Write(body)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// JS lives outside the exec owner, matching S6's external NATS boundary. Cold
// setup creates it once; witness reads never ensure, reset or recreate objects.
// origin: hot-update S6 plan §9.2 / T51
func (f *brokerStoreFixture) startJS() error {
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: filepath.Join(f.root, "jetstream"), AlwaysEnableNonce: true, NoLog: true, NoSigs: true})
	if err != nil {
		return err
	}
	f.nats = s
	s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		return errors.New("broker fixture: external JS server not ready")
	}
	f.nc, err = nats.Connect(s.ClientURL())
	if err != nil {
		return err
	}
	f.js, err = f.nc.JetStream()
	if err != nil {
		return err
	}
	f.objects, err = f.js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "S6_FIXTURE", Storage: nats.FileStorage})
	if err != nil {
		return err
	}
	if _, err := f.objects.PutBytes("original", bytes.Repeat([]byte("original-object-chunk\x00"), 16384)); err != nil {
		return err
	}
	if _, err := f.js.AddStream(&nats.StreamConfig{Name: "S6_AUDIT", Subjects: []string{"s6.audit"}, Storage: nats.FileStorage}); err != nil {
		return err
	}
	if _, err := f.js.Publish("s6.audit", []byte("terminal:request-original:exactly-once")); err != nil {
		return err
	}
	if _, err := f.js.AddConsumer("S6_AUDIT", &nats.ConsumerConfig{Durable: "s6-consumer", FilterSubject: "s6.audit", AckPolicy: nats.AckExplicitPolicy}); err != nil {
		return err
	}
	sub, err := f.js.PullSubscribe("s6.audit", "s6-consumer", nats.Bind("S6_AUDIT", "s6-consumer"))
	if err != nil {
		return err
	}
	defer func() { _ = sub.Unsubscribe() }()
	msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil || len(msgs) != 1 {
		return errors.Join(err, errors.New("broker fixture: terminal audit not delivered"))
	}
	return msgs[0].AckSync()
}
func (f *brokerStoreFixture) jsWitness() (*BrokerJSWitness, error) {
	info, err := f.objects.GetInfo("original")
	if err != nil {
		return nil, err
	}
	body, err := f.objects.GetBytes("original")
	if err != nil {
		return nil, err
	}
	stream, err := f.js.StreamInfo("OBJ_S6_FIXTURE")
	if err != nil {
		return nil, err
	}
	config, err := json.Marshal(stream.Config)
	if err != nil {
		return nil, err
	}
	consumer, err := f.js.ConsumerInfo("S6_AUDIT", "s6-consumer")
	if err != nil {
		return nil, err
	}
	audit, err := f.js.GetLastMsg("S6_AUDIT", "s6.audit")
	if err != nil {
		return nil, err
	}
	return &BrokerJSWitness{ServerID: f.nats.ID(), NUID: info.NUID, Chunks: info.Chunks, Bytes: info.Size, BytesSHA256: storeFixtureDigest(body), ConfigSHA256: storeFixtureDigest(config), ConsumerAck: consumer.AckFloor.Consumer, ConsumerStream: consumer.AckFloor.Stream, AuditSHA256: storeFixtureDigest(audit.Data)}, nil
}

// BrokerImageDataDirectory exposes custody to independent lock/process oracles;
// it does not add paths to public receipts or create a second store fixture.
func BrokerImageDataDirectory(f *BrokerImageFixture, nodeID string) (string, error) {
	if f == nil {
		return "", errors.New("broker fixture: missing owner")
	}
	owner, ok := f.api.(*brokerStoreFixture)
	if !ok {
		return "", errors.New("broker fixture: foreign owner")
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	for _, p := range owner.processes {
		if p.config.NodeID == nodeID {
			return p.config.Root, nil
		}
	}
	return "", errors.New("broker fixture: unknown node")
}

func (f *brokerStoreFixture) snapshotConnection() (*tls.Conn, error) {
	p := f.processes[f.selected]
	ca, err := os.ReadFile(p.config.CAPath)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("broker fixture: invalid snapshot CA")
	}
	leaf, err := tls.LoadX509KeyPair(p.config.CertPath, p.config.KeyPath)
	if err != nil {
		return nil, err
	}
	address, err := p.configAddress()
	if err != nil {
		return nil, err
	}
	return tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", address, &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{leaf}, MinVersion: tls.VersionTLS12})
}
func (p *storeFixtureProcess) configAddress() (string, error) {
	row, err := p.call(storeFixtureRequest{Operation: "status"})
	if err != nil {
		return "", err
	}
	if row.Address == "" {
		return "", errors.New("broker fixture: missing live transport address")
	}
	return row.Address, nil
}
func (f *brokerStoreFixture) snapshotHeader(row *BrokerStoreEvidence, bytes int64) (raft.InstallSnapshotRequest, error) {
	var peers []raft.Server
	for _, p := range f.processes {
		address, err := p.configAddress()
		if err != nil {
			return raft.InstallSnapshotRequest{}, err
		}
		peers = append(peers, raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(p.config.NodeID), Address: raft.ServerAddress(address)})
	}
	return raft.InstallSnapshotRequest{SnapshotVersion: 1, Term: row.Seal.Role.Term, LastLogIndex: row.Store.AppliedIndex + 100, LastLogTerm: max(row.Store.AppliedTerm, row.Seal.Role.Term), ConfigurationIndex: row.Seal.Role.ConfigIndex, Configuration: raft.EncodeConfiguration(raft.Configuration{Servers: peers}), Size: bytes}, nil
}

func (f *brokerStoreFixture) stallSnapshot(row *BrokerStoreEvidence) error {
	if row.Topology == "single" || f.stalled != nil {
		return errors.New("broker fixture: stalled snapshot requires a live Raft owner")
	}
	conn, err := f.snapshotConnection()
	if err != nil {
		return err
	}
	f.stalled = conn // Register before the first payload write or failure.
	if _, err := conn.Write([]byte{2}); err != nil {
		return err
	}
	header, err := f.snapshotHeader(row, 1<<20)
	if err != nil {
		return err
	}
	if err := codec.NewEncoder(conn, &codec.MsgpackHandle{}).Encode(&header); err != nil {
		return err
	}
	prefix := []byte("real-stalled-payload-prefix")
	if _, err := conn.Write(prefix); err != nil {
		return err
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		paths, err := filepath.Glob(filepath.Join(f.processes[f.selected].config.Root, "rpc-snapshot-*.db"))
		if err != nil {
			return err
		}
		for _, path := range paths {
			stat, err := os.Stat(path)
			if err == nil && stat.Size() >= int64(len(prefix)) {
				row.Counters["stalled_snapshot_stage_bytes"] = uint64(stat.Size())
				return nil
			}
		}
		time.Sleep(10 * time.Millisecond) // Observe real staging, not a guessed admission sleep.
	}
	return errors.New("broker fixture: RPC did not enter real snapshot staging")
}

func (f *brokerStoreFixture) rejectStaleSnapshot(row *BrokerStoreEvidence) error {
	if row.Topology == "single" {
		return errors.New("broker fixture: snapshot RPC requires Raft")
	}
	payload, err := os.ReadFile(filepath.Join(f.processes[f.selected].config.Root, "saved-snapshot.db"))
	if err != nil {
		return err
	}
	conn, err := f.snapshotConnection()
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	header, err := f.snapshotHeader(row, int64(len(payload)))
	if err != nil {
		return err
	}
	if _, err := conn.Write([]byte{2}); err != nil {
		return err
	}
	if err := codec.NewEncoder(conn, &codec.MsgpackHandle{}).Encode(&header); err != nil {
		return err
	}
	if _, err := io.Copy(conn, bytes.NewReader(payload)); err != nil {
		return err
	}
	decoder := codec.NewDecoder(conn, &codec.MsgpackHandle{})
	var remoteError string
	var response raft.InstallSnapshotResponse
	if err := decoder.Decode(&remoteError); err != nil {
		return err
	}
	if err := decoder.Decode(&response); err != nil {
		return err
	}
	if remoteError == "" || response.Success {
		return errors.New("broker fixture: runtime accepted an obsolete snapshot after a new durable ACK")
	}
	if !strings.Contains(remoteError, "current command floor") {
		return errors.New("broker fixture: stale RPC rejected before the intended current-floor guard")
	}
	row.Counters["snapshot_floor_rejected"] = 1
	return nil
}

type storeFixtureOwner struct {
	Config          storeFixtureConfig
	Peers           []raft.Server
	Evidence        BrokerStoreEvidence
	Fault           BrokerImageFault
	lock            *cluster.DataDirLock
	node            *cluster.Node
	db              *sql.DB
	transport       *raft.NetworkTransport
	cut             brokerimage.Cut
	admissionFenced bool
	staleDB         []byte
	closed          bool
	stop            bool
}

func runStoreFixtureOwner() int {
	o := &storeFixtureOwner{}
	restored, err := handoff.Restore()
	if err != nil {
		return 1
	}
	if restored != nil {
		if restored.State.Role != "broker-store-fixture" || json.Unmarshal(restored.State.Data, o) != nil {
			_ = restored.Close()
			return 1
		}
		o.Evidence.RunningSHA = "" // Observe this newly mapped executable, never the exporter's cached hash.
		file, err := restored.TakeFile("broker-store-lock", "lock")
		if err != nil {
			_ = restored.Close()
			return 1
		}
		o.lock, err = cluster.AdoptDataDirLock(o.Config.Root, file)
		if err != nil {
			_ = file.Close()
			_ = restored.Close()
			return 1
		}
		o.Evidence.Generation = restored.State.Generation
		second, secondErr := handoff.Restore()
		o.Evidence.SecondRestoreEmpty = second == nil && secondErr == nil
		if second != nil {
			_ = second.Close()
		}
		err = errors.Join(restored.Close(), o.openCurrent())
		if err != nil {
			_ = o.shutdown()
			_ = json.NewEncoder(os.Stdout).Encode(storeFixtureReply{Error: err.Error()})
			return 1
		}
		row, readErr := o.evidence()
		if readErr != nil || json.NewEncoder(os.Stdout).Encode(storeFixtureReply{Evidence: row}) != nil {
			return 1
		}
		runtime.KeepAlive(restored)
	}
	defer func() { _ = o.shutdown() }()
	decoder := json.NewDecoder(bufio.NewReader(os.Stdin))
	for !o.stop {
		var request storeFixtureRequest
		if decoder.Decode(&request) != nil {
			return 0
		}
		err := o.execute(request)
		row, readErr := o.evidence()
		reply := storeFixtureReply{Evidence: row}
		if err := errors.Join(err, readErr); err != nil {
			reply.Error = err.Error()
		}
		if json.NewEncoder(os.Stdout).Encode(reply) != nil {
			return 1
		}
		// A leaked untaken *os.File must remain reachable during the live FD
		// oracle. Its finalizer cannot replace the import owner's explicit Close.
		runtime.KeepAlive(restored)
	}
	return 0
}

func (o *storeFixtureOwner) event(name string) {
	now, _ := rawsocket.BootNow()
	o.Evidence.Events = append(o.Evidence.Events, BrokerStoreEvent{Name: name, Seq: uint64(len(o.Evidence.Events) + 1), BootNS: now})
}
func storeFixtureDeadline() (controlbudget.Deadline, error) {
	now, err := rawsocket.BootNow()
	return controlbudget.Deadline(now + int64(5*time.Second)), err
}
func (o *storeFixtureOwner) transportConfig(bind string) (cluster.MTLSTransportConfig, error) {
	ca, err := os.ReadFile(o.Config.CAPath)
	if err != nil {
		return cluster.MTLSTransportConfig{}, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return cluster.MTLSTransportConfig{}, errors.New("broker fixture: invalid CA")
	}
	leaf, err := tls.LoadX509KeyPair(o.Config.CertPath, o.Config.KeyPath)
	return cluster.MTLSTransportConfig{BindAddr: bind, CACert: pool, Leaf: leaf}, err
}
func (o *storeFixtureOwner) nodeConfig() cluster.Config {
	return cluster.Config{LocalID: raft.ServerID(o.Config.NodeID), DataDir: o.Config.Root, DBPath: filepath.Join(o.Config.Root, "state.db"), Transport: o.transport,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), BootstrapPeers: o.Peers,
		HeartbeatTimeout: cluster.MultinodeHeartbeatTimeout, ElectionTimeout: cluster.MultinodeElectionTimeout, LeaderLeaseTimeout: cluster.MultinodeLeaderLeaseTimeout,
		ImageRegistry: storage.DefaultDriverRegistry(), ImageReadOnly: storage.OpenReadOnly,
		ImageStamp: func(db *sql.DB) (storage.StoreStamp, error) {
			id, err := storage.CanonicalStoreIdentity(filepath.Join(o.Config.Root, "state.db"), o.Config.Root)
			if err != nil {
				return storage.StoreStamp{}, err
			}
			return storage.ReadStoreStamp(db, id)
		},
		ImageOpenCurrent: func(cfg storage.OpenImageConfig) (*sql.DB, error) {
			db, err := storage.OpenCurrent(cfg)
			if err != nil {
				return db, err
			}
			o.Evidence.BeforeReplay, _, err = storeFixtureACKs(db)
			var stamp storage.StoreStamp
			if err == nil {
				stamp, err = storage.ReadStoreStamp(db, cfg.Stamp.Identity)
			}
			o.Evidence.BeforeReplayCursor = stamp.AppliedIndex
			o.Evidence.ReplayGateCount++
			o.event("pre-replay-read")
			return db, err
		},
	}
}

func (o *storeFixtureOwner) execute(r storeFixtureRequest) error {
	switch r.Operation {
	case "prepare":
		if r.Config == nil || o.lock != nil {
			return errors.New("broker fixture: prepare requires a fresh config")
		}
		o.Config = *r.Config
		o.Evidence.Generation, o.Evidence.Topology, o.Evidence.NodeID = 1, o.Config.Topology, o.Config.NodeID
		var err error
		o.lock, err = cluster.AcquireImageDataDirLock(o.Config.Root)
		if err != nil {
			return err
		}
		if o.Config.Topology != "single" {
			cfg, err := o.transportConfig("127.0.0.1:0")
			if err != nil {
				return err
			}
			o.transport, err = cluster.NewMTLSTransport(cfg)
			if err != nil {
				return err
			}
			o.Evidence.Address = string(o.transport.LocalAddr())
		}
		o.event("lock-acquired")
	case "start":
		o.Peers = r.Peers
		var err error
		if o.Config.Topology == "single" {
			o.db, err = storage.Open(filepath.Join(o.Config.Root, "state.db"))
		} else {
			config := o.nodeConfig()
			o.recordNodeTimers(config)
			o.node, err = cluster.New(config)
			if o.node != nil {
				o.db = o.node.DB()
			}
		}
		if err != nil {
			return err
		}
		if o.node != nil {
			if err := o.node.RODB().Ping(); err != nil {
				return err
			}
		}
		o.staleDB, err = os.ReadFile(filepath.Join(o.Config.Root, "state.db"))
		return err
	case "status":
	case "write":
		if o.closed || o.admissionFenced || o.db == nil || !storeFixtureValidID(r.ID) {
			return errors.New("broker fixture: invalid ACK admission")
		}
		// Value includes the effect counter. A duplicate command must be deduped,
		// rather than hidden by INSERT OR IGNORE or identical last-write values.
		statement := cluster.Statement{SQL: "INSERT INTO cluster_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=substr(value,1,64)||':'||(CAST(substr(value,66) AS INTEGER)+1)", Args: []any{"t:s6-ack:" + r.ID, r.Value + ":1"}}
		if o.node != nil {
			return o.node.ProposeWithReqID(storeFixtureDigest([]byte(r.ID)), func(*sql.DB) (*cluster.Command, error) {
				return cluster.NewCommand(cluster.OpClusterMetaSet, statement), nil
			})
		}
		return cluster.ExecCommand(o.db, cluster.NewCommand(cluster.OpClusterMetaSet, statement))
	case "barrier":
		if o.node == nil {
			return errors.New("broker fixture: barrier requires Raft")
		}
		return o.node.Barrier(5 * time.Second)
	case "snapshot-save":
		if o.node == nil {
			return errors.New("broker fixture: snapshot requires Raft")
		}
		// ctx-root: the fixture owns this independent offline backup operation.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return o.node.BackupDBTo(ctx, filepath.Join(o.Config.Root, "saved-snapshot.db"))
	case "store-closed":
		return o.closeForImage()
	case "final-export":
		if o.cut == nil || !o.closed {
			return errors.New("broker fixture: final export requires the completed cut")
		}
		_, _, err := o.cut.Export(o.Evidence.Seal)
		if err == nil {
			o.event("final-export")
		}
		return err
	case "component-exec", "component-exec-error":
		if !o.closed {
			if err := o.closeForImage(); err != nil {
				return err
			}
		}
		if o.Fault.Point == "restore-old-db" {
			if err := os.WriteFile(filepath.Join(o.Config.Root, "state.db"), o.staleDB, 0600); err != nil {
				return err
			}
		}
		resource, err := o.lock.Export()
		if err != nil {
			return err
		}
		o.event("prepare")
		o.event("exec-enter")
		o.Evidence.ExecSeq = uint64(len(o.Evidence.Events))
		data, err := json.Marshal(o)
		if err != nil {
			return err
		}
		// An untaken sealed file makes post-import Close observable in the live
		// receiver. Process exit must never be what cleans inherited custody.
		custody, err := sealedstate.New("fixture-custody", []byte("fixture-custody-sentinel"))
		if err != nil {
			return err
		}
		defer func() { _ = custody.Close() }()
		prepared, err := handoff.Prepare(handoff.State{Role: "broker-store-fixture", Instance: "s6-store", Generation: o.Evidence.Generation + 1, Data: data}, append([]handoff.Resource{resource}, custody.Resources...))
		if err != nil {
			return err
		}
		defer func() { _ = prepared.Close() }()
		path := o.Config.Candidate
		if r.Operation == "component-exec-error" {
			path += ".absent"
		}
		err = prepared.Exec(path, []string{path, "-test.run=^TestBrokerStoreFixtureReceiver$", "-test.count=1"}, storeFixtureEnvironment(o.Config.Root, "owner"))
		if r.Operation != "component-exec-error" || err == nil {
			return errors.Join(err, errors.New("broker fixture: exec unexpectedly returned"))
		}
		if err := prepared.Close(); err != nil {
			return err
		}
		o.event("exec-returned")
		return o.openCurrent()
	case "normal-stop", "stop":
		if err := o.shutdown(); err != nil {
			return err
		}
		if r.Operation == "stop" {
			o.stop = true
		}
	case "fault":
		if r.Fault == nil || (r.Fault.Point != "restore-old-db" && r.Fault.Point != "constructor-error") {
			return errors.New("broker fixture: unknown fault")
		}
		o.Fault = *r.Fault
		data, _ := json.Marshal(r.Fault)
		o.Evidence.MutationDigest = storeFixtureDigest(data)
	case "preflight":
		if _, err := os.Stat(r.Artifact); err != nil {
			return errors.New("broker fixture: preflight requires a supplied real tether artifact")
		}
		if _, err := os.Stat(r.PreviousArtifact); err != nil {
			return errors.New("broker fixture: preflight requires a supplied real previous artifact")
		}
		row, err := o.evidence()
		if err != nil {
			return err
		}
		configPath := filepath.Join(o.Config.Root, "probe.yaml")
		config := []byte("broker:\n  nats:\n    url: nats://127.0.0.1:1\n  storage:\n    db: " + filepath.Join(o.Config.Root, "state.db") + "\n")
		if err := os.WriteFile(configPath, config, 0600); err != nil {
			return err
		}
		deadline, err := storeFixtureDeadline()
		if err != nil {
			return err
		}
		candidate, previous, err := brokerimage.Preflight(brokerimage.PreflightRequest{CandidatePath: r.Artifact, PreviousPath: r.PreviousArtifact, Config: config,
			AllowedArgs: []string{r.Artifact, "serve", "--config", configPath}, Stamp: row.Store, RequiredDomains: []string{brokerimage.Domain}, Deadline: deadline})
		if err != nil {
			return err
		}
		if o.Evidence.OracleInputs == nil {
			o.Evidence.OracleInputs = make(map[string]string)
		}
		o.Evidence.OracleInputs["candidate_proof"], o.Evidence.OracleInputs["previous_proof"] = candidate.SHA256, previous.SHA256
	default:
		return errors.New("broker fixture: unknown checkpoint")
	}
	return nil
}

func (o *storeFixtureOwner) closeForImage() error {
	if o.closed {
		return nil
	}
	deadline, err := storeFixtureDeadline()
	if err != nil {
		return err
	}
	mode := brokerimage.ModeSingle
	switch o.Config.Topology {
	case "sole-voter":
		mode = brokerimage.ModeSoleVoter
	case "quorum":
		mode = brokerimage.ModeQuorum
	}
	o.cut, err = brokerimage.Freeze(brokerimage.Config{Mode: mode, Store: &fixtureStoreAdapter{o}, Control: &fixtureControlAdapter{o}, Deadline: deadline})
	if err != nil {
		return err
	}
	o.event("producers-fenced")
	if o.node != nil {
		if err := cluster.PrepareImageShutdown(o.node, deadline); err != nil {
			return err
		}
		o.event("snapshot-and-transport-joined")
		if err := o.node.Shutdown(); err != nil {
			return err
		}
		o.Evidence.Seal, err = cluster.ClosedImageStore(o.node)
		if err != nil {
			return err
		}
	} else {
		row, err := o.evidence()
		if err != nil {
			return err
		}
		seal := storage.StoreSeal{Stamp: row.Store, Raft: storage.CloseClosed, Bolt: storage.CloseClosed, Transport: storage.CloseClosed}
		seal.CheckpointBusy, seal.CheckpointLog, seal.CheckpointDone, err = storage.CheckpointForImage(o.db)
		if err != nil {
			return err
		}
		if err := o.db.Close(); err != nil {
			return err
		}
		seal.Writer, seal.ReadOnly = storage.CloseClosed, storage.CloseClosed
		seal.DriverWriters, seal.DriverReaders = storage.DefaultDriverRegistry().Connections(row.Store.Identity)
		o.Evidence.Seal = seal
	}
	o.event("node-store-closed")
	o.Evidence.NodeCloseSeq = uint64(len(o.Evidence.Events))
	if !brokerimage.StoreSealClosed(o.Evidence.Seal) {
		return errors.New("broker fixture: actual closed seal missing")
	}
	o.Evidence.Store, o.closed, o.db = o.Evidence.Seal.Stamp, true, nil
	lockResource, err := o.lock.Export()
	if err != nil {
		return err
	}
	if o.Evidence.OracleInputs == nil {
		o.Evidence.OracleInputs = make(map[string]string)
	}
	o.Evidence.OracleInputs["lock_resource_digest"] = storeFixtureDigest(lockResource.Data)
	o.event("seal-observed")
	return nil
}

func (o *storeFixtureOwner) openCurrent() error {
	if o.Config.Topology == "single" {
		var err error
		o.db, err = o.nodeConfig().ImageOpenCurrent(storage.OpenImageConfig{Stamp: o.Evidence.Seal.Stamp, Registry: storage.DefaultDriverRegistry()})
		if err != nil {
			return err
		}
	} else {
		cfg, err := o.transportConfig(o.Evidence.Address)
		if err != nil {
			return err
		}
		o.transport, err = cluster.NewMTLSTransport(cfg)
		if err != nil {
			return err
		}
		currentConfig := o.nodeConfig()
		o.recordNodeTimers(currentConfig)
		if o.Fault.Point == "constructor-error" {
			o.Fault = BrokerImageFault{}
			open := currentConfig.ImageOpenCurrent
			currentConfig.ImageOpenCurrent = func(config storage.OpenImageConfig) (*sql.DB, error) {
				db, err := open(config)
				return db, errors.Join(err, errors.New("broker fixture: injected constructor failure after real driver open"))
			}
		}
		o.node, err = cluster.NewForImage(currentConfig, o.Evidence.Seal.Stamp)
		if err != nil {
			return err
		}
		o.db = o.node.DB()
		if o.Config.Topology == "sole-voter" {
			if err := o.node.WaitForLeader(5 * time.Second); err != nil {
				return err
			}
		}
	}
	o.closed = false
	o.admissionFenced = false
	o.event("current-store-open")
	return nil
}

func (o *storeFixtureOwner) evidence() (BrokerStoreEvidence, error) {
	row := o.Evidence
	row.PID = os.Getpid()
	stat, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return row, err
	}
	end := strings.LastIndexByte(string(stat), ')')
	if end < 0 {
		return row, errors.New("broker fixture: invalid process identity")
	}
	fields := strings.Fields(string(stat)[end+1:])
	if len(fields) < 20 {
		return row, errors.New("broker fixture: incomplete process identity")
	}
	row.StartTicks, err = strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return row, err
	}
	if o.Evidence.RunningSHA == "" {
		o.Evidence.RunningSHA, err = storeFixtureHash("/proc/self/exe")
		if err != nil {
			return row, err
		}
	}
	row.RunningSHA = o.Evidence.RunningSHA
	if o.db != nil {
		id, err := storage.CanonicalStoreIdentity(filepath.Join(o.Config.Root, "state.db"), o.Config.Root)
		if err != nil {
			return row, err
		}
		row.Store, err = storage.ReadStoreStamp(o.db, id)
		if err != nil {
			return row, err
		}
		o.Evidence.Store = row.Store
		row.ACKs, row.Effects, err = storeFixtureACKs(o.db)
		if err != nil {
			return row, err
		}
		// Keep the last independently observed live rows for a closed receipt.
		o.Evidence.ACKs, o.Evidence.Effects = row.ACKs, row.Effects
	}
	row.Drivers[0], row.Drivers[1] = storage.DefaultDriverRegistry().Connections(row.Store.Identity)
	if o.node != nil && !o.closed {
		row.Leader = o.node.IsLeader()
		role, err := cluster.ReadImageRole(o.node)
		if err != nil && row.Store.AppliedIndex > 0 {
			return row, err
		}
		row.Seal.Role = role
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return row, err
	}
	row.Counters = map[string]uint64{"handoff_fds": 0, "sqlite_fds": 0, "sealed_custody_fds": 0, "fd_count": uint64(len(entries))}
	for _, entry := range entries {
		path, _ := os.Readlink("/proc/self/fd/" + entry.Name())
		if strings.Contains(path, "memfd:tether-handoff") {
			row.Counters["handoff_fds"]++
		}
		if strings.Contains(path, "memfd:tether-sealed-state") {
			row.Counters["sealed_custody_fds"]++
		}
		if strings.HasPrefix(path, filepath.Join(o.Config.Root, "state.db")) {
			row.Counters["sqlite_fds"]++
		}
	}
	stages, err := filepath.Glob(filepath.Join(o.Config.Root, "rpc-snapshot-*.db"))
	if err != nil {
		return row, err
	}
	row.Counters["snapshot_stage_files"] = uint64(len(stages))
	return row, nil
}

func (o *storeFixtureOwner) recordNodeTimers(config cluster.Config) {
	// Record the inputs actually supplied to New/NewForImage. Reading constants
	// later would let a test shrink election timers while reporting production.
	// origin: hot-update S6 plan §5 / T37
	if o.Evidence.OracleInputs == nil {
		o.Evidence.OracleInputs = make(map[string]string)
	}
	o.Evidence.OracleInputs["raft_heartbeat_ns"] = strconv.FormatInt(int64(config.HeartbeatTimeout), 10)
	o.Evidence.OracleInputs["raft_election_ns"] = strconv.FormatInt(int64(config.ElectionTimeout), 10)
	o.Evidence.OracleInputs["raft_lease_ns"] = strconv.FormatInt(int64(config.LeaderLeaseTimeout), 10)
}

func storeFixtureACKs(db *sql.DB) (map[string]string, map[string]int, error) {
	rows, err := db.Query("SELECT key,value FROM cluster_meta WHERE key LIKE 't:s6-ack:%' ORDER BY key")
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rows.Close() }()
	acks, effects := make(map[string]string), make(map[string]int)
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, nil, err
		}
		digest, count, ok := strings.Cut(value, ":")
		if !ok {
			return nil, nil, errors.New("broker fixture: malformed ACK row")
		}
		n, err := strconv.Atoi(count)
		if err != nil {
			return nil, nil, err
		}
		id := strings.TrimPrefix(key, "t:s6-ack:")
		acks[id], effects[id] = digest, n
	}
	return acks, effects, rows.Err()
}
func storeFixtureValidID(id string) bool {
	if len(id) == 0 || len(id) > 64 {
		return false
	}
	for _, char := range id {
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '-' && char != '_' {
			return false
		}
	}
	return true
}
func (o *storeFixtureOwner) shutdown() error {
	var errs []error
	if o.node != nil {
		errs = append(errs, o.node.Shutdown())
	} else if o.db != nil {
		errs = append(errs, o.db.Close())
	} else if o.transport != nil {
		errs = append(errs, o.transport.Close())
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	o.db, o.closed = nil, true
	if o.lock != nil {
		return o.lock.Release()
	}
	return nil
}

// These adapters own the fixture's actual serial command admission, store and
// original lock. There is no auth/business/transfer service in this component
// receiver; attempting to start one fails explicitly. Only product checkpoints
// can claim those services. Freeze still uses the production cut implementation.
// origin: hot-update S6 plan §3.2 / T09 / §12.1 P26
type fixtureStoreAdapter struct{ owner *storeFixtureOwner }

func (s *fixtureStoreAdapter) Stamp() (storage.StoreStamp, error) {
	row, err := s.owner.evidence()
	return row.Store, err
}
func (s *fixtureStoreAdapter) Quiesce(deadline controlbudget.Deadline) error {
	if s.owner.node != nil {
		return cluster.QuiesceImageStore(s.owner.node, deadline)
	}
	return fixtureDeadlineAlive(deadline)
}
func (s *fixtureStoreAdapter) CloseForImage(controlbudget.Deadline) (storage.StoreSeal, error) {
	if !s.owner.closed {
		return storage.StoreSeal{}, errors.New("broker fixture: store not yet closed")
	}
	return s.owner.Evidence.Seal, nil
}
func (s *fixtureStoreAdapter) LockResource() (handoff.Resource, error) { return s.owner.lock.Export() }
func (s *fixtureStoreAdapter) ReopenCurrent(stamp storage.StoreStamp, deadline controlbudget.Deadline) (brokerimage.StoreOwner, error) {
	if err := fixtureDeadlineAlive(deadline); err != nil {
		return nil, err
	}
	if !s.owner.closed || stamp.Identity != s.owner.Evidence.Seal.Stamp.Identity {
		return nil, errors.New("broker fixture: current reopen without a closed same-identity store")
	}
	err := s.owner.openCurrent()
	return s, err
}

type fixtureControlAdapter struct{ owner *storeFixtureOwner }

func (c *fixtureControlAdapter) FenceTransfers(deadline controlbudget.Deadline) (uint32, error) {
	return 0, fixtureDeadlineAlive(deadline)
}
func (c *fixtureControlAdapter) ResumeTransfers() error { return nil } // The fixture has no transfer producer.
func (c *fixtureControlAdapter) Fence(deadline controlbudget.Deadline) error {
	if err := fixtureDeadlineAlive(deadline); err != nil {
		return err
	}
	c.owner.admissionFenced = true
	return nil
}
func (c *fixtureControlAdapter) JoinStoreUsers(deadline controlbudget.Deadline) error {
	// Decoder dispatch is synchronous. A write/backup reply was completed before
	// this request could enter Freeze; Node.Quiesce joins asynchronous SQL work.
	if !c.owner.admissionFenced {
		return errors.New("broker fixture: store users joined before command admission fenced")
	}
	return fixtureDeadlineAlive(deadline)
}
func (*fixtureControlAdapter) StopTransport(controlbudget.Deadline) error {
	return errors.New("broker fixture: no NATS control transport")
}
func (*fixtureControlAdapter) StartAuth(controlbudget.Deadline) error {
	return errors.New("broker fixture: no auth service")
}
func (*fixtureControlAdapter) StartBusiness(controlbudget.Deadline) error {
	return errors.New("broker fixture: no broker business service")
}
func (c *fixtureControlAdapter) StopCurrent(controlbudget.Deadline) error { return c.owner.shutdown() }
func fixtureDeadlineAlive(deadline controlbudget.Deadline) error {
	now, err := rawsocket.BootNow()
	if err != nil {
		return err
	}
	if int64(deadline) <= now {
		return errors.New("broker fixture: cut deadline expired")
	}
	return nil
}

// Fixture key generation normally supplies a signer. Reject a changed supplier
// before certificate creation rather than panic and lose partial-owner cleanup.
// origin: hot-update S6 plan §2.1 / §12.1 P26
func brokerFixturePublicKey(privateKey any) (crypto.PublicKey, error) {
	signer, ok := privateKey.(crypto.Signer)
	if !ok || signer == nil {
		return nil, errors.New("broker fixture: certificate key is not a signer")
	}
	return signer.Public(), nil
}

// Echo and SSH witnesses can reach cleanup after either their peer or the
// fixture owner has closed them. Only terminal EOF/already-closed is expected;
// other close failures must make the fixture's test fail.
// origin: hot-update S6 plan §9.3 / §12.1 P26
func closeBrokerFixtureWitness(t interface {
	Helper()
	Errorf(string, ...any)
}, witness io.Closer) {
	t.Helper()
	if err := witness.Close(); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) && !errors.Is(err, os.ErrClosed) {
		t.Errorf("broker fixture: close witness: %v", err)
	}
}
