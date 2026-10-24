// custody.go owns acceptance collector lifetimes and the read-only broker source.
// Sealed event bytes stay with the private collector; this adapter sees only
// typed samples, public indexes, and native raw admin responses.
// origin: hot-update S10 plan §5.3 / §10.2 P16
package fullstackharness

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

var ErrCustodyIncomplete = errors.New("fullstack: incomplete private custody")
var ErrCustodyPending = errors.New("fullstack: pending same-stamp owner observation")

// CustodyCollector consumes independently authenticated sources. Close stops
// collection; only Join proves their workers and descriptors have retired. The
// underlying permanent product custody writer is never closed by this adapter.
type CustodyCollector struct {
	mu        sync.Mutex
	sources   map[contract.Key]contract.Snapshotter
	closed    bool
	poison    error
	joined    bool
	busy      map[contract.Key]bool
	nonces    map[contract.Key]map[string]bool
	closeDone chan struct{}
}

// NewCustodyCollector validates the exact required census before taking source
// ownership. A partial failure leaves every supplied source with its caller.
func NewCustodyCollector(profile contract.Profile, sources map[contract.Key]contract.Snapshotter) (*CustodyCollector, error) {
	if len(profile.Required) == 0 || len(profile.Required) != len(sources) {
		return nil, ErrCustodyIncomplete
	}
	owned := make(map[contract.Key]contract.Snapshotter, len(sources))
	for _, key := range profile.Required {
		if _, err := contract.EncodeJSON("key", key); err != nil {
			return nil, ErrCustodyIncomplete
		}
		if owned[key] != nil || sources[key] == nil {
			return nil, ErrCustodyIncomplete
		}
		owned[key] = sources[key]
	}
	return &CustodyCollector{sources: owned, busy: make(map[contract.Key]bool),
		nonces: make(map[contract.Key]map[string]bool), closeDone: make(chan struct{})}, nil
}

func (c *CustodyCollector) begin(ctx context.Context, key contract.Key, nonce string, drain bool) (contract.Snapshotter, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.closed || c.poison != nil || nonce == "" || c.sources[key] == nil || c.busy[key] {
		return nil, errors.Join(ErrCustodyIncomplete, c.poison)
	}
	if drain {
		if c.nonces[key] == nil {
			c.nonces[key] = make(map[string]bool)
		}
		if c.nonces[key][nonce] {
			c.poison = ErrCustodyIncomplete
			return nil, c.poison
		}
		c.nonces[key][nonce] = true
	}
	c.busy[key] = true
	return c.sources[key], nil
}

func (c *CustodyCollector) finish(key contract.Key, err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.busy, key)
	if err != nil && !errors.Is(err, ErrCustodyPending) && c.poison == nil {
		c.poison = err
	}
	return errors.Join(err, c.poison)
}

func (c *CustodyCollector) Sample(ctx context.Context, key contract.Key, nonce string) (contract.Sample, error) {
	source, err := c.begin(ctx, key, nonce, false)
	if err != nil {
		return contract.Sample{}, err
	}
	sample, err := source.Sample(ctx, key, nonce)
	if err == nil && (sample.Stamp.Process.Key != key || sample.Stamp.QueryNonce != nonce ||
		sample.ExeSHA == (contract.Digest{}) || sample.Goroutines <= 0 || sample.FDs < 0) {
		err = ErrCustodyIncomplete
	}
	if err == nil {
		_, err = contract.EncodeJSON("sample", sample)
	}
	if err == nil && !sample.Complete {
		err = ErrCustodyPending
	}
	if err = c.finish(key, err); err != nil {
		if errors.Is(err, ErrCustodyPending) {
			return sample, err
		}
		return contract.Sample{}, err
	}
	return sample, nil
}

// Drain propagates the source's nonce-bound barrier, including poison. It does
// not interpret a product Flush or a prior raw index as source retirement.
func (c *CustodyCollector) Drain(ctx context.Context, key contract.Key, nonce string) (contract.RawRef, error) {
	source, err := c.begin(ctx, key, nonce, true)
	if err != nil {
		return contract.RawRef{}, err
	}
	raw, err := source.Drain(ctx, key, nonce)
	if err == nil && (raw.Bytes <= 0 || raw.SHA == (contract.Digest{})) {
		err = ErrCustodyIncomplete
	}
	if err == nil {
		_, err = contract.EncodeJSON("raw_ref", raw)
	}
	if err = c.finish(key, err); err != nil {
		return contract.RawRef{}, err
	}
	return raw, nil
}

func (c *CustodyCollector) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		<-c.closeDone
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.poison
	}
	c.closed = true
	c.mu.Unlock()
	var err error
	for _, source := range c.sources {
		err = errors.Join(err, source.Close())
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.poison = errors.Join(c.poison, err)
	close(c.closeDone)
	return c.poison
}

// origin: hot-update S10 plan §4.2 / §10.2 P16 / Flush is not Join
func (c *CustodyCollector) Join(ctx context.Context) error {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if !closed {
		return ErrCustodyIncomplete
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closeDone:
	}
	// Each actual source owns cancellation and its returned worker waiter. Even
	// after one failure, join the other sources rather than leaking their closure.
	var err error
	for _, source := range c.sources {
		err = errors.Join(err, source.Join(ctx))
	}
	if err != nil {
		c.mu.Lock()
		c.poison = errors.Join(c.poison, err)
		c.mu.Unlock()
		return err
	}
	// Source Join must unblock its in-flight Sample/Drain. Cancellation cannot
	// make a live adapter disappear from the evidence census.
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.busy) != 0 {
		return ErrCustodyIncomplete
	}
	c.joined = true
	return c.poison
}

func (c *CustodyCollector) Joined() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed && c.joined && c.poison == nil
}

// BrokerPeerReader must use SO_PEERCRED on the borrowed SyscallConn descriptor.
// P04 owns the Linux implementation and unsupported platform twin. Requiring it
// here avoids a Linux-only syscall in this portable, frozen owned filename.
type BrokerPeerReader func(syscall.RawConn) (pid int, uid uint32, err error)

type BrokerCustodyOptions struct {
	Path, EvidenceRoot string
	Current            func(context.Context, contract.Key, string) (contract.Stamp, error)
	Peer               BrokerPeerReader
	BootNow            func() (int64, error)
	Owners             func(context.Context, contract.Stamp) (contract.Sample, error)
}

type brokerCustodyEvidence struct {
	Schema           string          `json:"schema"`
	SourceKind       string          `json:"source_kind"`
	Before           json.RawMessage `json:"before"`
	After            json.RawMessage `json:"after"`
	PeerPID          int             `json:"peer_pid"`
	PeerUID          uint32          `json:"peer_uid"`
	SentNS           int64           `json:"sent_ns"`
	ReceivedNS       int64           `json:"received_ns"`
	Response         []byte          `json:"response_bytes"`
	ResponseComplete bool            `json:"response_complete"`
	Owners           json.RawMessage `json:"owners"`
}

type brokerCustodyArchive struct {
	mu     sync.Mutex
	root   string
	bytes  int64
	poison bool
}

// The long-run raw budget is shared by every call from this factory. Old raw is
// never deleted to make room: exhaustion retains the first failure and stops
// certification. Disk evidence here is admin runtime, never private event data.
func (a *brokerCustodyArchive) write(value brokerCustodyEvidence) (contract.RawRef, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return contract.RawRef{}, ErrCustodyIncomplete
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.poison || a.bytes+int64(len(data)) > 256<<20 {
		a.poison = true
		return contract.RawRef{}, ErrCustodyIncomplete
	}
	a.bytes += int64(len(data))
	file, err := os.CreateTemp(a.root, "broker-custody-*.json")
	if err == nil {
		_, writeErr := file.Write(data)
		err = errors.Join(writeErr, file.Sync(), file.Close())
	}
	if err != nil {
		a.poison = true
		return contract.RawRef{}, err
	}
	return contract.RawRef{Path: file.Name(), SHA: sha256.Sum256(data), Bytes: int64(len(data)), Schema: value.Schema}, nil
}

// NewBrokerCustodySource performs only local authorized reads. Bind this source
// through P04 BindSnapshotSource so held exe/pidfd evidence independently checks
// pidns, start ticks, executing SHA, and cut before publishing a Sample.
func NewBrokerCustodySource(options BrokerCustodyOptions) (SampleSource, error) {
	if !filepath.IsAbs(options.Path) || !filepath.IsAbs(options.EvidenceRoot) ||
		options.Current == nil || options.Peer == nil || options.BootNow == nil {
		return nil, ErrCustodyIncomplete
	}
	archive := &brokerCustodyArchive{root: options.EvidenceRoot}
	return func(ctx context.Context, key contract.Key, nonce string) (contract.Sample, error) {
		return readBrokerCustody(ctx, key, nonce, options, archive)
	}, nil
}

func readBrokerCustody(ctx context.Context, key contract.Key, nonce string, options BrokerCustodyOptions, archive *brokerCustodyArchive) (result contract.Sample, err error) {
	if key.Role != "broker" || nonce == "" {
		return contract.Sample{}, ErrCustodyIncomplete
	}
	before, err := options.Current(ctx, key, nonce)
	if err != nil {
		return contract.Sample{}, err
	}
	if before.Process.Key != key || before.QueryNonce != nonce {
		return contract.Sample{}, ErrIdentityChanged
	}
	beforeJSON, err := contract.EncodeJSON("stamp", before)
	if err != nil {
		return contract.Sample{}, err
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", options.Path)
	if err != nil {
		return contract.Sample{}, err
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return contract.Sample{}, ErrCustodyIncomplete
	}
	rawConn, err := unixConn.SyscallConn()
	if err != nil {
		return contract.Sample{}, err
	}
	pid, uid, err := options.Peer(rawConn)
	if err != nil || pid != before.Process.PID || uid != key.UID {
		return contract.Sample{}, errors.Join(ErrIdentityChanged, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	if limit, ok := ctx.Deadline(); ok && limit.Before(deadline) {
		deadline = limit
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return contract.Sample{}, err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	defer stop()
	sent, err := options.BootNow()
	if err != nil {
		return contract.Sample{}, err
	}
	if _, err := io.WriteString(conn, "{\"op\":\"runtime\"}\n"); err != nil {
		return contract.Sample{}, err
	}
	response, readErr := bufio.NewReader(io.LimitReader(conn, (1<<20)+1)).ReadBytes('\n')
	received, clockErr := options.BootNow()
	responseComplete := readErr == nil && len(response) <= 1<<20 && clockErr == nil && sent > 0 && received >= sent
	// Save the unmodified response and local interval even when its schema,
	// counters or subsequent current stamp prove unusable. Failure raw is evidence.
	nativeRaw, err := archive.write(brokerCustodyEvidence{Schema: "fullstack-broker-custody-v1", SourceKind: "admin_runtime",
		Before: beforeJSON, PeerPID: pid, PeerUID: uid, SentNS: sent, ReceivedNS: received,
		Response: response, ResponseComplete: responseComplete})
	if err != nil {
		return contract.Sample{}, err
	}
	defer func() {
		if err != nil && result.Raw.Path == "" {
			result.Raw = nativeRaw
		}
	}()
	if !responseComplete {
		return contract.Sample{}, errors.Join(ErrCustodyIncomplete, readErr, clockErr)
	}
	var native adminsock.Response
	if !custodyUniqueJSON(response) || json.Unmarshal(response, &native) != nil {
		return contract.Sample{}, ErrCustodyIncomplete
	}
	var envelope map[string]json.RawMessage
	var runtimeFields map[string]json.RawMessage
	if json.Unmarshal(response, &envelope) != nil || json.Unmarshal(envelope["runtime"], &runtimeFields) != nil {
		return contract.Sample{}, ErrCustodyIncomplete
	}
	for _, field := range []string{"schema", "schema_version", "goroutines", "open_fds", "broker_image"} {
		if len(runtimeFields[field]) == 0 || bytes.Equal(runtimeFields[field], []byte("null")) {
			return contract.Sample{}, ErrCustodyIncomplete
		}
	}
	report := native.Runtime
	if native.Error != "" || report == nil || report.Schema != "admin_runtime" || report.SchemaVersion != 2 ||
		report.Goroutines <= 0 || report.OpenFDs < 0 || report.BrokerImage == nil {
		return contract.Sample{}, ErrCustodyIncomplete
	}
	image := report.BrokerImage
	sha, err := hex.DecodeString(image.RunningSHA256)
	if err != nil || len(sha) != 32 || image.Generation != before.Generation || image.CutID != before.CutID {
		return contract.Sample{}, ErrIdentityChanged
	}
	after, err := options.Current(ctx, key, nonce)
	if err != nil {
		return contract.Sample{}, err
	}
	if !custodySameStamp(before, after) {
		return contract.Sample{}, ErrIdentityChanged
	}
	afterJSON, err := contract.EncodeJSON("stamp", after)
	if err != nil {
		return contract.Sample{}, err
	}
	var exe contract.Digest
	copy(exe[:], sha)
	sample := contract.Sample{Stamp: after, ExeSHA: exe, Goroutines: int64(report.Goroutines), FDs: int64(report.OpenFDs)}
	var ownersJSON json.RawMessage
	if options.Owners != nil {
		owners, ownerErr := options.Owners(ctx, after)
		if ownerErr != nil || !custodySameStamp(after, owners.Stamp) || owners.ExeSHA != exe {
			return contract.Sample{}, errors.Join(ErrIdentityChanged, ownerErr)
		}
		ownersJSON, err = contract.EncodeJSON("sample", owners)
		if err != nil {
			return contract.Sample{}, err
		}
		sample.Resources, sample.Consumers, sample.Temps = owners.Resources, owners.Consumers, owners.Temps
		sample.Locks, sample.OldImages, sample.Complete = owners.Locks, owners.OldImages, owners.Complete
		final, finalErr := options.Current(ctx, key, nonce)
		if finalErr != nil || !custodySameStamp(after, final) {
			return contract.Sample{}, errors.Join(ErrIdentityChanged, finalErr)
		}
	}
	sample.Raw, err = archive.write(brokerCustodyEvidence{Schema: "fullstack-broker-custody-v1", SourceKind: "admin_runtime",
		Before: beforeJSON, After: afterJSON, PeerPID: pid, PeerUID: uid, SentNS: sent, ReceivedNS: received,
		Response: response, ResponseComplete: true, Owners: ownersJSON})
	if err != nil {
		return contract.Sample{}, err
	}
	// Admin runtime has no full resource/consumer/retention census. Its genuine G
	// and FD counts cannot fabricate that missing same-stamp owner evidence.
	return sample, nil
}

// Duplicate fields can turn an explicit unknown/-1 into a plausible zero after
// normal unmarshalling. Validate syntax before interpreting the native schema.
// origin: hot-update S10 plan §5.1 / §5.3 / P16
func custodyUniqueJSON(raw []byte) bool {
	if !utf8.Valid(raw) {
		return false
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var value func(int) bool
	value = func(depth int) bool {
		if depth > 64 {
			return false
		}
		token, err := d.Token()
		if err != nil {
			return false
		}
		delim, compound := token.(json.Delim)
		if !compound {
			return true
		}
		if delim != '{' && delim != '[' {
			return false
		}
		seen := make(map[string]bool)
		for d.More() {
			if delim == '{' {
				key, err := d.Token()
				name, ok := key.(string)
				// encoding/json matches struct tags without case sensitivity.
				// Reject that alias collision before a second key can erase -1.
				name = strings.ToLower(name)
				if err != nil || !ok || seen[name] {
					return false
				}
				seen[name] = true
			}
			if !value(depth + 1) {
				return false
			}
		}
		end, err := d.Token()
		return err == nil && (delim == '{' && end == json.Delim('}') || delim == '[' && end == json.Delim(']'))
	}
	if !value(0) {
		return false
	}
	_, err := d.Token()
	return errors.Is(err, io.EOF)
}

func custodySameStamp(a, b contract.Stamp) bool {
	return a.Process == b.Process && a.TxID == b.TxID && a.CutID == b.CutID && a.Direction == b.Direction &&
		a.QueryNonce != "" && a.QueryNonce == b.QueryNonce && a.ScopeDigest == b.ScopeDigest &&
		a.Generation == b.Generation && a.Revision == b.Revision && b.Sequence >= a.Sequence && b.AtBootNS >= a.AtBootNS
}

var _ contract.Snapshotter = (*CustodyCollector)(nil)
