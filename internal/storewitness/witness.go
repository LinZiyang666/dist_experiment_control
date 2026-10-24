// Package storewitness reads registered business ACKs at the current-store RO
// boundary and observes the external lock without becoming a store owner.
// origin: hot-update S10 plan §5.3 / §5.4 / P08
package storewitness

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/storage"
)

const (
	Kind                 = "store-before-replay"
	Domain               = "store"
	Version              = 1
	MaxKeys              = 64
	maxKeyBytes          = 256
	maxRegistrationBytes = 66 + MaxKeys*(2+maxKeyBytes)
)

var (
	ErrWitness = errors.New("storewitness: incomplete or invalid pre-replay evidence")
	ErrLock    = errors.New("storewitness: store lock was available or unobservable")
	active     atomic.Pointer[Witness]
)

// Registration contains only exact business keys, never SQL identifiers or
// expected values. The independent ACK ledger remains the judge's input.
// ScopeDigest binds the predeclared runtime/cut; Nonce prevents receipt reuse.
type Registration struct {
	Device, Inode uint64
	Nonce         [12]byte
	ScopeDigest   [32]byte
	Keys          []string
}

type Stamp struct {
	// These fields describe the reader's actual snapshot and driver census,
	// rather than copying the importing image's expected floor into evidence.
	Device, Inode             uint64
	SchemaDigest              [32]byte
	Journal                   string
	Synchronous               int
	AppliedIndex, AppliedTerm uint64
	Writers, Readers          uint32
}

type Row struct {
	// Present distinguishes absence from a successful read of SQL NULL. Only
	// the note digest enters evidence; business text never enters diagnostics.
	Key           string
	Present, Null bool
	Digest        [32]byte
}

type Receipt struct {
	// Scope and nonce are private registration bindings; custody supplies the
	// producer incarnation, ordering and timestamp around this payload.
	Stamp       Stamp
	Nonce       [12]byte
	ScopeDigest [32]byte
	Rows        []Row
}

type Witness struct{ registration Registration }

// EncodeRegistration is a bounded private binary configuration. Its bytes must
// travel through sealed custody, never argv, environment or a public bundle.
func EncodeRegistration(r Registration) ([]byte, error) {
	if !validRegistration(r) {
		return nil, ErrWitness
	}
	b := append([]byte("TSW1"), r.Nonce[:]...)
	b = binary.LittleEndian.AppendUint64(b, r.Device)
	b = binary.LittleEndian.AppendUint64(b, r.Inode)
	b = append(b, r.ScopeDigest[:]...)
	b = binary.LittleEndian.AppendUint16(b, uint16(len(r.Keys)))
	for _, key := range r.Keys {
		b = binary.LittleEndian.AppendUint16(b, uint16(len(key)))
		b = append(b, key...)
	}
	return b, nil
}

// Register consumes and closes exactly the supplied immutable descriptor.
// Ordinary disk files and unsealed/trailing configuration are refused. One
// process has one current-store witness; replacing it mid-cut is forbidden.
// origin: hot-update S10 plan §5.3 / P08
func Register(src rawsocket.FileSource, id, name string) (*Witness, error) {
	b, file, err := sealedstate.Read(src, id, name, maxRegistrationBytes)
	if file != nil {
		err = errors.Join(err, file.Close())
	}
	if err != nil {
		return nil, ErrWitness
	}
	r, err := decodeRegistration(b)
	if err != nil {
		return nil, err
	}
	w := &Witness{registration: r}
	if !active.CompareAndSwap(nil, w) {
		return nil, ErrWitness
	}
	// storage cannot import this package (import cycle through testharness),
	// so the witness hands storage its capture hook instead.
	storage.SetStampObserver(captureObserved)
	return w, nil
}

func captureObserved(tx *sql.Tx, s storage.ObservedStamp) {
	if Enabled() {
		Capture(tx, Stamp(s))
	}
}

func (w *Witness) Close() error {
	if w != nil && active.CompareAndSwap(w, nil) {
		storage.SetStampObserver(nil)
	}
	return nil
}

func Enabled() bool { return drillobserve.Hook != nil && active.Load() != nil }

// Capture uses the caller's already-open RO transaction. No second pool,
// migration, writable probe or observer acknowledgement can delay recovery.
// Errors become a sticky-invalid receipt for the collector, not a product
// recovery failure. Missing rows are affirmative absence, never a read error.
// origin: hot-update S10 plan §5.3 / TestFullstackStoreWitnessPrecedesReplay
func Capture(tx *sql.Tx, stamp Stamp) {
	w := active.Load()
	if w == nil || drillobserve.Hook == nil {
		return
	}
	r := w.registration
	// Fixed schema v1: Values[0:10] are device, inode, SQL index, SQL term,
	// synchronous, journal (1=DELETE, 2=WAL), actual writers, actual readers,
	// key count, version. Slot 10 is failure; slots 11..15 stay zero.
	// Digest binds the applied/support migration AND schema-object digests.
	// Data starts with the 32-byte scope digest, followed by exactly key-count
	// rows: LE uint16 key length, key bytes, state (0=absent, 1=note, 2=NULL),
	// 32-byte note SHA256. Absent/NULL rows have a zero digest. Keeping row
	// state separate prevents a failed read or NULL from inventing an ACK.
	e := drillobserve.Event{Kind: Kind, Domain: Domain, Nonce: r.Nonce, Digest: stamp.SchemaDigest}
	e.Values = [16]uint64{stamp.Device, stamp.Inode, stamp.AppliedIndex, stamp.AppliedTerm,
		uint64(stamp.Synchronous), journalCode(stamp.Journal), uint64(stamp.Writers), uint64(stamp.Readers), uint64(len(r.Keys)), Version}
	e.Data = append(e.Data, r.ScopeDigest[:]...)
	if tx == nil || stamp.Device != r.Device || stamp.Inode != r.Inode || !validStamp(stamp) {
		e.Values[10] = 1
	} else {
		for _, key := range r.Keys {
			var note sql.NullString
			// Fixed production whitelist: the session-allow CLI's fingerprint
			// and nonce-bearing note. No caller-controlled table/column names.
			err := tx.QueryRow("SELECT note FROM session_creators WHERE fp=?", key).Scan(&note)
			row := Row{Key: key}
			switch {
			case errors.Is(err, sql.ErrNoRows):
			case err != nil:
				e.Values[10] = 1
			case !note.Valid:
				row.Present, row.Null = true, true
			default:
				row.Present, row.Digest = true, sha256.Sum256([]byte(note.String))
			}
			e.Data = appendRow(e.Data, row)
		}
	}
	drillobserve.Hook(e)
}

// Decode checks the complete private event, including reserved slots and exact
// suffix length. A failed SQL query must not look like a valid absent ACK.
// origin: hot-update S10 plan §5.3 / P08
func Decode(e drillobserve.Event) (Receipt, error) {
	if e.Kind != Kind || e.Domain != Domain || e.Values[9] != Version || e.Values[10] != 0 ||
		e.Values[8] == 0 || e.Values[8] > MaxKeys || len(e.Data) < 32 || len(e.Data) > drillobserve.MaxData ||
		e.Values[4] != 2 || e.Values[6] != 0 || e.Values[7] != 1 {
		return Receipt{}, ErrWitness
	}
	for _, v := range e.Values[11:] {
		if v != 0 {
			return Receipt{}, ErrWitness
		}
	}
	r := Receipt{Nonce: e.Nonce, Stamp: Stamp{Device: e.Values[0], Inode: e.Values[1],
		SchemaDigest: e.Digest, Synchronous: 2, AppliedIndex: e.Values[2], AppliedTerm: e.Values[3], Readers: 1}}
	switch e.Values[5] {
	case 1:
		r.Stamp.Journal = "delete"
	case 2:
		r.Stamp.Journal = "wal"
	default:
		return Receipt{}, ErrWitness
	}
	copy(r.ScopeDigest[:], e.Data[:32])
	b := e.Data[32:]
	for range int(e.Values[8]) {
		key, rest, ok := takeKey(b)
		if !ok || len(rest) < 33 {
			return Receipt{}, ErrWitness
		}
		row := Row{Key: key}
		switch rest[0] {
		case 0:
		case 1:
			row.Present = true
		case 2:
			row.Present, row.Null = true, true
		default:
			return Receipt{}, ErrWitness
		}
		copy(row.Digest[:], rest[1:33])
		if (!row.Present || row.Null) && row.Digest != ([32]byte{}) {
			return Receipt{}, ErrWitness
		}
		r.Rows = append(r.Rows, row)
		b = rest[33:]
	}
	keys := make([]string, 0, len(r.Rows))
	for _, row := range r.Rows {
		keys = append(keys, row.Key)
	}
	if len(b) != 0 || !validStamp(r.Stamp) || !validRegistration(Registration{
		Device: r.Stamp.Device, Inode: r.Stamp.Inode, Nonce: r.Nonce, ScopeDigest: r.ScopeDigest, Keys: keys}) {
		return Receipt{}, ErrWitness
	}
	return r, nil
}

// Verify cross-checks actual pre-replay rows with the independent external ACK
// ledger. A completed-store scan cannot substitute for this earlier receipt.
// origin: hot-update S10 plan §5.3 / P08
func Verify(r Receipt, registration Registration, expected []Row) error {
	if !validRegistration(registration) || !validStamp(r.Stamp) || r.Stamp.Device != registration.Device ||
		r.Stamp.Inode != registration.Inode || r.Nonce != registration.Nonce || r.ScopeDigest != registration.ScopeDigest ||
		len(r.Rows) != len(registration.Keys) || len(expected) != len(r.Rows) {
		return ErrWitness
	}
	for i, key := range registration.Keys {
		if r.Rows[i].Key != key || expected[i].Key != key || r.Rows[i] != expected[i] {
			return ErrWitness
		}
	}
	return nil
}

func journalCode(s string) uint64 {
	switch s {
	case "delete":
		return 1
	case "wal":
		return 2
	default:
		return 0
	}
}

// origin: hot-update S10 plan §5.3 / P08
func validStamp(s Stamp) bool {
	// Single stores legitimately have no Raft cursor; requiring floor >= 1
	// would discard their real durable ACK evidence.
	return s.Device != 0 && s.Inode != 0 && s.SchemaDigest != ([32]byte{}) &&
		journalCode(s.Journal) != 0 && s.Synchronous == 2 && s.Writers == 0 && s.Readers == 1 &&
		(s.AppliedIndex == 0 || s.AppliedTerm != 0)
}

func validRegistration(r Registration) bool {
	// A finite predeclared population bounds both query work and custody data.
	// Duplicate keys would let a repeated ACK stand in for a missing witness.
	if r.Device == 0 || r.Inode == 0 || r.Nonce == ([12]byte{}) || r.ScopeDigest == ([32]byte{}) || len(r.Keys) == 0 || len(r.Keys) > MaxKeys {
		return false
	}
	seen := make(map[string]bool, len(r.Keys))
	for _, key := range r.Keys {
		if len(key) == 0 || len(key) > maxKeyBytes || bytes.IndexByte([]byte(key), 0) >= 0 || seen[key] {
			return false
		}
		seen[key] = true
	}
	return true
}

func decodeRegistration(b []byte) (Registration, error) {
	// Inspect the envelope before allocating rows. Immutable custody protects
	// bytes from mutation, but does not establish format, version or length.
	if len(b) < 66 || len(b) > maxRegistrationBytes || string(b[:4]) != "TSW1" {
		return Registration{}, ErrWitness
	}
	r := Registration{Device: binary.LittleEndian.Uint64(b[16:24]), Inode: binary.LittleEndian.Uint64(b[24:32])}
	copy(r.Nonce[:], b[4:16])
	copy(r.ScopeDigest[:], b[32:64])
	n := int(binary.LittleEndian.Uint16(b[64:66]))
	if n == 0 || n > MaxKeys {
		return Registration{}, ErrWitness
	}
	b = b[66:]
	for range n {
		key, rest, ok := takeKey(b)
		if !ok {
			return Registration{}, ErrWitness
		}
		r.Keys, b = append(r.Keys, key), rest
	}
	if len(b) != 0 || !validRegistration(r) {
		return Registration{}, ErrWitness
	}
	return r, nil
}

func takeKey(b []byte) (string, []byte, bool) {
	if len(b) < 2 {
		return "", nil, false
	}
	n := int(binary.LittleEndian.Uint16(b[:2]))
	if n == 0 || n > maxKeyBytes || len(b) < 2+n {
		return "", nil, false
	}
	return string(b[2 : 2+n]), b[2+n:], true
}

func appendRow(b []byte, row Row) []byte {
	b = binary.LittleEndian.AppendUint16(b, uint16(len(row.Key)))
	b = append(b, row.Key...)
	var state byte
	if row.Present {
		state = 1
		if row.Null {
			state = 2
		}
	}
	b = append(b, state)
	return append(b, row.Digest[:]...)
}

type LockReceipt struct {
	// The first hit is an attempt ordinal on this OFD. Relocking and terminal
	// cleanup cannot rewrite it into a clean end-of-cut snapshot.
	Device, Inode, Attempts, Acquired, FirstAcquiredAttempt uint64
}

// Contender owns one independent CLOEXEC OFD for the entire cut. It never
// creates/chowns the lock, adopts the product OFD, or retains a successful lock.
type Contender struct {
	mu         sync.Mutex
	file       *os.File
	path       string
	receipt    LockReceipt
	err        error
	stop, done chan struct{}
	closeOnce  sync.Once
}

// origin: hot-update S10 plan §5.4 / P08
func StartContender(path string) (*Contender, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, ErrLock
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.Join(ErrLock, f.Close())
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return nil, errors.Join(ErrLock, f.Close())
	}
	c := &Contender{file: f, path: path, stop: make(chan struct{}), done: make(chan struct{}),
		receipt: LockReceipt{Device: uint64(stat.Dev), Inode: uint64(stat.Ino)}}
	// The first real attempt precedes return, so constructor success is never
	// mistaken for a contender that has not yet reached the lock syscall.
	_ = c.Probe()
	go c.run()
	return c, nil
}

// Probe supplements the continuous loop at a deterministic cut barrier. Every
// acquisition, including a brief unlock/relock, remains in the final receipt.
// origin: hot-update S10 plan §5.4 / P08
func (c *Contender) Probe() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.file == nil {
		return ErrLock
	}
	c.receipt.Attempts++
	info, err := os.Lstat(c.path)
	if err != nil || !info.Mode().IsRegular() {
		c.err = ErrLock
		return c.err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	// A replaced/unlinked lock inode makes this OFD's contention irrelevant
	// to newly arriving writers. Treat that as missing evidence, not success.
	if !ok || stat.Nlink != 1 || uint64(stat.Dev) != c.receipt.Device || uint64(stat.Ino) != c.receipt.Inode {
		c.err = ErrLock
		return c.err
	}
	err = syscall.Flock(int(c.file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	switch {
	case err == nil:
		c.receipt.Acquired++
		if c.receipt.FirstAcquiredAttempt == 0 {
			c.receipt.FirstAcquiredAttempt = c.receipt.Attempts
		}
		// Release immediately: the observer must not repair the product's
		// missing lifetime lock by blocking a competing writer itself.
		_ = c.release()
		c.err = ErrLock
	case errors.Is(err, syscall.EWOULDBLOCK), errors.Is(err, syscall.EAGAIN), errors.Is(err, syscall.EINTR):
	default:
		c.err = ErrLock
	}
	return c.err
}

func (c *Contender) release() error {
	for {
		err := syscall.Flock(int(c.file.Fd()), syscall.LOCK_UN)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			// Closing is the last-resort release, never observer-held custody.
			err = errors.Join(err, c.file.Close())
			c.file = nil
		}
		return err
	}
}

func (c *Contender) run() {
	// The loop owns terminal FD close; Close only signals it. Join therefore
	// establishes both observer retirement and the final immutable counters.
	defer close(c.done)
	for {
		select {
		case <-c.stop:
			c.mu.Lock()
			if c.file != nil {
				if c.file.Close() != nil {
					c.err = ErrLock
				}
				c.file = nil
			}
			c.mu.Unlock()
			return
		default:
			_ = c.Probe()
			// Avoid deliberate polling sleeps during the cut. Yield between
			// nonblocking attempts so the owner can still make progress.
			runtime.Gosched()
		}
	}
}

func (c *Contender) Close() error { c.closeOnce.Do(func() { close(c.stop) }); return nil }
func (c *Contender) Join() error  { <-c.done; _, err := c.Result(); return err }
func (c *Contender) Result() (LockReceipt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.receipt, c.err
}
