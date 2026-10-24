package xferserve

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/xferauth"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

const authorityStream = "tether_xfer_authority"
const authorityPrefix = proto.SubjectPrefix + ".cluster.transfer-authority."

func fmtSequence(n uint64) string { return strconv.FormatUint(n, 10) }

// Directory retains PUBLIC creation authority and issuer trust independently
// of the current roster and of admission. No capsule or signing seed enters
// this stream. Subject CAS fixes the original permit before its first reply;
// a lost reply or concurrent home cannot renew that creation window.
// origin: hot-update S7 plan §6.1 / §6.3 / T26 / T28 / T39
type Directory struct {
	js      jetstream.JetStream
	created time.Time
}

type originRecord struct {
	Origin x.Origin `json:"origin"`
	Issuer string   `json:"issuer"`
}
type permitRecord struct {
	Permit x.CreationPermit `json:"permit"`
	Issuer string           `json:"issuer"`
}

func authorityContext(d x.Deadline) (context.Context, context.CancelFunc, error) {
	left, err := controlbudget.Remaining(d)
	if err != nil {
		return nil, nil, err
	}
	// ctx-none: synchronous authority ports carry the original absolute deadline.
	ctx, cancel := context.WithTimeout(context.Background(), min(left, 5*time.Second))
	return ctx, cancel, nil
}

func openDirectory(js jetstream.JetStream, replicas int, d x.Deadline, roots ...string) (*Directory, error) {
	ctx, cancel, err := authorityContext(d)
	if err != nil {
		return nil, err
	}
	defer cancel()
	stream, err := js.Stream(ctx, authorityStream)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		for _, root := range roots {
			if _, err := os.Stat(filepath.Join(root, "directory.json")); !os.IsNotExist(err) {
				return nil, xferauth.ControlRefusal(x.CodeOwnerRecovering)
			}
		}
		// CREATE is idempotent and cannot replace an existing full configuration.
		// Retained origins never age out or get evicted to admit another writer.
		stream, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: authorityStream, Subjects: []string{authorityPrefix + ">"}, Storage: jetstream.FileStorage,
			Replicas: max(1, replicas), Discard: jetstream.DiscardNew, MaxBytes: 64 << 20, MaxMsgs: -1, MaxMsgsPerSubject: -1, DenyDelete: true, DenyPurge: true})
		if err != nil {
			stream, err = js.Stream(ctx, authorityStream)
		}
	}
	if err != nil {
		return nil, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, err
	}
	cfg := info.Config
	if !slices.Equal(cfg.Subjects, []string{authorityPrefix + ">"}) || cfg.MaxAge != 0 || cfg.Discard != jetstream.DiscardNew || cfg.MaxMsgs > 0 || cfg.MaxMsgsPerSubject > 0 ||
		cfg.Retention != jetstream.LimitsPolicy || !cfg.DenyDelete || !cfg.DenyPurge || cfg.AllowRollup || cfg.Replicas < max(1, replicas) {
		return nil, xferauth.ControlRefusal(x.CodeOwnerRecovering)
	}
	return &Directory{js: js, created: info.Created}, nil
}

func authorityKey(parts ...string) string {
	bytes, _ := json.Marshal(parts)
	sum := sha256.Sum256(bytes)
	return hex.EncodeToString(sum[:])
}

// Every read proves the original stream incarnation and non-evicting policy.
// A delete/recreate or operator TTL must not turn retained authority into absence.
// origin: hot-update S7 plan §6.1 / T28
func (a *Directory) open(ctx context.Context) (jetstream.Stream, error) {
	// The SDK Stream.Info cache is mutable and has no synchronization. Each
	// concurrent claim/auth lookup gets a fresh handle, so INFO and raw reads
	// share one operation's incarnation without racing another operation.
	// origin: hot-update S7 plan §6.1 / T26 / T28
	stream, err := a.js.Stream(ctx, authorityStream)
	if err != nil {
		return nil, err
	}
	info := stream.CachedInfo()
	c := info.Config
	if !info.Created.Equal(a.created) || c.MaxAge != 0 || c.Discard != jetstream.DiscardNew || c.MaxMsgs > 0 || c.MaxMsgsPerSubject > 0 || !c.DenyDelete || !c.DenyPurge || c.Retention != jetstream.LimitsPolicy || c.AllowRollup {
		return nil, xferauth.ControlRefusal(x.CodeOwnerRecovering)
	}
	return stream, nil
}

func (a *Directory) read(subject string, d x.Deadline) ([]byte, uint64, error) {
	ctx, cancel, err := authorityContext(d)
	if err != nil {
		return nil, 0, err
	}
	defer cancel()
	stream, err := a.open(ctx)
	if err != nil {
		return nil, 0, err
	}
	m, err := stream.GetLastMsgForSubject(ctx, subject)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	return m.Data, m.Sequence, nil
}

func (a *Directory) publish(subject string, v any, previous uint64, d x.Deadline) error {
	ctx, cancel, err := authorityContext(d)
	if err != nil {
		return err
	}
	defer cancel()
	if _, err = a.open(ctx); err != nil {
		return err
	}
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	m := &nats.Msg{Subject: subject, Data: data, Header: nats.Header{}}
	m.Header.Set(nats.ExpectedLastSubjSeqHdr, fmtSequence(previous))
	_, err = a.js.PublishMsg(ctx, m)
	return err
}

func (a *Directory) register(origin x.Origin, issuer string, d x.Deadline) error {
	subject := authorityPrefix + "origin." + authorityKey(origin.BrokerID, origin.InstanceID, origin.BootID, fmtSequence(origin.Generation))
	want := originRecord{Origin: origin, Issuer: issuer}
	if err := a.publish(subject, want, 0, d); err != nil {
		data, _, readErr := a.read(subject, d)
		var actual originRecord
		if readErr != nil || json.Unmarshal(data, &actual) != nil || actual != want {
			return xferauth.ControlRefusal(x.CodeOwnerRecovering)
		}
	}
	return nil
}

// scan uses a fixed high-water mark and subject-filtered reads. Any missing,
// unreadable or over-budget prefix stays unknown; it never returns partial absence.
func (a *Directory) scan(kind string, d x.Deadline) ([][]byte, error) {
	ctx, cancel, err := authorityContext(d)
	if err != nil {
		return nil, err
	}
	defer cancel()
	stream, err := a.open(ctx)
	if err != nil {
		return nil, err
	}
	info := stream.CachedInfo()
	var result [][]byte
	for next := info.State.FirstSeq; next != 0 && next <= info.State.LastSeq; {
		m, err := stream.GetMsg(ctx, next, jetstream.WithGetMsgSubject(authorityPrefix+kind+".>"))
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			break
		}
		if err != nil {
			return nil, err
		}
		if m.Sequence > info.State.LastSeq {
			break
		}
		if m.Sequence < next || len(result) >= 65536 {
			return nil, xferauth.ControlRefusal(x.CodeOwnerRecovering)
		}
		result = append(result, m.Data)
		next = m.Sequence + 1
	}
	return result, ctx.Err()
}

func (a *Directory) origins(d x.Deadline) ([]originRecord, error) {
	data, err := a.scan("origin", d)
	if err != nil {
		return nil, err
	}
	result := make([]originRecord, 0, len(data))
	seen := make(map[x.Origin]bool)
	for _, bytes := range data {
		var r originRecord
		if json.Unmarshal(bytes, &r) != nil || r.Origin.BrokerID == "" || r.Origin.InstanceID == "" || r.Origin.BootID == "" || r.Origin.Generation == 0 ||
			(!nkeys.IsValidPublicAccountKey(r.Issuer) && !nkeys.IsValidPublicUserKey(r.Issuer)) || seen[r.Origin] {
			return nil, xferauth.ControlRefusal(x.CodeOwnerRecovering)
		}
		seen[r.Origin] = true
		result = append(result, r)
	}
	return result, nil
}

func (a *Directory) issuer(origin x.Origin, d x.Deadline) (string, error) {
	all, err := a.origins(d)
	if err != nil {
		return "", err
	}
	for _, r := range all {
		if r.Origin == origin {
			return r.Issuer, nil
		}
	}
	return "", xferauth.ErrDenied
}

func (a *Directory) lookup(sid, tid string, d x.Deadline) (*permitRecord, error) {
	data, _, err := a.read(authorityPrefix+"permit."+authorityKey(sid, tid), d)
	if err != nil || data == nil {
		return nil, err
	}
	var r permitRecord
	if json.Unmarshal(data, &r) != nil || r.Permit.Identity.SID != sid || r.Permit.Identity.TransferID != tid || a.verify(r, d) != nil {
		return nil, xferauth.ControlRefusal(x.CodeOwnerRecovering)
	}
	return &r, nil
}

func (a *Directory) verify(r permitRecord, d x.Deadline) error {
	issuer, err := a.issuer(r.Permit.Origin, d)
	if err != nil || issuer != r.Issuer {
		return xferauth.ErrDenied
	}
	p := r.Permit
	wp := wirePermit(p)
	body, err := proto.CanonicalCreationPermitBytes(&wp)
	if err != nil {
		return xferauth.ErrDenied
	}
	key, err := nkeys.FromPublicKey(issuer)
	if err != nil {
		return xferauth.ErrDenied
	}
	sig, err := base64.RawURLEncoding.DecodeString(p.Signature)
	if err != nil || key.Verify(body, sig) != nil {
		return xferauth.ErrDenied
	}
	if p.IssuedAt.IsZero() || !p.CreateUntil.After(p.IssuedAt) || p.CreateUntil.Sub(p.IssuedAt) > 5*time.Minute || p.Identity.EntryNonce == "" || p.PermitNonce == "" {
		return xferauth.ErrDenied
	}
	return nil
}

func (a *Directory) claim(p x.CreationPermit, d x.Deadline) (*permitRecord, error) {
	issuer, err := a.issuer(p.Origin, d)
	if err != nil {
		return nil, err
	}
	r := permitRecord{Permit: p, Issuer: issuer}
	if err = a.verify(r, d); err != nil {
		return nil, err
	}
	// Both a losing CAS and a lost ACK reconcile ONLY this business key. The
	// exact original signed bytes are authoritative, irrespective of current home.
	// origin: hot-update S7 plan §6.1 / T26 / M07
	if err = a.publish(authorityPrefix+"permit."+authorityKey(p.Identity.SID, p.Identity.TransferID), r, 0, d); err != nil {
		// A clustered CAS can reject against a proposal before its winner is
		// visible to GET. One absent read is not a definitive admission refusal:
		// keep reading this immutable key within the ORIGINAL request deadline.
		// No second permit, publish, nonce or creation window is introduced.
		// origin: S7 internal review TRANSFER-OVERLAY-REPAIR F4 / hot-update S7 plan §6.1 / T39
		for {
			retained, readErr := a.lookup(p.Identity.SID, p.Identity.TransferID, d)
			if readErr != nil || retained != nil {
				return retained, readErr
			}
			left, deadlineErr := controlbudget.Remaining(d)
			if deadlineErr != nil {
				return nil, deadlineErr
			}
			time.Sleep(min(10*time.Millisecond, left))
		}
	}
	return &r, nil
}

// Peers combines every retained origin with the current authority roster. A
// member not yet registered makes the census incomplete; removal never prunes
// a retained origin. The caller probes the current roster anew for capability.
func (a *Directory) Peers(current func(x.Deadline) ([]x.Origin, error), d x.Deadline) ([]x.Origin, error) {
	if current == nil {
		return nil, xferauth.ControlRefusal(x.CodeOwnerRecovering)
	}
	members, err := current(d)
	if err != nil {
		return nil, err
	}
	records, err := a.origins(d)
	if err != nil {
		return nil, err
	}
	var out []x.Origin
	for _, r := range records {
		out = append(out, r.Origin)
	}
	for _, m := range members {
		found := false
		for _, o := range out {
			if o.BrokerID == m.BrokerID && (m.InstanceID == "" || m == o) {
				found = true
			}
		}
		if !found {
			return nil, xferauth.ControlRefusal(x.CodeOwnerRecovering)
		}
	}
	if len(members) == 0 {
		return nil, xferauth.ControlRefusal(x.CodeOwnerRecovering)
	}
	return out, nil
}

func (a *Directory) protect(c *x.LedgerCensus, rows Rows, d x.Deadline) error {
	data, err := a.scan("permit", d)
	if err != nil {
		return err
	}
	for _, bytes := range data {
		var r permitRecord
		if json.Unmarshal(bytes, &r) != nil || a.verify(r, d) != nil {
			return xferauth.ControlRefusal(x.CodeOwnerRecovering)
		}
		if r.Permit.Origin != c.Origin || !time.Now().Before(r.Permit.CreateUntil) {
			continue
		}
		found := false
		for _, e := range c.Entries {
			found = found || e.Identity == r.Permit.Identity
		}
		if !found {
			if rows != nil {
				if retained, err := rows(r.Permit.Identity, d); err == nil && retained.Identity == r.Permit.Identity && retained.Origin == c.Origin && retained.EntryNonce == r.Permit.Identity.EntryNonce && retained.Revision > 0 {
					c.Rows = append(c.Rows, retained)
					c.Entries = append(c.Entries, x.Protection{Identity: retained.Identity, Phase: retained.Phase, ObjectVersion: retained.Identity.ObjectVersion, CreationUntil: r.Permit.CreateUntil, WriterRetired: retained.Cleanup.WriterFence.WriterRetired, OwnerGeneration: c.Origin.Generation})
					continue
				}
			}
			c.Entries = append(c.Entries, x.Protection{Identity: r.Permit.Identity, Phase: x.PhaseOpen, ObjectVersion: r.Permit.Identity.ObjectVersion, CreationUntil: r.Permit.CreateUntil, OwnerGeneration: c.Origin.Generation})
		}
	}
	// The public digest covers issued authority as well as admitted rows. It
	// contains neither capsules nor canonical private delivery state.
	public, _ := json.Marshal(struct {
		Origin  x.Origin
		Entries []x.Protection
	}{c.Origin, c.Entries})
	digest := sha256.Sum256(public)
	c.Digest = hex.EncodeToString(digest[:])
	return nil
}

func ResolveOrigin(owners Owners, req x.Request, d x.Deadline) (x.Request, bool) {
	var r *permitRecord
	var err error
	if owners.Directory != nil {
		r, err = owners.Directory.lookup(req.Identity.SID, req.Identity.TransferID, d)
	} else {
		// Active ledger rows retire before the result retention window ends.
		// Read the original signed permit on single brokers without creating a
		// new permit or depending on an already consumed ledger row.
		// origin: S7 internal review MAKETEST-TRANSFER F7; S7 plan §6.1 / T28
		if owners.Permits == nil || owners.PermitRoot == "" {
			return req, false
		}
		sum := sha256.Sum256([]byte(req.Identity.SID + "\n" + req.Identity.TransferID))
		var permit x.CreationPermit
		err = readRetirementEvidence(filepath.Join(owners.PermitRoot, hex.EncodeToString(sum[:])+".json"), &permit)
		if err == nil {
			if permit.Origin != owners.Origin || owners.Permits.Verify(permit) != nil {
				return req, false
			}
			r = &permitRecord{Permit: permit}
		} else if os.IsNotExist(err) {
			err = nil // Only the complete census below can prove absence.
		}
	}
	if err != nil {
		return req, false
	}
	if r != nil {
		want, got := req.Identity, r.Permit.Identity
		// Discovery fills omitted bindings; it cannot replace supplied ones.
		// origin: S7 internal review MAKETEST-TRANSFER F7; S7 plan §6.1
		if want.SID != got.SID || want.NID != got.NID || want.TransferID != got.TransferID ||
			want.ActorFP != "" && want.ActorFP != got.ActorFP || want.AgentInstance != "" && want.AgentInstance != got.AgentInstance ||
			want.EntryNonce != "" && want.EntryNonce != got.EntryNonce || want.IntentDigest != "" && want.IntentDigest != got.IntentDigest ||
			want.ObjectVersion != "" && want.ObjectVersion != got.ObjectVersion {
			return req, false
		}
		req.Identity, req.Origin = r.Permit.Identity, r.Permit.Origin
		return req, true
	}
	censuses, err := owners.SessionCensus(req.Identity.SID, d)
	if err != nil {
		return req, false
	}
	for _, c := range censuses {
		if !c.Ready || !c.Complete || c.Code != "" {
			return req, false
		}
		for _, e := range c.Entries {
			if e.Identity.SID == req.Identity.SID && e.Identity.TransferID == req.Identity.TransferID {
				return req, false
			}
		}
	}
	req.Origin = owners.Origin
	return req, true
}
