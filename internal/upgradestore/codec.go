package upgradestore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

const (
	maxJournalBytes = 16 << 20
	maxItems        = 4096
	maxGrants       = 256
	retainedHistory = 128
	historyAge      = 30 * 24 * time.Hour
)

// These disk DTOs are independent of wire and adjunct envelopes. Prepared
// authorizations must survive driver death; keeping only accepted records would
// let a retry manufacture authority from a client-supplied Grant.
// origin: hot-update S8 plan §3.1 / §12.2
type diskFile struct {
	SchemaVersion uint32      `json:"schema_version"`
	Version       uint32      `json:"version"`
	Entries       []diskEntry `json:"entries"`
	Checksum      string      `json:"checksum"`
	legacyProjection
}

type diskEntry struct {
	Key            upgradetx.Key        `json:"key"`
	Revision       uint64               `json:"revision"`
	Generation     uint64               `json:"generation,omitempty"`
	Current        *upgradetx.Record    `json:"current"`
	Archives       []upgradetx.Record   `json:"archives"`
	Metadata       []upgradetx.Metadata `json:"metadata"`
	Grants         []preparedGrant      `json:"grants"`
	AcceptedGrants []upgradetx.Grant    `json:"accepted_grants"`
	// origin: S8 internal review remaining-orchestration F6 (R6): the saved
	// plan shares this journal checksum and its parent execution retention.
	Plans []upgradetx.Plan `json:"plans,omitempty"`
}

type preparedGrant struct {
	Grant   upgradetx.Grant `json:"grant"`
	Scope   upgradetx.Scope `json:"scope"`
	Reverse *reverseBinding `json:"reverse,omitempty"`
	Pins    []string        `json:"pins,omitempty"`
}

type reverseBinding struct {
	HistoricalTxID string          `json:"historical_tx_id"`
	Fence          upgradetx.Fence `json:"fence"`
}

// Retain the v1 projection byte meanings without allowing it to sign a v2
// receipt. P17 owns migration of a pure v1 marker and its old writer callbacks.
// This explicit list also prevents arbitrary fields from bypassing the codec.
type legacyProjection struct {
	State                 string     `json:"state,omitempty"`
	PrevSHA               string     `json:"prev_sha,omitempty"`
	NewSHA                string     `json:"new_sha,omitempty"`
	PrevVersion           string     `json:"prev_version,omitempty"`
	NewVersion            string     `json:"new_version,omitempty"`
	Deadline              *time.Time `json:"deadline,omitempty"`
	BootCount             int        `json:"boot_count,omitempty"`
	BootBudget            int        `json:"boot_budget,omitempty"`
	Detail                string     `json:"detail,omitempty"`
	TargetSID             string     `json:"target_sid,omitempty"`
	TargetNID             string     `json:"target_nid,omitempty"`
	UpgradeID             string     `json:"upgrade_id,omitempty"`
	TargetInstance        string     `json:"target_instance,omitempty"`
	Generation            uint64     `json:"generation,omitempty"`
	ResourcePhase         string     `json:"resource_phase,omitempty"`
	ImportedResources     int        `json:"imported_resources,omitempty"`
	FreezeWindowMS        int64      `json:"freeze_window_ms,omitempty"`
	CandidateImageABI     int        `json:"candidate_image_abi,omitempty"`
	CandidateImageDomains []string   `json:"candidate_image_domains,omitempty"`
	CandidateImageSHA256  string     `json:"candidate_image_sha256,omitempty"`
	PrevImageABI          int        `json:"prev_image_abi,omitempty"`
	PrevImageDomains      []string   `json:"prev_image_domains,omitempty"`
	PrevImageSHA256       string     `json:"prev_image_sha256,omitempty"`
}

func (s *Store) read() (diskFile, error) {
	if err := regularPath(s.options.Path); err != nil {
		return diskFile{}, err
	}
	f, err := os.Open(s.options.Path)
	if errors.Is(err, os.ErrNotExist) {
		return diskFile{SchemaVersion: upgradetx.DiskSchemaVersion, Version: 2}, nil
	}
	if err != nil {
		return diskFile{}, corrupt()
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(io.LimitReader(f, maxJournalBytes+1))
	if err != nil || len(data) > maxJournalBytes {
		return diskFile{}, corrupt()
	}
	var d diskFile
	// encoding/json otherwise accepts duplicate keys, case-insensitive keys,
	// trailing values and unknown fields. All are ambiguous journal evidence.
	if err := strictJSON(data, &d); err != nil {
		return diskFile{}, corrupt()
	}
	if d.SchemaVersion == 0 && d.Version == 0 && d.State != "" {
		// P17 enables this only after disabling the legacy writers. A terminal
		// v1 projection is retained, never turned into an accepted v2 record;
		// legacy pending/failed rollback still requires its original recovery.
		if s.options.AllowLegacyMigration && s.options.Key.Component == upgradetx.ComponentAgent &&
			(d.State == "committed" || d.State == "rolled_back") && validHex(d.PrevSHA, 64) && validHex(d.NewSHA, 64) &&
			(d.TargetSID == "" || d.TargetSID == s.options.Key.SID) && (d.TargetNID == "" || d.TargetNID == s.options.Key.ConfiguredNID) {
			return diskFile{SchemaVersion: upgradetx.DiskSchemaVersion, Version: 2, legacyProjection: d.legacyProjection}, nil
		}
		return diskFile{}, failure(upgradetx.CodeDeferred, "legacy journal requires migration", false)
	}
	if d.SchemaVersion != upgradetx.DiskSchemaVersion || d.Version != 2 || !validHex(d.Checksum, 64) {
		return diskFile{}, corrupt()
	}
	checksum := d.Checksum
	d.Checksum = ""
	encoded, err := json.Marshal(d)
	if err != nil || digestBytes(encoded) != checksum {
		return diskFile{}, corrupt()
	}
	d.Checksum = checksum
	if err := validateDisk(d); err != nil {
		return diskFile{}, err
	}
	return d, nil
}

// write publishes only after both syncs. On a post-rename failure the visible
// intent is deliberately retained: callers must query/reconcile, never repeat
// an effect because a receipt could not be persisted.
// origin: hot-update S8 plan §3.1 / §11.2 P02 (fsync failure)
func (s *Store) write(d *diskFile, accepted bool) error {
	if err := validateDisk(*d); err != nil {
		return err
	}
	d.Checksum = ""
	data, err := json.Marshal(d)
	if err != nil {
		return corrupt()
	}
	d.Checksum = digestBytes(data)
	data, err = json.Marshal(d)
	if err != nil {
		return corrupt()
	}
	if len(data) > maxJournalBytes {
		return failure(upgradetx.CodeDeferred, "journal capacity exhausted; pending history retained", accepted)
	}
	f, err := os.CreateTemp(filepath.Dir(s.options.Path), ".upgrade-state-*")
	if err != nil {
		return failure(upgradetx.CodeDeferred, "journal write unavailable", accepted)
	}
	name := f.Name()
	defer func() { _ = f.Close(); _ = os.Remove(name) }()
	if _, err = f.Write(data); err == nil {
		err = s.syncFile(f, "file")
	}
	if err == nil {
		err = f.Close()
	}
	if err == nil {
		err = os.Rename(name, s.options.Path)
	}
	if err != nil {
		return failure(upgradetx.CodeUnsettled, "journal write not durable", accepted)
	}
	dir, err := os.Open(filepath.Dir(s.options.Path))
	if err == nil {
		defer func() { _ = dir.Close() }()
		err = s.syncFile(dir, "directory")
	}
	if err != nil {
		return failure(upgradetx.CodeUnsettled, "journal rename durability unknown", accepted)
	}
	return nil
}

func (s *Store) syncFile(f *os.File, kind string) error {
	if s.beforeSync != nil {
		if err := s.beforeSync(kind); err != nil {
			return err
		}
	}
	return f.Sync()
}

func regularPath(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !info.Mode().IsRegular() {
		return corrupt()
	}
	return nil
}

func strictJSON(data []byte, dst *diskFile) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := uniqueValue(dec, 0); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return corrupt()
	}
	dec = json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

func uniqueValue(dec *json.Decoder, depth int) error {
	if depth > 64 {
		return corrupt()
	}
	token, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	seen := map[string]bool{}
	count := 0
	for dec.More() {
		count++
		if count > maxItems {
			return corrupt()
		}
		if delim == '{' {
			key, err := dec.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] || name != strings.ToLower(name) {
				return corrupt()
			}
			seen[name] = true
		}
		if err := uniqueValue(dec, depth+1); err != nil {
			return err
		}
	}
	_, err = dec.Token()
	return err
}

func validateDisk(d diskFile) error {
	if d.SchemaVersion != upgradetx.DiskSchemaVersion || d.Version != 2 || len(d.Entries) > maxGrants {
		return corrupt()
	}
	keys := map[upgradetx.Key]bool{}
	for _, e := range d.Entries {
		if !validKey(e.Key) || keys[e.Key] || len(e.Grants) > maxGrants || len(e.Archives) > maxItems || len(e.Metadata) > maxItems || len(e.AcceptedGrants) > maxItems {
			return corrupt()
		}
		keys[e.Key] = true
		// origin: S8 internal review remaining-orchestration F6 (R6)
		if err := validatePlans(&e); err != nil {
			return err
		}
		ids, nonces := map[string]bool{}, map[string]bool{}
		for _, p := range e.Grants {
			_, digest, err := canonicalScope(p.Scope)
			id := p.Grant.Handle.TxID + "/" + p.Grant.Handle.CutID
			if err != nil || !validGrant(p.Grant, e.Key) || p.Grant.ScopeDigest != digest || ids[id] || nonces[p.Grant.ClientNonce] || p.Grant.Handle.Revision > e.Revision {
				return corrupt()
			}
			ids[id], nonces[p.Grant.ClientNonce] = true, true
			if p.Reverse != nil && (!validHex(p.Reverse.HistoricalTxID, 32) || p.Reverse.Fence.Current.Key != e.Key || !validOwner(p.Reverse.Fence.Owner)) {
				return corrupt()
			}
		}
		executions := map[string]bool{}
		for _, r := range records(&e) {
			id := r.Grant.Handle.TxID
			if ids[id] || !validRecord(r, e.Key, e.AcceptedGrants) {
				return corrupt()
			}
			ids[id], executions[id] = true, true
		}
		for _, grant := range e.AcceptedGrants {
			id := grant.Handle.TxID + "/" + grant.Handle.CutID
			if !validGrant(grant, e.Key) || !executions[grant.Handle.TxID] || ids[id] || nonces[grant.ClientNonce] {
				return corrupt()
			}
			ids[id], nonces[grant.ClientNonce] = true, true
		}
		for _, m := range e.Metadata {
			if !executions[m.TxID] || len(m.Pins) > maxItems || m.ArchivedAt < 0 {
				return corrupt()
			}
			delete(executions, m.TxID)
			if m.Window != upgradetx.WindowOpen && m.Window != upgradetx.WindowClosing && m.Window != upgradetx.WindowClosed {
				return corrupt()
			}
			for _, pin := range m.Pins {
				if !validHex(pin, 64) && (!filepath.IsAbs(pin) || filepath.Clean(pin) != pin || !validText(pin)) {
					return corrupt()
				}
			}
		}
		if len(executions) != 0 {
			return corrupt()
		}
	}
	return nil
}

func validRecord(r upgradetx.Record, key upgradetx.Key, grants []upgradetx.Grant) bool {
	a := attempt(r)
	g := grantForAttempt(r, grants)
	_, digest, err := canonicalScope(a.Scope)
	return r.Key == key && err == nil && digest == g.ScopeDigest && sameAttempt(a.Handle, g.Handle) && a.SourceSHA == g.SourceSHA && a.TargetSHA == g.TargetSHA && len(r.Actions) <= maxItems && upgradetx.ValidateRecord(r) == nil
}

func grantForAttempt(r upgradetx.Record, grants []upgradetx.Grant) upgradetx.Grant {
	for _, grant := range grants {
		if sameAttempt(attempt(r).Handle, grant.Handle) {
			return grant
		}
	}
	return r.Grant
}

// Reopening validates the joint vocabulary, not merely a phase string. P01
// still owns event legality and completion against fresh runtime observations.
func validJoint(a upgradetx.Attempt) bool { return upgradetx.ValidateAttempt(a) == nil }

func validGrant(g upgradetx.Grant, key upgradetx.Key) bool {
	return g.Handle.Key == key && validHex(g.Handle.TxID, 32) && validHex(g.Handle.CutID, 32) && g.Handle.Generation > 0 && g.Handle.Revision > 0 && validOwner(g.Issuer) && validHex(g.SourceSHA, 64) && validHex(g.TargetSHA, 64) && validHex(g.ScopeDigest, 64) && validHex(g.CapabilityDigest, 64) && validText(g.ClientNonce) && validText(g.ActorFP) && validText(g.SourceRelease) && validText(g.TargetRelease) && g.ExpiresAt > 0 && (g.Mode == upgradetx.ModeHot || g.Mode == upgradetx.ModeLegacyInterrupting) && ((g.ParentTxID == "" && g.ParentSlot == "") || (validHex(g.ParentTxID, 32) && validText(g.ParentSlot)))
}

func validRequest(r upgradetx.PrepareRequest, key upgradetx.Key) bool {
	if !validText(r.ClientNonce) || !validText(r.ActorFP) || !validText(r.SourceRelease) || !validText(r.TargetRelease) || !validHex(r.SourceSHA, 64) || !validHex(r.TargetSHA, 64) || !validHex(r.CapabilityDigest, 64) || r.Mode != upgradetx.ModeHot && r.Mode != upgradetx.ModeLegacyInterrupting || (r.ParentTxID == "") != (r.ParentSlot == "") || r.ParentTxID != "" && (!validHex(r.ParentTxID, 32) || !validText(r.ParentSlot)) {
		return false
	}
	if upgradetx.OrchestrationKey(key) {
		return len(r.Scope.Participants) > 0
	}
	for _, participant := range r.Scope.Participants {
		if participant == key {
			return true
		}
	}
	return false
}

// Preserve the older agent/broker record shapes. New infrastructure keys
// must carry a bounded configured identity and a clean managed config path
// on both Prepare and Decode; accepting only Open would strand the journal.
// origin: hot-update S8 plan §2.1 / §3.1; S9 plan §8 / T44
func validKey(k upgradetx.Key) bool { return upgradetx.ValidateKey(k) == nil }

func validOwner(o upgradetx.Owner) bool {
	return validText(o.BootID) && o.PID > 0 && o.StartTicks > 0 && validText(o.InstanceID)
}
func validText(s string) bool {
	return len(s) > 0 && len(s) <= 4096 && !strings.ContainsAny(s, "\x00\r\n")
}
func validHex(s string, size int) bool {
	if len(s) != size || strings.ToLower(s) != s {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
func digestBytes(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

func canonicalScope(scope upgradetx.Scope) (upgradetx.Scope, string, error) {
	s, err := upgradetx.CanonicalScope(scope)
	if err != nil {
		return scope, "", err
	}
	digest, err := upgradetx.ScopeDigest(s)
	return s, digest, err
}

func matchesRequest(g upgradetx.Grant, r upgradetx.PrepareRequest, scope string) bool {
	return g.ClientNonce == r.ClientNonce && g.ActorFP == r.ActorFP && g.Mode == r.Mode && g.ParentTxID == r.ParentTxID && g.ParentSlot == r.ParentSlot && g.SourceRelease == r.SourceRelease && g.TargetRelease == r.TargetRelease && g.SourceSHA == r.SourceSHA && g.TargetSHA == r.TargetSHA && g.ScopeDigest == scope && g.CapabilityDigest == r.CapabilityDigest && (r.ExpiresAt == 0 || g.ExpiresAt == r.ExpiresAt)
}

func collect(e *diskEntry, now int64) {
	grants := e.Grants[:0]
	for _, p := range e.Grants {
		if p.Grant.ExpiresAt > now {
			grants = append(grants, p)
		}
	}
	e.Grants = grants
	// Keep the union of the newest 128 and the last 30 days. Pending records,
	// open windows and pinned artifacts are never sacrificed for capacity.
	cutoff := now - int64(historyAge)
	archives := e.Archives[:0]
	removed := map[string]bool{}
	for i, r := range e.Archives {
		m := metadata(e, r.Grant.Handle.TxID)
		if i >= len(e.Archives)-retainedHistory || m.ArchivedAt == 0 || m.ArchivedAt >= cutoff || m.Window != upgradetx.WindowClosed || len(m.Pins) != 0 || !settled(r) {
			archives = append(archives, r)
		} else {
			removed[r.Grant.Handle.TxID] = true
		}
	}
	e.Archives = archives
	metadataRows := e.Metadata[:0]
	for _, m := range e.Metadata {
		if !removed[m.TxID] {
			metadataRows = append(metadataRows, m)
		}
	}
	e.Metadata = metadataRows
	accepted := e.AcceptedGrants[:0]
	for _, grant := range e.AcceptedGrants {
		if !removed[grant.Handle.TxID] {
			accepted = append(accepted, grant)
		}
	}
	e.AcceptedGrants = accepted
	// Pending parents never enter removed; their execution authority and full
	// plan therefore survive the same 128-record / 30-day union as history.
	plans := e.Plans[:0]
	for _, plan := range e.Plans {
		if !removed[plan.Parent.TxID] {
			plans = append(plans, plan)
		}
	}
	e.Plans = plans
}

func expectedDigest(scope upgradetx.Scope) string {
	canonical, _, err := canonicalScope(scope)
	if err != nil {
		return ""
	}
	data, _ := json.Marshal(canonical.Expected)
	return digestBytes(data)
}
