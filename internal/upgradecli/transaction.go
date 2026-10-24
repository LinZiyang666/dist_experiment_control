package upgradecli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// grantCacheDir keeps the inert grants this ctl home prepared or started.
// Every later --tx-id verb needs the target's exact Key, and the frozen
// request carries no other way to learn it: a guessed Key is denied by the
// target, and a Key copied from another home could name another component.
// The cache is evidence the actor already holds, never execution authority —
// the target re-checks actor, key and revision on every operation.
// origin: hot-update S8 plan §3.1 / §7.1 / §11.2 P19
func grantCacheDir(home string) string { return filepath.Join(home, "upgrades", "grants") }

// RememberGrant records a target-issued grant under its TxID. An existing entry
// for the same ID must be byte-identical: a second, different grant under one ID
// would let a later status/rollback address a component the actor never chose.
func RememberGrant(home string, grant upgradetx.Grant) error {
	if home == "" || !hexValue(grant.Handle.TxID, 32) || upgradetx.ValidateKey(grant.Handle.Key) != nil {
		return rejected("grant has no target-issued transaction identity")
	}
	dir := grantCacheDir(home)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return unsettled("transaction grant cache is unavailable")
	}
	path := filepath.Join(dir, grant.Handle.TxID+".json")
	if previous, err := RecallGrant(home, grant.Handle.TxID); err == nil {
		if previous != grant {
			return rejected("a different grant is already recorded for this transaction")
		}
		return nil
	} else if _, statErr := os.Lstat(path); statErr == nil {
		return err
	}
	return writeJSONFile(path, grant)
}

// RecallGrant returns the grant recorded for txID by this ctl home. A missing
// entry is an operator error (64): it was prepared elsewhere, so the actor must
// run the command where its grant lives instead of guessing the target Key.
func RecallGrant(home, txID string) (upgradetx.Grant, error) {
	var grant upgradetx.Grant
	if home == "" || !hexValue(txID, 32) {
		return grant, rejected("a 32-hex transaction ID is required")
	}
	path := filepath.Join(grantCacheDir(home), txID+".json")
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return grant, rejected("transaction was not prepared from this ctl home; run the command where its grant was issued")
	}
	if err := readJSON(path, &grant); err != nil || grant.Handle.TxID != txID || upgradetx.ValidateKey(grant.Handle.Key) != nil {
		return upgradetx.Grant{}, &upgradetx.Error{Code: upgradetx.CodeStateCorrupt, Detail: "recorded transaction grant is unreadable"}
	}
	return grant, nil
}

// WriteGrant publishes the inert grant for the root stage step. Like the
// receipt writer it never replaces an input and refuses to overwrite a grant
// file that belongs to another transaction.
func WriteGrant(path string, grant upgradetx.Grant) error {
	if path == "" {
		return rejected("prepare-only requires --grant-out")
	}
	output, err := filepath.Abs(path)
	if err != nil {
		return rejected("invalid grant output path")
	}
	if info, statErr := os.Lstat(output); statErr == nil {
		var old upgradetx.Grant
		if !info.Mode().IsRegular() || readJSON(output, &old) != nil || old != grant {
			return rejected("grant output already holds a different authorization")
		}
		return nil
	}
	if err := writeJSONFile(output, grant); err != nil {
		return unsettled("grant persistence is uncertain; prepare again with the same request")
	}
	return nil
}

// ReadArtifactReceipt strictly decodes the stage output. Duplicate, unknown or
// differently cased fields are refused before the receipt can bind a Start.
func ReadArtifactReceipt(path string) (upgradeartifact.Receipt, error) {
	var receipt upgradeartifact.Receipt
	if path == "" {
		return receipt, rejected("an artifact receipt file is required")
	}
	if err := readJSON(path, &receipt); err != nil {
		return upgradeartifact.Receipt{}, rejected("artifact receipt file is invalid")
	}
	return receipt, nil
}

// Resume starts exactly the original grant named by txID. The receipt must be
// bound to that grant, and when this home already recorded the grant the two
// must agree; a receipt cannot retarget another component or transaction.
// A retry after a lost reply reaches the same accepted record on the target.
// origin: hot-update S8 plan §3.1 / §5.2 / §7.1 / M16
func Resume(ctx context.Context, endpoint upgradeport.Endpoint, home, txID string, receipt upgradeartifact.Receipt) (upgradetx.View, error) {
	grant := receipt.Grant
	if grant.Handle.TxID != txID || receipt.SourceSHA != grant.SourceSHA || receipt.TargetSHA != grant.TargetSHA {
		return upgradetx.View{}, rejected("artifact receipt does not belong to the requested transaction")
	}
	if recorded, err := RecallGrant(home, txID); err == nil && recorded != grant {
		return upgradetx.View{}, rejected("artifact receipt names a different grant than the recorded transaction")
	}
	if endpoint == nil {
		return upgradetx.View{}, &upgradetx.Error{Code: upgradetx.CodeNotWired, Detail: "upgrade endpoint is required"}
	}
	view, err := endpoint.Start(ctx, grant, receipt)
	if err != nil {
		return view, err
	}
	if view.Historical.Grant != grant {
		return view, &upgradetx.Error{Code: upgradetx.CodeUnsettled, Accepted: true, Detail: "start reply does not describe the original grant"}
	}
	// The grant is cached only after the target accepted it; a failed cache
	// write leaves the accepted transaction intact and queryable by receipt.
	if err := RememberGrant(home, grant); err != nil {
		return view, err
	}
	return view, nil
}

// ReverseFor derives a manual reverse request from a FRESH query of the
// original transaction. The fence names the current attempt and running owner
// exactly as the target just reported them, so a stale or historical view
// cannot authorize a reverse of a later transaction (§8.1 M12). The target
// SHA is the record's recorded previous image, never a caller choice.
// origin: hot-update S8 plan §4.1 / §8.1 / §11.2 P19
func ReverseFor(view upgradetx.View, actorFP string, now time.Time) (upgradetx.Fence, upgradetx.ReverseRequest, error) {
	record, current := view.Historical, view.Observation.Current
	if !view.Observation.OwnerKnown || current.Handle.TxID != record.Grant.Handle.TxID || current.Handle.Key != record.Key ||
		current.Handle.Revision != record.Revision || !hexValue(view.Observation.RunningSHA, 64) || !hexValue(record.RecordedPreviousSHA, 64) {
		return upgradetx.Fence{}, upgradetx.ReverseRequest{}, unsettled("current owner or recorded previous image is unknown; query the transaction")
	}
	fence := upgradetx.Fence{Current: current.Handle, Owner: view.Observation.Census.Stamp.Producer,
		RunningSHA: view.Observation.RunningSHA, PreviousSHA: record.RecordedPreviousSHA}
	if record.Key.Component == upgradetx.ComponentAgent {
		incarnation, ok := authorityIncarnation(view.Observation.Census)
		if !ok {
			return upgradetx.Fence{}, upgradetx.ReverseRequest{}, &upgradetx.Error{Code: upgradetx.CodeDeferred,
				Detail: "reverse_incarnation_unavailable: the current authority incarnation was not observed"}
		}
		fence.Incarnation = incarnation
	}
	nonce, err := randomHex()
	if err != nil {
		return upgradetx.Fence{}, upgradetx.ReverseRequest{}, err
	}
	scope, err := currentReverseScope(current.Scope, view.Observation.Census)
	if err != nil {
		return upgradetx.Fence{}, upgradetx.ReverseRequest{}, err
	}
	reverse := upgradetx.ReverseRequest{ClientNonce: nonce, ActorFP: actorFP, HistoricalTxID: current.Handle.TxID,
		TargetSHA: record.RecordedPreviousSHA, Scope: scope, ExpiresAt: now.Add(upgradebudget.ComponentHandover).UnixNano()}
	return fence, reverse, nil
}

// authorityIncarnation reads the single authority resource of a fresh census.
// Its identity is the session creation digest; an absent, duplicated or
// malformed row is unknown, never a placeholder the target would accept.
func authorityIncarnation(census upgradetx.Census) (string, bool) {
	found := ""
	for _, resource := range census.Resources {
		if resource.Domain != upgradetx.DomainAuthority || upgradetx.ResourceNotApplicable(resource) {
			continue
		}
		if found != "" || !hexValue(resource.ID, 64) {
			return "", false
		}
		found = resource.ID
	}
	return found, found != ""
}

// CurrentHandle is the CAS handle for cancel/archive/close-window: the fresh
// current attempt of the original transaction, never a remembered one.
func CurrentHandle(view upgradetx.View) (upgradetx.Handle, error) {
	current := view.Observation.Current.Handle
	if current.TxID != view.Historical.Grant.Handle.TxID || current.Key != view.Historical.Key || current.Revision != view.Historical.Revision || !hexValue(current.CutID, 32) {
		return upgradetx.Handle{}, unsettled("current attempt of the original transaction is unknown")
	}
	return current, nil
}

// SameView reports whether a mutation reply still describes the queried
// transaction. A reply about another record cannot settle this command.
func SameView(a, b upgradetx.View) bool {
	return a.Historical.Key == b.Historical.Key && reflect.DeepEqual(a.Historical.Grant, b.Historical.Grant)
}

func randomHex() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", unsettled("cannot generate a request nonce")
	}
	return hex.EncodeToString(random[:]), nil
}
