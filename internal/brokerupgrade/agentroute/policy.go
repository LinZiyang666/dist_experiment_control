// Package agentroute owns the authenticated agent route inputs, target policy,
// replicated capability record and nonce-bound control-probe relay.
// origin: hot-update S8 plan §12.1; S8 BROKERUPGRADE-SLIM responsibility extraction
package agentroute

import (
	"database/sql"
	"errors"
	"strings"

	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradecompat"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// LegacyAgentRefusal is the common admission for old node upgrades and signed
// cluster children, including ReExecOnly. A positive live row proves busy; an
// empty or unreadable database cannot prove the absence of agent-owned work.
// Legacy ctl can admit work directly, so even an idle observation supplies no
// admission fence. ReleaseVersion and an account signature confer no S8 grant.
// Explicit migration belongs to upgradelegacy.Proxy with a durable issuer,
// reservation and single dispatch; it must never fall through this old route.
// origin: hot-update S8 plan §7.1 / §7.2 / S8 internal review rv-n1 F1
func LegacyAgentRefusal(db *sql.DB, sid, nid string) (string, string) {
	policy := upgradecompat.LegacyPolicy{Component: upgradetx.ComponentAgent}
	if db != nil {
		// These are positive refusal witnesses only, never a complete census.
		// Query separately so an unavailable domain cannot hide a known busy one.
		for _, query := range []string{
			`SELECT EXISTS(SELECT 1 FROM processes WHERE sid=? AND nid=? AND status='RUNNING')`,
			`SELECT EXISTS(SELECT 1 FROM port_allocations WHERE sid=? AND nid=? AND state='ALLOCATED')`,
		} {
			var busy bool
			if err := db.QueryRow(query, sid, nid).Scan(&busy); err == nil && busy {
				policy.Busy = true
			}
		}
	}
	err := upgradecompat.CheckLegacy(policy)
	var refusal *upgradetx.Error
	if errors.As(err, &refusal) {
		if refusal.Code == upgradetx.CodeDeferred {
			return "try_again", refusal.Detail
		}
		return proto.CodeBadRequest, refusal.Detail
	}
	return "try_again", "legacy_quiescence_unproved"
}

// CloneFamilyConflict rejects both a leased row and its configured
// basename. In the supported shared-home deployment they may execute one
// shared binary and marker; replacing it for either process makes a nominally
// single-node operation mutate the whole clone family, and a sibling restart
// can consume the target's pending boot proof. A historical lease row keeps
// the family conservative: the broker cannot prove a stopped sibling will not
// restart during the upgrade window.
func CloneFamilyConflict(db *sql.DB, sid, target string) bool {
	// len(provisioned) > 0, not the readable flag: this site means "are there bindings to
	// reason about at all". E1 (round 2) changed what the second return says — it is now
	// "the table was read" — so the intent is spelled out rather than inherited.
	provisioned, provReadable := node.ProvisionedNIDs(db, sid)
	bindingsKnown := provReadable && len(provisioned) > 0
	all, err := node.List(db)
	if err != nil {
		// An upgrade replaces an executable. If the authority cannot be read,
		// availability is the safe sacrifice: do not guess that this target has
		// an isolated upgrade domain.
		return true
	}
	for _, n := range all {
		if n.SID != sid {
			continue
		}
		base, _, shaped := proto.SplitLeaseName(n.NID)
		if !shaped || bindingsKnown && provisioned[n.NID] {
			continue
		}
		// With no auth-callout bindings, shape is not proof that n is leased.
		// It is nevertheless proof that an isolated binary cannot be proven,
		// so upgrade takes the conservative (possibly availability-reducing)
		// answer. The ordinary node-list classifier remains non-destructive and
		// therefore keeps its opposite fallback.
		if target == n.NID || target == base {
			return true
		}
	}
	return false
}

// URLAllowed returns true iff url has any prefix in allow. Empty
// allow → false (we don't trust an unconfigured allowlist; J.4 §
// 安全约束 makes the operator opt in explicitly).
func URLAllowed(url string, allow []string) bool {
	if len(allow) == 0 {
		return false
	}
	for _, p := range allow {
		if p == "" {
			continue
		}
		if strings.HasPrefix(url, p) {
			return true
		}
	}
	return false
}

// ConfiguredBasename returns the root of this agent's credential family. It is
// the register's family authority, decided together with RegisterAuthority
// before any lease or row write (moved from broker.go, P15 net-zero).
//
// It is what the agent SAYS it was configured with, falling back to the
// presented name when it says nothing — which is both the pre-feature case and
// the ordinary unleased case, and in each the presented name IS the root.
//
// Deliberately never SplitLeaseName: that would fold a real device named
// `gpu-02` into the `gpu` family (external review F2).
func ConfiguredBasename(db *sql.DB, sid, nid string, req *proto.NodeRegisterReq) string {
	if req == nil || req.ConfiguredNID == "" || req.ConfiguredNID == nid {
		return nid
	}
	// IT IS CLIENT-SUPPLIED, SO IT IS CHECKED AGAINST THE NAME THIS REGISTER
	// ARRIVED UNDER.
	//
	// The only legitimate case where the two differ is an agent that already
	// holds a lease: it registers as `<base>-NN` while its configured root is
	// `<base>`. Anything else is an agent claiming membership of a credential
	// family it did not authenticate into — and the external re-review showed
	// what that buys: an agent holding gpu1's credential could declare
	// ConfiguredNID "victim" and have the broker allocate victim-02,
	// victim-03 … reserving another family's suffix space in leaseHolder,
	// without ever being able to authenticate as one of those names.
	//
	// auth_callout already binds this connection to `nid`; this makes the
	// register body agree with it instead of overriding it. On a mismatch the
	// presented name is used, which is the pre-feature reading and cannot
	// reach outside the family the agent actually authenticated as.
	if base, _, leased := proto.SplitLeaseName(nid); leased && base == req.ConfiguredNID {
		// Name shape alone is not evidence that this is a lease. A real device
		// may be provisioned as `gpu-02`; letting that device claim configured
		// root `gpu` crosses into a different credential family. When bindings
		// are available, accept the relationship only when the presented name
		// has no binding of its own and the claimed root does.
		// len(provisioned) > 0, NOT the readable flag: this call site has always meant
		// "are there bindings to reason about at all", and E1's contract fix changed what
		// the second return says. Spelled out here so the two sites cannot drift again.
		provisioned, readable := node.ProvisionedNIDs(db, sid)
		if readable && len(provisioned) > 0 && (provisioned[nid] || !provisioned[req.ConfiguredNID]) {
			return nid
		}
		return req.ConfiguredNID
	}
	return nid
}

// TrustedPreviousNID restricts the PreviousNID row-migration hint to the
// credential family proved by the presented/configured-name relationship.
func TrustedPreviousNID(db *sql.DB, sid, nid string, req *proto.NodeRegisterReq) string {
	if req == nil || req.PreviousNID == "" {
		return ""
	}
	base := ConfiguredBasename(db, sid, nid, req)
	// A carry is meaningful only when the current presented name is an
	// assigned member of the proved family. In particular, an independently
	// provisioned `gpu-02` resolves its base to itself and cannot name `gpu` as
	// a process-row source.
	presentedBase, _, presentedLeased := proto.SplitLeaseName(nid)
	if !presentedLeased || presentedBase != base {
		return ""
	}
	if req.PreviousNID == base {
		return req.PreviousNID
	}
	previousBase, _, previousLeased := proto.SplitLeaseName(req.PreviousNID)
	if previousLeased && previousBase == base {
		return req.PreviousNID
	}
	return ""
}
