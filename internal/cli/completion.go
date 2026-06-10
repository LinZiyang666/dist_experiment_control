// Package cli — completion helpers.
//
// Dynamic shell completion for runtime-valued arguments (node ids,
// session ids, expose names). Static completion (command names, flag
// names) is handled by Cobra's generator and needs no code here.
//
// Cobra invokes `tether __complete <verb> <toComplete>` as a fresh
// subprocess per `<TAB>` event, so any in-process cache is intra-event
// only — it deduplicates the within-event repeat call some
// bash-completion scripts emit, but does NOT survive across user tab
// presses. Disk-backed cache is intentionally out of scope (see
// docs/reviews/dynamic-completion-plan.md "Out of Plan").
//
// All helpers share these contracts:
//   - 1 s end-to-end budget via outer context.WithTimeout.
//   - On any failure / no-identity / no-active-session, return
//     (nil, ShellCompDirectiveNoFileComp). Silent zero-candidate is
//     portable across bash/zsh/fish; the Error directive's interaction
//     with `complete -o default` is shell-specific.
//   - Helpers accept a Transport interface so tests can inject fakes
//     without spinning up NATS.

package cli

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
)

// CompletionBudget is the total time budget for one completion event.
// Includes NATS dial + RPC. Dial is further capped at 750 ms by the
// transport options so a slow-dial scenario doesn't starve the RPC.
const CompletionBudget = 1 * time.Second

// completionCacheTTL is the in-process cache window. Only meaningful
// within a single `tether __complete` subprocess: Cobra-generated bash
// completion sometimes triggers the helper twice for the same prefix
// to handle the second column of a candidate, and 5 s comfortably
// covers that without changing cross-event cost.
const completionCacheTTL = 5 * time.Second

// NodeInfo is the minimum projection of a node row that completion
// needs. Mirrors a subset of proto.NodeListEntry but lives in this
// package so the Transport interface doesn't pull internal/proto into
// every test fixture.
type NodeInfo struct {
	NID    string
	Status string // ONLINE | STALE | OFFLINE — completion filters to ONLINE
}

// SessionInfo mirrors the subset of proto.SessionEntry completion needs.
type SessionInfo struct {
	SID     string
	State   string // ACTIVE | DELETING
	IsOwner bool
}

// PortInfo mirrors the subset of proto.PsPortEntry completion needs.
type PortInfo struct {
	Name  string
	NID   string
	State string // ALLOCATED | REVOKED | FREED — completion filters to ALLOCATED
}

// Transport is the broker access layer for completion helpers. Production
// wires this to a fail-fast NATS connection (see completion_transport.go);
// unit tests inject a fake.
//
// Methods take a context.Context so the shared CompletionBudget can be
// propagated. A transport that cannot connect should return an error
// promptly, not block.
//
// Close is called from the cobra completion glue (typically via defer)
// once the helper returns. Implementations must be safe against being
// Close'd twice.
type Transport interface {
	ListNodes(ctx context.Context) ([]NodeInfo, error)
	ListSessions(ctx context.Context) ([]SessionInfo, error)
	ListPorts(ctx context.Context) ([]PortInfo, error)
	Close()
}

// CompletionContext carries the identity-/session-scoped fields that
// distinguish one completion call from another for cache purposes. The
// production NATS transport reads `actorPubKey` from ~/.tether identity
// and `sid` from ~/.tether/current_session (or TETHER_SESSION); a fake
// transport in tests provides both directly.
type CompletionContext struct {
	Home        string // resolved home dir (--home / TETHER_HOME / default)
	NATSURL     string // resolved NATS URL (flag / TETHER_NATS_URL / ~/.tether/broker_url)
	ActorPubKey string // from ReadIdentity(home); empty if no key on disk
	SID         string // active session id; empty if no active session
}

// cacheKey JSON-encodes the (helper, ctx, helperFilters) tuple so a
// single sync.Map can back all completion helpers.
type cacheKey struct {
	Helper        string            `json:"h"`
	NATSURL       string            `json:"u"`
	Home          string            `json:"d"`
	ActorPubKey   string            `json:"a"`
	SID           string            `json:"s"`
	HelperFilters map[string]string `json:"f,omitempty"`
}

type cacheEntry struct {
	candidates []string
	expiresAt  time.Time
}

var completionCache sync.Map // map[string]cacheEntry — key is cacheKey JSON

// cacheGet returns (candidates, true) if a non-expired entry exists.
func cacheGet(k cacheKey) ([]string, bool) {
	raw, err := json.Marshal(k)
	if err != nil {
		return nil, false
	}
	v, ok := completionCache.Load(string(raw))
	if !ok {
		return nil, false
	}
	entry, ok := v.(cacheEntry)
	if !ok || time.Now().After(entry.expiresAt) {
		return nil, false
	}
	return entry.candidates, true
}

// cachePut stores candidates for completionCacheTTL.
func cachePut(k cacheKey, candidates []string) {
	raw, err := json.Marshal(k)
	if err != nil {
		return
	}
	completionCache.Store(string(raw), cacheEntry{
		candidates: candidates,
		expiresAt:  time.Now().Add(completionCacheTTL),
	})
}

// ClearCompletionCacheForTest is exposed for tests that need to assert
// fresh transport calls. Not part of the public API; do not call from
// production code.
func ClearCompletionCacheForTest() {
	completionCache.Range(func(k, _ any) bool {
		completionCache.Delete(k)
		return true
	})
}

// prefixFilter returns the subset of candidates that have toComplete as
// a prefix. toComplete may be empty (everything passes).
func prefixFilter(candidates []string, toComplete string) []string {
	if toComplete == "" {
		out := make([]string, len(candidates))
		copy(out, candidates)
		sort.Strings(out)
		return out
	}
	out := make([]string, 0, len(candidates))
	for _, c := range candidates {
		if strings.HasPrefix(c, toComplete) {
			out = append(out, c)
		}
	}
	sort.Strings(out)
	return out
}

// CompleteOnlineNodes returns ONLINE node nids in the active session
// matching toComplete. Used by exec / run / expose / expose rm /
// node upgrade.
//
// Returns (nil, NoFileComp) when:
//   - no identity on disk (~/.tether/keys/default.nk missing);
//   - no active session;
//   - transport error / budget exhausted (silent fail).
func CompleteOnlineNodes(t Transport, cctx CompletionContext, toComplete string) ([]string, cobra.ShellCompDirective) {
	if cctx.ActorPubKey == "" || cctx.SID == "" {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	key := cacheKey{Helper: "OnlineNodes", NATSURL: cctx.NATSURL, Home: cctx.Home, ActorPubKey: cctx.ActorPubKey, SID: cctx.SID}
	if hit, ok := cacheGet(key); ok {
		return prefixFilter(hit, toComplete), cobra.ShellCompDirectiveNoFileComp
	}

	ctx, cancel := context.WithTimeout(context.Background(), CompletionBudget)
	defer cancel()

	nodes, err := t.ListNodes(ctx)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if n.Status == "ONLINE" {
			out = append(out, n.NID)
		}
	}
	cachePut(key, out)
	return prefixFilter(out, toComplete), cobra.ShellCompDirectiveNoFileComp
}

// CompleteVisibleSessions returns sessions the caller can see (owner OR
// member, State==ACTIVE). Used by `login -s`.
func CompleteVisibleSessions(t Transport, cctx CompletionContext, toComplete string) ([]string, cobra.ShellCompDirective) {
	if cctx.ActorPubKey == "" {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	key := cacheKey{Helper: "VisibleSessions", NATSURL: cctx.NATSURL, Home: cctx.Home, ActorPubKey: cctx.ActorPubKey}
	if hit, ok := cacheGet(key); ok {
		return prefixFilter(hit, toComplete), cobra.ShellCompDirectiveNoFileComp
	}

	ctx, cancel := context.WithTimeout(context.Background(), CompletionBudget)
	defer cancel()

	sess, err := t.ListSessions(ctx)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	out := make([]string, 0, len(sess))
	for _, s := range sess {
		if s.State == "ACTIVE" {
			out = append(out, s.SID)
		}
	}
	cachePut(key, out)
	return prefixFilter(out, toComplete), cobra.ShellCompDirectiveNoFileComp
}

// CompleteOwnedSessions returns sessions where IsOwner==true and
// State==ACTIVE. Used by `session rm` (server-side is owner-only).
func CompleteOwnedSessions(t Transport, cctx CompletionContext, toComplete string) ([]string, cobra.ShellCompDirective) {
	if cctx.ActorPubKey == "" {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	key := cacheKey{Helper: "OwnedSessions", NATSURL: cctx.NATSURL, Home: cctx.Home, ActorPubKey: cctx.ActorPubKey}
	if hit, ok := cacheGet(key); ok {
		return prefixFilter(hit, toComplete), cobra.ShellCompDirectiveNoFileComp
	}

	ctx, cancel := context.WithTimeout(context.Background(), CompletionBudget)
	defer cancel()

	sess, err := t.ListSessions(ctx)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	out := make([]string, 0, len(sess))
	for _, s := range sess {
		if s.IsOwner && s.State == "ACTIVE" {
			out = append(out, s.SID)
		}
	}
	cachePut(key, out)
	return prefixFilter(out, toComplete), cobra.ShellCompDirectiveNoFileComp
}

// CompleteAllocatedExposeNames returns expose names with
// State=="ALLOCATED" in the active session. If nid is non-empty, the
// result is filtered to that node.
//
// NOTE: the nid filter is a UX hint only — broker.handleExposeRmReq
// looks up port allocations by (sid, name) and ignores the typed nid,
// so the actual `expose rm` will still succeed against a name on a
// different node. Hiding such names in completion narrows the candidate
// set to "names you typed the right nid for", which matches the
// operator's intent in the typical case. See
// docs/reviews/dynamic-completion-plan.md concern #6.
func CompleteAllocatedExposeNames(t Transport, cctx CompletionContext, nid, toComplete string) ([]string, cobra.ShellCompDirective) {
	if cctx.ActorPubKey == "" || cctx.SID == "" {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	key := cacheKey{
		Helper: "AllocatedExposeNames", NATSURL: cctx.NATSURL, Home: cctx.Home,
		ActorPubKey: cctx.ActorPubKey, SID: cctx.SID,
		HelperFilters: map[string]string{"nid": nid},
	}
	if hit, ok := cacheGet(key); ok {
		return prefixFilter(hit, toComplete), cobra.ShellCompDirectiveNoFileComp
	}

	ctx, cancel := context.WithTimeout(context.Background(), CompletionBudget)
	defer cancel()

	ports, err := t.ListPorts(ctx)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	out := make([]string, 0, len(ports))
	for _, p := range ports {
		if p.State != "ALLOCATED" {
			continue
		}
		if nid != "" && p.NID != nid {
			continue
		}
		out = append(out, p.Name)
	}
	cachePut(key, out)
	return prefixFilter(out, toComplete), cobra.ShellCompDirectiveNoFileComp
}

// CompleteAdminSessions reads sessions via the broker's admin socket.
// Used by `tether admin audit <TAB>` and `tether admin evict <TAB>`
// (first positional). No NATS round-trip; the socket is broker-local.
func CompleteAdminSessions(socketPath, toComplete string) ([]string, cobra.ShellCompDirective) {
	if socketPath == "" {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	key := cacheKey{Helper: "AdminSessions", NATSURL: socketPath}
	if hit, ok := cacheGet(key); ok {
		return prefixFilter(hit, toComplete), cobra.ShellCompDirectiveNoFileComp
	}

	sess, err := adminListSessions(socketPath)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	cachePut(key, sess)
	return prefixFilter(sess, toComplete), cobra.ShellCompDirectiveNoFileComp
}

// CompleteAdminNodes reads nodes via the broker's admin socket. If sid
// is non-empty (i.e. `admin evict <sid>` already picked the first
// positional), results are filtered to that session.
func CompleteAdminNodes(socketPath, sid, toComplete string) ([]string, cobra.ShellCompDirective) {
	if socketPath == "" {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	key := cacheKey{
		Helper: "AdminNodes", NATSURL: socketPath, SID: sid,
	}
	if hit, ok := cacheGet(key); ok {
		return prefixFilter(hit, toComplete), cobra.ShellCompDirectiveNoFileComp
	}

	nids, err := adminListNodes(socketPath, sid)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	cachePut(key, nids)
	return prefixFilter(nids, toComplete), cobra.ShellCompDirectiveNoFileComp
}
