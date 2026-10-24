// Package infracli owns infrastructure command validation, transport and presentation.
// The CLI never enters the broker/agent image planner for an infra component.
// origin: hot-update S9 plan §7.1 / T52
package infracli

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

const Budget = 120 * time.Second

type Options struct {
	Component, Status, Recover, ConfigSHA256 string
	ToVersion, ExpectSHA256, Resume          string
	Rollback, ReloadConfig                   bool
	BrokerOnly, AckWriteFence, BackupTaken   bool
	DryRun                                   bool
}

// Selected also catches orphan infra flags. They must not silently start a
// whole-host roll just because the component selector was omitted.
func Selected(o Options) bool {
	return o.Component != "" || o.Status != "" || o.Recover != "" || o.Rollback || o.ReloadConfig || o.ConfigSHA256 != ""
}

func Flags(cmd *cobra.Command, o *Options, query bool) {
	f := cmd.Flags()
	f.StringVar(&o.Component, "component", "", "infrastructure component: nats or caddy; empty preserves the whole-host upgrade")
	f.BoolVar(&o.Rollback, "rollback", false, "restore each host's actual compatible previous component")
	f.BoolVar(&o.ReloadConfig, "reload-config", false, "reload the validated staged Caddy configuration in the same process")
	f.StringVar(&o.ConfigSHA256, "config-sha256", "", "SHA256 of the validated managed staged Caddy configuration")
	if query {
		f.StringVar(&o.Status, "status", "", "query the original infra transaction ID as infracontract.Status JSON")
		f.StringVar(&o.Recover, "recover", "", "recover the original infra transaction ID from its durable journal")
	}
}

// Validate runs before session, identity, network or legacy plan work. The
// wire has a closed grammar too, but checking here keeps invalid CLI requests
// from creating a new transaction or consuming a broker-only acknowledgement.
// origin: hot-update S9 plan §7.1 / T52 / M26
func Validate(o Options) error {
	if o.Component != "nats" && o.Component != "caddy" {
		return Failure(proto.CodeInfraUnsupported, "choose --component nats or caddy", "", "")
	}
	if o.BrokerOnly || o.AckWriteFence || o.BackupTaken || o.Resume != "" {
		return Failure(proto.CodeInfraConfigInvalid, "infra excludes broker-only, ack-writefence, backup-taken and resume", "", "")
	}
	if o.Status != "" || o.Recover != "" {
		if o.Status != "" && o.Recover != "" || o.ToVersion != "" || o.ExpectSHA256 != "" || o.Rollback || o.ReloadConfig || o.ConfigSHA256 != "" || o.DryRun {
			return Failure(proto.CodeInfraConfigInvalid, "status/recover accepts only the original transaction and component", "", "")
		}
		tx := o.Status
		if tx == "" {
			tx = o.Recover
		}
		if !hexValue(tx, 16) {
			return Failure(proto.CodeInfraConfigInvalid, "transaction ID must be 32 nonzero lowercase hex digits", "", "")
		}
		return nil
	}
	r := command(o, "01000000000000000000000000000000")
	if err := proto.ValidateInfraCommand(r); err != nil {
		return Failure(proto.InfraCode(err), "upgrade requires to-version and binary expect-sha256; rollback excludes targets; reload requires Caddy and config-sha256", "", "")
	}
	if o.Rollback && o.ReloadConfig {
		return Failure(proto.CodeInfraConfigInvalid, "rollback and reload-config are mutually exclusive", "", "")
	}
	return nil
}

func hexValue(s string, size int) bool {
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != size || hex.EncodeToString(b) != s {
		return false
	}
	for _, v := range b {
		if v != 0 {
			return true
		}
	}
	return false
}

func command(o Options, tx string) proto.InfraCommand {
	r := proto.InfraCommand{Version: proto.InfraRequestVersion, Purpose: proto.InfraRequestPurpose, RemainingNS: uint64(Budget), Op: "start", Component: o.Component, Action: "upgrade", TxID: tx, ToVersion: o.ToVersion, ExpectSHA256: o.ExpectSHA256, ConfigSHA256: o.ConfigSHA256}
	if o.Rollback {
		r.Action = "rollback"
	}
	if o.ReloadConfig {
		r.Action = "reload"
	}
	if o.Status != "" || o.Recover != "" {
		r.Op, r.Component, r.Action = "status", "", ""
		r.TxID = o.Status
		if o.Recover != "" {
			r.Op, r.TxID = "recover", o.Recover
		}
	}
	return r
}

type Call func(context.Context, proto.InfraCommand) (*proto.InfraUpgradeResp, error)

// Execute uses one stable transaction and one absolute budget across probe and
// effect. A transport failure after Start/Recover is unknown, never a retry of
// Start and never permission to fall back to the legacy reexec subject.
// origin: hot-update S9 plan §7.1 / §7.3 / T53
func Execute(ctx context.Context, out io.Writer, o Options, call Call) error {
	if err := Validate(o); err != nil {
		return err
	}
	if call == nil {
		return Failure(proto.CodeInfraUnsupported, "infra transport is unavailable", "", "")
	}
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return Failure(proto.CodeInfraInternal, "transaction identity unavailable", "", "")
	}
	r := command(o, hex.EncodeToString(id[:]))
	if r.Op == "start" {
		probe := proto.InfraCommand{Version: r.Version, Purpose: r.Purpose, Op: "probe", Component: o.Component, RemainingNS: r.RemainingNS}
		resp, err := call(ctx, probe)
		if err != nil {
			return err
		}
		if err := capability(resp, o.Component); err != nil {
			return err
		}
		if o.DryRun {
			_, err := fmt.Fprintf(out, "infra %s %s (dry-run; no effect)\n", o.Component, r.Action)
			return err
		}
	}
	if err := remaining(ctx, &r); err != nil {
		return err
	}
	resp, err := call(ctx, r)
	if err != nil {
		if r.Op == "status" {
			return err
		}
		return Failure(proto.CodeInfraResultUnknown, "request sent; query the original transaction before recovery", r.TxID, "unknown")
	}
	return Render(out, r, resp)
}

func remaining(ctx context.Context, r *proto.InfraCommand) error {
	deadline, ok := ctx.Deadline()
	left := time.Until(deadline)
	if !ok || ctx.Err() != nil || left <= 0 {
		return Failure(proto.CodeInfraBudgetExceeded, "original command budget exhausted", r.TxID, "unknown")
	}
	r.RemainingNS = uint64(min(left, Budget))
	return nil
}

func capability(r *proto.InfraUpgradeResp, component string) error {
	if r == nil {
		return Failure(proto.CodeInfraInternal, "missing infra probe reply", "", "")
	}
	if r.Code != "" {
		return Failure(r.Code, "infra capability probe refused", "", "")
	}
	c := r.Capability
	if !r.OK || c == nil || c.Version != proto.InfraRequestVersion || (component == "nats" && !c.NATS) || (component == "caddy" && !c.Caddy) {
		return Failure(proto.CodeInfraUnsupported, "target does not advertise this infra capability", "", "")
	}
	return nil
}

type AdminPorts struct {
	Call func(context.Context, string, adminsock.Request) (*adminsock.Response, error)
	Exit func(error) error
}

// CallAdmin retains the component's 120s budget instead of the legacy
// reload client's 5s read cap. Cancellation closes the socket and joins its
// callback before returning; no read worker outlives the command.
// origin: hot-update S9 plan §7.1 / T53
func CallAdmin(ctx context.Context, path string, req adminsock.Request) (*adminsock.Response, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		// The socket's UID boundary is an authorization decision. Retrying a
		// denied dial cannot grant this caller permission to manage the host.
		// origin: hot-update S9 plan §7.3 / T53
		if errors.Is(err, os.ErrPermission) {
			return nil, Failure(proto.CodeInfraPermissionDenied, "local admin socket denied this identity", "", "")
		}
		return nil, &ExitError{Class: 69, Err: fmt.Errorf("admin socket unavailable: %w", err)}
	}
	defer func() { _ = conn.Close() }()
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, Failure(proto.CodeInfraBudgetExceeded, "admin call has no deadline", "", "")
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	joined := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = conn.Close(); close(joined) })
	defer func() {
		if !stop() {
			<-joined
		}
	}()
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return nil, &ExitError{Class: 69, Err: fmt.Errorf("admin socket write: %w", err)}
	}
	line, err := bufio.NewReaderSize(conn, proto.InfraMaxRequestBytes).ReadSlice('\n')
	if err != nil {
		return nil, &ExitError{Class: 69, Err: fmt.Errorf("admin socket read: %w", err)}
	}
	var resp adminsock.Response
	if err := json.Unmarshal(line, &resp); err != nil {
		return nil, Failure(proto.CodeInfraInternal, "malformed admin infra reply", "", "")
	}
	return &resp, nil
}

// The local surface deliberately needs no session or cluster init. Unix socket
// permissions authorize management; root policy still authorizes each effect.
func AdminCommand(socket func() string, p AdminPorts) *cobra.Command {
	root := &cobra.Command{Use: "infra", Short: "Manage local NATS/Caddy infrastructure", Long: BootstrapGuide}
	for _, verb := range []string{"upgrade", "status", "recover"} {
		var o Options
		var tx string
		cmd := &cobra.Command{Use: verb, Short: verb + " a local infrastructure transaction", Args: cobra.NoArgs}
		cmd.RunE = func(cmd *cobra.Command, _ []string) error {
			if verb != "upgrade" {
				// Component is not present in the frozen status wire command. It
				// is a local validator selector; the journal owns the real component.
				o.Component = "nats"
				if verb == "status" {
					o.Status = tx
				} else {
					o.Recover = tx
				}
				if tx == "" {
					return p.Exit(Failure(proto.CodeInfraConfigInvalid, "--tx is required", "", ""))
				}
			}
			return p.Exit(Execute(cmd.Context(), cmd.OutOrStdout(), o, func(ctx context.Context, r proto.InfraCommand) (*proto.InfraUpgradeResp, error) {
				op := map[string]string{"probe": adminsock.OpInfraProbe, "start": adminsock.OpInfraUpgrade, "status": adminsock.OpInfraStatus, "recover": adminsock.OpInfraRecover}[r.Op]
				resp, err := p.Call(ctx, socket(), adminsock.Request{Op: op, Infra: &r})
				if err != nil {
					return nil, err
				}
				if resp == nil {
					return nil, Failure(proto.CodeInfraInternal, "missing admin reply", "", "")
				}
				if resp.Infra == nil {
					code := resp.Code
					if code == "" {
						code = proto.CodeInfraUnsupported
					}
					return &proto.InfraUpgradeResp{Code: code}, nil
				}
				return resp.Infra, nil
			}))
		}
		if verb == "upgrade" {
			Flags(cmd, &o, false)
			cmd.Flags().StringVar(&o.ToVersion, "to-version", "", "catalog version of the selected component")
			cmd.Flags().StringVar(&o.ExpectSHA256, "expect-sha256", "", "binary SHA256 from the component catalog")
		} else {
			cmd.Flags().StringVar(&tx, "tx", "", "original transaction ID (32 lowercase hex digits)")
		}
		root.AddCommand(cmd)
	}
	root.AddCommand(&cobra.Command{Use: "rotate-policy", Short: "Rotate the installed infra policy after credential rotation", Long: RotationGuide, Args: cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			if os.Geteuid() != 0 {
				return p.Exit(Failure(proto.CodeInfraPermissionDenied, "policy rotation requires local root", "", ""))
			}
			// No policy verb exists in the frozen public codec. Do not mint a
			// HostSupport Plan or bypass the root-only policy CAS to simulate it.
			return p.Exit(Failure(proto.CodeInfraUnsupported, "installed management protocol has no policy rotation binding; retain pending transactions and complete the root policy binding", "", ""))
		}})
	return root
}

type ClusterPorts struct {
	Connect     func(*cobra.Command, string, string, string, *cli.Identity, nats.Option) (*nats.Conn, error)
	Health      func(*nats.Conn, string) ([]proto.ClusterHealthResp, error)
	Manifest    func(context.Context, *nats.Conn, string) *proto.ClusterManifest
	Unavailable func(string, ...any) error
}

// RunCluster is selected before the old planner's target, backup and N2
// write-fence checks. Each targeted request uses the separate signed subject.
func RunCluster(cmd *cobra.Command, o Options, home, url, seedPath string, p ClusterPorts) error {
	if (cmd.Flags().Changed("status") && o.Status == "") || (cmd.Flags().Changed("recover") && o.Recover == "") {
		return Failure(proto.CodeInfraConfigInvalid, "status/recover requires a transaction ID", "", "")
	}
	if err := Validate(o); err != nil {
		return err
	}
	if seedPath == "" {
		return Failure(proto.CodeInfraConfigInvalid, "--account-seed is required for signed infra management", "", "")
	}
	seed, err := os.ReadFile(seedPath)
	if err != nil {
		return p.Unavailable("read account seed: %v", err)
	}
	defer clear(seed)
	accountPub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		return Failure(proto.CodeInfraPermissionDenied, "invalid account signing identity", "", "")
	}
	sid := cli.ReadCurrentSession(home)
	if sid == "" {
		return Failure(proto.CodeInfraConfigInvalid, "no active session; run tether login first", "", "")
	}
	id, err := cli.EnsureIdentity(home)
	if err != nil {
		return err
	}
	url = cli.ResolveNATSURLFromHome(url, cmd.Flags().Changed("nats-url"), home)
	nc, err := p.Connect(cmd, "cluster infra", home, url, id, nats.Name(cli.CtlNameForSession(sid)))
	if err != nil {
		return err
	}
	defer nc.Close()
	health, err := p.Health(nc, id.PublicKey)
	if err != nil {
		return err
	}
	if len(health) == 0 {
		return Failure(proto.CodeInfraUnsupported, "no infrastructure owner answered", "", "")
	}
	// A broadcast responder subset cannot define the upgrade roster. Missing
	// configured owners must refuse before any component effect.
	if p.Manifest == nil {
		return Failure(proto.CodeInfraInventoryIncomplete, "signed owner roster is unavailable", "", "")
	}
	manifest := p.Manifest(cmd.Context(), nc, id.PublicKey)
	if manifest == nil || manifest.Roster == nil || clusterroster.VerifyAt(manifest.Roster, accountPub, time.Now()) != nil {
		return Failure(proto.CodeInfraInventoryIncomplete, "current signed owner roster is unavailable", "", "")
	}
	expected := map[string]bool{}
	for _, b := range manifest.Roster.Brokers {
		if b.Phase == proto.RosterPhaseVoter {
			expected[b.NodeID] = true
		}
	}
	if len(expected) == 0 {
		return Failure(proto.CodeInfraInventoryIncomplete, "signed owner roster has no voters", "", "")
	}
	// Stable followers-first order is independent of the broker/agent planner.
	sort.Slice(health, func(i, j int) bool {
		if health[i].WritableLeaderConfirmed != health[j].WritableLeaderConfirmed {
			return !health[i].WritableLeaderConfirmed
		}
		return health[i].NodeID < health[j].NodeID
	})
	seen := map[string]bool{}
	for _, h := range health {
		if proto.ValidateClusterNodeID(h.NodeID) != nil || seen[h.NodeID] || !expected[h.NodeID] {
			return Failure(proto.CodeInfraInventoryIncomplete, "ambiguous infrastructure owner census", "", "")
		}
		seen[h.NodeID] = true
		if o.Status == "" && o.Recover == "" {
			if err := capability(&proto.InfraUpgradeResp{OK: true, Capability: h.InfraCapability}, o.Component); err != nil {
				return err
			}
		}
	}
	if len(seen) != len(expected) {
		return Failure(proto.CodeInfraInventoryIncomplete, "configured owner did not answer", "", "")
	}
	if o.Status != "" || o.Recover != "" {
		return queryCluster(cmd, nc, id.PublicKey, seed, health, o, p)
	}
	// Probe every owner before the first effect; a partial capability rollout
	// cannot silently upgrade the first node and fail on an old peer later.
	for _, h := range health {
		r := proto.InfraCommand{Version: proto.InfraRequestVersion, Purpose: proto.InfraRequestPurpose, Op: "probe", Component: o.Component, RemainingNS: uint64(Budget)}
		resp, err := Remote(cmd.Context(), nc, id.PublicKey, seed, h.NodeID, r, p.Unavailable)
		if err != nil {
			return err
		}
		if err := capability(resp, o.Component); err != nil {
			return err
		}
	}
	var completed bytes.Buffer
	for _, h := range health {
		call := func(ctx context.Context, r proto.InfraCommand) (*proto.InfraUpgradeResp, error) {
			return Remote(ctx, nc, id.PublicKey, seed, h.NodeID, r, p.Unavailable)
		}
		if err := Execute(cmd.Context(), &completed, o, call); err != nil {
			return err
		}
	}
	_, err = io.Copy(cmd.OutOrStdout(), &completed)
	return err
}

func queryCluster(cmd *cobra.Command, nc *nats.Conn, actor string, seed []byte, health []proto.ClusterHealthResp, o Options, p ClusterPorts) error {
	tx := o.Status
	if tx == "" {
		tx = o.Recover
	}
	var owner string
	var found *proto.InfraUpgradeResp
	query := command(Options{Status: tx}, tx)
	for _, h := range health {
		resp, err := Remote(cmd.Context(), nc, actor, seed, h.NodeID, query, p.Unavailable)
		if err != nil {
			return err
		}
		if resp != nil && resp.Report != nil && resp.Report.TxID == tx && resp.Report.Phase != "unknown" {
			if found != nil {
				return Failure(proto.CodeInfraInventoryIncomplete, "transaction has multiple owners", tx, "unknown")
			}
			owner, found = h.NodeID, resp
		} else if resp != nil && resp.Code != "" && resp.Code != proto.CodeInfraResultUnknown {
			return Failure(resp.Code, "transaction query refused", tx, "unknown")
		}
	}
	if found == nil {
		found = &proto.InfraUpgradeResp{OK: true, Report: &proto.InfraReport{TxID: tx, Phase: "unknown", Code: proto.CodeInfraResultUnknown}}
	}
	if o.Status != "" {
		return Render(cmd.OutOrStdout(), query, found)
	}
	if owner == "" {
		return Failure(proto.CodeInfraResultUnknown, "original owner is unknown; query before recovery", tx, "unknown")
	}
	return Execute(cmd.Context(), cmd.OutOrStdout(), o, func(ctx context.Context, r proto.InfraCommand) (*proto.InfraUpgradeResp, error) {
		return Remote(ctx, nc, actor, seed, owner, r, p.Unavailable)
	})
}

func Remote(ctx context.Context, nc *nats.Conn, actor string, seed []byte, node string, r proto.InfraCommand, unavailable func(string, ...any) error) (*proto.InfraUpgradeResp, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(r.RemainingNS))
	defer cancel()
	if err := remaining(ctx, &r); err != nil {
		return nil, err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, Failure(proto.CodeInfraInternal, "nonce unavailable", r.TxID, "unknown")
	}
	req := proto.InfraUpgradeReq{Request: r, TargetNode: node, Actor: actor, IssuedAt: time.Now().UTC().Format(time.RFC3339Nano), Nonce: hex.EncodeToString(nonce[:])}
	data, err := proto.InfraUpgradeSigningBytes(req)
	if err != nil {
		return nil, Failure(proto.InfraCode(err), "invalid infra request", r.TxID, "unknown")
	}
	sig, err := auth.SignWithSeed(seed, data)
	if err != nil {
		return nil, Failure(proto.CodeInfraPermissionDenied, "cannot sign infra request", r.TxID, "unknown")
	}
	req.Sig = hex.EncodeToString(sig)
	body, err := proto.EncodeInfraUpgradeRequest(req)
	if err != nil {
		return nil, Failure(proto.InfraCode(err), "cannot encode infra request", r.TxID, "unknown")
	}
	msg, err := nc.RequestWithContext(ctx, proto.SubjClusterInfra(node), body)
	if err != nil {
		return nil, unavailable("infra request to %s: %w", node, err)
	}
	var resp proto.InfraUpgradeResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, Failure(proto.CodeInfraInternal, "malformed infra reply", r.TxID, "unknown")
	}
	return &resp, nil
}

// Keep the legacy signing contract byte-identical while freeing cmd space for
// component dispatch. Infra requests never call this function.
func SignUpgradeTrigger(seed []byte, req *proto.ClusterUpgradeReq, now time.Time) ([]byte, error) {
	if req == nil {
		return nil, fmt.Errorf("missing upgrade request")
	}
	req.IssuedAt = now.UTC().Format(time.RFC3339)
	// origin: hot-update S6 plan §9.1 / T46
	// CanonicalUpgradeReqBytes uses nil as an invalid-input sentinel.
	if err := proto.ValidateUpgradeReq(req); err != nil {
		return nil, err
	}
	sig, err := auth.SignWithSeed(seed, proto.CanonicalUpgradeReqBytes(req))
	if err != nil {
		return nil, err
	}
	req.Sig = hex.EncodeToString(sig)
	return json.Marshal(req)
}
