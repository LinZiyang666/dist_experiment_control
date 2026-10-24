//go:build linux && infrabusiness_integration

package infrabusiness_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infraartifact"
	i "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infratransfer"
	"github.com/LinZiyang666/tether/internal/infratransport"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/test/infraharness"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"golang.org/x/sys/unix"
)

func deadline(t testing.TB) i.Deadline {
	t.Helper()
	d, err := controlbudget.After(upgradebudget.RequestLatency)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func digest(data []byte) string {
	d := sha256.Sum256(data)
	return hex.EncodeToString(d[:])
}

func validDigest(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == sha256.Size && !bytes.Equal(b, make([]byte, sha256.Size)) && strings.ToLower(s) == s
}

func readJSON(path string, dst any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(b) > 4<<20 {
		return errors.New("business receipt exceeds custody bound")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("business receipt has trailing data")
	}
	// Canonical records also reject duplicate keys: decoding alone would let a
	// later value erase the original redial or lost-slot evidence.
	canonical, err := json.Marshal(dst)
	var compact bytes.Buffer
	if err != nil || json.Compact(&compact, b) != nil || !bytes.Equal(canonical, compact.Bytes()) {
		return errors.New("business receipt is not canonical")
	}
	return nil
}

// runLive consumes pre-supplied artifacts only. The P32 producer must start the
// original cohort before the first cut and drive real product commands. Its
// CLI/receipt consumer contract is listed in the P30 handoff to P32; it is not a
// change to any P00 signature. Every child is under the shared L22 supervisor.
// origin: hot-update S9 plan §8 / §9.1 / §9.5 / T68–T71
func runLive(t *testing.T, scenario string) businessReceipt {
	t.Helper()
	path := os.Getenv("TETHER_INFRA_BUSINESS_SUPPLY")
	if path == "" {
		t.Fatal("NOT_RUN pending P22/P23/P26/P32: TETHER_INFRA_BUSINESS_SUPPLY must name a verified private live fixture manifest")
	}
	var supply fixtureSupply
	if err := readJSON(path, &supply); err != nil {
		t.Fatal("invalid business fixture supply: ", err)
	}
	schema := infratransfer.ObservationSchema()
	if supply.Version != 1 || !validDigest(supply.SourceSHA256) || supply.SchemaSHA256 != hex.EncodeToString(schema[:]) ||
		!filepath.IsAbs(supply.Root) || !filepath.IsAbs(supply.Workload) {
		t.Fatal("business fixture source/schema/private root is not bound")
	}
	private, err := filepath.Rel(os.TempDir(), filepath.Clean(supply.Root))
	if err != nil || private == "." || private == ".." || strings.HasPrefix(private, ".."+string(filepath.Separator)) {
		t.Fatal("live supply must be a private temporary tree")
	}
	required := map[string]bool{"tether": false, "nats-2.14.5": false, "nats-2.14.6": false,
		"caddy-2.7.5": false, "caddy-2.7.6": false, "workload": false}
	for _, artifact := range supply.Artifacts {
		if _, exists := required[artifact.Kind]; !exists || required[artifact.Kind] {
			t.Fatal("unexpected or duplicate business fixture artifact")
		}
		sha, err := hex.DecodeString(artifact.SHA256)
		if err != nil || !validDigest(artifact.SHA256) {
			t.Fatal("invalid business fixture artifact digest")
		}
		if artifact.Kind == "tether" && !artifact.Race {
			t.Fatal("inner product binary must carry race instrumentation")
		}
		if strings.HasPrefix(artifact.Kind, "nats-") || strings.HasPrefix(artifact.Kind, "caddy-") {
			catalog, err := infraartifact.New(infraartifact.Config{})
			if err != nil {
				t.Fatal(err)
			}
			kind, version := i.NATSBinary, strings.TrimPrefix(artifact.Kind, "nats-")
			if strings.HasPrefix(artifact.Kind, "caddy-") {
				kind, version = i.CaddyBinary, strings.TrimPrefix(artifact.Kind, "caddy-")
			}
			allowed, err := catalog.Resolve(kind, version, "linux", "amd64")
			if err != nil || artifact.Version != version || artifact.SHA256 != hex.EncodeToString(allowed.BinarySHA256[:]) {
				t.Fatal("stock artifact differs from the frozen official catalog")
			}
		}
		if artifact.Kind == "workload" && artifact.Path != supply.Workload {
			t.Fatal("workload executable is outside artifact custody")
		}
		if err := infraharness.VerifyArtifact(supply.Root, artifact.Path, i.Digest(sha), artifact.Race); err != nil {
			t.Fatal("unverified private fixture artifact: ", err)
		}
		required[artifact.Kind] = true
	}
	for _, present := range required {
		if !present {
			t.Fatal("NOT_RUN pending P26/P32: incomplete business fixture artifact set")
		}
	}
	evidence := t.TempDir()
	// A live original transfer may need its fixed startup pacing to finish.
	// Keep this one producer below the 600s matrix unit instead of imposing
	// the 180s performance target as an accidental acceptance deadline.
	ctx, cancel := context.WithTimeout(t.Context(), 4*upgradebudget.ComponentHandover)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/usr/bin/python3", "-B", supply.Workload,
		"--scenario", scenario, "--supply", path, "--evidence", evidence)
	cmd.Dir = evidence
	// All product homes must be below this private root; an inherited production
	// home or handoff environment must never select the installed broker.
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "HOME=") && !strings.HasPrefix(entry, "TETHER_") && !strings.HasPrefix(entry, "_TETHER_") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, "HOME="+evidence, "TETHER_HOME="+evidence, "PYTHONDONTWRITEBYTECODE=1")
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	process, err := infraharness.StartProcess(cmd)
	if err != nil {
		t.Fatal("start supervised business fixture: ", err)
	}
	t.Cleanup(func() { _ = process.Stop() })
	if err := process.Wait(); err != nil {
		// Fixture output may contain private tuples. Keep raw evidence in its
		// private directory, and report only a digest/native failure here.
		t.Fatalf("live business fixture failed: %v output_sha256=%s", err, digest(output.Bytes()))
	}
	var receipt businessReceipt
	if err := readJSON(filepath.Join(evidence, "business-receipt.json"), &receipt); err != nil {
		t.Fatal("missing or invalid live business receipt: ", err)
	}
	if receipt.Scenario != scenario || receipt.SourceSHA256 != supply.SourceSHA256 || receipt.SchemaSHA256 != supply.SchemaSHA256 {
		t.Fatal("live receipt lost its scenario/source/schema custody")
	}
	for _, hop := range receipt.Hops {
		kind := "nats-2.14.6"
		if hop.Component == "caddy" {
			kind = "caddy-2.7.6"
		}
		if hop.Action == "rollback" {
			kind = "nats-2.14.5"
			if hop.Component == "caddy" {
				kind = "caddy-2.7.5"
			}
		}
		var matched bool
		for _, artifact := range supply.Artifacts {
			if artifact.Kind == kind && artifact.SHA256 == hex.EncodeToString(hop.Current.ExeSHA256[:]) {
				matched = true
			}
		}
		if !matched {
			t.Fatal("independently observed running bytes differ from the supplied stock target")
		}
	}
	return receipt
}

// origin: hot-update S9 plan §9.1 / T68
func TestInfraOriginalMixedCohort(t *testing.T) {
	t.Run("oracle_controls", exerciseBusinessOracle)
	t.Run("live", func(t *testing.T) {
		r := runLive(t, "mixed")
		if err := checkMixed(r); err != nil {
			t.Fatal(err)
		}
		if err := checkAddresses(r.Addresses); err != nil {
			t.Fatal(err)
		}
		if err := checkTransfers(r); err != nil {
			t.Fatal(err)
		}
		if err := checkAuth(r); err != nil {
			t.Fatal(err)
		}
	})
}

// origin: hot-update S9 plan §9.1 / T68 / M24 / M25
func checkMixed(r businessReceipt) error {
	if r.Version != businessReceiptVersion || !reflect.DeepEqual(r.DependencyOrder, []string{"agent", "broker", "nats", "caddy"}) ||
		!reflect.DeepEqual(r.Executed, []string{"nats", "caddy"}) {
		return errors.New("four-layer dependency selection or infra-only execution is incomplete")
	}
	roles := map[string]int{"ssh": 1, "tcp": 4, "ss-alice": 1, "ss-bob": 1, "pty": 1, "exec": 1, "transfer": 4}
	identities := make(map[string]businessIdentity)
	for _, original := range r.Required {
		if original.ID == "" || original.Nonce == "" || original.PID <= 0 || original.StartTicks == 0 || roles[original.Role] == 0 {
			return errors.New("missing original required business identity")
		}
		if _, exists := identities[original.ID]; exists {
			return errors.New("duplicate original business identity")
		}
		if original.Role == "transfer" {
			if original.TransferID == "" || original.NUID == "" || original.CTLPID <= 0 || original.CTLStartTicks == 0 || original.Size <= 0 || original.RateLimitBPS <= 0 || original.StartedNS <= 0 || original.DeadlineNS <= original.StartedNS {
				return errors.New("transfer invocation is not original")
			}
		} else if original.Role == "pty" || original.Role == "exec" {
			if original.TaskID == "" || original.CTLPID <= 0 || original.CTLStartTicks == 0 {
				return errors.New("task or ctl identity is missing")
			}
		} else if original.Inode == 0 || original.Cookie == 0 {
			return errors.New("original business socket has no kernel identity")
		}
		identities[original.ID] = original
		roles[original.Role]--
	}
	for _, count := range roles {
		if count != 0 {
			return errors.New("original required set is incomplete")
		}
	}
	seen := make(map[string]bool)
	for _, stream := range r.Streams {
		original, present := identities[stream.Original.ID]
		if !present || original.Role == "transfer" || seen[original.ID] || original != stream.Original || original != stream.Current {
			return errors.New("business socket/child/invocation was replaced")
		}
		seen[original.ID] = true
		if stream.Accepts != 1 || stream.Redials != 0 || stream.CarrierTransition != "unchanged" {
			return errors.New("business accept/redial/carrier custody changed")
		}
		if stream.UpBytes == 0 || stream.DownBytes == 0 || !validDigest(stream.UpSent) || !validDigest(stream.DownSent) ||
			stream.UpSent != stream.UpRead || stream.DownSent != stream.DownRead || stream.MaxSilenceNS < 0 ||
			stream.MaxSilenceNS > int64(upgradebudget.LiveFlowStall) {
			return errors.New("business byte stream has loss, duplication or excessive silence")
		}
		if original.Role == "ss-alice" {
			if !stream.Revoked {
				return errors.New("independent SS victim was not revoked")
			}
		} else if stream.Revoked || !stream.FINUp || !stream.FINDown {
			return errors.New("surviving original stream was revoked or lost its FIN")
		}
		if original.Role == "exec" && stream.NativeRC != 23 || original.Role == "pty" && stream.NativeRC != 0 {
			return errors.New("original child/ctl native result changed")
		}
		if original.Role == "pty" && !reflect.DeepEqual(stream.Controls, []string{"input", "output", "resize", "signal", "keepalive", "ps", "kill-victim"}) {
			return errors.New("PTY control and independent victim effects are incomplete")
		}
	}
	if len(seen) != len(identities)-4 {
		return errors.New("a required original stream disappeared")
	}
	if err := checkHops(r); err != nil {
		return err
	}
	if err := checkAttachments(r); err != nil {
		return err
	}
	return checkSlots(r)
}

func completeProcess(p i.Process) bool {
	return p.BootID != "" && p.PID > 0 && p.StartTicks > 0 && p.InvocationID != "" &&
		p.ExeDevice > 0 && p.ExeInode > 0 && p.ExeSHA256 != (i.Digest{}) && p.Cgroup != ""
}

// origin: hot-update S9 plan §2.3 / §9.1 / T68
func checkHops(r businessReceipt) error {
	if len(r.Hosts) != 1 && len(r.Hosts) != 3 || len(r.Hops) != len(r.Hosts)*3*len(r.Executed) {
		return errors.New("adjacent cycle does not cover every host and direction")
	}
	seenHosts := make(map[string]bool)
	for _, host := range r.Hosts {
		if host == "" || seenHosts[host] {
			return errors.New("duplicate or missing cycle host")
		}
		seenHosts[host] = true
	}
	current := make(map[string]i.Process)
	initial, upgraded := make(map[string]i.Digest), make(map[string]i.Digest)
	for n, hop := range r.Hops {
		component := r.Executed[n/(len(r.Hosts)*3)]
		action := []string{"upgrade", "rollback", "upgrade"}[(n/len(r.Hosts))%3]
		if hop.Step != uint32(n+1) || hop.Component != component || hop.Action != action || hop.Host != r.Hosts[n%len(r.Hosts)] ||
			!completeProcess(hop.Before) || !completeProcess(hop.Current) || hop.Before == hop.Current ||
			!hop.OldExited || !hop.TemporaryExited || hop.LiveBusiness == 0 || hop.OldOwnerPending != 0 || !validDigest(hop.Evidence) ||
			hop.StartNS < 0 || hop.CutNS < hop.StartNS || hop.EndNS <= hop.CutNS || hop.EndNS-hop.StartNS > int64(upgradebudget.ComponentHandover) {
			return errors.New("hop lacks actual new runtime or retirement while business is live")
		}
		key := component + "/" + hop.Host
		if previous, exists := current[key]; exists && previous != hop.Before {
			return errors.New("cycle did not use the previous hop's current incarnation")
		}
		switch (n / len(r.Hosts)) % 3 {
		case 0:
			if hop.Before.ExeSHA256 == hop.Current.ExeSHA256 {
				return errors.New("upgrade changed no running binary bytes")
			}
			initial[key], upgraded[key] = hop.Before.ExeSHA256, hop.Current.ExeSHA256
		case 1:
			if hop.Current.ExeSHA256 != initial[key] {
				return errors.New("rollback did not run the actual previous SHA")
			}
		case 2:
			if hop.Current.ExeSHA256 != upgraded[key] {
				return errors.New("re-upgrade did not return to the new pin")
			}
		}
		current[key] = hop.Current
	}
	return nil
}

// origin: hot-update S9 plan §9.1 / T68 / M30
func checkSlots(r businessReceipt) error {
	const interval = int64(time.Second) / upgradebudget.ProbeRatePerSecond
	if r.WindowNS <= 0 || r.WindowNS%int64(time.Second) != 0 || int64(len(r.Slots)) != r.WindowNS/interval {
		return errors.New("missing scheduled business slot")
	}
	kinds := []string{"node-ls", "ps", "read", "write", "short-exec"}
	ids := make(map[string]bool)
	for n, slot := range r.Slots {
		if slot.Index != n || slot.ScheduledNS != int64(n)*interval || slot.DispatchedNS < slot.ScheduledNS ||
			slot.TerminalNS < slot.DispatchedNS || slot.TerminalNS-slot.ScheduledNS > int64(upgradebudget.RequestLatency) ||
			slot.NativeRC != 0 || slot.Kind != kinds[n%len(kinds)] || slot.ID == "" || ids[slot.ID] || len(slot.Attempts) == 0 || !validDigest(slot.RequestDigest) {
			return errors.New("business slot missed its absolute schedule, deadline or stable invocation")
		}
		ids[slot.ID] = true
		last := slot.DispatchedNS
		for _, attempt := range slot.Attempts {
			if attempt.StartNS < last || attempt.EndNS < attempt.StartNS || attempt.EndNS > slot.TerminalNS || attempt.Code == "" {
				return errors.New("business attempt ledger is incomplete")
			}
			last = attempt.EndNS
		}
		if slot.Attempts[len(slot.Attempts)-1].Code != "ok" || !slot.Attempts[len(slot.Attempts)-1].Sent {
			return errors.New("business slot has no actual successful terminal attempt")
		}
		if slot.Kind == "write" || slot.Kind == "short-exec" {
			if slot.Effects != 1 || !validDigest(slot.EffectDigest) {
				return errors.New("business mutation did not have exactly one actual effect")
			}
		} else if slot.Effects != 0 {
			return errors.New("read-only business slot mutated state")
		}
	}
	for _, hop := range r.Hops {
		if hop.EndNS > r.WindowNS {
			return errors.New("cut outlived offered business load")
		}
		var overlap bool
		for _, slot := range r.Slots {
			if slot.ScheduledNS >= hop.CutNS && slot.ScheduledNS < hop.EndNS {
				overlap = true
			}
		}
		if !overlap {
			return errors.New("no actual business request crossed a cut")
		}
	}
	return nil
}

// origin: hot-update S9 plan §9.4 / T68 / B-6
func checkAddresses(rows []addressReceipt) error {
	expected := map[string]bool{"wss443-tunnel7000": false, "ipv6-bracket": false,
		"7000-as-wss": false, "443-as-tunnel": false, "missing-tunnel": false}
	for _, row := range rows {
		if _, ok := expected[row.Case]; !ok || expected[row.Case] || !row.Flagless || !validDigest(row.ConfigDigest) {
			return errors.New("B-6 lacks actual flagless config parse/connection evidence")
		}
		positive := row.Case == "wss443-tunnel7000" || row.Case == "ipv6-bracket"
		if positive && (!row.Connected || row.Rejected || row.NATSCID == 0 || row.TunnelCookie == 0) || !positive && (!row.Rejected || row.Connected) {
			return errors.New("B-6 mixed endpoint was accepted or correct endpoint failed")
		}
		if row.Case == "wss443-tunnel7000" && (row.BrokerURL != "wss://localhost:443" || row.TunnelAddr != "localhost:7000") ||
			row.Case == "ipv6-bracket" && (row.BrokerURL != "wss://[::1]:443" || row.TunnelAddr != "[::1]:7000") ||
			row.Case == "7000-as-wss" && row.BrokerURL != "wss://localhost:7000" ||
			row.Case == "443-as-tunnel" && row.TunnelAddr != "localhost:443" || row.Case == "missing-tunnel" && row.TunnelAddr != "" {
			return errors.New("B-6 did not exercise the specified addresses")
		}
		expected[row.Case] = true
	}
	for _, present := range expected {
		if !present {
			return errors.New("B-6 address case is missing")
		}
	}
	return nil
}

func oracleProcess(n byte) i.Process {
	return i.Process{BootID: "boot", PID: int32(n), StartTicks: uint64(n), InvocationID: fmt.Sprint(n),
		ExeDevice: 1, ExeInode: uint64(n), ExeSHA256: sha256.Sum256([]byte{n}), Cgroup: "fixture"}
}

func copyReceipt(r businessReceipt) businessReceipt {
	b, _ := json.Marshal(r)
	var out businessReceipt
	_ = json.Unmarshal(b, &out)
	return out
}

func exerciseBusinessOracle(t *testing.T) {
	good := oracleReceipt()
	if err := checkMixed(good); err != nil {
		t.Fatal("positive reader control: ", err)
	}
	mutations := map[string]func(*businessReceipt){
		"empty_required_set": func(r *businessReceipt) { r.Required = nil },
		"missing_original":   func(r *businessReceipt) { r.Streams = r.Streams[1:] },
		"business_redial":    func(r *businessReceipt) { r.Streams[0].Redials++ },
		"replacement_socket": func(r *businessReceipt) { r.Streams[0].Current.Cookie++ },
		"duplicate_bytes":    func(r *businessReceipt) { r.Streams[0].DownRead = digest([]byte("duplicate")) },
		"flow_stall":         func(r *businessReceipt) { r.Streams[0].MaxSilenceNS = int64(upgradebudget.LiveFlowStall) + 1 },
		"missing_slot":       func(r *businessReceipt) { r.Slots = append(r.Slots[:4], r.Slots[5:]...) },
		"duplicate_slot":     func(r *businessReceipt) { r.Slots[5] = r.Slots[4] },
		"late_dispatch": func(r *businessReceipt) {
			r.Slots[0].DispatchedNS += int64(6 * time.Second)
			r.Slots[0].TerminalNS = r.Slots[0].DispatchedNS + int64(time.Millisecond)
			r.Slots[0].Attempts[0].StartNS = r.Slots[0].DispatchedNS
			r.Slots[0].Attempts[0].EndNS = r.Slots[0].TerminalNS
		},
		"empty_attempts":           func(r *businessReceipt) { r.Slots[0].Attempts = nil },
		"duplicate_effect":         func(r *businessReceipt) { r.Slots[3].Effects++ },
		"disk_only":                func(r *businessReceipt) { r.Hops[0].Current = r.Hops[0].Before },
		"old_alive":                func(r *businessReceipt) { r.Hops[0].OldExited = false },
		"cleanup_before_retire":    func(r *businessReceipt) { r.Hops[0].LiveBusiness = 0 },
		"old_pending":              func(r *businessReceipt) { r.Hops[0].OldOwnerPending = 1 },
		"skipped_host_hop":         func(r *businessReceipt) { r.Hops = r.Hops[1:] },
		"wrong_layer_order":        func(r *businessReceipt) { r.DependencyOrder[0] = "caddy" },
		"image_cut_in_infra":       func(r *businessReceipt) { r.Executed[0] = "broker" },
		"carrier_rebuilt":          func(r *businessReceipt) { r.Streams[0].CarrierTransition = "replaced" },
		"missing_data_nc":          func(r *businessReceipt) { r.Attachments = r.Attachments[:len(r.Attachments)-1] },
		"pool_only_nc":             func(r *businessReceipt) { r.Attachments[0].New = r.Attachments[0].Old },
		"prepare_ack_missing":      func(r *businessReceipt) { r.Attachments[0].PrepareACK = false },
		"return_ack_missing":       func(r *businessReceipt) { r.Attachments[0].ReturnACK = false },
		"rollback_wrong_sha":       func(r *businessReceipt) { r.Hops[1].Current.ExeSHA256 = i.Digest{99} },
		"terminal_attempt_missing": func(r *businessReceipt) { r.Slots[0].Attempts[0].Code = "auth-failed" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			r := copyReceipt(good)
			mutate(&r)
			if err := checkMixed(r); err == nil {
				t.Fatal("business oracle accepted incomplete or interrupted evidence")
			}
		})
	}
	t.Run("address_controls", func(t *testing.T) {
		rows := []addressReceipt{
			{Case: "wss443-tunnel7000", BrokerURL: "wss://localhost:443", TunnelAddr: "localhost:7000", Connected: true, NATSCID: 1, TunnelCookie: 1},
			{Case: "ipv6-bracket", BrokerURL: "wss://[::1]:443", TunnelAddr: "[::1]:7000", Connected: true, NATSCID: 2, TunnelCookie: 2},
			{Case: "7000-as-wss", BrokerURL: "wss://localhost:7000", TunnelAddr: "localhost:7000", Rejected: true},
			{Case: "443-as-tunnel", BrokerURL: "wss://localhost:443", TunnelAddr: "localhost:443", Rejected: true},
			{Case: "missing-tunnel", BrokerURL: "wss://localhost:443", Rejected: true},
		}
		for n := range rows {
			rows[n].Flagless, rows[n].ConfigDigest = true, digest([]byte(rows[n].Case))
		}
		if err := checkAddresses(rows); err != nil {
			t.Fatal(err)
		}
		for n := range rows {
			mutant := append([]addressReceipt(nil), rows...)
			mutant[n].Connected, mutant[n].Rejected = !mutant[n].Connected, !mutant[n].Rejected
			if checkAddresses(mutant) == nil {
				t.Fatal("address oracle accepted a missing connection or negative rejection")
			}
		}
	})
	t.Run("canonical_codec", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "receipt.json")
		b, _ := json.Marshal(good)
		if err := os.WriteFile(path, b, 0o600); err != nil {
			t.Fatal(err)
		}
		var decoded businessReceipt
		if err := readJSON(path, &decoded); err != nil {
			t.Fatal(err)
		}
		for _, input := range [][]byte{
			append(append([]byte(nil), b...), []byte("{}")...),
			bytes.Replace(b, []byte(`"Version":2`), []byte(`"Version":2,"Version":2`), 1),
			bytes.Replace(b, []byte(`"Version":2`), []byte(`"Version":2,"unknown":0`), 1),
		} {
			if err := os.WriteFile(path, input, 0o600); err != nil {
				t.Fatal(err)
			}
			if readJSON(path, &decoded) == nil {
				t.Fatal("duplicate/unknown/trailing receipt bytes accepted")
			}
		}
	})
}

// origin: hot-update S9 plan §12 P30 / §9.5
func TestInfraBusinessLifecycle(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBusinessLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "business stream and NC retirement x5", g)
	testharness.AssertNoFDLeak(t, "business stream and NC retirement x5", fd, 0)
}

func embedded(t *testing.T) *server.Server {
	t.Helper()
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(upgradebudget.RequestLatency) {
		s.Shutdown()
		s.WaitForShutdown()
		t.Fatal("embedded server did not become ready")
	}
	return s
}

func exerciseBusinessLifecycle(t *testing.T) {
	s := embedded(t)
	defer func() { s.Shutdown(); s.WaitForShutdown() }()
	client, target := businessSockets(t)
	defer closeTestResource(t, client)
	defer closeTestResource(t, target)
	before := socketInode(t, client)
	for generation := uint64(1); generation <= 3; generation++ {
		transport, err := infratransport.New(generation, deadline(t), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		nc, err := transport.Connect(s.ClientURL(), func() []nats.Option { return []nats.Option{nats.NoReconnect()} })
		if err != nil {
			transport.Interrupt()
			t.Fatal(err)
		}
		cid, err := nc.GetClientID()
		if err != nil || cid == 0 {
			t.Fatal("actual NC has no CID")
		}
		if err := infratransport.Move(transport, nc, deadline(t), nil); err != nil {
			t.Fatal(err)
		}
		// Measure the business bytes after retiring NC. A disconnected NATS
		// callback alone is neither a loss nor proof of data-plane continuity.
		for _, pair := range [][2]*net.TCPConn{{client, target}, {target, client}} {
			body := []byte(fmt.Sprintf("original-direction-%d\n", generation))
			if err := pair[0].SetDeadline(time.Now().Add(upgradebudget.LiveFlowStall)); err != nil {
				t.Fatal(err)
			}
			if err := pair[1].SetDeadline(time.Now().Add(upgradebudget.LiveFlowStall)); err != nil {
				t.Fatal(err)
			}
			if _, err := pair[0].Write(body); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(body))
			if _, err := io.ReadFull(pair[1], got); err != nil || !bytes.Equal(body, got) || socketInode(t, client) != before {
				t.Fatal("NC retirement interrupted the original business stream")
			}
		}
	}
	if err := client.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Read(make([]byte, 1)); err != io.EOF {
		t.Fatal("original half-close lost FIN")
	}
	if _, err := target.Write([]byte("reverse tail")); err != nil {
		t.Fatal(err)
	}
	if err := target.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if tail, err := io.ReadAll(client); err != nil || string(tail) != "reverse tail" {
		t.Fatal("reverse tail or FIN changed")
	}
	// The resource rounds also own real JS consumer/Sync/rebind/retirement,
	// so a leak in the transfer seam cannot hide behind a TCP-only baseline.
	exerciseRetainedTransferPorts(t)
}

func businessSockets(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, listener)
	client, err := net.DialTCP("tcp4", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	target, err := listener.AcceptTCP()
	if err != nil {
		closeTestResource(t, client)
		t.Fatal(err)
	}
	return client, target
}

func socketInode(t *testing.T, c *net.TCPConn) uint64 {
	t.Helper()
	raw, err := c.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var stat unix.Stat_t
	var inspectErr error
	if err := raw.Control(func(fd uintptr) { inspectErr = unix.Fstat(int(fd), &stat) }); err != nil || inspectErr != nil {
		t.Fatal("cannot inspect original business socket")
	}
	return stat.Ino
}
