//go:build linux

// Real embedded NATS, broker handlers and SQLite writers are exercised here.
// No authority writer is replaced. Fleet/deployment receipts remain separate.
// origin: hot-update S8 plan §6.2 / P24
package hotupdate_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeauthority"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/nats-io/nats-server/v2/server"
	natsservertest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

func authorityPIN(value byte) string {
	return "$argon2id$v=19$m=65536,t=3,p=2$" + base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{value}, 16)) + "$" + base64.RawStdEncoding.EncodeToString(make([]byte, 32))
}

func authorityCreate(t *testing.T, db *sql.DB, owner string, salt byte) string {
	t.Helper()
	now := time.Now().UTC()
	if _, err := session.Create(db, "lab", "lab", owner, authorityPIN(salt), now); err != nil {
		t.Fatal(err)
	}
	incarnation, err := upgradeauthority.Derive("lab", owner, now.UnixNano(), bytes.Repeat([]byte{salt}, 16))
	if err != nil {
		t.Fatal(err)
	}
	return incarnation
}

// Teardown joins Run before shutting down its server: async Drain otherwise
// survives the leak baseline and hides the lifetime being tested.
func authorityBroker(t *testing.T, db *sql.DB, allow string) (string, *nats.Conn) {
	t.Helper()
	return authorityBrokerOptions(t, db, allow, &server.Options{Host: "127.0.0.1", Port: -1, AlwaysEnableNonce: true})
}

func authorityBrokerOptions(t *testing.T, db *sql.DB, allow string, options *server.Options) (string, *nats.Conn) {
	t.Helper()
	srv := natsservertest.RunServer(options)
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	url := srv.ClientURL()
	b, err := broker.New(broker.Config{DB: db, NATSURL: url, Logger: testharness.SilentLog(), UpgradeURLAllowlist: []string{allow}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && ctx.Err() == nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("authority broker did not join")
		}
		authorityAwait(t, func() bool { return srv.NumClients() == 0 })
	})
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	authorityAwait(t, func() bool {
		_, err := nc.Request(proto.SubjNodeRegister("lab", "readiness"), []byte(`{}`), 50*time.Millisecond)
		return err == nil
	})
	return url, nc
}

func authorityAwait(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("authority fixture did not reach observable barrier")
		case <-tick.C:
		}
	}
}

func authorityRequest(t *testing.T, nc *nats.Conn, subject string, request any) map[string]json.RawMessage {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := nc.Request(subject, body, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var reply map[string]json.RawMessage
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		t.Fatal(err)
	}
	return reply
}

func authorityRegister(t *testing.T, nc *nats.Conn, incarnation, instance string) map[string]json.RawMessage {
	t.Helper()
	return authorityRequest(t, nc, proto.SubjNodeRegister("lab", "node"), map[string]any{
		"nid": "node", "proto_version": proto.ProtoVersion, "release_version": proto.ReleaseVersion,
		"boot_id": "boot", "instance_id": instance, "incarnation": incarnation,
	})
}

func requireAuthorityReply(t *testing.T, reply map[string]json.RawMessage, incarnation string) {
	t.Helper()
	var ok bool
	var actual string
	_ = json.Unmarshal(reply["ok"], &ok)
	_ = json.Unmarshal(reply["incarnation"], &actual)
	if !ok || actual != incarnation {
		t.Fatal("NOT_RUN pending P13/P15/P16: real register must acknowledge the creation identity")
	}
}

// Identical SID, configured NID, owner, PID and boot identity deliberately
// remove older fences as explanations. Each destructive handler sees A after
// B has already registered and must leave B's rows byte-for-byte intact.
// origin: hot-update S8 plan §6.2 / §8.3 M17 / P24
func TestLateCleanupCannotDeleteNewIncarnation(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		for _, arm := range []string{"g1", "lease", "reconnect", "late-exit", "farewell"} {
			t.Run(fmt.Sprintf("%d/%s", round, arm), func(t *testing.T) { exerciseLateAuthorityCleanup(t, arm) })
		}
	}
	testharness.AssertNoGoroutineLeak(t, "late authority cleanup x5", g)
	testharness.AssertNoFDLeak(t, "late authority cleanup x5", fd, 0)
}

func exerciseLateAuthorityCleanup(t *testing.T, arm string) {
	t.Helper()
	db := testharness.OpenDB(t)
	owner := "SHA256:" + strings.Repeat("A", 43)
	old := authorityCreate(t, db, owner, 1)
	_, nc := authorityBroker(t, db, "http://127.0.0.1:")
	instance := strings.Repeat("a", 26)
	requireAuthorityReply(t, authorityRegister(t, nc, "", instance), old)
	if _, err := db.Exec(`DELETE FROM sessions WHERE sid='lab'`); err != nil {
		t.Fatal(err)
	}
	current := authorityCreate(t, db, owner, 2)
	if old == current {
		t.Fatal("recreation did not change creation identity")
	}
	requireAuthorityReply(t, authorityRegister(t, nc, "", instance), current)
	pid := proc.NewPID()
	if err := proc.Insert(db, proc.Process{PID: pid, SID: "lab", NID: "node", Argv: []string{"/bin/true"}, StartedAt: time.Now().UTC(), StartedByFP: owner, BootID: "boot", StartTimeTicks: 99}); err != nil {
		t.Fatal(err)
	}
	if _, err := port.Allocate(db, "lab", "node", "replacement-port", 18080, 14000, owner, false, nil); err != nil {
		t.Fatal(err)
	}
	// A full snapshot observes every replicated column, including a liveness
	// update that would otherwise let stale reconnect overwrite the new node.
	before := authorityRows(t, db)
	switch arm {
	case "late-exit":
		_ = authorityRequest(t, nc, proto.SubjEvProc("lab", "node", pid, "exit"), map[string]any{
			"pid": pid, "sid": "lab", "nid": "node", "exit_code": 23, "ended_at": time.Now().UTC(),
			"incarnation": old, "instance_id": instance,
		})
	case "farewell":
		body, _ := json.Marshal(map[string]string{"incarnation": old, "instance_id": instance})
		if err := nc.Publish(proto.SubjNodeUnregister("lab", "node"), body); err != nil {
			t.Fatal(err)
		}
		if err := nc.FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
		// The register handler serializes farewell with its following request.
		_ = authorityRegister(t, nc, old, instance)
	default:
		request := map[string]any{"nid": "node", "proto_version": proto.ProtoVersion, "boot_id": "boot", "instance_id": instance, "incarnation": old}
		if arm == "g1" {
			request["local_processes"] = []any{}
			request["local_ports"] = []any{}
		}
		if arm == "lease" {
			request["leased_nid"] = true
		}
		reply := authorityRequest(t, nc, proto.SubjNodeRegister("lab", "node"), request)
		var code string
		_ = json.Unmarshal(reply["code"], &code)
		if code != "session_incarnation_mismatch" {
			t.Fatalf("stale %s was not rejected: %s", arm, code)
		}
	}
	if after := authorityRows(t, db); !bytes.Equal(before, after) {
		t.Fatalf("M17: stale %s changed replacement authority rows", arm)
	}
}

func authorityRows(t *testing.T, db *sql.DB) []byte {
	t.Helper()
	var result [][][]string
	for _, query := range []string{`SELECT nid,sid,boot_id,status,CAST(last_heartbeat_at AS TEXT) FROM nodes ORDER BY sid,nid`, `SELECT pid,sid,nid,status,COALESCE(exit_code,''),COALESCE(CAST(ended_at AS TEXT),'') FROM processes ORDER BY pid`, `SELECT port,sid,nid,name,token_hash,state FROM port_allocations ORDER BY port`} {
		rows, err := db.Query(query)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		var table [][]string
		for rows.Next() {
			values := make([]sql.NullString, len(columns))
			targets := make([]any, len(columns))
			for i := range values {
				targets[i] = &values[i]
			}
			if err := rows.Scan(targets...); err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			var record []string
			for _, value := range values {
				record = append(record, fmt.Sprintf("%t:%s", value.Valid, value.String))
			}
			table = append(table, record)
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, table)
	}
	body, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// ReadCreation is intentionally completed before replacement. The rendered
// statement must still bind that compared row when executed later, which is
// also the cluster planner's defense against compare-then-recreate.
// origin: hot-update S8 plan §6.2 / P24
func TestAuthorityCompareThenRecreateRejectsWrite(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseAuthorityPredicate(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "authority predicate x5", g)
	testharness.AssertNoFDLeak(t, "authority predicate x5", fd, 0)
}

func exerciseAuthorityPredicate(t *testing.T) {
	t.Helper()
	db := testharness.OpenDB(t)
	owner := "SHA256:" + strings.Repeat("A", 43)
	old := authorityCreate(t, db, owner, 1)
	creation, err := upgradeauthority.ReadCreation(db, upgradeauthority.Guard{SID: "lab", Incarnation: old, InstanceID: "instance", ConfiguredNID: "node"})
	if err != nil {
		t.Fatal(err)
	}
	predicate, args, err := upgradeauthority.CreationPredicate(creation)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Register(db, node.RegisterInput{SID: "lab", NID: "node"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	result, err := db.Exec(`UPDATE nodes SET boot_id='positive' WHERE sid='lab' AND `+predicate, args...)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		t.Fatal("predicate positive control did not write")
	}
	if _, err := db.Exec(`DELETE FROM sessions WHERE sid='lab'`); err != nil {
		t.Fatal(err)
	}
	authorityCreate(t, db, owner, 2)
	// Isolate the creation salt: timestamp, SID and owner deliberately remain equal.
	if _, err := db.Exec(`UPDATE sessions SET created_at=? WHERE sid='lab'`, creation.CreatedAtText); err != nil {
		t.Fatal(err)
	}
	if _, err := upgradeauthority.ReadCreation(db, upgradeauthority.Guard{SID: "lab", Incarnation: old, InstanceID: "instance", ConfiguredNID: "node"}); err == nil || !strings.Contains(err.Error(), upgradetx.CodeSessionIncarnationMismatch) {
		t.Fatal("changed creation salt did not reject the old authority guard")
	}
	if err := node.Register(db, node.RegisterInput{SID: "lab", NID: "node", BootID: "replacement"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	result, err = db.Exec(`UPDATE nodes SET boot_id='stale' WHERE sid='lab' AND `+predicate, args...)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := result.RowsAffected(); err != nil || n != 0 {
		t.Fatal("compare-before-recreate authorized a stale node mutation")
	}
	var boot string
	if err := db.QueryRow(`SELECT boot_id FROM nodes WHERE sid='lab' AND nid='node'`).Scan(&boot); err != nil || boot != "replacement" {
		t.Fatal("replacement node was changed")
	}
}

type authorityParallelReceipt struct {
	Round           int                    `json:"round"`
	RunID           string                 `json:"run_id"`
	SourceCommit    string                 `json:"source_commit"`
	BinarySHA       string                 `json:"binary_sha"`
	OldGuard        upgradeauthority.Guard `json:"old_guard"`
	NewGuard        upgradeauthority.Guard `json:"new_guard"`
	Original        upgradetx.Census       `json:"original"`
	Before          upgradetx.Census       `json:"before"`
	After           upgradetx.Census       `json:"after"`
	OldReleased     upgradetx.Census       `json:"old_released"`
	ParallelCalls   []string               `json:"parallel_calls"`
	OldOwnersReaped bool                   `json:"old_owners_reaped"`
	OldFDsReleased  bool                   `json:"old_fds_released"`
	NativeRC        int                    `json:"native_rc"`
}

func judgeAuthorityParallel(r authorityParallelReceipt, round int) error {
	if r.Round != round || r.RunID != fmt.Sprintf("authority-parallel/round-%d", round) || !upgradeHex(r.SourceCommit, 40) || !upgradeHex(r.BinarySHA, 64) || r.NativeRC != 0 ||
		r.OldGuard.SID != r.NewGuard.SID || r.OldGuard.ConfiguredNID != r.NewGuard.ConfiguredNID || r.OldGuard.InstanceID == "" || r.NewGuard.InstanceID == "" || !upgradeHex(r.OldGuard.Incarnation, 64) || !upgradeHex(r.NewGuard.Incarnation, 64) || r.OldGuard.Incarnation == r.NewGuard.Incarnation {
		return fmt.Errorf("authority parallel receipt lacks distinct guarded creation identities")
	}
	if !reflect.DeepEqual(r.ParallelCalls, []string{"flow-close", "transfer-terminal", "transfer-gc"}) || !r.OldOwnersReaped || !r.OldFDsReleased {
		return fmt.Errorf("parallel flow/transfer cleanup was not exercised and joined")
	}
	for _, c := range []upgradetx.Census{r.Original, r.Before, r.After, r.OldReleased} {
		if !c.Known || !c.Complete || len(c.Resources) == 0 || c.Admitting != 0 || !upgradeHex(c.ExpectedDigest, 64) || !upgradeHex(c.Stamp.Nonce, 32) || c.Stamp.Sequence == 0 || c.Stamp.Producer.PID <= 0 || c.Stamp.Producer.StartTicks == 0 {
			return fmt.Errorf("authority resource census is incomplete")
		}
	}
	if r.Before.Stamp.Attempt != r.After.Stamp.Attempt || r.Before.ExpectedDigest != r.After.ExpectedDigest || r.After.Stamp.Sequence <= r.Before.Stamp.Sequence || r.Before.Stamp.Nonce == r.After.Stamp.Nonce || !reflect.DeepEqual(r.Before.Resources, r.After.Resources) {
		return fmt.Errorf("M17: old parallel cleanup changed replacement resource identities")
	}
	if r.Original.ExpectedDigest != r.OldReleased.ExpectedDigest || len(r.Original.Resources) != len(r.OldReleased.Resources) {
		return fmt.Errorf("old terminal cleanup omitted an original resource")
	}
	oldDomains, newDomains := map[upgradetx.Domain]bool{}, map[upgradetx.Domain]bool{}
	for i, old := range r.Original.Resources {
		released := r.OldReleased.Resources[i]
		if old.Domain != released.Domain || old.ID != released.ID || old.Owner != released.Owner || old.Origin != released.Origin || old.Generation != released.Generation || released.Custody != "released" || released.State != "terminal_durable" {
			return fmt.Errorf("old resource lacks original-owner terminal release")
		}
		oldDomains[old.Domain] = true
	}
	for _, resource := range r.Before.Resources {
		if resource.Custody != "runtime" || resource.State != "running" {
			return fmt.Errorf("replacement resource did not remain live")
		}
		newDomains[resource.Domain] = true
	}
	for _, domain := range []upgradetx.Domain{upgradetx.DomainPTY, upgradetx.DomainFlow, upgradetx.DomainTransferWorker, upgradetx.DomainTransferTracker} {
		if !oldDomains[domain] || !newDomains[domain] {
			return fmt.Errorf("parallel termination omitted a required authority domain")
		}
	}
	return nil
}

// P17/P26 export exact original/current resource sets while old flow close and
// transfer terminal/GC are concurrently finishing. This test consumes their
// real observations through the frozen Census/Guard contracts; missing ports
// remain a failing prerequisite when evidence was explicitly supplied, with no
// synthetic worker/GC implementation. Without the P26 live supplier this is
// NOT_RUN; it cannot certify the authority boundary in a default hermetic run.
// origin: hot-update S8 plan §6.2 / P24 / M17
// origin: S7 internal review TRANSFER-MAKETEST-2 F4
func TestAuthorityParallelFlowTransferTermination(t *testing.T) {
	// P26 owns the live collector, while P24 consumes its exact observations
	// (S8 plan §6.2 and §12.2). Missing supply is SETUP-RED (a skipped receipt
	// would read as a pass to the hard gates); supplied missing or invalid
	// observations still fail in the exercise.
	// origin: S7 internal review TRANSFER-MAKETEST-2 F4; hot-update S10 lead review (Skip-as-pass)
	if *upgradeCompatibilityEvidence == "" {
		t.Fatal("SETUP-RED: NOT_RUN pending P26: exact parallel flow/transfer authority observations are required (hot-update S8 plan §6.2 / §12.2)")
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAuthorityParallelTermination(t, round)
	}
	testharness.AssertNoGoroutineLeak(t, "parallel authority cleanup x5", g)
	testharness.AssertNoFDLeak(t, "parallel authority cleanup x5", fd, 0)
}

func exerciseAuthorityParallelTermination(t *testing.T, round int) {
	t.Helper()
	if *upgradeCompatibilityEvidence == "" {
		t.Fatal("NOT_RUN pending P13-P17: exact P26 parallel flow/transfer authority observations are required")
	}
	var receipt authorityParallelReceipt
	if err := readUpgradeCompatibilityJSON(filepath.Join(*upgradeCompatibilityEvidence, fmt.Sprintf("round-%d", round), "authority-parallel.json"), &receipt); err != nil {
		t.Fatal(err)
	}
	if err := judgeAuthorityParallel(receipt, round); err != nil {
		t.Fatal(err)
	}
}

func authorityParallelOracleRecord() authorityParallelReceipt {
	owner := upgradetx.Owner{BootID: "boot", PID: 100, StartTicks: 101, InstanceID: "original"}
	r := authorityParallelReceipt{RunID: "authority-parallel/round-0", SourceCommit: strings.Repeat("1", 40), BinarySHA: strings.Repeat("2", 64), OldGuard: upgradeauthority.Guard{SID: "lab", ConfiguredNID: "node", InstanceID: "original", Incarnation: strings.Repeat("a", 64)}, NewGuard: upgradeauthority.Guard{SID: "lab", ConfiguredNID: "node", InstanceID: "replacement", Incarnation: strings.Repeat("b", 64)}, ParallelCalls: []string{"flow-close", "transfer-terminal", "transfer-gc"}, OldOwnersReaped: true, OldFDsReleased: true}
	r.Original = upgradetx.Census{Known: true, Complete: true, ExpectedDigest: strings.Repeat("3", 64), Stamp: upgradetx.Stamp{Producer: owner, Nonce: strings.Repeat("4", 32), Sequence: 1}}
	for _, domain := range []upgradetx.Domain{upgradetx.DomainPTY, upgradetx.DomainFlow, upgradetx.DomainTransferWorker, upgradetx.DomainTransferTracker} {
		r.Original.Resources = append(r.Original.Resources, upgradetx.Resource{Domain: domain, ID: string(domain) + "-original", Origin: "agent", Owner: owner, Generation: 1, State: "running", Custody: "runtime"})
	}
	r.Before = r.Original
	r.Before.Resources = append([]upgradetx.Resource(nil), r.Original.Resources...)
	for i := range r.Before.Resources {
		r.Before.Resources[i].ID = string(r.Before.Resources[i].Domain) + "-replacement"
		r.Before.Resources[i].Owner.InstanceID = "replacement"
		r.Before.Resources[i].Generation = 2
	}
	r.Before.ExpectedDigest = strings.Repeat("5", 64)
	r.After = r.Before
	r.After.Resources = append([]upgradetx.Resource(nil), r.Before.Resources...)
	r.After.Stamp.Nonce = strings.Repeat("6", 32)
	r.After.Stamp.Sequence = 2
	r.OldReleased = r.Original
	r.OldReleased.Resources = append([]upgradetx.Resource(nil), r.Original.Resources...)
	for i := range r.OldReleased.Resources {
		r.OldReleased.Resources[i].State = "terminal_durable"
		r.OldReleased.Resources[i].Custody = "released"
	}
	return r
}

// These synthetic rows validate the observer oracle, not a product cut.
// origin: hot-update S8 plan §6.2 / P24 / M17
func TestAuthorityParallelTerminationOracleControls(t *testing.T) {
	if err := judgeAuthorityParallel(authorityParallelOracleRecord(), 0); err != nil {
		t.Fatal(err)
	}
	for _, defect := range []string{"new-incarnation-cleaned", "missing-transfer-gc", "unknown-empty", "old-owner-live", "old-fd-retained", "old-resource-omitted", "new-resource-replaced", "same-incarnation", "stale-observation"} {
		t.Run(defect, func(t *testing.T) {
			r := authorityParallelOracleRecord()
			switch defect {
			case "new-incarnation-cleaned":
				r.After.Resources[0].Custody = "released"
			case "missing-transfer-gc":
				r.ParallelCalls = r.ParallelCalls[:2]
			case "unknown-empty":
				r.After.Known = false
				r.After.Resources = nil
			case "old-owner-live":
				r.OldOwnersReaped = false
			case "old-fd-retained":
				r.OldFDsReleased = false
			case "old-resource-omitted":
				r.OldReleased.Resources = r.OldReleased.Resources[1:]
			case "new-resource-replaced":
				r.After.Resources[0].ID = "other"
			case "same-incarnation":
				r.NewGuard.Incarnation = r.OldGuard.Incarnation
			case "stale-observation":
				r.After.Stamp = r.Before.Stamp
			}
			if judgeAuthorityParallel(r, 0) == nil {
				t.Fatal("authority resource defect accepted")
			}
		})
	}
}
