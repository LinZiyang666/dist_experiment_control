//go:build linux

package brokerimage

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/testharness"
	"go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

// A candidate simulator exercises only the pure reader protocol; it is not the
// product dispatcher. The full bootstrap command integration is P22/P26-owned.
// The after-exec branch runs before testing parses deliberately incorrect argv.
func init() {
	if len(os.Args) > 1 && (os.Args[1] == "version" || os.Args[1] == PreflightProbeCommand) {
		if os.Args[1] == "version" {
			fmt.Printf("%s\n%s/%s\nimage-domains %s\n%s\n", proto.VersionLine(), runtime.GOOS, runtime.GOARCH, Domain, proto.ImageABILine())
			os.Exit(0)
		}
		request, err := DecodeProbeRequest(os.Stdin)
		if err != nil {
			os.Exit(2)
		}
		config := sha256.Sum256(request.Config)
		args, _ := json.Marshal(request.AllowedArgs)
		argsSHA := sha256.Sum256(args)
		mappedSHA, err := hashArtifact("/proc/self/exe")
		if err != nil {
			os.Exit(2)
		}
		reply := ProbeReply{Platform: runtime.GOOS + "/" + runtime.GOARCH, Proof: ReaderProof{SHA256: mappedSHA, ABI: proto.ImageABIVersion, Domains: []string{Domain}, MigrationDigest: migrationDigest(request.Schema.Supported)}, ConfigDigest: hex.EncodeToString(config[:]), ArgsDigest: hex.EncodeToString(argsSHA[:]), Supported: request.Schema.Supported}
		switch os.Getenv("BROKER_READER_CAP_TEST_CASE") {
		case "abi":
			reply.Proof.ABI = 0
		case "domains":
			reply.Proof.Domains = []string{"unsupported"}
		case "domain-order":
			reply.Proof.Domains = []string{Domain, Domain}
		case "platform":
			reply.Platform = "unsupported/unsupported"
		}
		data, _ := json.Marshal(reply)
		_, _ = os.Stdout.Write(data)
		os.Exit(0)
	}
	switch os.Getenv("BROKER_BOOT_TEST_PHASE") {
	case "after", "returned":
		phase := os.Getenv("BROKER_BOOT_TEST_PHASE")
		// This is the fixture's sole collector. Boot must consume the already held
		// envelope and cannot perform a second Restore or choose by incoming argv.
		restored, err := handoff.Restore()
		if err != nil || restored == nil {
			fmt.Fprintln(os.Stderr, "restore failed")
			os.Exit(2)
		}
		image, err := Boot(restored)
		if err != nil {
			fmt.Fprintln(os.Stderr, "boot failed", err)
			os.Exit(2)
		}
		bundle := image.Bundle()
		if bundle.Generation != 8 || bundle.Role != ImageRole || image.CheckConfig([]byte("fixture-config"), bundle.Args) != nil {
			os.Exit(2)
		}
		if _, present := os.LookupEnv("_TETHER_HANDOFF"); present {
			os.Exit(2)
		}
		if phase == "after" {
			prepared, err := image.Repack()
			if err != nil {
				fmt.Fprintln(os.Stderr, "repack failed", err)
				os.Exit(2)
			}
			env := replaceTestEnv(os.Environ(), "BROKER_BOOT_TEST_PHASE", "returned")
			if err = prepared.Exec("/proc/self/exe", bundle.Args, env); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
		}
		before, _ := strconv.Atoi(os.Getenv("BROKER_BOOT_TEST_PID"))
		// Independently compare process identity and generation after both execs.
		if before != os.Getpid() || os.Args[1] != "-test.run=TestBrokerBootRoleOnce" {
			os.Exit(2)
		}
		if err = image.Close(); err != nil {
			os.Exit(2)
		}
		fmt.Printf("broker role receipt pid=%d generation=%d\n", os.Getpid(), bundle.Generation)
		os.Exit(0)
	}
}
func replaceTestEnv(env []string, key, value string) []string {
	out := []string{}
	for _, entry := range env {
		if !strings.HasPrefix(entry, key+"=") {
			out = append(out, entry)
		}
	}
	return append(out, key+"="+value)
}

// origin: hot-update S6 plan §4.1 / T03
func TestBrokerBootRoleOnce(t *testing.T) {
	if os.Getenv("BROKER_BOOT_TEST_PHASE") == "before" {
		f := newImportFixture(t)
		f.bundle.Args = []string{os.Args[0], "-test.run=TestBrokerBootRoleOnce"}
		refreshBootFixture(t, f)
		data, err := Encode(f.bundle)
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := handoff.Prepare(handoff.State{Role: ImageRole, Instance: f.bundle.InstanceID, Generation: f.bundle.Generation, Data: data}, f.resources)
		if err != nil {
			t.Fatal(err)
		}
		env := replaceTestEnv(os.Environ(), "BROKER_BOOT_TEST_PHASE", "after")
		env = replaceTestEnv(env, "BROKER_BOOT_TEST_PID", strconv.Itoa(os.Getpid()))
		// There is no broker argv or daemon hint. The sealed Role and saved Args
		// alone determine redispatch. The original process stack never unwinds.
		if err = prepared.Exec("/proc/self/exe", []string{os.Args[0], "wrong-argv-with-no-hint"}, env); err != nil {
			t.Fatal(err)
		}
		return
	}
	// ctx-root: parent owns the same-PID exec fixture process lifetime.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=TestBrokerBootRoleOnce")
	cmd.Env = replaceTestEnv(os.Environ(), "BROKER_BOOT_TEST_PHASE", "before")
	output, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "broker role receipt pid=") || !strings.Contains(string(output), "generation=8") {
		t.Fatalf("same-PID role redispatch failed: %v\n%s", err, output)
	}
}

// origin: hot-update S6 plan §3.1 / T06
func TestBrokerPreflightNeverOpensStore(t *testing.T) {
	assertPurePreflightSource(t)
	// A real held Bolt OFD demonstrates why even a read-only second open is not
	// a smoke test. The separate helper is killed AND waited before release.
	path := filepath.Join(t.TempDir(), "held-bolt.db")
	owner, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = owner.Close() }()
	ready := make(chan struct{})
	cmd := exec.Command(os.Args[0], "-test.run=TestBrokerSecondOpenHelper")
	cmd.Env = replaceTestEnv(os.Environ(), "BROKER_BOLT_TEST_PATH", path)
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	go func() {
		scanner := bufio.NewScanner(pipe)
		if scanner.Scan() && scanner.Text() == "second open entered" {
			close(ready)
		}
	}()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		<-waited
		t.Fatal("second-open entry barrier missing")
	}
	// sleep-fixture: the child entry barrier has proved the real open is next;
	// this timeout observes kernel flock blocking, not daemon readiness.
	select {
	case err := <-waited:
		t.Fatalf("real second open did not block: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	b := fixtureBundle(t)
	now, err := rawsocket.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	candidate, previous, err := Preflight(PreflightRequest{CandidatePath: os.Args[0], PreviousPath: os.Args[0], Config: []byte("fixture-config"), AllowedArgs: b.Args, Stamp: b.Store, RequiredDomains: b.RequiredDomains, Deadline: controlbudget.Deadline(now + int64(10*time.Second))})
	if err != nil || candidate.SHA256 != b.CandidateSHA256 || previous.SHA256 != candidate.SHA256 || candidate.MigrationDigest != b.Store.Schema.Digest {
		_ = cmd.Process.Kill()
		<-waited
		t.Fatalf("pure probe failed with a live store: %v", err)
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = <-waited; err == nil {
		t.Fatal("blocked helper was not killed")
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
	// ctx-root: released-lock positive helper has its own bounded lifetime.
	positiveCtx, positiveCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer positiveCancel()
	positive := exec.CommandContext(positiveCtx, os.Args[0], "-test.run=TestBrokerSecondOpenHelper")
	positive.Env = replaceTestEnv(os.Environ(), "BROKER_BOLT_TEST_PATH", path)
	output, err := positive.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "second open completed") {
		t.Fatalf("released lock did not permit a real second open: %v %s", err, output)
	}
}
func assertPurePreflightSource(t *testing.T) {
	t.Helper()
	tree, err := parser.ParseFile(token.NewFileSet(), "preflight.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	ast.Inspect(tree, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		owner, ok := selector.X.(*ast.Ident)
		if !ok {
			return true
		}
		// Enumerate the actual storage/cluster openers rather than banning all Open
		// calls: reading candidate bytes is an essential part of this pure gate.
		if (owner.Name == "storage" && (strings.HasPrefix(selector.Sel.Name, "Open") || selector.Sel.Name == "ApplyMigrations")) ||
			(owner.Name == "sql" && selector.Sel.Name == "Open") ||
			(owner.Name == "cluster" && (strings.HasPrefix(selector.Sel.Name, "New") || strings.HasPrefix(selector.Sel.Name, "Acquire"))) ||
			(owner.Name == "bbolt" && selector.Sel.Name == "Open") {
			t.Errorf("preflight opens live store: %s.%s", owner.Name, selector.Sel.Name)
		}
		return true
	})
}

// origin: hot-update S6 plan §3.1 / T06
func TestBrokerSecondOpenHelper(t *testing.T) {
	path := os.Getenv("BROKER_BOLT_TEST_PATH")
	if path == "" {
		return
	}
	fmt.Println("second open entered")
	db, err := bbolt.Open(path, 0600, &bbolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	fmt.Println("second open completed")
}

// origin: hot-update S6 plan §3.1 / T06
func TestBrokerPreflightRejectsSchemaAndDeadline(t *testing.T) {
	b := fixtureBundle(t)
	now, err := rawsocket.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	request := PreflightRequest{CandidatePath: os.Args[0], PreviousPath: os.Args[0], Config: []byte("fixture-config"), AllowedArgs: b.Args, Stamp: b.Store, RequiredDomains: b.RequiredDomains, Deadline: controlbudget.Deadline(now - 1)}
	if _, _, err = Preflight(request); err == nil {
		t.Fatal("expired deadline admitted a probe")
	}
	request.Deadline = controlbudget.Deadline(now + int64(10*time.Second))
	request.Stamp.Schema.Supported = nil
	if _, _, err = Preflight(request); err == nil {
		t.Fatal("unknown compiled schema admitted")
	}
	request.Stamp = b.Store
	request.RequiredDomains = []string{Domain, "unknown-domain"}
	if _, _, err = Preflight(request); err == nil {
		t.Fatal("reader missing a required domain admitted")
	}
}

// origin: hot-update S6 plan §4.1 / T27
func TestBrokerActualFDWhitelist(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerFDWhitelist(t)
	}
	testharness.AssertNoGoroutineLeak(t, "actual broker FD whitelist", beforeG)
	testharness.AssertNoFDLeak(t, "actual broker FD whitelist", beforeFD, 0)
}
func exerciseBrokerFDWhitelist(t *testing.T) {
	t.Helper()
	file, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	prepared, err := handoff.Prepare(handoff.State{Role: "fixture", Instance: "fd-test", Generation: 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prepared.Close() }()
	// ForkLock and actual /proc census reject this undeclared inheritable FD.
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var setErr error
	_ = raw.Control(func(fd uintptr) { _, setErr = unix.FcntlInt(fd, unix.F_SETFD, 0) })
	if setErr != nil {
		t.Fatal(setErr)
	}
	defer func() { _ = raw.Control(func(fd uintptr) { _, _ = unix.FcntlInt(fd, unix.F_SETFD, unix.FD_CLOEXEC) }) }()
	if err = prepared.Exec("/missing-broker-fixture", []string{"fixture"}, nil); err == nil || !strings.Contains(err.Error(), "unexpected inheritable descriptor") {
		t.Fatalf("actual FD not checked: %v", err)
	}
	// Distinct IDs cannot assign the same original OFD to two terminal owners.
	resources := []handoff.Resource{{ID: "first", Kind: "fixture", Files: map[string]*os.File{"file": file}}, {ID: "second", Kind: "fixture", Files: map[string]*os.File{"file": file}}}
	_ = raw.Control(func(fd uintptr) { _, setErr = unix.FcntlInt(fd, unix.F_SETFD, unix.FD_CLOEXEC) })
	if err = handoff.ValidateResourceOwners(resources); err == nil {
		t.Fatal("cross-ID OFD aliases admitted")
	}
}

// origin: hot-update S6 plan §3.1 / T06
func TestBrokerProbeInputPreservesExactConfig(t *testing.T) {
	request := ProbeRequest{Config: []byte("credentials:\n  opaque: keep exact bytes\n"), AllowedArgs: []string{"broker", "serve"}}
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := DecodeProbeRequest(bytes.NewReader(data))
	if err != nil || !reflect.DeepEqual(parsed, request) {
		t.Fatalf("pure probe changed configuration bytes: %v", err)
	}
	if _, err = DecodeProbeRequest(io.MultiReader(bytes.NewReader(data), strings.NewReader("{}"))); err == nil {
		t.Fatal("trailing probe input accepted")
	}
}

// origin: hot-update S6 plan §3.1 / T06
func TestBrokerPreflightPinsMappedReaderSHA(t *testing.T) {
	b := fixtureBundle(t)
	now, err := rawsocket.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	// A shim's stable pathname bytes cannot claim the reader's mapped SHA. The
	// version and schema outputs are otherwise valid, so only the artifact binding
	// distinguishes this from an admissible same-code candidate.
	shim := filepath.Join(t.TempDir(), "reader-shim")
	quoted := "'" + strings.ReplaceAll(os.Args[0], "'", "'\"'\"'") + "'"
	if err = os.WriteFile(shim, []byte("#!/bin/sh\nexec "+quoted+" \"$@\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	_, _, err = Preflight(PreflightRequest{CandidatePath: shim, PreviousPath: os.Args[0], Config: []byte("fixture-config"), AllowedArgs: b.Args, Stamp: b.Store, RequiredDomains: b.RequiredDomains, Deadline: controlbudget.Deadline(now + int64(10*time.Second))})
	if err == nil {
		t.Fatal("stable shim falsely claimed the mapped reader SHA")
	}
}

// The protocol simulator covers malformed proofs; product preflight and its
// embedded schema/config validation remain covered by the real serve fixture.
// origin: hot-update S6 plan §3.1 / T06 / T37
func TestBrokerPureProbeProofRequiresCapabilities(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerPureProbeCapabilities(t)
	}
	testharness.AssertNoGoroutineLeak(t, "pure reader capability proof", g)
	testharness.AssertNoFDLeak(t, "pure reader capability proof", fd, 0)
}
func exerciseBrokerPureProbeCapabilities(t *testing.T) {
	t.Helper()
	bundle := fixtureBundle(t)
	t.Setenv("GORACE", strings.TrimSpace(os.Getenv("GORACE")+" atexit_sleep_ms=0"))
	for _, fault := range []string{"", "abi", "domains", "domain-order", "platform"} {
		t.Setenv("BROKER_READER_CAP_TEST_CASE", fault)
		deadline, err := NewStoreCutDeadline()
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = Preflight(PreflightRequest{CandidatePath: os.Args[0], PreviousPath: os.Args[0], Config: []byte("pure-proof"), AllowedArgs: bundle.Args, Stamp: bundle.Store, RequiredDomains: bundle.RequiredDomains, Deadline: deadline})
		if fault == "" && err != nil {
			t.Fatal("valid pure capabilities refused", err)
		}
		if fault != "" && err == nil {
			t.Fatalf("accepted malformed pure proof: %s", fault)
		}
	}
}

// A hostile reader must be drained without allowing stdout to grow the broker
// heap or silently treating a truncated declaration as a valid complete proof.
// origin: hot-update S6 plan §3.1 / T06
func TestBrokerProbeOutputIsBounded(t *testing.T) {
	output := &probeOutput{}
	prefix := bytes.Repeat([]byte("a"), maxProbeBytes)
	if n, err := output.Write(prefix); n != len(prefix) || err != nil || output.overflow || output.Len() != maxProbeBytes {
		t.Fatal("exact-limit proof capture changed")
	}
	if n, err := output.Write([]byte("surplus")); n != len("surplus") || err != nil || !output.overflow || output.Len() != maxProbeBytes {
		t.Fatal("surplus proof output was retained or not drained")
	}
	if !bytes.Equal(output.Bytes(), prefix) {
		t.Fatal("surplus output changed the captured proof")
	}
}
