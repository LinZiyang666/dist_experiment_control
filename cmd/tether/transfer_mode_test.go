package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// transfer_mode_test.go — a forced overwrite replaces CONTENT, not permissions.
//
// origin: prerelease audit round 2, I-F3 / I-F7 / G9 / J8.
//
// The atomic-replace shape creates its temp at 0600 and renames it over the
// destination, so without an explicit carry a `--force` overwrite silently reset the
// destination's mode: a 0755 script came back non-executable, a 0644 config unreadable
// to the group that had been reading it. The operator asked to replace the bytes.
//
// The FIRST fix landed on the inline path only and left the tier-B path — every file
// over the 8 MiB tier-A ceiling, i.e. exactly what `--force` is usually used for —
// still resetting the mode. It survived all four gates because the change had no test
// at all on either side. This file is that test.

func TestForcedLocalWriteKeepsTheDestinationsMode(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "app.sh")
	if err := os.WriteFile(dst, []byte("old\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	// Defeat umask: WriteFile's mode is masked, and the whole point is the mode we
	// observe on disk, not the one we asked for.
	if err := os.Chmod(dst, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := prepareLocalTransferCommit(t, dst, []byte("new content\n"), "forced", true)(); err != nil {
		t.Fatalf("commit forced local write: %v", err)
	}

	st, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o755 {
		t.Fatalf("a forced overwrite reset the destination's mode to %o, want 0755.\n\n"+
			"The operator asked to replace the CONTENT. A 0755 script that comes back 0600 is "+
			"not executable any more, and nothing in the output says so.", st.Mode().Perm())
	}
	body, _ := os.ReadFile(dst)
	if string(body) != "new content\n" {
		t.Errorf("content not replaced: %q", string(body))
	}
}

// TestAFreshLocalWriteDoesNotInventAMode: with no destination to inherit from, the
// temp file's own 0600 stands. Without this the test above is satisfied by a chmod
// that fires unconditionally against a stat of a file that does not exist.
func TestAFreshLocalWriteDoesNotInventAMode(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "brand-new.bin")

	if err := prepareLocalTransferCommit(t, dst, []byte("x"), "fresh", false)(); err != nil {
		t.Fatalf("commit fresh local write: %v", err)
	}
	st, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Errorf("a first-time write landed at %o; with no destination to inherit from it must "+
			"keep the temp's own 0600", st.Mode().Perm())
	}
}

// TestBothPullPathsCarryTheMode is the WIRING half, and the reason it exists is that
// I-F3 was a missing call site rather than a broken helper: the inline path had the
// carry and the tier-B path did not, and no behavioural test could tell, because
// neither path had one.
//
// The following records the original source-scanning rationale, retained as an asset.
// S7 drives the real result stream and Cobra entry instead.
// Driving the tier-B lander needs an ObjectStore result stream; asserting the call site
// in the source is what is affordable here, and it is exactly what a future edit would
// drop.
// The old text counter accepted two copies in one path and stopped seeing the
// real implementation after a leaf move. Drive the retained Cobra entry with
// real inline/object bytes; the fixture supplies only the remote control replies.
// origin: hot-update S7 plan §7 / §11.4 / P31.
func TestBothPullPathsCarryTheMode(t *testing.T) {
	before := runtime.NumGoroutine()
	fdBefore := testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePullModePaths(t)
	}
	testharness.AssertNoGoroutineLeak(t, "pull mode paths", before)
	testharness.AssertNoFDLeak(t, "pull mode paths", fdBefore, 0)
}

func exercisePullModePaths(t *testing.T) {
	t.Helper()
	for _, tc := range []struct {
		tier  string
		force bool
	}{{"a", true}, {"b", true}, {"a", false}, {"b", false}} {
		tier, force := tc.tier, tc.force
		t.Run(fmt.Sprintf("%s/force=%t", tier, force), func(t *testing.T) {
			url := startJetStreamNATS(t)
			nc, err := nats.Connect(url)
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			home := t.TempDir()
			writeReviewPsIdentity(t, home, "lab")
			id, err := cli.EnsureIdentity(home)
			if err != nil {
				t.Fatal(err)
			}
			payload := []byte("new permission-preserving bytes\n")
			if tier == "b" {
				payload = bytes.Repeat(payload, int(proto.XferTierAMaxBytes)/len(payload)+1)
			}
			digest := sha256.Sum256(payload)
			dir := t.TempDir()
			dst := filepath.Join(dir, "app.sh")
			wantMode := os.FileMode(0o600)
			if force {
				wantMode = 0o755
				if err := os.WriteFile(dst, []byte("old\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(dst, wantMode); err != nil {
					t.Fatal(err)
				}
			}
			js, err := jetstream.New(nc)
			if err != nil {
				t.Fatal(err)
			}
			var store jetstream.ObjectStore
			if tier == "b" {
				store, err = js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: proto.XferBucketName("lab")})
				if err != nil {
					t.Fatal(err)
				}
			}
			prepared, finalized := make(chan proto.PullPrepareReq, 1), make(chan proto.TransferFinalize, 1)
			callbackErrors := make(chan error, 2)
			subscribe := func(subject string, handler nats.MsgHandler) {
				t.Helper()
				sub, err := nc.Subscribe(subject, handler)
				if err != nil {
					t.Fatal(err)
				}
				deferUnsubscribe := func() { _ = sub.Unsubscribe() }
				t.Cleanup(deferUnsubscribe)
			}
			respond := func(msg *nats.Msg, value any) {
				body, err := json.Marshal(value)
				if err == nil {
					err = msg.Respond(body)
				}
				if err != nil {
					callbackErrors <- err
				}
			}
			subscribe(proto.SubjCtrlCaps(id.PublicKey, "lab"), func(msg *nats.Msg) {
				respond(msg, proto.CapsResp{OK: true, MaxPayload: nc.MaxPayload()})
			})
			subscribe(proto.SubjCmdBy("lab", id.PublicKey, "lab-1", "pull"), func(msg *nats.Msg) {
				var req proto.PullPrepareReq
				if err := json.Unmarshal(msg.Data, &req); err != nil {
					callbackErrors <- err
					return
				}
				prepared <- req
				resp := proto.PullPrepareResp{OK: true, Tier: tier, Size: int64(len(payload)), SHA256: hex.EncodeToString(digest[:])}
				if tier == "a" {
					resp.InlineData = payload
				} else {
					// SDK Put is fixture setup for a negotiated legacy remote,
					// never a capable writer or a retry inside the tested ctl.
					if _, err := store.PutBytes(t.Context(), req.TransferID, payload); err != nil {
						callbackErrors <- err
						return
					}
					resp.Bucket, resp.ObjectKey = proto.XferBucketName("lab"), req.TransferID
				}
				respond(msg, resp)
			})
			subscribe(proto.SubjCtrlTransferFinalize(id.PublicKey, "lab", "*"), func(msg *nats.Msg) {
				var fin proto.TransferFinalize
				if err := json.Unmarshal(msg.Data, &fin); err != nil {
					callbackErrors <- err
					return
				}
				finalized <- fin
				respond(msg, proto.TransferFinalizeResp{OK: true})
			})
			if err := nc.Flush(); err != nil {
				t.Fatal(err)
			}
			cmd := newPullCmd()
			args := []string{"lab-1:/remote/app.sh", dst, "--home", home, "--nats-url", url, "--timeout", "5s"}
			if force {
				args = append(args, "--force")
			}
			cmd.SetArgs(args)
			var output, stderr bytes.Buffer
			cmd.SetOut(&output)
			cmd.SetErr(&stderr)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			if err := cmd.ExecuteContext(ctx); err != nil {
				t.Fatalf("pull tier %s: %v; stderr=%s", tier, err, stderr.String())
			}
			select {
			case err := <-callbackErrors:
				t.Fatal(err)
			default:
			}
			var req proto.PullPrepareReq
			select {
			case req = <-prepared:
				if req.TransferID == "" || req.Force != force {
					t.Fatalf("pull did not prepare the original invocation: %+v", req)
				}
			default:
				t.Fatal("pull returned without invoking the remote prepare")
			}
			select {
			case fin := <-finalized:
				if fin.TransferID != req.TransferID || fin.Kind != "complete" || fin.Tier != tier || fin.Bytes != int64(len(payload)) {
					t.Fatalf("pull finalized a different invocation or incomplete bytes: %+v", fin)
				}
			default:
				t.Fatal("pull returned without completing its original invocation")
			}
			st, err := os.Stat(dst)
			if err != nil {
				t.Fatal(err)
			}
			if st.Mode().Perm() != wantMode {
				t.Fatalf("tier %s local write mode=%o, want %o (force=%t)", tier, st.Mode().Perm(), wantMode, force)
			}
			got, err := os.ReadFile(dst)
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("tier %s content not replaced completely: bytes=%d err=%v", tier, len(got), err)
			}
		})
	}
}
