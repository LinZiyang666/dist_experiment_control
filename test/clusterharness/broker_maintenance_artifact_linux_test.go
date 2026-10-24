//go:build linux

package clusterharness

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

func buildN3MaintenanceArtifact(t *testing.T) string {
	t.Helper()
	root := testharness.GuardProcessDirectory(t, t.TempDir())
	_, source, _, _ := runtime.Caller(0)
	repo := filepath.Dir(filepath.Dir(filepath.Dir(source)))
	replacements := map[string]string{}
	for _, flag := range strings.Fields(os.Getenv("GOFLAGS")) {
		if path, ok := strings.CutPrefix(flag, "-overlay="); ok {
			var parent struct{ Replace map[string]string }
			data, err := os.ReadFile(path)
			if err != nil || json.Unmarshal(data, &parent) != nil {
				t.Fatal("cannot read mutation overlay", err)
			}
			for from, to := range parent.Replace {
				replacements[from] = to
			}
		}
	}
	read := func(name string) string {
		path := filepath.Join(repo, name)
		if replacements[path] != "" {
			path = replacements[path]
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	write := func(name, data string) {
		path := filepath.Join(root, strings.ReplaceAll(name, "/", "_"))
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		replacements[filepath.Join(repo, name)] = path
	}
	sourceCode := read("internal/broker/reexec.go")
	sourceCode = strings.Replace(sourceCode, "c.history = &history", `c.history = &history
 if err:=s6N3CutObservation(c.broker,history);err!=nil{return err}`, 1)
	sourceCode += `
// Test artifact observation only: the original cache expiry is never rebased.
func s6N3Observe(name string, value any) error {
 root:=filepath.Dir(os.Args[0]);data,err:=json.Marshal(value);if err!=nil{return err}
 path:=filepath.Join(root,name+".json");if err=os.WriteFile(path+".tmp",data,0600);err!=nil{return err};return os.Rename(path+".tmp",path)
}
func s6N3CutObservation(b *Broker,history HomeMaintenanceHistory) error {
 if history.CutRecord.CutID=="" {return nil}
 if err:=s6N3Observe("cut",map[string]any{"cut_id":history.CutRecord.CutID,"cut_deadline":history.CutDeadline,"history":history});err!=nil{return err}
 deadline:=time.Now().Add(time.Second)
 for {if _,err:=os.Stat(filepath.Join(filepath.Dir(os.Args[0]),"release-cut"));err==nil{return nil};if time.Now().After(deadline){return errors.New("N3 observation barrier expired")};time.Sleep(time.Millisecond)}
}
`
	sourceCode = strings.Replace(sourceCode, "if err := b.wireClusterLate(c.root, b.nc.Load()); err != nil {", `if c.cutID!="" {
 _,proofs,now,err:=homeMaintenanceSnapshot(b);if err!=nil{return err}
 lease:=proofs[b.selfID];record,err:=cluster.ReadBrokerMaintenance(b.cl.node.RODB(),b.selfID);if err!=nil{return err}
 if err=s6N3Observe("current-proof",map[string]any{"cut_id":c.cutID,"protected":brokermaint.Protects(lease,record,now),"expiry":lease.ExpiresLocalBootNS});err!=nil{return err}
 }
 if err := b.wireClusterLate(c.root, b.nc.Load()); err != nil {`, 1)
	write("internal/broker/reexec.go", sourceCode)
	sourceCode = read("internal/broker/runtime_introspect.go")
	// Admission now runs inside the shared pre-queue wrapper in every topology.
	// Observe the callback's actual refusal without changing its return value.
	// origin: S6 lead decision D-C / plan §7.1
	armGuard := "return captureBrokerReloadAdmission(b, req, admission)"
	if !strings.Contains(sourceCode, armGuard) {
		t.Fatal("N3 arm observation lost its production anchor")
	}
	sourceCode = strings.Replace(sourceCode, armGuard, `if err:=captureBrokerReloadAdmission(b, req, admission);err!=nil { _=s6N3Observe("arm-error",map[string]string{"error":err.Error()});return err };return nil`, 1)
	write("internal/broker/runtime_introspect.go", sourceCode)
	sourceCode = read("internal/broker/cluster_upgrade_trigger.go")
	sourceCode = strings.Replace(sourceCode, "if err != nil || current != req.Record {", `if err != nil || current != req.Record {
 _=s6N3Observe("record-mismatch",map[string]any{"current":current,"requested":req.Record,"read_failed":err!=nil})`, 1)
	sourceCode = strings.Replace(sourceCode, "ack, err := acceptBrokerMaintenance(b, request)", `ack, err := acceptBrokerMaintenance(b, request)
 if err!=nil {_=s6N3Observe("observer-error",map[string]string{"error":err.Error()})}`, 1)
	sourceCode = strings.Replace(sourceCode, "ack.Sig, err = maintenanceSign(b)(body)\n\treturn ack, err", `ack.Sig, err = maintenanceSign(b)(body)
 if err==nil && ack.Accepted {err=s6N3Observe("observer",ack)}
 return ack,err`, 1)
	write("internal/broker/cluster_upgrade_trigger.go", sourceCode)
	body, err := json.Marshal(struct{ Replace map[string]string }{replacements})
	if err != nil {
		t.Fatal(err)
	}
	overlay := filepath.Join(root, "overlay.json")
	if err = os.WriteFile(overlay, body, 0600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "n3-product")
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, "go", "build", "-race", "-trimpath", "-overlay", overlay, "-o", binary, "./cmd/tether")
	build.Dir = repo
	if out, err := testharness.CombinedOutput(build); err != nil {
		t.Fatalf("N3 product build: %v\n%s", err, out)
	}
	return binary
}
