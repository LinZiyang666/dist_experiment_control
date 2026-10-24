//go:build linux

package hotupdate

import (
	"bytes"
	"debug/buildinfo"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// Keep this build receipt reachable without the integration tag. The same
// builder copies tagged fixtures for the exec tests, so a missing inner tag
// must fail the fast suite even when no real SS upgrade is being exercised.
// origin: hot-update S5 plan §8 / T42; GATES-FINAL-2 helper tag regression.
func TestSSProbeBuildIncludesFixtureTags(t *testing.T) {
	binary := ssBuildProbe(t, "TestSSProbeHelper")
	info, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	settings := make(map[string]string)
	for _, setting := range info.Settings {
		settings[setting.Key] = setting.Value
	}
	if settings["-tags"] != "hotupdate_integration" || settings["-race"] != "true" {
		t.Fatalf("SS helper build omitted fixture tags or race instrumentation: tags=%q race=%q", settings["-tags"], settings["-race"])
	}
}

// Trace the actual product AEAD primitive without adding a public hook or
// exposing subkeys/nonces. A duplicate digest means a repeated (subkey,nonce)
// even when re-Seal produces identical ciphertext and the wire oracle passes.
// The overlay only exists in the temporary helper build, never on disk in
// a production owner. Caller overlays are merged so mutations reach both hops.
// origin: hot-update S5 plan §6.2 / T42
const ssSealProbeSource = `package ssproxy
import (
 "crypto/cipher"
 "crypto/sha256"
 "os"
 "strconv"
 "sync"
 "syscall"
 "encoding/hex"
 "golang.org/x/crypto/chacha20poly1305"
)
type ssProbeCipher struct { cipher.AEAD; key []byte }
var ssProbeLock sync.Mutex
func ssProbeAEAD(key []byte) (cipher.AEAD,error) {
 a,err:=chacha20poly1305.New(key); if err!=nil{return nil,err}
 return &ssProbeCipher{AEAD:a,key:append([]byte(nil),key...)},nil
}
func (a *ssProbeCipher) Seal(dst,nonce,plain,additional []byte) []byte {
 h:=sha256.New(); h.Write(a.key); h.Write(nonce)
 event:=[]byte("seal "+hex.EncodeToString(h.Sum(nil))+"\n")
 fd,err:=strconv.Atoi(os.Getenv("TETHER_TEST_SS_SEAL_FD")); if err!=nil{panic("SS Seal trace descriptor unavailable")}
 ssProbeLock.Lock(); n,err:=syscall.Write(fd,event); ssProbeLock.Unlock()
 if err!=nil || n!=len(event){panic("SS Seal trace write failed")}
 return a.AEAD.Seal(dst,nonce,plain,additional)
}
`

const ssShortWriteProbeSource = `package ssproxy
import ("os";"strconv";"syscall";"fmt")
func ssProbeClientWrite(s actorSocket,p []byte) (int,error) {
 n,err:=s.Write(p)
 if n>0 && n<len(p) {
  fd,parseErr:=strconv.Atoi(os.Getenv("TETHER_TEST_SS_SEAL_FD")); if parseErr!=nil{panic("SS Write trace descriptor unavailable")}
  event:=[]byte(fmt.Sprintf("short %d %d\n",n,len(p)))
  written,writeErr:=syscall.Write(fd,event); if writeErr!=nil || written!=len(event){panic("SS Write trace failed")}
 }
 return n,err
}
`

func ssBuildProbe(t *testing.T, helperTestName string) string {
	t.Helper()
	_, source, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "../.."))
	dir := t.TempDir()
	module, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	module = bytes.Replace(module, []byte("module github.com/LinZiyang666/tether"), []byte("module github.com/LinZiyang666/tether/test/hotupdate/ssprobe"), 1)
	module = append(module, []byte("\nrequire github.com/LinZiyang666/tether v0.0.0\nreplace github.com/LinZiyang666/tether => "+strconv.Quote(root)+"\n")...)
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), module, 0600); err != nil {
		t.Fatal(err)
	}
	sums, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "go.sum"), sums, 0600); err != nil {
		t.Fatal(err)
	}
	overlay := struct{ Replace map[string]string }{Replace: make(map[string]string)}
	// GOFLAGS is a build input, not a credential. Its overlay must reach both
	// production sources and copied fixture sources in the helper graph.
	// origin: hot-update S5 plan §8 T42 / T43
	flags, err := testharness.Output(exec.Command("go", "env", "GOFLAGS"))
	if err != nil {
		t.Fatal(err)
	}
	for _, flag := range strings.Fields(string(flags)) {
		if strings.HasPrefix(flag, "-overlay=") {
			data, err := os.ReadFile(strings.TrimPrefix(flag, "-overlay="))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(data, &overlay); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, name := range []string{"ss_exec_test.go", "ss_revoke_test.go", "ss_probe_build_test.go"} {
		input := filepath.Join(root, "test/hotupdate", name)
		if replacement, ok := overlay.Replace[input]; ok {
			input = replacement
		}
		data, err := os.ReadFile(input)
		if err != nil {
			t.Fatal(err)
		}
		data = bytes.Replace(data, []byte("package hotupdate"), []byte("package main"), 1)
		if err := os.WriteFile(filepath.Join(dir, strings.TrimSuffix(name, "_test.go")+".go"), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	main := fmt.Sprintf("package main\nimport (\"testing\";\"regexp\")\nfunc main(){ testing.Main(regexp.MatchString, []testing.InternalTest{{Name:%q, F:ssExecHelper}}, nil,nil) }\n", helperTestName)
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(main), 0600); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(root, "internal/agent/ssproxy/*.go"))
	if err != nil {
		t.Fatal(err)
	}
	replaced := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		input := file
		if replacement, ok := overlay.Replace[file]; ok {
			input = replacement
		}
		data, err := os.ReadFile(input)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(data, []byte("chacha20poly1305.New(")) {
			continue
		}
		replaced += bytes.Count(data, []byte("chacha20poly1305.New("))
		data = bytes.ReplaceAll(data, []byte("chacha20poly1305.New("), []byte("ssProbeAEAD("))
		// A constructor-only import becomes unused after interception.
		if !bytes.Contains(data, []byte("chacha20poly1305.")) {
			data = bytes.ReplaceAll(data, []byte("\"golang.org/x/crypto/chacha20poly1305\""), nil)
		}
		copyPath := filepath.Join(dir, filepath.Base(file)+".overlay")
		if err := os.WriteFile(copyPath, data, 0600); err != nil {
			t.Fatal(err)
		}
		overlay.Replace[file] = copyPath
	}
	if replaced == 0 {
		t.Fatal("actual SS AEAD constructor probe matched no product call")
	}
	probePath := filepath.Join(dir, "seal-probe.go.overlay")
	if err := os.WriteFile(probePath, []byte(ssSealProbeSource), 0600); err != nil {
		t.Fatal(err)
	}
	overlay.Replace[filepath.Join(root, "internal/agent/ssproxy/ss_probe_trace.go")] = probePath
	writePath := filepath.Join(root, "internal/agent/ssproxy/actor.go")
	input := writePath
	if replacement, ok := overlay.Replace[writePath]; ok {
		input = replacement
	}
	writeSource, err := os.ReadFile(input)
	if err != nil {
		t.Fatal(err)
	}
	writeSignature := []byte("n, err = sock.Write(p)")
	if bytes.Count(writeSource, writeSignature) != 1 {
		t.Fatal("actual SS Write probe matched no unique product IO owner")
	}
	// Count ciphertext writes only, for both the original netpoll wrapper and
	// the restored raw wrapper. Upstream plaintext progress is not a witness.
	writeSource = bytes.Replace(writeSource, writeSignature, []byte("if sock == a.client { n, err = ssProbeClientWrite(sock, p) } else { n, err = sock.Write(p) }"), 1)
	writeCopy := filepath.Join(dir, "raw-write.go.overlay")
	if err := os.WriteFile(writeCopy, writeSource, 0600); err != nil {
		t.Fatal(err)
	}
	overlay.Replace[writePath] = writeCopy
	writeProbe := filepath.Join(dir, "raw-probe.go.overlay")
	if err := os.WriteFile(writeProbe, []byte(ssShortWriteProbeSource), 0600); err != nil {
		t.Fatal(err)
	}
	overlay.Replace[filepath.Join(root, "internal/agent/ssproxy/ss_probe_write.go")] = writeProbe
	data, err := json.Marshal(overlay)
	if err != nil {
		t.Fatal(err)
	}
	overlayPath := filepath.Join(dir, "overlay.json")
	if err := os.WriteFile(overlayPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	binaryPath := filepath.Join(dir, "ss-probe")
	// Copied fixtures retain their build constraints; select the same suite
	// explicitly rather than relying on the outer go test flags to propagate.
	// origin: hot-update S5 plan §8 / T42; GATES-FINAL-2.
	cmd := exec.CommandContext(t.Context(), "go", "build", "-buildvcs=false", "-tags", "hotupdate_integration", "-race", "-mod=mod", "-overlay="+overlayPath, "-o", binaryPath, ".")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = time.Second
	defer func() {
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
	}()
	cmd.Dir = dir
	if output, err := testharness.CombinedOutput(cmd); err != nil {
		t.Fatalf("build instrumented real SS helper: %v\n%s", err, output)
	}
	return binaryPath
}
