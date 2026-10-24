//go:build linux

package clusterharness

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// Serving status can be sampled between SetPhase and the emitted readiness
// event. Only releaseOnReady owns the application gate: releasing it here from
// an earlier status sample lets OPEN finish and the last twenty records start
// before the very event the independent oracle uses as its ready boundary.
// origin: hot-update S6 plan §8.1 / T48; S8 internal review P13-COHORT F2
func awaitCohortReady(ctx context.Context, c *productCohort) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	select {
	case <-c.post:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("cohort readiness event was not observed: %w", ctx.Err())
	}
}

// Observation overlays add barriers and read-only receipts at real call sites.
// They never implement a cut, import, opener, retry or transport. GOFLAGS from
// the parent is merged first so production mutants reach the locally built role.
// origin: hot-update S6 plan §3.1 / §3.3 / T01 / T12 / T13
func productObservationOverlay(t *testing.T, repo, root string, victim int) string {
	t.Helper()
	replacement := map[string]string{}
	for _, flag := range strings.Fields(os.Getenv("GOFLAGS")) {
		if path, ok := strings.CutPrefix(flag, "-overlay="); ok {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var overlay struct{ Replace map[string]string }
			if err := json.Unmarshal(data, &overlay); err != nil {
				t.Fatal(err)
			}
			for from, to := range overlay.Replace {
				replacement[from] = to
			}
		}
	}
	read := func(name string) string {
		path := filepath.Join(repo, name)
		if replacement[path] != "" {
			path = replacement[path]
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	write := func(name, source string) {
		path := filepath.Join(root, strings.ReplaceAll(name, "/", "_")+".go")
		if err := os.WriteFile(path, []byte(source), 0600); err != nil {
			t.Fatal(err)
		}
		replacement[filepath.Join(repo, name)] = path
	}
	source := read("internal/broker/reexec.go")
	// Add each import the injected observer needs only when reexec.go does not
	// already import it (both merged producers rely on a different subset).
	for _, imp := range []struct{ after, add string }{
		{`"encoding/hex"`, `"encoding/json"`},
		{`"sort"`, `"sync"`},
		{`"time"`, `"strconv"`},
		{`"time"`, `"fmt"`},
		{`"time"`, `"golang.org/x/sys/unix"`},
	} {
		if !strings.Contains(source, imp.add) {
			source = strings.Replace(source, imp.after, imp.after+"\n "+imp.add, 1)
		}
	}
	source = strings.Replace(source, "b.reconcilers.start(b.cfg.Now())", `if _,faultErr:=os.Stat(filepath.Join(`+fmt.Sprintf("%q", root)+`,"active-fault.json")); faultErr==nil {
  s6ProductObserve("active-fault-entered",map[string]any{"generation":bundle.Generation})
  until:=time.Now().Add(5*time.Second)
  for { if _,err:=os.Stat(filepath.Join(`+fmt.Sprintf("%q", root)+`,"active-fault-release.json")); err==nil { break }; if time.Now().After(until) { return errors.New("product fault barrier expired") }; time.Sleep(time.Millisecond) }
  _=os.Remove(filepath.Join(`+fmt.Sprintf("%q", root)+`,"active-fault.json"))
  return errors.New("product injected ACTIVE refusal after new ACK and bytes")
 }
 b.reconcilers.start(b.cfg.Now())`, 1)
	source = strings.Replace(source, `emitBrokerImageEvent(b, "exec_enter", 0)`, `emitBrokerImageEvent(b, "exec_enter", 0)
 s6ProductObserve("exec", map[string]any{"seq":time.Now().UnixNano()})`, 1)
	productStoreOpenObservation(t, read, write)
	source = strings.Replace(source, "return brokerimage.NewSingleStoreForImage(stamp, file, d)", `store,err:=brokerimage.NewSingleStoreForImage(stamp,file,d)
 if err==nil { db,dbErr:=brokerimage.SingleStoreDB(store); if dbErr==nil { s6ProductBeforeReplay(db) } }
 return store,err`, 1)
	source += fmt.Sprintf(`
// Observation-only instrumentation in the private product artifact.
func s6ProductObserve(name string, value any) {
 data,err:=json.Marshal(value); if err!=nil { panic(err) }
 if err=os.WriteFile(filepath.Join(%q,name+".tmp"),data,0600); err!=nil { panic(err) }; if err=os.Rename(filepath.Join(%q,name+".tmp"),filepath.Join(%q,name+".json")); err!=nil { panic(err) }
}
func s6ProductClosed(b *Broker, seal storage.StoreSeal) {
 if _,err:=os.Stat(filepath.Join(%q,"closed.json")); err==nil { return }
 s6ProductObserve("closed",map[string]any{"seal":seal,"seq":time.Now().UnixNano()})
 until:=time.Now().Add(3*time.Second)
 for { if _,err:=os.Stat(filepath.Join(%q,"release.json")); err==nil { break }; if time.Now().After(until) { panic("closed-store observer was not released") }; time.Sleep(time.Millisecond) }
 revoked:=false
 if _,faultErr:=os.Stat(filepath.Join(%q,"active-fault.json")); faultErr!=nil { if _,preparedErr:=os.Stat(filepath.Join(s6ProductRoot(),"prepared-fault.json"));preparedErr!=nil {revoked=b.tunnelSrv.Load().CloseProxy(%d) } }
 s6ProductObserve("revoked",map[string]any{"effective":revoked})
}
func s6ProductBeforeReplay(db *sql.DB) {
 rows,err:=db.Query("SELECT fp,note FROM session_creators ORDER BY fp"); if err!=nil { panic(err) }
 values:=map[string]string{}
 for rows.Next() { var fp,note string; if err=rows.Scan(&fp,&note); err!=nil { panic(err) }; values[fp]=note }
 if err=rows.Close(); err!=nil { panic(err) }
 var cursor uint64
 err=db.QueryRow("SELECT value FROM cluster_meta WHERE key='applied_index'").Scan(&cursor)
 if err!=nil && err!=sql.ErrNoRows { panic(err) }
 s6ProductObserve("pre-replay",map[string]any{"rows":values,"cursor":cursor})
}
`, root, root, root, root, root, root, victim)
	source += `
// Kernel-only listener census; no image decoder supplies these identities.
func s6ProductListenerCookies() {
 entries,err:=os.ReadDir("/proc/self/fd");if err!=nil {panic(err)}
 var rows []listenerimage.Descriptor
 for _,entry:=range entries {
  fd,err:=strconv.Atoi(entry.Name());if err!=nil {continue}
  accepting,err:=unix.GetsockoptInt(fd,unix.SOL_SOCKET,unix.SO_ACCEPTCONN);if err!=nil || accepting!=1 {continue}
  address,err:=unix.Getsockname(fd);if err!=nil {continue}
  port,family:=0,unix.AF_INET;switch tcp:=address.(type) {case *unix.SockaddrInet4:port=tcp.Port;case *unix.SockaddrInet6:port=tcp.Port;family=unix.AF_INET6;default:continue}
  cookie,err:=unix.GetsockoptUint64(fd,unix.SOL_SOCKET,unix.SO_COOKIE);if err!=nil {panic(err)}
  var stat unix.Stat_t;if err=unix.Fstat(fd,&stat);err!=nil {panic(err)}
  flags,err:=unix.FcntlInt(uintptr(fd),unix.F_GETFL,0);if err!=nil {panic(err)}
  fdflags,err:=unix.FcntlInt(uintptr(fd),unix.F_GETFD,0);if err!=nil {panic(err)}
  rows=append(rows,listenerimage.Descriptor{ID:fmt.Sprintf("public-%d",port),Network:"tcp4",Address:fmt.Sprintf("127.0.0.1:%d",port),Device:uint64(stat.Dev),Inode:stat.Ino,Cookie:cookie,Family:family,SockType:unix.SOCK_STREAM,Nonblock:flags&unix.O_NONBLOCK!=0,CloseOnExec:fdflags&unix.FD_CLOEXEC!=0})
 }
 s6ProductObserve("cohort-listeners",rows)
}
var s6ProductCensusMu sync.Mutex
func s6ProductCensus(b *Broker) {
 s6ProductCensusMu.Lock();defer s6ProductCensusMu.Unlock()
 c:=b.imageControl
 s6ProductListenerCookies()
 if b.cl!=nil {s6ProductObserve("leader",map[string]any{"leader":b.cl.node.IsLeader()})}
 if _,err:=os.Stat(filepath.Join(s6ProductRoot(),"census-request.json"));err!=nil {return}
 if c==nil || !c.users.Begin() { return };defer c.users.End()
 stamp,err:=c.store.Stamp();if err!=nil {return}
 w,r:=storage.DefaultDriverRegistry().Connections(stamp.Identity)
 oracle:=map[string]string{}
 leader:=false
 if b.cl!=nil { leader=b.cl.node.IsLeader();oracle=cluster.S6ProductTimings(b.cl.node) }
 restored,restoreErr:=handoff.Restore()
 empty:=restored==nil && restoreErr==nil
 if restored!=nil { _=restored.Close() }
 _=os.Remove(filepath.Join(s6ProductRoot(),"census-request.json"))
 s6ProductObserve("census",map[string]any{"store":stamp,"driver_counts":[2]uint32{w,r},"leader":leader,"oracle_inputs":oracle,"second_restore_empty":empty,"seq":time.Now().UnixNano()})
}
`
	source += `
func s6ProductRoot() string {
 for i:=2;i<len(os.Args);i++ { name,value,inline:=strings.Cut(os.Args[i],"="); if name=="--db" { if !inline && i+1<len(os.Args) { value=os.Args[i+1] }; return filepath.Dir(value) } }; panic("product observer: database argument missing")
}
func s6ProductVictim() int { data,err:=os.ReadFile(filepath.Join(s6ProductRoot(),"victim-port.json")); if err!=nil { panic(err) }; var row struct{Port int}; if err=json.Unmarshal(data,&row); err!=nil { panic(err) }; return row.Port }
`
	source = strings.ReplaceAll(source, fmt.Sprintf("%q", root), "s6ProductRoot()")
	source = strings.Replace(source, fmt.Sprintf("CloseProxy(%d)", victim), "CloseProxy(s6ProductVictim())", 1)
	write("internal/broker/reexec.go", source)
	source = read("internal/broker/broker.go")
	source = strings.Replace(source, "runErr = brokercontrol.JoinCleanupError(runErr, brokerimage.CloseColdStore", `s6ProductObserve("run-defer",map[string]any{"entered":true,"generation":brokerImageSnapshot(b).Generation})
 runErr = brokercontrol.JoinCleanupError(runErr, brokerimage.CloseColdStore`, 1)
	write("internal/broker/broker.go", source)
	source = read("internal/tunnel/client_transport.go")
	source = strings.Replace(source, "var refusal *flow.Failure", `c.logger.Warn("product observer: redial refused", "err",err)
 var refusal *flow.Failure`, 1)
	write("internal/tunnel/client_transport.go", source)
	source = read("internal/broker/runtime_introspect.go")
	source = strings.Replace(source, "rep.BrokerImage = brokerImageSnapshot(b)", "s6ProductCensus(b)\n rep.BrokerImage = brokerImageSnapshot(b)", 1)
	write("internal/broker/runtime_introspect.go", source)
	source = read("internal/cluster/node.go")
	// The appended witness owns its import; cluster refactoring can remove an incidental production use.
	// origin: hot-update S6 plan §9.3 / CLUSTER-SLIM
	if !strings.Contains(source, `"strconv"`) {
		source = strings.Replace(source, `"time"`, "\"time\"\n\"strconv\"", 1)
	}
	source += `
// Read-only production timing witness in the private observation artifact.
func S6ProductTimings(n *Node) map[string]string {
 cfg:=raftConfig(n.cfg)
 return map[string]string{"raft_heartbeat_ns":strconv.FormatInt(int64(cfg.HeartbeatTimeout),10),"raft_election_ns":strconv.FormatInt(int64(cfg.ElectionTimeout),10),"raft_lease_ns":strconv.FormatInt(int64(cfg.LeaderLeaseTimeout),10)}
}

`
	write("internal/cluster/node.go", source)

	productOpeningObservation(read, write)
	productLifecycleObservation(read, write)
	path := filepath.Join(root, "observation-overlay.json")
	data, err := json.Marshal(map[string]any{"Replace": replacement})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

// Only the private agent artifact holds the approved original pre-SYN socket.
// No connect, codec, owner transition or reply is replaced by this barrier.
// origin: hot-update S6 plan §6.3 / T48
func productOpeningObservation(read func(string) string, write func(string, string)) {
	source := read("internal/tunnel/flow_client.go")
	// The observation owns its imports even when production moves its OS work.
	// origin: hot-update S6 plan §6.3 / T48
	if !strings.Contains(source, `"os"`) {
		source = strings.Replace(source, `"encoding/binary"`, "\"encoding/binary\"\n\"os\"", 1)
	}
	source = strings.Replace(source, "\"encoding/binary\"", "\"encoding/binary\"\n \"encoding/json\"\n \"path/filepath\"\n \"strconv\"\n \"golang.org/x/sys/unix\"", 1)
	source = strings.Replace(source, "observeFlowSocket(id, flow.EndpointID{}, tuple)", "s6ProductOpeningBound(port,tuple,stop)\n observeFlowSocket(id, flow.EndpointID{}, tuple)", 1)
	source += `
func s6ProductOpeningBound(port uint16,tuple rawsocket.Tuple,stop <-chan struct{}) {
 root,err:=os.Getwd();if err!=nil {panic(err)}
 data,err:=os.ReadFile(filepath.Join(root,"cohort-opening.json"));if os.IsNotExist(err) {return};if err!=nil {panic(err)}
 var config struct{Port int};if err=json.Unmarshal(data,&config);err!=nil {panic(err)};if config.Port!=int(port) {return}
 entries,err:=os.ReadDir("/proc/self/fd");if err!=nil {panic(err)};bound:=-1
 for _,entry:=range entries {fd,err:=strconv.Atoi(entry.Name());if err!=nil {continue};addr,err:=unix.Getsockname(fd);if err!=nil {continue};if tcp,ok:=addr.(*unix.SockaddrInet4);ok && tcp.Port==int(tuple.Local.Port()) {bound=fd;break}}
 if bound<0 {panic("opening observer: original bound socket absent")}
 data,err=json.Marshal(map[string]any{"fd":bound,"pid":os.Getpid(),"bound":true});if err!=nil {panic(err)}
 staging:=filepath.Join(root,"cohort-opening-held.tmp");if err=os.WriteFile(staging,data,0600);err!=nil {panic(err)};if err=os.Rename(staging,filepath.Join(root,"cohort-opening-held.json"));err!=nil {panic(err)}
 for {if _,err:=os.Stat(filepath.Join(root,"cohort-opening-release"));err==nil {return};select{case <-stop:return;case <-time.After(time.Millisecond):}}
}
`
	write("internal/tunnel/flow_client.go", source)
}

var productArtifact struct {
	sync.Mutex
	key  string
	data []byte
}

// The observation sources resolve each fixture's home at runtime. Reuse only
// identical build inputs, not the fixture's processes or mutable state. Hash
// replacement contents as well as the worktree so an inherited production
// mutant can never receive the unmutated executable. Keep bytes, not a live FD
// or a pathname whose cleanup belongs to a previous test round.
// origin: hot-update S6 plan §11 / T48 / T52
func productObservationBuild(ctx context.Context, repo, binary, sourceDigest, overlay string) error {
	data, err := os.ReadFile(overlay)
	if err != nil {
		return err
	}
	var replacements struct{ Replace map[string]string }
	if err := json.Unmarshal(data, &replacements); err != nil {
		return err
	}
	inputs := map[string]string{"source": sourceDigest, "flags": os.Getenv("GOFLAGS"), "repo": repo}
	for from, to := range replacements.Replace {
		if to == "" {
			inputs[from] = "deleted"
			continue
		}
		body, err := os.ReadFile(to)
		if err != nil {
			return err
		}
		inputs[from] = storeFixtureDigest(body)
	}
	data, err = json.Marshal(inputs)
	if err != nil {
		return err
	}
	key := storeFixtureDigest(data)
	productArtifact.Lock()
	defer productArtifact.Unlock()
	if productArtifact.key == key {
		return os.WriteFile(binary, productArtifact.data, 0700)
	}
	build := exec.CommandContext(ctx, "go", "build", "-race", "-trimpath", "-ldflags=-s -w", "-overlay", overlay, "-o", binary, "./cmd/tether")
	build.Dir = repo
	if output, err := testharness.CombinedOutput(build); err != nil {
		return fmt.Errorf("product build: %w: %s", err, output)
	}
	data, err = os.ReadFile(binary)
	if err != nil {
		return err
	}
	productArtifact.key, productArtifact.data = key, data
	return nil
}

// The barriers below only hold the real command before ownership and after Run
// returns. Stop still comes from SIGTERM through Run's current slot; the parent
// never closes the child's descriptors to manufacture a clean terminal census.
// origin: hot-update S6 plan §4.2 / T02 / T49 / T52
func (f *brokerProductFixture) awaitMarker(name string) error {
	until := time.Now().Add(30 * time.Second)
	response := f.preparedResponse
	for time.Now().Before(until) {
		if _, err := os.Stat(filepath.Join(f.root, name+".json")); err == nil {
			return nil
		}
		select {
		case err := <-response:
			// A refused reload cannot reach the next barrier. Keep the reply
			// for the checkpoint that owns it, including successful arming.
			// origin: S10 internal review MAKETEST-BROKER-IMAGE F2
			f.preparedResponse <- err
			response = nil
			if err != nil {
				return fmt.Errorf("product reload failed before %s: %w\n%s", name, err, f.logs.String())
			}
		case <-f.done:
			return fmt.Errorf("product exited before %s: %v\n%s", name, f.waitErr, f.logs.String())
		default:
		}
		time.Sleep(time.Millisecond)
	}
	return fmt.Errorf("product marker expired: %s", name)
}
