//go:build linux

package clusterharness

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/storage"
)

func (f *brokerProductFixture) lifecycleCheckpoint(label string) (BrokerImageReceipt, error) {
	switch label {
	case "runtime-idle":
		if err := f.awaitMarker(label); err != nil {
			return BrokerImageReceipt{}, err
		}
		return f.lifecycleReceipt(label, true)
	case "before-reload":
		if f.peer == nil {
			if err := os.WriteFile(filepath.Join(f.root, "idle-release.json"), []byte("{}"), 0600); err != nil {
				return BrokerImageReceipt{}, err
			}
			if err := f.startPeers(); err != nil {
				return BrokerImageReceipt{}, err
			}
		}
		if _, err := f.productCheckpoint(label); err != nil {
			return BrokerImageReceipt{}, err
		}
	case "active-new-ack-and-bytes":
		if err := os.WriteFile(filepath.Join(f.root, "active-observe-only.json"), []byte("{}"), 0600); err != nil {
			return BrokerImageReceipt{}, err
		}
		if _, err := f.productCheckpoint("reload-active-refusal"); err != nil {
			return BrokerImageReceipt{}, err
		}
		f.activeHeld = true
	case "active-rollback-control-ready":
		if !f.activeHeld {
			return BrokerImageReceipt{}, errors.New("product rollback has no ACTIVE barrier")
		}
		if err := os.WriteFile(filepath.Join(f.root, "active-fault-release.json"), []byte("{}"), 0600); err != nil {
			return BrokerImageReceipt{}, err
		}
		if _, err := f.waitStatus(3); err != nil {
			return BrokerImageReceipt{}, err
		}
		if err := errors.Join(f.exchange(), f.exchangeSS(), f.exchangeVictim()); err != nil {
			return BrokerImageReceipt{}, err
		}
		f.activeHeld = false
	case "prepared-revoke-barrier":
		if err := f.startPreparedCut(); err != nil {
			return BrokerImageReceipt{}, err
		}
		return f.lifecycleReceipt(label, true)
	case "hard-revoke-acked":
		if err := os.WriteFile(filepath.Join(f.root, "revoke-request.json"), []byte("{}"), 0600); err != nil {
			return BrokerImageReceipt{}, err
		}
		if err := f.awaitMarker("lifecycle-revoked"); err != nil {
			return BrokerImageReceipt{}, err
		}
		if err := f.victimPeer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			return BrokerImageReceipt{}, err
		}
		n, err := f.victimPeer.Read(make([]byte, 1))
		if n != 0 || !errors.Is(err, io.EOF) {
			return BrokerImageReceipt{}, errors.New("revoked original victim peer has no EOF")
		}
		return f.lifecycleReceipt(label, true)
	case "revoke-next-prepared-control-ready":
		if err := os.WriteFile(filepath.Join(f.root, "prepared-release.json"), []byte("{}"), 0600); err != nil {
			return BrokerImageReceipt{}, err
		}
		if err := <-f.preparedResponse; err != nil {
			return BrokerImageReceipt{}, err
		}
		if _, err := f.waitStatus(2); err != nil {
			return BrokerImageReceipt{}, err
		}
		if err := errors.Join(f.exchange(), f.exchangeSS()); err != nil {
			return BrokerImageReceipt{}, err
		}
	case "exec-error-new-ack":
		if _, err := f.productCheckpoint("reload-control-ready"); err != nil {
			return BrokerImageReceipt{}, err
		}
	case "normal-stop":
		if err := f.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			return BrokerImageReceipt{}, err
		}
		if err := f.awaitMarker("runtime-stopped"); err != nil {
			return BrokerImageReceipt{}, err
		}
		return f.lifecycleReceipt(label, true)
	default:
		return BrokerImageReceipt{}, fmt.Errorf("product lifecycle: unobserved checkpoint %s", label)
	}
	return f.lifecycleReceipt(label, false)
}

type productOwnedFD struct {
	ID     string `json:"id"`
	Kind   string `json:"kind"`
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
	Count  int    `json:"count"`
}

func (f *brokerProductFixture) lifecycleReceipt(label string, dormant bool) (BrokerImageReceipt, error) {
	var state map[string]any
	if dormant {
		marker := "runtime-idle"
		if label == "normal-stop" {
			marker = "runtime-stopped"
		}
		if label == "prepared-revoke-barrier" {
			marker = "lifecycle-prepared"
		}
		if label == "hard-revoke-acked" {
			marker = "lifecycle-revoked"
		}
		data, err := os.ReadFile(filepath.Join(f.root, marker+".json"))
		if err != nil {
			return BrokerImageReceipt{}, err
		}
		if err = json.Unmarshal(data, &state); err != nil {
			return BrokerImageReceipt{}, err
		}
	} else {
		var err error
		state, err = f.lifecycleCensus()
		if err != nil {
			return BrokerImageReceipt{}, err
		}
	}
	if message, ok := state["error"].(string); ok && message != "" {
		return BrokerImageReceipt{}, errors.New(message)
	}
	pid := f.cmd.Process.Pid
	ticks, err := storeFixtureStartTicks(pid)
	if err != nil {
		return BrokerImageReceipt{}, err
	}
	sha, err := storeFixtureHash(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil {
		return BrokerImageReceipt{}, err
	}
	state["mode"], state["running_sha256"] = f.topology, sha
	var objects []productOwnedFD
	if data, err := json.Marshal(state["owned_fds"]); err == nil {
		if err = json.Unmarshal(data, &objects); err != nil {
			return BrokerImageReceipt{}, err
		}
	}
	entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil {
		return BrokerImageReceipt{}, err
	}
	for i := range objects {
		objects[i].Count = 0
		for _, entry := range entries {
			var stat syscall.Stat_t
			if syscall.Stat(fmt.Sprintf("/proc/%d/fd/%s", pid, entry.Name()), &stat) == nil && uint64(stat.Dev) == objects[i].Device && stat.Ino == objects[i].Inode {
				objects[i].Count++
			}
		}
		if objects[i].Count == 0 {
			return BrokerImageReceipt{}, fmt.Errorf("product owned object disappeared before %s receipt: %+v", label, objects[i])
		}
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].ID < objects[j].ID })
	state["owned_fds"] = objects
	if !dormant {
		data, err := os.ReadFile(filepath.Join(f.root, "census.json"))
		if err != nil {
			return BrokerImageReceipt{}, err
		}
		var census map[string]any
		if err = json.Unmarshal(data, &census); err != nil {
			return BrokerImageReceipt{}, err
		}
		state["store"] = census["store"]
		rows, err := f.queryProductACKs()
		if err != nil {
			return BrokerImageReceipt{}, err
		}
		state["ack_rows"] = rows
		var newIDs []string
		for _, id := range []string{"current", "candidate"} {
			if f.ackIDs[id] != "" {
				newIDs = append(newIDs, id)
			}
		}
		state["new_ack_ids"] = newIDs
		if label == "before-reload" {
			state["new_ack_ids"] = []string{}
		}
		state["ack_read_stage"] = "before-replay"
		if label == "active-rollback-control-ready" {
			data, err := os.ReadFile(filepath.Join(f.root, "pre-replay.json"))
			if err != nil {
				return BrokerImageReceipt{}, err
			}
			var gate struct{ Rows map[string]string }
			if err = json.Unmarshal(data, &gate); err != nil {
				return BrokerImageReceipt{}, err
			}
			for id, fp := range f.ackIDs {
				if note := gate.Rows[fp]; note != "" && note != f.acknowledged[fp] {
					return BrokerImageReceipt{}, fmt.Errorf("pre-replay ACK changed: %s", id)
				}
			}
			// The independent pre-replay query must contain every candidate ACK.
			for _, row := range rows {
				if gate.Rows[f.ackIDs[row.RequestID]] != row.ValueSHA {
					return BrokerImageReceipt{}, errors.New("candidate ACK absent before previous replay")
				}
			}
		}
		f.lifecycleState = state
	} else if label == "normal-stop" && f.lifecycleState != nil {
		data, err := json.Marshal(f.lifecycleState["store"])
		if err != nil {
			return BrokerImageReceipt{}, err
		}
		var previous storage.StoreStamp
		if err = json.Unmarshal(data, &previous); err != nil {
			return BrokerImageReceipt{}, err
		}
		db, err := storage.OpenReadOnly(previous.Identity.Path)
		if err != nil {
			return BrokerImageReceipt{}, err
		}
		stamp, stampErr := storage.ReadStoreStamp(db, previous.Identity)
		closeErr := db.Close()
		if err = errors.Join(stampErr, closeErr); err != nil {
			return BrokerImageReceipt{}, err
		}
		rows, err := f.queryProductACKs()
		if err != nil {
			return BrokerImageReceipt{}, err
		}
		state["store"], state["ack_rows"] = stamp, rows
		// Compare the independent post-close read to the cursor observed after
		// Raft joined, not to an earlier live sample while commits could advance.
		// origin: hot-update S6 plan §4.2 / T52
		var terminalIndex uint64
		if f.topology != "single" {
			data, err := os.ReadFile(filepath.Join(f.root, "terminal-index.json"))
			if err != nil {
				return BrokerImageReceipt{}, err
			}
			if err := json.Unmarshal(data, &terminalIndex); err != nil {
				return BrokerImageReceipt{}, err
			}
		}
		state["terminal_index"] = terminalIndex
	}
	state["unix_path"] = filepath.Join(f.root, "admin.sock")
	state["rollback_boundary"] = "ACTIVE"
	if label == "revoke-next-prepared-control-ready" {
		data, err := os.ReadFile(filepath.Join(f.root, "lifecycle-revoked.json"))
		if err != nil {
			return BrokerImageReceipt{}, err
		}
		var revoked map[string]any
		if err = json.Unmarshal(data, &revoked); err != nil {
			return BrokerImageReceipt{}, err
		}
		state["old_resume_refused"] = revoked["old_resume_refused"]
		data, err = os.ReadFile(filepath.Join(f.root, "next-prepare.json"))
		if err != nil {
			return BrokerImageReceipt{}, err
		}
		var prepared map[string]any
		if err = json.Unmarshal(data, &prepared); err != nil {
			return BrokerImageReceipt{}, err
		}
		state["next_prepare_seq"] = prepared["seq"]
		data, err = os.ReadFile(filepath.Join(f.root, "exec.json"))
		if err != nil {
			return BrokerImageReceipt{}, err
		}
		var execution map[string]any
		if err = json.Unmarshal(data, &execution); err != nil {
			return BrokerImageReceipt{}, err
		}
		state["exec_seq"] = execution["seq"]
	}
	value := map[string]any{
		"claim": label, "test_or_arm": "broker-lifecycle", "command": "private broker-product serve", "native_rc": 0, "verdict": "GREEN",
		"source_sha": f.sourceSHA, "artifact_sha": sha, "previous_sha": f.previousSHA, "pid": pid, "startticks": ticks,
		"wall_seconds": time.Since(f.started).Seconds(), "phase_intersections": []any{}, "oracle_inputs": map[string]any{"kernel": "/proc", "checkpoint": label}, "mutation_digest": "",
		"stdout_path": "product.stdout", "stderr_path": "product.stderr", "log_mapping": map[string]string{"observer": "lifecycle-census.json"}, "counters": state,
	}
	data, err := json.Marshal(value)
	if err != nil {
		return BrokerImageReceipt{}, err
	}
	return BrokerImageReceipt{Label: label, JSON: data, Digest: storeFixtureDigest(data)}, nil
}

// A temporary fresh-connect probe must finish its real protocol retirement
// before a live FD census. Otherwise one-core scheduling consistently samples
// its raw identity just before the last pump closes it. The full original-flow
// identity and byte oracle still runs on the resulting independent snapshot.
// origin: hot-update S6 plan §9.3 / T52
func (f *brokerProductFixture) lifecycleCensus() (map[string]any, error) {
	waitFresh := len(f.freshPeers) != 0
	for _, peer := range f.freshPeers {
		_ = peer.Close()
	}
	f.freshPeers = nil
	rawIDs := func(state map[string]any) (map[string]bool, error) {
		data, err := json.Marshal(state["owned_fds"])
		if err != nil {
			return nil, err
		}
		var objects []productOwnedFD
		if err := json.Unmarshal(data, &objects); err != nil {
			return nil, err
		}
		ids := make(map[string]bool)
		for _, object := range objects {
			if object.Kind == "raw" {
				ids[object.ID] = true
			}
		}
		return ids, nil
	}
	previous, err := rawIDs(f.lifecycleState)
	if err != nil {
		return nil, err
	}
	until := time.Now().Add(5 * time.Second)
	for {
		if time.Now().After(until) {
			return nil, errors.New("temporary fresh flow did not retire before lifecycle census")
		}
		if err := os.WriteFile(filepath.Join(f.root, "census-request.json"), []byte("{}"), 0600); err != nil {
			return nil, err
		}
		if _, err := (&adminsock.Client{Path: filepath.Join(f.root, "admin.sock"), Timeout: time.Until(until)}).Call(adminsock.Request{Op: adminsock.OpRuntime}); err != nil {
			return nil, err
		}
		data, err := os.ReadFile(filepath.Join(f.root, "lifecycle-census.json"))
		if err != nil {
			return nil, err
		}
		var state map[string]any
		if err := json.Unmarshal(data, &state); err != nil {
			return nil, err
		}
		current, err := rawIDs(state)
		if err != nil {
			return nil, err
		}
		// Revocation may remove an original flow during the same cut. A count
		// can therefore balance that removal against the still-live probe;
		// only its absence by identity proves retirement.
		// origin: hot-update S6 plan §4.2 / T02 / T52
		retired := true
		for id := range current {
			retired = retired && previous[id]
		}
		if !waitFresh || retired {
			return state, nil
		}
		time.Sleep(time.Millisecond)
	}
}

type productACKRow struct {
	RequestID string `json:"request_id"`
	ValueSHA  string `json:"value_sha256"`
	Effects   uint32 `json:"effects"`
}

func (f *brokerProductFixture) queryProductACKs() (result []productACKRow, resultErr error) {
	db, err := storageProductReadOnly(filepath.Join(f.root, "tether.db"))
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, db.Close()) }()
	for id, fp := range f.ackIDs {
		var note string
		if err := db.QueryRow("SELECT note FROM session_creators WHERE fp=?", fp).Scan(&note); err != nil {
			return nil, err
		}
		result = append(result, productACKRow{RequestID: id, ValueSHA: note, Effects: 1})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].RequestID < result[j].RequestID })
	return result, nil
}

// All injected reads remain under the real owner locks. No freeze/export is
// used as a sampler: observing a live flow cannot mutate its carrier lineage.
func productLifecycleObservation(read func(string) string, write func(string, string)) {
	source := read("internal/tunnel/server_image.go")
	source += `
func S6ProductLiveServer(s *Server) (ServerImage,[]map[string]any,uint32,uint32) {
 if s==nil{return ServerImage{},nil,0,0}
 s.mu.Lock();defer s.mu.Unlock()
 im:=ServerImage{};var fds []map[string]any
 if s.flowState==nil{return im,nil,uint32(len(s.monitors)),uint32(len(s.callbacks))}
 st:=s.flowState
 im.IssuerBootID,im.InstanceID,im.OwnerGeneration=st.cfg.BootID,st.cfg.InstanceID,st.cfg.OwnerGen
 for _,e:=range st.endpoints {
  if e.phase>=EndpointPhaseClosing{continue}
  endpoint:=EndpointImage{ID:e.id,OwnerGen:e.ownerGen,CarrierGen:e.carrierGen}
  for _,r:=range flow.States(st.manager,e.id){ endpoint.FlowIDs=append(endpoint.FlowIDs,r.ID) }
  im.Endpoints=append(im.Endpoints,endpoint)
  if e.keeper!=nil { d:=e.keeper.Descriptor();fds=append(fds,map[string]any{"id":d.ID,"kind":"listener","device":d.Device,"inode":d.Inode}) }
 }
 sort.Slice(im.Endpoints,func(i,j int)bool{return bytes.Compare(im.Endpoints[i].ID[:],im.Endpoints[j].ID[:])<0})
 flows,fdsFlow:=flow.S6ProductLiveFlows(st.manager)
 im.Flows=flows
 fds=append(fds,fdsFlow...)
 return im,fds,uint32(len(s.monitors)),uint32(len(s.callbacks))
}
`
	// Cold public views may not have keepers until the first cut; identify their
	// listener OFD through SyscallConn without capture or creating a duplicate.
	source = strings.Replace(source, "if e.keeper!=nil { d:=e.keeper.Descriptor();fds=append(fds,map[string]any{\"id\":d.ID,\"kind\":\"listener\",\"device\":d.Device,\"inode\":d.Inode}) }", `if e.keeper!=nil { d:=e.keeper.Descriptor();fds=append(fds,map[string]any{"id":d.ID,"kind":"listener","device":d.Device,"inode":d.Inode}) } else if native,ok:=e.listener.(interface{SyscallConn()(syscall.RawConn,error)});ok { raw,err:=native.SyscallConn();if err==nil { _=raw.Control(func(fd uintptr){ var stat syscall.Stat_t;if syscall.Fstat(int(fd),&stat)==nil {fds=append(fds,map[string]any{"id":endpointListenerRef(e.id),"kind":"listener","device":uint64(stat.Dev),"inode":stat.Ino})} }) } }`, 1)
	if !strings.Contains(source, "\"syscall\"") {
		source = strings.Replace(source, "\"sync\"", "\"sync\"\n\"syscall\"", 1)
	}
	source = strings.Replace(source, "\"sync\"", "\"sync\"\n\"sync/atomic\"\n\"time\"", 1)
	source += `
var s6CallbackEntered,s6CallbackJoined atomic.Uint64
func S6ProductFiredCallback(s *Server) {
 work:=&endpointTimer{done:make(chan struct{})};s.mu.Lock();if s.callbacks==nil{s.callbacks=map[*endpointTimer]struct{}{}};s.callbacks[work]=struct{}{};s.mu.Unlock()
 entered:=make(chan struct{})
 time.AfterFunc(0,func(){s6CallbackEntered.Store(uint64(time.Now().UnixNano()));close(entered);time.Sleep(20*time.Millisecond);s6CallbackJoined.Store(uint64(time.Now().UnixNano()));finishEndpointTimer(s,work)})
 <-entered
}
func S6ProductCallbackSequences()(uint64,uint64){return s6CallbackEntered.Load(),s6CallbackJoined.Load()}
func S6ProductVictimIDs(s *Server,port int)map[string]bool {
 s.mu.Lock();defer s.mu.Unlock();out:=map[string]bool{};e:=s.flowState.endpoints[port];if e==nil{return out};out[endpointListenerRef(e.id)]=true;for _,r:=range flow.States(s.flowState.manager,e.id){out["flow."+hex.EncodeToString(r.ID[:])]=true};return out
}
func S6ProductRevokeVictim(s *Server,port int)map[string]bool {
 s.mu.Lock();e:=s.flowState.endpoints[port];if e==nil{s.mu.Unlock();return nil}
 req:=resumeRequest{Scope:flow.ResumeScopeEndpoint,Mode:flow.ResumeModeReconnect,RequestID:[16]byte{71},Endpoint:e.id,Owner:e.ownerGen,Target:e.targetPort,Ticket:append([]byte(nil),e.ticket...)}
 var file *os.File
 for _,r:=range flow.States(s.flowState.manager,e.id){file=flow.S6ProductBorrowRaw(s.flowState.manager,r.ID);if file!=nil{break}}
 s.mu.Unlock()
 effective:=s.CloseProxy(port)
 _,_,resumeErr:=resumeEndpoint(s,e,req,nil,time.Now().Add(time.Second),nil)
 refused:=false;if file!=nil{n,err:=file.Write([]byte("revoked"));refused=n==0&&err!=nil}
 return map[string]bool{"effective":effective,"old_resume_refused":resumeErr!=nil,"victim_write_refused":refused}
}
`
	// The private observer cannot rely on an incidental production import.
	// origin: hot-update S6 plan §6.3 / T48
	if !strings.Contains(source, `"os"`) {
		source = strings.Replace(source, "import (", "import (\n\"os\"", 1)
	}
	write("internal/tunnel/server_image.go", source)
	source = read("internal/flow/manager.go")
	source += `
func S6ProductLiveFlows(m *Manager) (Image,[]map[string]any) {
 im:=Image{Version:Version};var fds []map[string]any
 if m==nil||m.state==nil{return im,nil}
 st:=m.state;st.mu.Lock();defer st.mu.Unlock()
 for _,r:=range st.records {
  r.mu.Lock()
  if !r.closed {
   v:=stateLocked(r);ref:="flow."+hex.EncodeToString(v.ID[:])
   im.Records=append(im.Records,RecordImage{ID:v.ID,OpenSeq:v.OpenSeq,Intent:v.Intent,Phase:v.Phase,TxBase:v.TxBase,TxEnd:v.TxEnd,RxApplied:v.RxApplied,RxReceived:v.RxReceived,TxFIN:v.TxFIN,RxFIN:v.RxFIN,FINFlags:v.FINFlags,RawRef:ref,ReplayRef:ref})
   if r.raw!=nil {fp:=r.raw.Fingerprint();fds=append(fds,map[string]any{"id":ref,"kind":"raw","device":fp.Dev,"inode":fp.Ino})}
  }
  r.mu.Unlock()
 }
 sort.Slice(im.Records,func(i,j int)bool{return bytes.Compare(im.Records[i].ID[:],im.Records[j].ID[:])<0})
 return im,fds
}
`
	source = strings.Replace(source, "import (", "import (\n\"bytes\"\n\"encoding/hex\"", 1)
	source += `
func S6ProductBorrowRaw(m *Manager,id ID)*os.File {st:=m.state;st.mu.Lock();defer st.mu.Unlock();r:=st.records[id];if r==nil{return nil};r.mu.Lock();defer r.mu.Unlock();file,_:=rawsocket.BorrowHeldFile(r.raw);return file}
`
	write("internal/flow/manager.go", source)
	// Follow the preparation owner after the S7 responsibility move. The
	// observer hook exists only in the instrumented binary and retains the
	// broker control through the real wrapper instead of reviving its old type.
	// origin: hot-update S7 plan §11.3 / S6 TESTFIX moved Run regressions
	source = read("internal/xferbridge/bindings.go")
	source = strings.Replace(source, "type Cut struct {", "type Cut struct {\n S6ProductPrepared func(brokerimage.Bundle, *handoff.Prepared)\n S6ProductExported func(brokerimage.Bundle)", 1)
	source = strings.Replace(source, "return c.project(bundle, resources)", `bundle,resources,err=c.project(bundle,resources)
 if err==nil && c.S6ProductExported!=nil { c.S6ProductExported(bundle) }
 return bundle,resources,err`, 1)
	prepareStart := strings.Index(source, "func (c *Cut) Prepare(")
	if prepareStart < 0 {
		panic("product observer: transfer cut Prepare anchor missing")
	}
	prepareEnd := strings.Index(source[prepareStart:], "func (c *Cut) Thaw(")
	if prepareEnd < 0 {
		panic("product observer: transfer cut Thaw anchor missing")
	}
	prepareEnd += prepareStart
	prepare := source[prepareStart:prepareEnd]
	prepare = strings.Replace(prepare, "return brokerimage.PrepareFinal", "var finalBundle brokerimage.Bundle\n prepared,prepareErr:=brokerimage.PrepareFinal", 1)
	prepare = strings.Replace(prepare, "c.state = nil", "c.state = nil\n finalBundle=bundle", 1)
	prepare = strings.Replace(prepare, "\n\t})\n}", "\n\t})\n if prepareErr==nil && c.S6ProductPrepared!=nil {c.S6ProductPrepared(finalBundle,prepared)}\n return prepared,prepareErr\n}", 1)
	source = source[:prepareStart] + prepare + source[prepareEnd:]
	write("internal/xferbridge/bindings.go", source)
	source = read("internal/broker/reexec.go")
	source = strings.Replace(source, "c.cut = wrapped", "wrapped.S6ProductPrepared = func(bundle brokerimage.Bundle, prepared *handoff.Prepared) { s6ProductPrepared(c,cut,bundle,prepared) }\n wrapped.S6ProductExported = func(bundle brokerimage.Bundle) { s6ProductClosed(c.broker,bundle.Seal) }\n c.cut = wrapped", 1)
	source = strings.Replace(source, "cut, err := brokerimage.Freeze(cfg)", `if _,err:=os.Stat(filepath.Join(s6ProductRoot(),"prepared-fault.json"));err==nil {tunnel.S6ProductFiredCallback(cfg.Server)}
 cut, err := brokerimage.Freeze(cfg)`, 1)
	source += `
func s6ProductPrepared(c *brokerImageControl,cut brokerimage.Cut,bundle brokerimage.Bundle,p *handoff.Prepared) {
 root:=s6ProductRoot();if _,err:=os.Stat(filepath.Join(root,"prepared-fault.json"));err!=nil{return}
 if _,err:=os.Stat(filepath.Join(root,"lifecycle-prepared.json"));err==nil{s6ProductObserve("next-prepare",map[string]any{"seq":time.Now().UnixNano()});return}
 b:=c.broker
 s6ProductLifecycle(b,false)
 data,err:=os.ReadFile(filepath.Join(root,"lifecycle-census.json"));if err!=nil{panic(err)};var state map[string]any;if err=json.Unmarshal(data,&state);err!=nil{panic(err)}
 state["store"]=bundle.Store
 im,objects,_,_:=tunnel.S6ProductLiveServer(b.tunnelSrv.Load());_ =im
 rawIDs,listenIDs:=[]string{},[]string{}
 victim:=tunnel.S6ProductVictimIDs(b.tunnelSrv.Load(),s6ProductVictim())
 for _,obj:=range objects {id:=obj["id"].(string);if victim[id] {if obj["kind"]=="raw" {rawIDs=append(rawIDs,id)} else if obj["kind"]=="listener" {listenIDs=append(listenIDs,id)}}}
 state["victim_raw_ids"],state["victim_listener_ids"]=rawIDs,listenIDs
 entered,joined:=tunnel.S6ProductCallbackSequences();state["callback_entered_seq"],state["callback_joined_seq"]=entered,joined
 state["prepare_seq"]=time.Now().UnixNano()
 s6ProductObserve("lifecycle-prepared",state)
 until:=time.Now().Add(3*time.Second)
 for {if _,err:=os.Stat(filepath.Join(root,"revoke-request.json"));err==nil{break};if time.Now().After(until){panic("prepared revoke observer expired")};time.Sleep(time.Millisecond)}
 revoke:=tunnel.S6ProductRevokeVictim(b.tunnelSrv.Load(),s6ProductVictim())
 staleErr:=brokerimage.ExecCommit(cut,p,func(brokerimage.Bundle)error{return errors.New("stale Prepared reached exec validation")},bundle.Args[0],bundle.Args,nil)
 s6ProductLifecycle(b,false)
 data,err=os.ReadFile(filepath.Join(root,"lifecycle-census.json"));if err!=nil{panic(err)};if err=json.Unmarshal(data,&state);err!=nil{panic(err)}
 state["store"],state["revoke_ack_seq"],state["stale_exec_refused"]=bundle.Store,time.Now().UnixNano(),errors.Is(staleErr,brokerimage.ErrCutChanged)
 state["old_resume_refused"],state["victim_write_refused"],state["victim_peer_eof"]=revoke["old_resume_refused"],revoke["victim_write_refused"],revoke["effective"]
 s6ProductObserve("lifecycle-revoked",state)
 for {if _,err:=os.Stat(filepath.Join(root,"prepared-release.json"));err==nil{return};if time.Now().After(until){panic("prepared revoke release expired")};time.Sleep(time.Millisecond)}
}
`
	source = strings.Replace(source, "s6ProductObserve(\"census\",", "s6ProductLifecycle(b,false)\n s6ProductObserve(\"census\",", 1)
	source += `
func S6ProductStopped(b *Broker,err error) {
 if _,e:=os.Stat(filepath.Join(s6ProductRoot(),"lifecycle-mode.json"));e!=nil{return}
 s6ProductLifecycle(b,true)
 if err!=nil&&!errors.Is(err,context.Canceled){s6ProductObserve("runtime-stopped",map[string]any{"error":err.Error()})}
}
func s6ProductLifecycle(b *Broker,stopped bool) {
 if _,err:=os.Stat(filepath.Join(s6ProductRoot(),"lifecycle-mode.json"));err!=nil{return}
 c:=b.imageControl;if c==nil{return}
 status:=brokerImageSnapshot(b)
 im,fds,monitors,callbacks:=tunnel.S6ProductLiveServer(b.tunnelSrv.Load())
 if !stopped {file,err:=c.store.LockResource();if err==nil {var stat syscall.Stat_t;if syscall.Fstat(int(file.Files["lock"].Fd()),&stat)==nil{fds=append(fds,map[string]any{"id":file.ID,"kind":"lock","device":uint64(stat.Dev),"inode":stat.Ino})}}}
 value:=map[string]any{"generation":status.Generation,"cut_id":status.CutID,"old_defer_count":0,"owned_fds":fds,"active_monitors":monitors,"active_callbacks":callbacks,"tunnel":im,"nc_closed":b.nc.Load()==nil||b.nc.Load().IsClosed(),"node_closed":b.cl==nil}
 var rpcs,backups uint32
 if b.cl!=nil {rpcs,backups=cluster.S6ProductNodeCounts(b.cl.node)}
 value["active_rpcs"],value["active_backups"]=rpcs,backups

 if data,err:=os.ReadFile(filepath.Join(s6ProductRoot(),"run-defer.json"));err==nil {var old struct{Generation uint64};if json.Unmarshal(data,&old)!=nil||old.Generation!=status.Generation||!stopped {value["old_defer_count"]=1}}
 if !stopped {for _,k:=range c.keepers {d:=k.Descriptor();fds=append(fds,map[string]any{"id":d.ID,"kind":"listener","device":d.Device,"inode":d.Inode})};value["owned_fds"]=fds}
 if stopped {value["stop_current_calls"]=brokerimage.S6ProductStopCount();if b.cl!=nil{value["node_closed"]=cluster.S6ProductClosed(b.cl.node)}} else {stamp,err:=c.store.Stamp();if err!=nil{return};w,r:=storage.DefaultDriverRegistry().Connections(stamp.Identity);value["driver_writers"],value["driver_readers"]=w,r}
 if stopped {data,err:=os.ReadFile(filepath.Join(s6ProductRoot(),"census.json"));if err!=nil{panic(err)};var prior struct{Store storage.StoreStamp};if err=json.Unmarshal(data,&prior);err!=nil{panic(err)};w,r:=storage.DefaultDriverRegistry().Connections(prior.Store.Identity);value["driver_writers"],value["driver_readers"]=w,r}
 name:="lifecycle-census";if stopped{name="runtime-stopped"};s6ProductObserve(name,value)
}
`
	if !strings.Contains(source, "\"syscall\"") {
		source = strings.Replace(source, "\"sync\"", "\"sync\"\n\"syscall\"", 1)
	}
	source = strings.Replace(source, "until:=time.Now().Add(5*time.Second)", "until:=time.Now().Add(30*time.Second)", 1)
	source = strings.Replace(source, "setBrokerImagePhase(b, proto.BrokerImagePhaseServing)\n\t\treturn nil", `setBrokerImagePhase(b, proto.BrokerImagePhaseServing)
 s6ProductObserve("exec-recovered",map[string]any{"reopens":1})
 return nil`, 1)
	source = strings.Replace(source, "name:=\"lifecycle-census\";", `if _,err:=os.Stat(filepath.Join(s6ProductRoot(),"exec-error.json"));err==nil {value["exec_errors"]=1;value["current_reopens"]=1}
 name:="lifecycle-census";`, 1)
	write("internal/broker/reexec.go", source)
	source = read("internal/brokerimage/runtime.go")
	source = strings.Replace(source, "\"sync\"", "\"sync\"\n\"sync/atomic\"", 1)
	source = strings.Replace(source, "r.stopMu.Lock()", "s6ProductStopCalls.Add(1)\n r.stopMu.Lock()", 1)
	source += `
var s6ProductStopCalls atomic.Uint32
func S6ProductStopCount()uint32{return s6ProductStopCalls.Load()}
`
	write("internal/brokerimage/runtime.go", source)
	source = read("internal/brokerimage/single_store.go")
	source = strings.Replace(source, "\"encoding/hex\"", "\"encoding/hex\"\n\"encoding/json\"", 1)
	if !strings.Contains(source, "\"path/filepath\"") {
		source = strings.Replace(source, "\"os\"", "\"os\"\n\"path/filepath\"", 1)
	}
	source = strings.Replace(source, "next := &singleStoreOwner", `if db!=nil&&err==nil {
  rows,readErr:=db.Query("SELECT fp,note FROM session_creators ORDER BY fp");if readErr!=nil{panic(readErr)}
  values:=map[string]string{};for rows.Next(){var fp,note string;if readErr=rows.Scan(&fp,&note);readErr!=nil{panic(readErr)};values[fp]=note};if readErr=rows.Close();readErr!=nil{panic(readErr)}
  data,_:=json.Marshal(map[string]any{"rows":values,"cursor":stamp.AppliedIndex});if readErr=os.WriteFile(filepath.Join(stamp.Identity.DataDir,"pre-replay.json"),data,0600);readErr!=nil{panic(readErr)}
 }
 next := &singleStoreOwner`, 1)
	write("internal/brokerimage/single_store.go", source)
	source = read("internal/brokerimage/cut.go")
	source = strings.Replace(source, "\"reflect\"", "\"reflect\"\n\"path/filepath\"", 1)
	source = strings.Replace(source, "return prepared.Exec(path, args, handoff.CleanEnvironment(env))", `root:=c.preparedBundle.Store.Identity.DataDir
 if _,err:=os.Stat(filepath.Join(root,"exec-error-fault.json"));err==nil {
  _=os.Remove(filepath.Join(root,"exec-error-fault.json"))
  err=prepared.Exec(filepath.Join(root,"missing-product-executable"),args,handoff.CleanEnvironment(env))
  if err==nil{return errors.New("injected syscall unexpectedly succeeded")}
  if writeErr:=os.WriteFile(filepath.Join(root,"exec-error.json"),[]byte("{}"),0600);writeErr!=nil{return writeErr}
  return err
 }
 return prepared.Exec(path,args,handoff.CleanEnvironment(env))`, 1)
	write("internal/brokerimage/cut.go", source)
	source = read("internal/cluster/node.go")
	source += `
func S6ProductClosed(n *Node) bool {if n==nil{return true};life:=n.image;life.shutdownMu.Lock();defer life.shutdownMu.Unlock();return life.shutdownDone&&life.shutdownErr==nil}
func S6ProductNodeCounts(n *Node)(uint32,uint32) {if n==nil{return 0,0};if transport,ok:=n.transport.(*imageNetworkTransport);ok{return transport.tracker.Counts()};panic("product node has no RPC/backup tracker")}
`
	write("internal/cluster/node.go", source)
	// Read only after Raft/transport have joined, immediately before writer
	// close. This witnesses the final cursor without fencing or changing work.
	// origin: hot-update S6 plan §4.2 / T52
	source = read("internal/cluster/production.go")
	source = strings.Replace(source, "errs = append(errs, closeNodeImageObject(life, \"writer\", &life.seal.Writer, n.db.Close))", `index,observeErr:=readAppliedIndexDB(n.db);if observeErr!=nil{panic(observeErr)}
 if observeErr=os.WriteFile(n.cfg.DataDir+"/terminal-index.json",[]byte(strconv.FormatUint(index,10)),0600);observeErr!=nil{panic(observeErr)}
 errs = append(errs, closeNodeImageObject(life, "writer", &life.seal.Writer, n.db.Close))`, 1)
	write("internal/cluster/production.go", source)
	source = read("cmd/tether/serve.go")
	source = strings.Replace(source, "\"io\"", "\"io\"\n\"net\"", 1)
	source = strings.Replace(source, "var store brokerimage.StoreOwner", `if brokerBoot==nil {s6ProductIdle(dbPath)}
 var store brokerimage.StoreOwner`, 1)
	source = strings.Replace(source, "if err := b.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {", `runResult:=b.Run(ctx)
 broker.S6ProductStopped(b,runResult)
 s6ProductAfterRun(dbPath)
 if err := runResult; err != nil && !errors.Is(err, context.Canceled) {`, 1)
	source += `
func s6ProductIdle(db string) {
 root:=filepath.Dir(db);if _,err:=os.Stat(filepath.Join(root,"lifecycle-mode.json"));err!=nil{return}
 ln,err:=net.Listen("tcp4","127.0.0.1:0");if err!=nil{panic(err)};_ =ln.Close()
 var lineage brokerimage.RuntimeStatus
 lineage.SetPhase(proto.BrokerImagePhaseServing,"single")
 data,_:=json.Marshal(map[string]any{"generation":lineage.Snapshot().Generation})
 if err=os.WriteFile(filepath.Join(root,"runtime-idle.json"),data,0600);err!=nil{panic(err)}
 until:=time.Now().Add(30*time.Second)
 for {if _,err:=os.Stat(filepath.Join(root,"idle-release.json"));err==nil{return};if time.Now().After(until){panic("product idle barrier expired")};time.Sleep(time.Millisecond)}
}
func s6ProductAfterRun(db string) {
 root:=filepath.Dir(db);if _,err:=os.Stat(filepath.Join(root,"lifecycle-mode.json"));err!=nil{return}
 until:=time.Now().Add(30*time.Second)
 for {if _,err:=os.Stat(filepath.Join(root,"stop-release.json"));err==nil{return};if time.Now().After(until){panic("product terminal observation expired")};time.Sleep(time.Millisecond)}
}
`
	source = strings.Replace(source, "\"github.com/LinZiyang666/tether/internal/storage\"", "\"github.com/LinZiyang666/tether/internal/storage\"\n\"github.com/LinZiyang666/tether/internal/proto\"", 1)
	write("cmd/tether/serve.go", source)
}

// Keep the real read-only opener visible to the fixture's ACK oracle.
// Its lifetime ends before any production barrier is advanced.
var storageProductReadOnly = storage.OpenReadOnly

func (f *brokerProductFixture) startPreparedCut() error {
	if err := os.WriteFile(filepath.Join(f.root, "idle-release.json"), []byte("{}"), 0600); err != nil {
		return err
	}
	if err := f.startPeers(); err != nil {
		return err
	}
	var err error
	if err = f.openVictim(); err != nil {
		return err
	}
	if err = os.Rename(filepath.Join(f.root, "candidate"), filepath.Join(f.root, "broker-product")); err != nil {
		return err
	}
	f.preparedResponse = make(chan error, 1)
	// The authorized S8 route: inert describe/Prepare/stage, then Start
	// (broker_product_transaction_linux.go; S8 BROKER-EXPORTER R-11).
	if f.transaction != nil {
		grant, receipt, err := f.transaction.prepare(f.test.Context(), f.cmd.Process.Pid, f.candidateSHA)
		if err != nil {
			return fmt.Errorf("authorized broker cut before Start: %w", err)
		}
		go func() { f.preparedResponse <- f.transaction.start(f.test.Context(), grant, receipt) }()
		if err = f.awaitMarker("closed"); err != nil {
			return err
		}
		if err = os.WriteFile(filepath.Join(f.root, "release.json"), []byte("{}"), 0600); err != nil {
			return err
		}
		return f.awaitMarker("lifecycle-prepared")
	}
	go func() {
		response, err := (&adminsock.Client{Path: filepath.Join(f.root, "admin.sock"), Timeout: 6 * time.Second}).Call(adminsock.Request{Op: adminsock.OpBrokerUpgradeReload, ExpectSHA256: f.candidateSHA})
		if err == nil && !response.OK {
			err = fmt.Errorf("reload refused: %+v", response)
		}
		f.preparedResponse <- err
	}()
	if err = f.awaitMarker("closed"); err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(f.root, "release.json"), []byte("{}"), 0600); err != nil {
		return err
	}
	return f.awaitMarker("lifecycle-prepared")
}
