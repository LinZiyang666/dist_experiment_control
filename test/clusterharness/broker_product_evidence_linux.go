//go:build linux

package clusterharness

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/storage"
)

// Product evidence combines an admitted in-process registry census with a
// separate SQL query and kernel identities. No copied StoreStamp stands in for
// the pre-replay row query, and no parent-side driver registry certifies a child.
// origin: hot-update S6 plan §3.3 / T13 / T37 / §12.1 P26
func (f *brokerProductFixture) addStoreEvidence(value map[string]any, label string, rows map[string]string) error {
	if err := os.WriteFile(filepath.Join(f.root, "census-request.json"), []byte("{}"), 0600); err != nil {
		return err
	}
	if _, err := (&adminsock.Client{Path: filepath.Join(f.root, "admin.sock"), Timeout: 5 * time.Second}).Call(adminsock.Request{Op: adminsock.OpRuntime}); err != nil {
		return err
	}
	data, err := os.ReadFile(filepath.Join(f.root, "census.json"))
	if err != nil {
		return err
	}
	var census struct {
		Store   storage.StoreStamp `json:"store"`
		Drivers [2]uint32          `json:"driver_counts"`
		Leader  bool               `json:"leader"`
		Oracle  map[string]string  `json:"oracle_inputs"`
		Second  bool               `json:"second_restore_empty"`
	}
	if err = json.Unmarshal(data, &census); err != nil {
		return err
	}
	acks, effects := map[string]string{}, map[string]int{}
	for id, fp := range f.ackIDs {
		if note, ok := rows[fp]; ok {
			acks[id] = note
			effects[id] = 1
		}
	}
	prefix := map[string]string{}
	var gate struct {
		Rows   map[string]string
		Cursor uint64
	}
	gateCount := uint32(0)
	if data, err := os.ReadFile(filepath.Join(f.root, "pre-replay.json")); err == nil {
		if err = json.Unmarshal(data, &gate); err != nil {
			return err
		}
		gateCount = 1
		for id, fp := range f.originalIDs {
			if note, ok := gate.Rows[fp]; ok {
				prefix[id] = note
			}
		}
	}
	previous := f.previousSHA
	counters := map[string]uint64{"handoff_fds": 0, "sealed_custody_fds": 0, "sqlite_fds": 0}
	entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", f.cmd.Process.Pid))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		link, err := os.Readlink(filepath.Join(fmt.Sprintf("/proc/%d/fd", f.cmd.Process.Pid), entry.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if strings.Contains(link, "memfd:tether-handoff") {
			counters["handoff_fds"]++
		}
		if link == census.Store.Identity.Path || link == census.Store.Identity.Path+"-wal" || link == census.Store.Identity.Path+"-shm" {
			counters["sqlite_fds"]++
		}
	}
	value["source_sha"], value["source_digest"] = f.sourceSHA, f.sourceDigest
	value["claim"], value["test_or_arm"], value["command"] = label, "broker-product", "private broker-product serve"
	value["native_rc"], value["verdict"] = 0, "OBSERVED"
	value["artifact_sha"], value["previous_sha"] = value["running_sha256"], previous
	value["artifact_kind"], value["topology"], value["node_id"] = "broker-product", f.topology, "product"
	value["wall_seconds"] = time.Since(f.started).Seconds()
	value["store"], value["seal"], value["driver_counts"] = census.Store, f.seal, census.Drivers
	value["leader"], value["oracle_inputs"], value["second_restore_empty"] = census.Leader, census.Oracle, census.Second
	value["ack_ids"], value["effects"], value["pre_replay_ack"] = acks, effects, prefix
	value["pre_replay_cursor"], value["replay_gate_count"] = gate.Cursor, gateCount
	for name, value := range f.cutTimes {
		counters[name] = value
	}
	for _, line := range strings.Split(f.logs.String(), "\n") {
		var event struct {
			Time  string `json:"time"`
			Event string `json:"event"`
		}
		if json.Unmarshal([]byte(line), &event) == nil && event.Event != "" {
			if stamp, err := time.Parse(time.RFC3339Nano, event.Time); err == nil {
				counters[event.Event] = uint64(stamp.UnixNano())
			}
		}
	}
	value["counters"], value["mutation_digest"] = counters, ""
	value["stdout_path"], value["stderr_path"] = "product.stdout", "product.stderr"
	value["log_mapping"] = map[string]string{"kernel": "/proc", "pre-replay": "pre-replay.json", "driver-census": "census.json"}
	return nil
}

// A later same-PID exec resets the race runtime's exit status. The joined
// broker log retains diagnostics from every image, including its predecessors.
// origin: S6 internal review SINGLEACK F5 / plan §4.2 / T49
func productRaceEvidence(log string) error {
	if strings.Contains(log, "WARNING: DATA RACE") {
		return errors.New("product fixture: broker reported WARNING: DATA RACE across exec")
	}
	return nil
}
