package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExternalReReviewVerifyClusterSeamRejectsWrongRaftAddr(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "broker.yaml")
	if err := os.WriteFile(cfg, []byte("broker:\n  cluster:\n    data_dir: /var/lib/tether\n    raft_addr: stale:7400\n    secrets_dir: /etc/tether/secrets\n    nats_conf_path: /etc/tether/nats.d/nats.conf\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := verifyClusterSeam(cfg, "brk2:7400", "/var/lib/tether", "/etc/tether/secrets", "/etc/tether/nats.d/nats.conf")
	if err == nil {
		t.Fatal("verifyClusterSeam must reject a stale broker.cluster.raft_addr that does not match the joiner's requested raft addr")
	}
	if !strings.Contains(err.Error(), "stale:7400") || !strings.Contains(err.Error(), "brk2:7400") {
		t.Fatalf("error should show both stale and expected raft_addr, got: %v", err)
	}
}

func TestExternalReReviewVerifyClusterSeamRejectsIncompleteMatchingSeam(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "broker.yaml")
	if err := os.WriteFile(cfg, []byte("broker:\n  cluster:\n    raft_addr: brk2:7400\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := verifyClusterSeam(cfg, "brk2:7400", "/var/lib/tether", "/etc/tether/secrets", "/etc/tether/nats.d/nats.conf")
	if err == nil {
		t.Fatal("verifyClusterSeam must reject a partial seam that has the right raft_addr but lacks broker.cluster.data_dir")
	}
	if !strings.Contains(err.Error(), "data_dir") {
		t.Fatalf("error should name the missing data_dir cluster-mode trigger, got: %v", err)
	}
}

func TestExternalReReviewVerifyClusterSeamRejectsWrongDataDir(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "broker.yaml")
	if err := os.WriteFile(cfg, []byte("broker:\n  cluster:\n    data_dir: /stale/tether\n    raft_addr: brk2:7400\n    secrets_dir: /etc/tether/secrets\n    nats_conf_path: /etc/tether/nats.d/nats.conf\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := verifyClusterSeam(cfg, "brk2:7400", "/var/lib/tether", "/etc/tether/secrets", "/etc/tether/nats.d/nats.conf")
	if err == nil {
		t.Fatal("verifyClusterSeam must reject a full-looking seam whose data_dir does not match this cluster add's data dir")
	}
	if !strings.Contains(err.Error(), "data_dir") || !strings.Contains(err.Error(), "/stale/tether") {
		t.Fatalf("error should name the stale data_dir, got: %v", err)
	}
}

func TestExternalReReviewVerifyClusterSeamRejectsWrongNonRaftFields(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "wrong secrets_dir",
			body: "broker:\n  cluster:\n    data_dir: /var/lib/tether\n    raft_addr: brk2:7400\n    secrets_dir: /stale/secrets\n    nats_conf_path: /etc/tether/nats.d/nats.conf\n",
			want: "secrets_dir",
		},
		{
			name: "wrong nats_conf_path",
			body: "broker:\n  cluster:\n    data_dir: /var/lib/tether\n    raft_addr: brk2:7400\n    secrets_dir: /etc/tether/secrets\n    nats_conf_path: /etc/tether/nats.conf\n",
			want: "nats_conf_path",
		},
	}
	for _, tc := range cases {
		cfg := filepath.Join(dir, tc.name+".yaml")
		if err := os.WriteFile(cfg, []byte(tc.body), 0o644); err != nil {
			t.Fatal(err)
		}
		err := verifyClusterSeam(cfg, "brk2:7400", "/var/lib/tether", "/etc/tether/secrets", "/etc/tether/nats.d/nats.conf")
		if err == nil {
			t.Fatalf("%s: verifyClusterSeam must reject wrong-but-nonempty %s", tc.name, tc.want)
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: error should name %s, got: %v", tc.name, tc.want, err)
		}
	}
}
