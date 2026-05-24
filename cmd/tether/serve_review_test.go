package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestServeInvalidProcRetentionDoesNotOpenDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "tether.db")
	configPath := filepath.Join(dir, "broker.yaml")
	body := []byte("broker:\n" +
		"  nats:\n" +
		"    url: nats://127.0.0.1:1\n" +
		"  storage:\n" +
		"    db: " + dbPath + "\n" +
		"    proc_retention: -1h\n")
	if err := os.WriteFile(configPath, body, 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := newServeCmd()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{"--config", configPath})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected invalid proc_retention to fail")
	}
	if !strings.Contains(err.Error(), "proc_retention") {
		t.Fatalf("expected proc_retention error, got %v", err)
	}
	if _, statErr := os.Stat(dbPath); !os.IsNotExist(statErr) {
		t.Fatalf("invalid config should fail before opening DB; stat err=%v", statErr)
	}
}
