package agent

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStateStoreDurableReplaceLeavesReadableStateAndNoTemp(t *testing.T) {
	store := newStateStore(t.TempDir(), "lab")
	want := PortToken{Name: "svc", Port: 14001, LocalPort: 8080, Token: "secret"}
	if err := store.AddPort(want); err != nil {
		t.Fatal(err)
	}

	state, err := store.load()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.PortTokens) != 1 || state.PortTokens[0] != want {
		t.Fatalf("persisted state=%+v, want token %+v", state, want)
	}
	info, err := os.Stat(store.path)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Fatalf("state mode=%o, want 0600", mode)
	}
	if matches, err := filepath.Glob(filepath.Join(filepath.Dir(store.path), ".state.json.tmp-*")); err != nil {
		t.Fatal(err)
	} else if len(matches) != 0 {
		t.Fatalf("state temp files remain after atomic replace: %v", matches)
	}
}

func TestStateStoreDoesNotFollowPredictableTempSymlink(t *testing.T) {
	home := t.TempDir()
	store := newStateStore(home, "lab")
	if err := os.MkdirAll(filepath.Dir(store.path), 0o700); err != nil {
		t.Fatal(err)
	}
	victim := filepath.Join(home, "victim")
	if err := os.WriteFile(victim, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victim, store.path+".tmp"); err != nil {
		t.Fatal(err)
	}

	if err := store.SetProxy(&ProxyState{PublicPort: 14001, Token: "secret"}); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(victim)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "keep" {
		t.Fatalf("predictable temp symlink target was overwritten: %q", body)
	}
}
