package tunnel

import "testing"

// kill_gen_prune_test.go — the per-session kill-generation map must not grow without bound.
//
// This is a DIFFERENT invariant from the fence-vs-in-flight-REGISTER one in kill_fence_test.go, and it
// is kept separate for that reason: it is about bookkeeping lifetime, not about a race. It shared a file
// with the round-5 fence test only because both were written in the same review round — the exact
// coupling B6's renaming rule exists to break.
//
// origin: p13 external review round 5 (self-review item)

// TestForgetSessionPrunesKillGen: ForgetSession prunes the per-session kill-generation entry so
// killGenSession does not grow unbounded across session lifecycles — and prunes ONLY that session's.
func TestForgetSessionPrunesKillGen(t *testing.T) {
	srv := NewServer("127.0.0.1:0", "127.0.0.1", func(_, _ string, _ int, _ string, _ int64) error { return nil }, silentLog())

	srv.CloseSession("lab")   // bumps killGenSession["lab"]
	srv.CloseSession("other") // bumps killGenSession["other"]
	srv.mu.Lock()
	_, hadLab := srv.killGenSession["lab"]
	srv.mu.Unlock()
	if !hadLab {
		t.Fatal("precondition: CloseSession should record a session kill generation")
	}

	srv.ForgetSession("lab")
	srv.mu.Lock()
	_, stillLab := srv.killGenSession["lab"]
	_, stillOther := srv.killGenSession["other"]
	srv.mu.Unlock()
	if stillLab {
		t.Fatal("ForgetSession did not prune killGenSession[lab]")
	}
	if !stillOther {
		t.Fatal("ForgetSession pruned an unrelated session's entry — the prune must be per-session, or a " +
			"forgotten session silently removes a live one's fence")
	}
}
