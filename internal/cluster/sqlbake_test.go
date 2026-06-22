package cluster

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/LinZiyang666/tether/internal/storage" // register the modernc "sqlite" driver
)

func openMem(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open mem sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestLitTimeMatchesBoundParam is the BLOCKING gate for every timestamped D2 op
// (d2-plan §0/§2): a value baked by LitTime MUST land byte-identical to today's
// bound-time.Time path (tx.Exec(sql, t)) for the SAME time.Time. modernc stores a
// bound time.Time as exactly time.Time.String() — zone- and monotonic-faithful
// (NOT RFC3339, NOT forced UTC). Cases cover UTC, a +0800 zone (must NOT be
// converted to UTC), and time.Now() (monotonic suffix preserved). If modernc's
// serialization ever drifts, this fails BEFORE any op can silently diverge §13.2
// equivalence or break the live lexical OFFLINE->REVOKE reconciler.
func TestLitTimeMatchesBoundParam(t *testing.T) {
	db := openMem(t)
	if _, err := db.Exec(`CREATE TABLE bound(t)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE baked(t)`); err != nil {
		t.Fatal(err)
	}

	cases := map[string]time.Time{
		"fixed_nanos":     time.Date(2026, 6, 21, 13, 14, 15, 123456789, time.UTC),
		"whole_second":    time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		"non_utc_input":   time.Date(2026, 6, 21, 21, 14, 15, 500000000, time.FixedZone("CST", 8*3600)),
		"now_w_monotonic": time.Now(),
	}
	for name, tm := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := db.Exec(`DELETE FROM bound`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`DELETE FROM baked`); err != nil {
				t.Fatal(err)
			}
			// today's path: bind the EXACT time.Time the live mutator would bind
			// (LitTime does NOT force .UTC(); the caller must pass the same value).
			if _, err := db.Exec(`INSERT INTO bound(t) VALUES(?)`, tm); err != nil {
				t.Fatal(err)
			}
			// D2 path: bake the literal from the SAME time.Time
			if _, err := db.Exec(`INSERT INTO baked(t) VALUES(` + LitTime(tm) + `)`); err != nil {
				t.Fatalf("baked insert (%s): %v", LitTime(tm), err)
			}
			var boundS, bakedS string
			if err := db.QueryRow(`SELECT CAST(t AS TEXT) FROM bound`).Scan(&boundS); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT CAST(t AS TEXT) FROM baked`).Scan(&bakedS); err != nil {
				t.Fatal(err)
			}
			if boundS != bakedS {
				t.Fatalf("LitTime drift vs bound param:\n bound=%q\n baked=%q", boundS, bakedS)
			}
			// modernc stores exactly time.Time.String() (zone- and monotonic-faithful).
			if want := tm.String(); bakedS != want {
				t.Fatalf("stored=%q, want time.Time.String()=%q", bakedS, want)
			}
		})
	}
}

// TestLitText_Adversarial proves litText is a safe single text->literal surface:
// quotes/dashes/semicolons round-trip byte-exact (no injection, no truncation),
// and an embedded NUL is REJECTED (fail-closed before propose).
func TestLitText_Adversarial(t *testing.T) {
	db := openMem(t)
	if _, err := db.Exec(`CREATE TABLE x(v TEXT)`); err != nil {
		t.Fatal(err)
	}
	safe := []string{
		`jupyter`,
		`o'brien`,             // single quote
		`a'';DROP TABLE x;--`, // classic injection attempt
		`tab	newline
end`, // control chars (not NUL)
		`emoji-✓-Ünïçödé`, // multibyte
		``,                // empty
		`'`, `''`, `'''`,  // quote runs
		`back\slash`, // backslash is literal in SQLite (no C escapes)
	}
	for _, s := range safe {
		lit, err := LitText(s)
		if err != nil {
			t.Fatalf("LitText(%q) unexpected error: %v", s, err)
		}
		if _, err := db.Exec(`DELETE FROM x`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO x(v) VALUES(` + lit + `)`); err != nil {
			t.Fatalf("insert LitText(%q)=%s: %v", s, lit, err)
		}
		var got string
		if err := db.QueryRow(`SELECT v FROM x`).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != s {
			t.Fatalf("round-trip mismatch: in=%q out=%q (lit=%s)", s, got, lit)
		}
		// exactly one row — an injection that closed the literal + ran a second
		// statement would change the row count.
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM x`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("LitText(%q) produced %d rows, want 1 (injection?)", s, n)
		}
	}
	// NUL must be rejected.
	if _, err := LitText("ab\x00cd"); err == nil {
		t.Fatal("litText must REJECT an embedded NUL byte")
	}
}

// TestLitInt_ExactInt64 proves litInt preserves an int64 past 2^53 exactly —
// the precision the JSON []any path loses (the reason Args is forbidden).
func TestLitInt_ExactInt64(t *testing.T) {
	db := openMem(t)
	if _, err := db.Exec(`CREATE TABLE x(n INTEGER)`); err != nil {
		t.Fatal(err)
	}
	vals := []int64{0, 1, -1, 1750512345123456789, 9223372036854775807, -9223372036854775808}
	for _, v := range vals {
		if _, err := db.Exec(`DELETE FROM x`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO x(n) VALUES(` + LitInt(v) + `)`); err != nil {
			t.Fatalf("insert LitInt(%d): %v", v, err)
		}
		var got int64
		if err := db.QueryRow(`SELECT n FROM x`).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != v {
			t.Fatalf("litInt round-trip: in=%d out=%d", v, got)
		}
	}
}
