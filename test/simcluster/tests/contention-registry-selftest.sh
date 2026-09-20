#!/bin/sh
# contention-registry-selftest.sh — gate-control for tests/contention-registry-check.sh: a consistent
# synthetic registry + drill tree is GREEN; each rule C1–C5 goes RED on its own injection. POSIX sh.
# origin: simcluster-speed plan §6.3 D-8 / D-9 / R-1.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
CHECK="$HERE/contention-registry-check.sh"
T="${TMPDIR:-/tmp}/contention-registry-selftest.$$"
mkdir -p "$T/d"
trap 'rm -rf "$T"' EXIT INT TERM
FAIL=0
ok()   { printf 'ok   %s\n' "$1"; }
fail() { printf 'FAIL %s\n' "$1" >&2; FAIL=1; }

good_reg() {
printf '%s\n' '# sensor	regime	units	evidence' \
  '70-grow-timing	live-grow	10-plain 96-split.A	VOTER timeout under concurrent grows' \
  'post-grow-window	live-grow	96-split.A	eligibility window after grow' \
  'old-sensor	none-after-split	-	nothing samples it since the split' \
  'lucky-default	default	10-plain	a default-sweep sensor'
}
good_tree() {
printf '#!/bin/sh\ngrow_to_3 2 1\nassert_ok "x" true\n' > "$T/d/10-plain.sh"
cat > "$T/d/96-split.sh" <<'EOF'
#!/bin/sh
# arms: A D
# fixture: A=N3-live D=N3-live
# grows: A=2 D=2
# worst: A=600 D=600
# forgoes: A=- D="post-grow-window 70-grow-timing: fresh cluster; D no longer runs after A"
grow_to_3 2 1
"$SIM" grow brk3
case "${ARM:?}" in
    A) assert_ok "a" true ;;
    D) assert_ok "d" true ;;
    *) setup_fail "unknown arm" ;;
esac
drill_end
EOF
}
run() { REGISTRY="$T/reg.tsv" DRILLS="$T/d" sh "$CHECK" 2>&1; }
expect_red() { out=$(run); if printf '%s\n' "$out" | grep -q "$2"; then ok "$1"; else fail "$1 — expected '$2', got: $(printf '%s' "$out" | tr '\n' '|')"; fi; }

good_reg > "$T/reg.tsv"; good_tree
if run >/dev/null; then ok "consistent registry + tree: green"; else fail "consistent set red: $(run | tr '\n' '|')"; fi

good_reg | sed 's/^lucky-default\tdefault/lucky-default\tsometimes/' > "$T/reg.tsv"; expect_red "C2 unknown regime" "C2 sensor 'lucky-default' regime"
good_reg | sed 's/^70-grow-timing\tlive-grow\t10-plain 96-split.A/70-grow-timing\tlive-grow\t10-plain 97-nope/' > "$T/reg.tsv"; expect_red "C3 unit names a missing drill (R-1)" "97-nope.sh does not exist"
good_reg | sed 's/^post-grow-window\tlive-grow\t96-split.A/post-grow-window\tlive-grow\t96-split.Z/' > "$T/reg.tsv"; expect_red "C3 unit names a non-arm" "Z is not an arm of 96-split"
good_reg | sed 's/^lucky-default\tdefault\t10-plain/lucky-default\tdefault\t-/' > "$T/reg.tsv"; expect_red "C3 live regime with no unit" "lists no unit"
good_reg | sed 's/^post-grow-window\tlive-grow\t96-split.A/post-grow-window\tlive-grow\t96-split/' > "$T/reg.tsv"; expect_red "C3 a split drill named as a whole unit (74 after its split)" "arm-split (A D) — name the arm"
good_reg | sed 's/^old-sensor\tnone-after-split\t-/old-sensor\tnone-after-split\t10-plain/' > "$T/reg.tsv"; expect_red "C3 none-after-split with a unit" "none-after-split but lists"
good_reg | sed 's/^70-grow-timing\t/#70-grow-timing\t/' > "$T/reg.tsv"; expect_red "C4 forgoes names a sensor that vanished (D-8; a #-prefixed id is a comment)" "C4 96-split arm D forgoes \"70-grow-timing\""
{ good_reg; printf '70-grow-timing\tdefault\t10-plain\tdup id\n'; } > "$T/reg.tsv"; expect_red "C1 duplicate id" "C1 duplicate sensor id"
{ good_reg; printf 'bad id\tdefault\t10-plain\tspace in id\n'; } > "$T/reg.tsv"; expect_red "C1 id charset" "is not \[A-Za-z0-9-\]+"
good_reg | sed 's/^lucky-default\tdefault\t10-plain\ta default-sweep sensor/lucky-default\tdefault\t10-plain\teligibility window after grow/' > "$T/reg.tsv"; expect_red "C5 two ids share evidence text" "C5"
good_reg > "$T/reg.tsv"; printf '#!/bin/sh\n# arms: Q\n# fixture: Q=N1\n# grows: Q=0\n# worst: Q=60\n# forgoes: Q="unknown-sensor: text after the colon is not parsed"\ncase "${ARM:?}" in Q) true ;; *) setup_fail x ;; esac\ndrill_end\n' > "$T/d/97-other.sh"; expect_red "C4 forgoes names an unregistered sensor (D-9)" "C4 97-other arm Q forgoes \"unknown-sensor\""
rm -f "$T/d/97-other.sh"
if run >/dev/null; then ok "restored set: green"; else fail "restored set red"; fi

[ "$FAIL" = 0 ] && echo "contention-registry-selftest: PASS" || { echo "contention-registry-selftest: FAIL" >&2; exit 1; }
