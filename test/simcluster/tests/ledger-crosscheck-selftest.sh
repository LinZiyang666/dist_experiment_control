#!/bin/sh
# ledger-crosscheck-selftest.sh — positive and negative controls for tests/ledger-crosscheck.sh's
# HEADING-ONLY status read (internal review round 1 R6-F11).
#
# The gate's first version read an entry's status from "the heading plus its next 3 lines" (closure) and
# "the heading or the first 3 body lines" (CANDIDATE). That is positional: a cross-reference in a
# neighbouring sentence carried its word into the entry's status, and DOC-28 was read as closed for months
# on the strength of "源码 SB-96-3 已闭合行为面" — a sentence about something else. This selftest runs the
# gate against synthetic ledgers and pins that ONLY the heading line decides:
#
#   S-1  closure word in the heading            → closed, no owner needed          (rc 0)
#   S-2  closure word ONLY in the body's 2nd line → still OPEN → UNOWNED             (rc 1)  ← the laundering
#   S-3  CANDIDATE in the heading               → R6-CAND, no owner needed          (rc 0)
#   S-4  CANDIDATE ONLY in the body's 1st line  → still OPEN → UNOWNED             (rc 1)
#   S-5  OPEN entry with a non-GREEN owner cell → ok                                (rc 0)
#   S-6  OPEN entry, GREEN owner cell only      → UNOWNED (a GREEN cell owns nothing) (rc 1)
#   S-7  heading cites a FIXED neighbour        → still OPEN → UNOWNED             (rc 1)  ← round-2 R3-F2
#   S-8  heading quotes FIXED in a code span    → still OPEN → UNOWNED             (rc 1)
#   S-9  `已修复 by #80` (status BEFORE the ref)  → closed                           (rc 0)
#   S-10 heading cites a CANDIDATE neighbour    → still OPEN → UNOWNED             (rc 1)
#
# Run: cd test/simcluster && sh tests/ledger-crosscheck-selftest.sh
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
GATE="$HERE/ledger-crosscheck.sh"
T="${TMPDIR:-/tmp}/ledger-xc-selftest.$$"
mkdir -p "$T"
trap 'rm -rf "$T"' EXIT
FAIL=0

# run <case> <expected-rc> <expected-substring> — ledger in $T/ledger.md, verdicts in $T/verdicts.tsv
run() {
    out=$(LEDGER="$T/ledger.md" VERDICTS="$T/verdicts.tsv" sh "$GATE" 2>&1); rc=$?
    if [ "$rc" != "$2" ] || ! printf '%s' "$out" | grep -q -- "$3"; then
        printf 'FAIL %s: rc=%s (want %s), want output containing %s\n--- output ---\n%s\n' "$1" "$rc" "$2" "$3" "$out" >&2
        FAIL=$((FAIL+1))
    else
        printf 'ok   %s\n' "$1"
    fi
}
# verdicts: a comment header + rows with exactly 6 tab-separated fields (drill, verdict, nc, bands, owner, note)
verdicts() {
    printf '# drill\tverdict\tnc\tbands\towner\tnote\n' > "$T/verdicts.tsv"
    for row in "$@"; do printf '%s\n' "$row" >> "$T/verdicts.tsv"; done
}
tab="$(printf '\t')"

# S-1: closure word in the heading — closed regardless of the body.
verdicts
cat > "$T/ledger.md" <<'EOF'
### #901 — something that used to break（已修复，2026-01-01）
- 状态：FIXED.
EOF
run S-1-heading-closed 0 "0 open defect(s)"

# S-2: the laundering — the heading says nothing, the SECOND body line contains a closure word about
# something else. Positional reading called this closed; heading-only must call it OPEN and UNOWNED.
verdicts
cat > "$T/ledger.md" <<'EOF'
### #902 — a live defect whose neighbour sentence mentions closure
- 状态：登记（drill 96 臂 B 的理由）。
- 现象：源码 SB-96-3 已闭合行为面，但本条的文档缺口仍在。FIXED elsewhere, CLOSED elsewhere.
EOF
run S-2-body-closure-is-not-closure 1 "UNOWNED   #902"

# S-3: CANDIDATE in the heading — exempt (adjudication owns it).
verdicts
cat > "$T/ledger.md" <<'EOF'
### #903 — an unconfirmed observation（CANDIDATE，未归因）
- 状态：OPEN.
EOF
run S-3-heading-candidate 0 "R6-CAND   #903"

# S-4: CANDIDATE only in the first body line — NOT exempt.
verdicts
cat > "$T/ledger.md" <<'EOF'
### #904 — a confirmed defect that cites a candidate neighbour
- 见 #32（CANDIDATE）的同类现象；本条已确认。
EOF
run S-4-body-candidate-is-not-candidate 1 "UNOWNED   #904"

# S-5: OPEN with a non-GREEN owner cell.
verdicts "55-some-drill${tab}INCOMPLETE${tab}1${tab}-${tab}#905${tab}55-some-drill"
cat > "$T/ledger.md" <<'EOF'
### #905 — a pinned live defect（OPEN）
- 状态：OPEN.
EOF
run S-5-open-owned 0 "ok        #905"

# S-6: the owner cell is GREEN — a GREEN cell pins nothing.
verdicts "55-some-drill${tab}GREEN${tab}0${tab}-${tab}#906${tab}55-some-drill"
cat > "$T/ledger.md" <<'EOF'
### #906 — a live defect whose only owner cell is GREEN（OPEN）
- 状态：OPEN.
EOF
run S-6-green-owner-is-no-owner 1 "UNOWNED   #906"

# S-7..S-10 (round-2 review R3-F2): the heading is read by its TRAILING STATUS GROUP, clause by clause,
# and a cross-reference ends its clause — a heading that mentions a FIXED neighbour, or carries FIXED
# inside a code span, is still OPEN; a status word BEFORE the reference in its clause is the entry's own.
verdicts
cat > "$T/ledger.md" <<'EOF'
### #907 — an open defect that cites a fixed neighbour（OPEN；与 #80 的根因同族，#80 已修复）
- 状态：OPEN.
EOF
run S-7-heading-crossref-closure-is-not-closure 1 "UNOWNED   #907"

verdicts
cat > "$T/ledger.md" <<'EOF'
### #908 — an open defect whose heading quotes a code token `NOT_FIXED`（OPEN）
- 状态：OPEN.
EOF
run S-8-heading-code-span-is-not-closure 1 "UNOWNED   #908"

verdicts
cat > "$T/ledger.md" <<'EOF'
### #909 — a fixed defect attributed to a neighbour（已修复 by #80，2026-09-19 关闭）
- 状态：FIXED.
EOF
run S-9-status-before-crossref-is-the-entrys 0 "0 open defect(s)"

verdicts
cat > "$T/ledger.md" <<'EOF'
### #910 — an open defect whose heading cites a CANDIDATE neighbour（OPEN；同类见 #82 CANDIDATE）
- 状态：OPEN.
EOF
run S-10-heading-crossref-candidate-is-not-candidate 1 "UNOWNED   #910"

# Control: the gate refuses to run without its inputs (never a silent green).
out=$(LEDGER="$T/none.md" VERDICTS="$T/verdicts.tsv" sh "$GATE" 2>&1); rc=$?
if [ "$rc" != 2 ]; then printf 'FAIL control-missing-ledger: rc=%s want 2\n' "$rc" >&2; FAIL=$((FAIL+1)); else printf 'ok   control-missing-ledger\n'; fi

if [ "$FAIL" != 0 ]; then
    printf 'ledger-crosscheck-selftest: %s check(s) FAILED\n' "$FAIL" >&2
    exit 1
fi
printf 'ledger-crosscheck-selftest: ALL PASS (11 checks)\n'
