#!/bin/sh
# lint-install.sh — STATIC guard against command substitution inside install.sh heredocs (defect P9).
#
# WHY THIS EXISTS. install.sh runs as ROOT and generates config/unit files from heredocs. An UNQUOTED
# heredoc (`cat <<EOF`) makes its whole body shell-active: `$X` expands AND a backtick or $(…) is
# EXECUTED. That is fine for the intended `$bin`/`$etc` interpolations, but it means ordinary English
# prose in a generated comment is code. P9 was exactly that: the nats-server unit carried
#
#     # G4 §B / #23: Restart=always ... The `cluster add` grow cutover restarts nats-server ...
#
# so every broker install ran `cluster add` as root (printing "cluster: not found") and wrote a unit
# file with those two words silently deleted — the documented rationale for Restart=always became
# unreadable on the real machine. Nothing failed loudly, which is why a human review had missed it
# across several releases and why this has to be a static rule instead of vigilance.
#
# THE RULE. For every UNQUOTED heredoc, the body must contain no backtick and no $(…). Variable
# expansion is deliberately NOT banned: it is the entire reason those heredocs are unquoted. But
# command substitution has no legitimate use in a generated config file — anything it could produce
# should be computed into a variable before the heredoc, where it is reviewable and testable.
# QUOTED heredocs (`cat <<'EOF'`) are inert by construction, so their bodies are not scanned.
#
# The fix for a violation is one of:
#   * prose that only LOOKS like code  -> use single quotes:  'cluster add'
#   * a backtick that must be emitted  -> escape it:          \`
#   * a body that expands nothing      -> quote the heredoc:  <<'EOF'
#
# Run:  sh tests/lint-install.sh
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
# tests/ -> simcluster/ -> test/ -> repo root. Keep this resolved rather than relying on cwd, since
# the drill runner invokes the lints from several different directories.
TARGET="$(cd "$HERE/../../.." && pwd)/scripts/install.sh"

[ -f "$TARGET" ] || { echo "lint-install: cannot find $TARGET"; exit 1; }

echo "── install.sh heredoc scan ─────────────────────────────────────────────────────"

# The quote characters are passed IN rather than written into the awk program: a program that has to
# recognise both ' and " cannot embed either literal without a layer of shell-escaping that is far
# more error-prone than the rule it implements.
SQ=$(printf '\047')
DQ=$(printf '\042')

# One state-machine pass. Tracking heredoc state (rather than grepping for backticks globally) is
# what makes the rule precise: a backtick in ordinary script comments outside a heredoc is harmless,
# and a `<<` appearing INSIDE a body must not be mistaken for a new opener.
#
# KNOWN LIMITATION (N-7, record-only — accepted, NOT fixed): the opener detector is not quote-aware. A
# `<<` inside a QUOTED string or a left-shift expression can be misread as a heredoc opener, and the
# opener's own quote (`<<'EOF'` — semantic) cannot be told apart from an ENCLOSING quote without
# position-aware quote tracking. Both drift directions are LOUD, not silent, so a fix is disproportionate
# for one vendored file: a spurious opener either hits the never-terminated VIOLATION at EOF, or marks a
# real body "quoted ⇒ inert, not scanned" and prints a visible `skip` row (never a false GREEN that hides
# a command substitution). If install.sh ever grows a `<<`-bearing string this must be revisited.
OUT=$(awk -v sq="$SQ" -v dq="$DQ" '
    # --- inside a heredoc: look for the terminator, and scan the body if it is shell-active ---
    in_hd {
        if ($0 ~ "^[[:space:]]*" delim "[[:space:]]*$") {
            if (quoted)
                printf "  %-6s line %-4d quoted <<%s%s%s — body inert, not scanned\n", "skip", start, sq, delim, sq
            else if (hd_bad)
                printf "  %-6s line %-4d unquoted <<%s — %d violation(s) above\n", "FAIL", start, delim, hd_bad
            else
                printf "  %-6s line %-4d unquoted <<%s — body clean (no backtick, no $-paren)\n", "ok", start, delim
            in_hd = 0; hd_bad = 0
            next
        }
        if (!quoted) {
            # A backtick is command substitution regardless of position; \` is an escaped literal
            # and is therefore allowed. Checked before the generic backtick test so the escaped
            # form does not trip the rule.
            line = $0
            gsub(/\\`/, "", line)
            if (line ~ /`/) {
                printf "  VIOLATION line %d (heredoc opened at %d): backtick = COMMAND SUBSTITUTION, runs as root\n", NR, start
                printf "            | %s\n", $0
                bad++; hd_bad++
            }
            if (line ~ /\$\(/) {
                printf "  VIOLATION line %d (heredoc opened at %d): $( = COMMAND SUBSTITUTION, runs as root\n", NR, start
                printf "            | %s\n", $0
                bad++; hd_bad++
            }
        }
        next
    }

    # --- outside a heredoc: detect an opener ---
    # Covers <<EOF, <<-EOF and both quoted spellings. Whether a quote character immediately follows
    # << (or <<-) is the ONLY thing that decides if the body is shell-active, so that single
    # character is what the state machine has to read correctly.
    /<</ {
        s = $0
        # greedy: keeps whatever follows the last <<, and (external review M-4) ALSO strips any
        # blanks between << and the delimiter word. `cat << EOF` with a space is POSIX-legal; without
        # eating the blanks c would be " " (not a quote), the delim match would fail on " EOF", and the
        # whole shell-active body would be skipped un-linted — a lint blind spot a rename could exploit.
        sub(/^.*<<-?[ \t]*/, "", s)
        c = substr(s, 1, 1)
        quoted = (c == sq || c == dq) ? 1 : 0
        if (quoted) s = substr(s, 2)
        # No valid delimiter word means this was not a heredoc opener at all; stay out of the body
        # state rather than swallowing the rest of the file.
        if (match(s, /^[A-Za-z_][A-Za-z_0-9]*/)) {
            delim = substr(s, 1, RLENGTH)
            in_hd = 1; start = NR; total++
        }
        next
    }

    END {
        if (in_hd) {
            printf "  VIOLATION heredoc opened at line %d never terminated (delimiter %s)\n", start, delim
            bad++
        }
        printf "SUMMARY %d %d\n", total, bad
    }
' "$TARGET" 2>&1)

# Kept in a variable rather than a /tmp scratch file: a predictable path under a world-writable
# directory is a symlink target, and a lint that guards a root-run installer should not itself
# introduce one.
printf '%s\n' "$OUT" | grep -v '^SUMMARY '
TOTAL=$(printf '%s\n' "$OUT" | awk '/^SUMMARY /{print $2+0}')
BAD=$(printf '%s\n' "$OUT" | awk '/^SUMMARY /{print $3+0}')

echo "────────────────────────────────────────────────────────────────────────────────"
# A scan that found no heredoc at all would report "0 violations" and pass forever — the same
# permanently-true-oracle failure mode lint-drills.sh guards against with its empty-needle rule.
# Assert the parser actually saw the heredocs it is supposed to be policing.
if [ "${TOTAL:-0}" -lt 10 ]; then
    echo "lint-install: only ${TOTAL:-0} heredoc(s) parsed — the scanner is broken, not install.sh"
    exit 1
fi
if [ "$BAD" = 0 ]; then
    echo "lint-install: OK ($TOTAL heredocs, 0 command substitutions in unquoted bodies)"
    exit 0
fi
echo "lint-install: $BAD command-substitution violation(s) in unquoted heredoc bodies"
exit 1
