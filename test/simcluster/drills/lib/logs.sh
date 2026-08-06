#!/bin/sh
# logs.sh — the ONE place that knows WHERE each tether log stream lives.
#
# origin: docs/reviews/h1-external-review.md F3. h1 moved two of the four
# streams, and because every drill had inlined its own `journalctl …` or
# `tail /var/log/tether/broker.err`, the move turned working oracles into
# silent false verdicts: drill 94 reported two ASSERTION FAILURES against a
# product that was reconciling correctly, purely because the agent's slog was
# no longer in the journal. A failing oracle that reads as a product failure is
# the single worst outcome this harness can produce (README mandate: expose
# defects faithfully, never manufacture them), so the mapping now lives here.
#
# THE FOUR STREAMS, AND WHY THEY MUST STAY DISTINGUISHABLE
# -------------------------------------------------------
#   broker slog   -> /var/log/tether/broker.log   (process-owned, size-capped)
#   broker panic  -> journald (unit Standard*=journal; raw fd 2)
#   agent  slog   -> $HOME/.tether/agent/<sid>/agent.log (process-owned, capped)
#   agent  panic  -> $HOME/.tether/agent/<sid>/agent.boot.err (dup2'd fd 2)
#
# Before h1: broker slog went to broker.err (it is stderr output, and the unit
# had StandardError=append:broker.err); the agent's slog went to journald
# because the sim unit sets no Standard*=. A helper that merges the streams
# would let a panic satisfy a slog assertion — which is why these are separate
# functions rather than one "grep the logs" catch-all.
#
# THE RULE THAT DECIDES WHICH READER YOU WANT
# -------------------------------------------
# Ask WHEN the line is written, not who wrote it:
#
#   * anything emitted BEFORE the logger exists — config-validation refusals,
#     startup fail-closed diagnostics, "cannot open X, exiting" — is ONLY ever
#     in the BOOT stream (journald for the broker, agent.boot.err/journal for
#     the agent). It cannot be in the slog, because the slog sink is one of the
#     things that has not been built yet.
#   * anything a running daemon logs is in the SLOG.
#   * panics and stacktraces are in the BOOT stream at any time: the Go runtime
#     writes them to raw fd 2, bypassing every Go-level writer.
#
# This is not theoretical: drill 93's "startup fails loudly on a non-http(s)
# webhook URL" arm was pointed at the slog during the h1 migration and went red
# against a broker that was refusing exactly as designed — the unit really did
# fail to start, the message was simply in the other stream. A startup-refusal
# assertion that reads the slog is guaranteed to fail no matter how correct the
# product is.
#
# The broker readers deliberately include the LEGACY broker.err too: an image
# or host provisioned before h1 still has content there, and a drill that ran
# green yesterday must not go red merely because the file moved. Post-h1
# broker.err simply stops growing.

# sim_broker_slog <node> [lines] — the broker's application log.
sim_broker_slog() {
    dexec "$1" -- sh -c "cat /var/log/tether/broker.log /var/log/tether/broker.err 2>/dev/null | tail -n ${2:-80}"
}

# sim_broker_slog_grep <node> <extended-regex> — quiet grep over the broker
# slog; rc=0 iff matched.
sim_broker_slog_grep() {
    dexec "$1" -- sh -c "cat /var/log/tether/broker.log /var/log/tether/broker.err 2>/dev/null | grep -qE '$2'"
}

# sim_broker_slog_count <node> <extended-regex> — match count (stdout), or
# NOTHING if the log could not be read at all. Callers must treat empty as a
# failure, never as zero: "I read the log and saw none" and "I could not read
# the log" are different answers, and collapsing them is how an oracle becomes
# permanently green.
#
# NOTE the missing `|| echo 0`: `grep -c` prints 0 AND exits 1 when there is no
# match, so an `|| echo 0` fallback emits a SECOND zero. The two-line result
# then breaks every numeric comparison downstream (`[ "$n" -gt "$m" ]` on a
# multiline value is a shell error, not a false), which reads as a product
# failure. `tail -1` keeps the count and drops the fallback's echo if one is
# ever re-added.
# The "could not read" case is made EXPLICIT rather than inferred: a missing
# file also makes `cat | grep -c` print 0, so without the -f probe the two
# answers really would be indistinguishable.
sim_broker_slog_count() {
    dexec "$1" -- sh -c "[ -f /var/log/tether/broker.log ] || [ -f /var/log/tether/broker.err ] || exit 0
cat /var/log/tether/broker.log /var/log/tether/broker.err 2>/dev/null | grep -cE '$2'" 2>/dev/null | tr -d '\r' | tail -1
}

# sim_broker_panic_journal <node> <extended-regex> — the OTHER broker stream:
# panics, stacktraces and pre-logger boot output, which h1 routes to journald.
sim_broker_panic_journal() {
    dexec "$1" -- sh -c "journalctl -u tether-broker --no-pager 2>/dev/null | grep -qE '$2'"
}

# sim_broker_panic_journal_dump <node> [lines] — the same stream as CONTENT, for
# oracles that need to apply several greps to one sample (a startup refusal is
# usually asserted as a conjunction of clauses, and re-reading the journal
# between clauses would let a crash-loop rotate the evidence out from under
# them mid-assertion).
sim_broker_panic_journal_dump() {
    dexec "$1" -- sh -c "journalctl -u tether-broker --no-pager -n ${2:-800} 2>/dev/null"
}

# sim_agent_slog_cursor <node> — byte offset into the agent slog, for "did X
# appear AFTER this point" questions. A byte offset needs no clock-format
# agreement and cannot let a line written earlier false-pass.
sim_agent_slog_cursor() {
    dexec "$1" -- sh -c '. /etc/tether/agent.env 2>/dev/null; f=/home/sim/.tether/agent/${SID:-lab}/agent.log; [ -f "$f" ] && wc -c < "$f" || echo 0' 2>/dev/null | tr -d '\r '
}

# sim_agent_slog_grep <node> <extended-regex> [cursor] — quiet grep over the
# agent slog, optionally only the bytes after <cursor>.
sim_agent_slog_grep() {
    dexec "$1" -- sh -c ". /etc/tether/agent.env 2>/dev/null; f=/home/sim/.tether/agent/\${SID:-lab}/agent.log; [ -f \"\$f\" ] || exit 1; tail -c +\$(( ${3:-0} + 1 )) \"\$f\" 2>/dev/null | grep -qE '$2'"
}

# sim_agent_slog_count <node> <extended-regex> [cursor] — match count (stdout),
# or NOTHING when the slog is unreadable. Same contract and same `grep -c`
# double-zero hazard as sim_broker_slog_count above; the anti-vacuity guards in
# 71-expose-rehome-failover depend on empty≠zero, so a missing file must print
# nothing rather than a confident "0 matches".
sim_agent_slog_count() {
    dexec "$1" -- sh -c ". /etc/tether/agent.env 2>/dev/null; f=/home/sim/.tether/agent/\${SID:-lab}/agent.log; [ -f \"\$f\" ] || exit 0; tail -c +\$(( ${3:-0} + 1 )) \"\$f\" 2>/dev/null | grep -cE '$2'" 2>/dev/null | tr -d '\r' | tail -1
}

# sim_agent_slog_tail <node> [lines] — diagnostics.
sim_agent_slog_tail() {
    dexec "$1" -- sh -c ". /etc/tether/agent.env 2>/dev/null; tail -n ${2:-40} /home/sim/.tether/agent/\${SID:-lab}/agent.log 2>/dev/null"
}

# sim_agent_panic_sink <node> <extended-regex> — the agent's panic stream
# (dup2'd fd 2). Separate from the slog reader on purpose.
sim_agent_panic_sink() {
    dexec "$1" -- sh -c ". /etc/tether/agent.env 2>/dev/null; grep -qE '$2' /home/sim/.tether/agent/\${SID:-lab}/agent.boot.err 2>/dev/null"
}
