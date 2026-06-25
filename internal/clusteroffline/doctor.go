package clusteroffline

import (
	"fmt"
	"net"
	"strings"

	"github.com/LinZiyang666/tether/internal/natsconf"
	"github.com/LinZiyang666/tether/internal/storage"
)

// doctor.go (B3 item 2) — a CHEAP, READ-ONLY preflight an operator runs BEFORE the irreversible
// `cluster init --from-existing` (which stops the daemon). It never mutates anything: no DB copy,
// no migration run, no conf swap. Each check is PASS / ADVISORY / FATAL; advisories are VISIBLE
// (the FDE advisory previously only went to stderr). Exit-on-FATAL is the caller's job.

// DoctorStatus is a single check's verdict.
type DoctorStatus string

const (
	DoctorPass     DoctorStatus = "PASS"
	DoctorAdvisory DoctorStatus = "ADVISORY"
	DoctorFatal    DoctorStatus = "FATAL"
)

// DoctorCheck is one preflight result.
type DoctorCheck struct {
	Name   string       `json:"name"`
	Status DoctorStatus `json:"status"`
	Detail string       `json:"detail"`
}

// DoctorOptions parameterizes the preflight. RaftAddr/NatsRoute are optional — when supplied (e.g.
// by `init --check`) their port bindability is probed with the migration-aware classification.
type DoctorOptions struct {
	SecretsDir string
	DBPath     string
	ConfPath   string
	RaftAddr   string // host:7400 (NEW port; in-use = conflict = FATAL)
	NatsRoute  string // nats://host:6222 (may be the running daemon = ADVISORY)
}

// Doctor runs every non-mutating check and returns the results in a stable order.
func Doctor(opts DoctorOptions) []DoctorCheck {
	var checks []DoctorCheck

	// 1. secrets present + safe perms; the FDE psk_at_rest advisory becomes a VISIBLE row.
	adv, fatal := SecretsPreflight(opts.SecretsDir)
	if fatal != nil {
		checks = append(checks, DoctorCheck{"secrets", DoctorFatal, fatal.Error()})
	} else {
		checks = append(checks, DoctorCheck{"secrets", DoctorPass, "all §15 secret files present with safe (0600) private-key perms"})
	}
	for _, a := range adv {
		checks = append(checks, DoctorCheck{"secrets-advisory", DoctorAdvisory, a})
	}

	// 2. tunnel cert is readable + fingerprintable (a bad cert un-pins every agent at cutover).
	if _, err := TunnelCertFingerprint(opts.SecretsDir); err != nil {
		checks = append(checks, DoctorCheck{"tunnel-cert-fp", DoctorFatal, err.Error()})
	} else {
		checks = append(checks, DoctorCheck{"tunnel-cert-fp", DoctorPass, "tunnel cert readable + fingerprintable"})
	}

	// 3/4. port bindability — raft-addr is a NEW port (in-use = real conflict = FATAL); the
	// nats-route :6222 may be held by the very daemon you're about to stop (in-use = ADVISORY).
	if opts.RaftAddr != "" {
		checks = append(checks, portBindCheck("raft-addr", opts.RaftAddr, true))
	}
	if opts.NatsRoute != "" {
		checks = append(checks, portBindCheck("nats-route", stripScheme(opts.NatsRoute), false))
	}

	// 5. nats.conf fail-closed gate (no include / unrecognized directive) would pass.
	if _, err := natsconf.Preflight(opts.ConfPath); err != nil {
		checks = append(checks, DoctorCheck{"nats.conf", DoctorFatal, err.Error()})
	} else {
		checks = append(checks, DoctorCheck{"nats.conf", DoctorPass, "no include / unrecognized directive (takeover's fail-closed gate would pass)"})
	}

	// 6. DB opens read-only (the migration source must be reachable). NO schema mutation / copy.
	if db, err := storage.OpenReadOnly("file:" + opts.DBPath); err != nil {
		checks = append(checks, DoctorCheck{"db", DoctorFatal, fmt.Sprintf("open %s read-only: %v", opts.DBPath, err)})
	} else {
		_ = db.Close()
		checks = append(checks, DoctorCheck{"db", DoctorPass, "tether.db opens read-only (migration source reachable)"})
	}

	return checks
}

// DoctorSummary counts the verdicts.
func DoctorSummary(checks []DoctorCheck) (pass, advisory, fatal int) {
	for _, c := range checks {
		switch c.Status {
		case DoctorPass:
			pass++
		case DoctorAdvisory:
			advisory++
		case DoctorFatal:
			fatal++
		}
	}
	return
}

// portBindCheck probes whether addr is bindable. An in-use raft-addr is a real conflict (FATAL);
// an in-use nats-route is expected pre-cutover (the running daemon → ADVISORY). A non-in-use bind
// error (permission / malformed addr) is always FATAL.
func portBindCheck(name, addr string, conflictFatal bool) DoctorCheck {
	ln, err := net.Listen("tcp", addr)
	if err == nil {
		_ = ln.Close()
		return DoctorCheck{name, DoctorPass, addr + " is free / bindable"}
	}
	if strings.Contains(err.Error(), "address already in use") {
		if conflictFatal {
			return DoctorCheck{name, DoctorFatal, addr + " already bound — a process OTHER than the broker you're migrating holds it"}
		}
		return DoctorCheck{name, DoctorAdvisory, addr + " in use — likely the running broker you'll stop (expected pre-cutover)"}
	}
	return DoctorCheck{name, DoctorFatal, fmt.Sprintf("%s bind failed: %v", addr, err)}
}

// stripScheme turns "nats://host:6222" into "host:6222" (net.Listen wants no scheme).
func stripScheme(u string) string {
	if i := strings.Index(u, "://"); i >= 0 {
		return u[i+3:]
	}
	return u
}
