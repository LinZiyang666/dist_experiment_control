package main

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/spf13/cobra"
)

// agent_doctor.go (C2) — `tether agent doctor`: a READ-ONLY diagnosis of agent config, identity,
// discovery cache, and broker reachability. It NEVER mints a key, NEVER CONNECTs, NEVER writes. A
// security-FATAL (forged/rollback manifest, missing identity) exits non-zero.

type doctorCheck struct{ name, status, detail string }

// errAgentDoctorFatal signals at least one FATAL doctor check (mapped to a non-zero exit).
var errAgentDoctorFatal = fmt.Errorf("agent doctor: one or more checks FAILED")

func newAgentDoctorCmd() *cobra.Command {
	var sid string
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Diagnose agent config, identity, discovery cache, and broker reachability (read-only)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if sid == "" {
				return fmt.Errorf("agent doctor: --session is required")
			}
			checks := runAgentDoctor(cmd.Context(), cli.DefaultHome(), sid)
			out := cmd.OutOrStdout()
			fatal := false
			for _, c := range checks {
				_, _ = fmt.Fprintf(out, "[%-5s] %-14s %s\n", c.status, c.name, c.detail)
				if c.status == "FATAL" {
					fatal = true
				}
			}
			if fatal {
				// Stage-C m1: a security-FATAL doctor result maps to exit 77 (EX_NOPERM), matching the
				// `agent join` forged-manifest class — not the default 70.
				return &ExitError{Class: exitNoPerm, Err: errAgentDoctorFatal}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&sid, "session", "", "session id (required)")
	return cmd
}

func runAgentDoctor(ctx context.Context, home, sid string) []doctorCheck {
	var checks []doctorCheck
	add := func(name, status, detail string) { checks = append(checks, doctorCheck{name, status, detail}) }

	ay, err := loadAgentYAML(home, sid)
	if err != nil {
		add("config", "FATAL", "agent.yaml unreadable: "+err.Error())
		return checks
	}
	if ay.BrokerURL == "" {
		add("config", "FATAL", "no broker_url (run `tether agent join <invite>`)")
	} else {
		add("config", "OK", "broker_url="+ay.BrokerURL)
	}
	if ay.AccountPub == "" {
		add("pin", "WARN", "no account_pub pinned (prefer `agent join` with an invite)")
	} else {
		add("pin", "OK", "account_pub="+ay.AccountPub)
	}

	// identity — read-only existence check; NEVER mints.
	keyPath := filepath.Join(home, "agent", sid, "keys", "agent.nk")
	if _, serr := os.Stat(keyPath); serr != nil {
		if os.Getenv(cli.DevNoAuthEnv) == "1" {
			add("identity", "WARN", "no agent.nk (TETHER_DEV_NO_AUTH → anonymous)")
		} else {
			add("identity", "FATAL", "no agent identity key at "+keyPath+" (run the agent once to mint it)")
		}
	} else {
		add("identity", "OK", "agent.nk present")
	}

	rc, _ := agent.ReadRosterCache(home, sid)
	now := time.Now()
	pin := ay.AccountPub
	if pin == "" && rc != nil {
		pin = rc.PinAccountPub
	}
	switch {
	case rc == nil:
		add("seed_cache", "WARN", "no cached roster (cold start; will fetch on connect)")
	case pin == "":
		add("seed_cache", "WARN", "cache present but no pin to verify against")
	default:
		st, detail := "OK", fmt.Sprintf("roster_gen=%d seed_gen=%d", rc.Generation, rc.SeedGen)
		if rc.Roster != nil {
			if verr := clusterroster.VerifyAt(rc.Roster, pin, now); verr != nil {
				st, detail = "WARN", "cached roster: "+verr.Error()
			}
		}
		if st == "OK" && rc.Seeds != nil {
			if verr := clusterroster.VerifySeedsAt(rc.Seeds, pin, now); verr != nil {
				st, detail = "WARN", "cached seeds: "+verr.Error()
			}
		}
		add("seed_cache", st, detail)
	}

	// manifest — live fetch; security-FATAL on a forged/rollback manifest.
	if ay.BootstrapURL != "" && ay.AccountPub != "" {
		fctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		m, ferr := clusterroster.FetchManifest(fctx, ay.BootstrapURL, 0)
		cancel()
		switch {
		case ferr != nil:
			add("manifest", "WARN", "fetch failed (advisory): "+ferr.Error())
		case m.Roster != nil && clusterroster.VerifyAt(m.Roster, ay.AccountPub, now) != nil:
			add("manifest", "FATAL", "roster does NOT verify against the pinned account (possible MITM)")
		case m.Seeds != nil && clusterroster.VerifySeedsAt(m.Seeds, ay.AccountPub, now) != nil:
			add("manifest", "FATAL", "seed bundle does NOT verify against the pinned account (possible MITM)")
		case rc != nil && m.Roster != nil && m.Roster.Generation < rc.Generation:
			add("manifest", "FATAL", "manifest roster generation is OLDER than the cached one (rollback)")
		case rc != nil && m.Seeds != nil && m.Seeds.Generation < rc.SeedGen:
			// Stage-C m2: the dedicated seed_generation counter exists to defeat exactly this rollback.
			add("manifest", "FATAL", "manifest seed generation is OLDER than the cached one (rollback)")
		default:
			add("manifest", "OK", "well-known manifest verifies against the pin")
		}
	}

	// tcp_reachable — ADVISORY (port open, not NATS-verified); never gates exit.
	if hp := firstHostPort(ay.BrokerURL); hp != "" {
		conn, derr := net.DialTimeout("tcp", hp, 3*time.Second)
		if derr != nil {
			add("tcp_reachable", "WARN", "advisory: "+hp+" unreachable: "+derr.Error())
		} else {
			_ = conn.Close()
			add("tcp_reachable", "OK", "advisory: "+hp+" port open (not NATS-verified)")
		}
	}
	return checks
}

// firstHostPort extracts host:port from the first entry of a (possibly comma-listed) NATS URL,
// defaulting to the NATS port when none is present.
func firstHostPort(brokerURL string) string {
	first := brokerURL
	if i := strings.IndexByte(first, ','); i >= 0 {
		first = first[:i]
	}
	u, err := url.Parse(strings.TrimSpace(first))
	if err != nil || u.Hostname() == "" {
		return ""
	}
	if u.Port() == "" {
		return net.JoinHostPort(u.Hostname(), "4222")
	}
	return u.Host
}
