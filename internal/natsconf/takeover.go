package natsconf

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/natscluster"
)

// DryRun validates the merged conf with `nats-server -c <temp> -t` BEFORE Apply swaps it
// in (round-1 BLOCKER: takeover was the LAST live-box step, so an invalid conf was only
// discovered when nats-server failed to restart — bricking the broker). It writes the conf
// to a private temp file and runs the validator; a non-zero exit (an invalid directive, a
// bad passthrough value, a websocket re-emit edge case) is a hard refusal. natsServerBin is
// the validator path (PATH lookup or an explicit /usr/local/bin/nats-server). A missing
// binary is reported so the caller refuses rather than swap an unvalidated conf.
func DryRun(natsServerBin string, mergedConf string) error {
	if natsServerBin == "" {
		natsServerBin = "nats-server"
	}
	if _, err := exec.LookPath(natsServerBin); err != nil {
		return fmt.Errorf("natsconf: cannot validate the merged conf: %q not found (%w) — "+
			"install nats-server or pass --nats-server <path>; refusing to swap an unvalidated conf", natsServerBin, err)
	}
	tmp, err := os.CreateTemp("", "tether-natsconf-*.conf")
	if err != nil {
		return fmt.Errorf("natsconf: dry-run temp: %w", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.WriteString(mergedConf); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("natsconf: dry-run write: %w", err)
	}
	_ = tmp.Close()
	out, err := exec.Command(natsServerBin, "-c", tmp.Name(), "-t").CombinedOutput()
	if err != nil {
		return fmt.Errorf("natsconf: `nats-server -t` REJECTED the merged conf (not swapping): %v\n%s", err, out)
	}
	return nil
}

// takeover.go — the D9 §11 merge + atomic apply. BuildMergedConf renders the cluster
// directives (natscluster.Render: server_name + listen + jetstream + cluster mTLS +
// authorization/auth_callout + per-broker ACLs) and APPENDS the install.sh directives
// natscluster does NOT emit but that must survive: the websocket{} block (Caddy reverse-
// proxies :8222) and the recognized tuning passthrough (max_payload etc.). Apply swaps the
// file atomically, preserving a one-time pristine .bak. The cluster IDENTITY in the Config
// is the caller's responsibility to assemble from the §15 secrets + the existing conf's
// auth_callout (so server_name == nats_server_id == the agent's ConnectedServerName, and
// the account issuer matches the live one — SSOT by construction).

// BuildMergedConf renders cfg via natscluster.Render and appends the preserved install.sh
// websocket{} block + the recognized tuning passthrough directives from own. The result is
// the full nats-server.conf for the cluster-mode broker.
func BuildMergedConf(own *Ownership, cfg natscluster.Config) (string, error) {
	// C3-B1: when the caller (the reconciler) does not supply the routes-mTLS identity, harvest it
	// from the LIVE conf's cluster{} block so the render is COMPLETE (Render rejects empty cert paths).
	// Fail-closed via ClusterMTLS if the live conf carries no cluster TLS. v0.4.2 shrink (review minor):
	// a Standalone render has NO cluster{} block, so it never needs the mTLS identity — skip the
	// harvest, else de-clustering a hand-edited clustered conf whose cluster{} lacks tls{} would fail
	// confusingly while trying to DISCARD that very block.
	if !cfg.Standalone && cfg.CAFile == "" && cfg.CertFile == "" && cfg.KeyFile == "" {
		ca, cert, key, listen, name, err := own.ClusterMTLS()
		if err != nil {
			return "", err
		}
		cfg.CAFile, cfg.CertFile, cfg.KeyFile = ca, cert, key
		if cfg.ClusterListen == "" {
			cfg.ClusterListen = listen
		}
		if cfg.ClusterName == "" {
			cfg.ClusterName = name
		}
	}
	// C3-B3: when the caller does not set a monitor, PRESERVE the live conf's `http:` (if any) — never
	// hot-add one (nats-server rejects an http-port add on SIGHUP). A new monitor is established only by
	// a manual takeover that sets MonitorListen explicitly, followed by a restart.
	if cfg.MonitorListen == "" {
		cfg.MonitorListen = own.MonitorHTTP()
	}
	rendered, err := natscluster.Render(cfg)
	if err != nil {
		return "", fmt.Errorf("natsconf: render cluster conf: %w", err)
	}
	var b strings.Builder
	b.WriteString(rendered)
	if !strings.HasSuffix(rendered, "\n") {
		b.WriteString("\n")
	}
	// Preserve the install.sh websocket{} block verbatim-by-re-emit (natscluster.Render
	// does not emit it; Caddy reverse-proxies WSS :443 → this internal :8222). Re-emit from
	// the parsed map (deterministic; comments are lost, owned).
	if ws := websocketBlock(own); ws != "" {
		b.WriteString("\n")
		b.WriteString(ws)
	}
	// Preserve recognized tuning directives (max_payload etc.) verbatim — refusing them
	// would brick a docs-compliant file-transfer broker (usage.md §5.16).
	if pt := passthroughBlock(own); pt != "" {
		b.WriteString("\n")
		b.WriteString(pt)
	}
	return b.String(), nil
}

// websocketBlock re-emits the preserved websocket{host,port,no_tls} from the parsed conf.
func websocketBlock(own *Ownership) string {
	ws, ok := own.Parsed["websocket"].(map[string]any)
	if !ok {
		return ""
	}
	var b strings.Builder
	b.WriteString("websocket {\n")
	if h, ok := ws["host"].(string); ok && h != "" {
		fmt.Fprintf(&b, "  host: %q\n", h)
	}
	if p := intVal(ws["port"]); p != 0 {
		fmt.Fprintf(&b, "  port: %d\n", p)
	}
	if nt, ok := ws["no_tls"].(bool); ok && nt {
		b.WriteString("  no_tls: true\n")
	}
	b.WriteString("}\n")
	return b.String()
}

// passthroughBlock re-emits the recognized tuning directives (TetherPassthrough) verbatim.
func passthroughBlock(own *Ownership) string {
	var keys []string
	for _, e := range own.Entries {
		if e.Bucket == TetherPassthrough {
			keys = append(keys, e.Key)
		}
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		switch v := own.Parsed[k].(type) {
		case int64:
			fmt.Fprintf(&b, "%s: %d\n", k, v)
		case int:
			fmt.Fprintf(&b, "%s: %d\n", k, v)
		case string:
			fmt.Fprintf(&b, "%s: %q\n", k, v)
		}
	}
	return b.String()
}

func intVal(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	}
	return 0
}

// Apply atomically replaces confPath with merged, preserving a one-time pristine .bak
// (skip-if-exists: a prior takeover's .bak is the real pristine pre-takeover copy). It
// writes a sibling temp (same dir → same filesystem → atomic rename), fsyncs the temp and
// the directory, then renames. The caller should have run a `nats-server -t` dry-run first.
func Apply(confPath, merged string) error {
	bak := confPath + ".bak." + bakStamp()
	info, err := os.Stat(confPath)
	if err != nil {
		return fmt.Errorf("natsconf: stat source: %w", err)
	}
	confPerm := info.Mode().Perm()
	if confPerm == 0 {
		confPerm = 0o600
	}
	// One-time pristine backup: only if NO .bak.* already exists.
	if existing, _ := filepath.Glob(confPath + ".bak.*"); len(existing) == 0 {
		raw, err := os.ReadFile(confPath)
		if err != nil {
			return fmt.Errorf("natsconf: read for backup: %w", err)
		}
		if err := writeSync(bak, raw, confPerm); err != nil {
			return fmt.Errorf("natsconf: write .bak: %w", err)
		}
	}
	dir := filepath.Dir(confPath)
	tmp, err := os.CreateTemp(dir, ".nats.conf.*")
	if err != nil {
		return fmt.Errorf("natsconf: temp: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }() // no-op after a successful rename
	if _, err := tmp.WriteString(merged); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("natsconf: write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("natsconf: fsync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("natsconf: close temp: %w", err)
	}
	if err := os.Chmod(tmpName, confPerm); err != nil {
		return fmt.Errorf("natsconf: chmod temp: %w", err)
	}
	if err := os.Rename(tmpName, confPath); err != nil {
		return fmt.Errorf("natsconf: atomic rename: %w", err)
	}
	return fsyncDir(dir)
}

func writeSync(path string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, perm)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

func bakStamp() string { return time.Now().UTC().Format("20060102T150405Z") }

// OwnershipTable renders the before/after ownership table (printed to stdout + embedded in
// broker-ops.md §2.3) so the operator can see which directives tether now owns.
func OwnershipTable(own *Ownership) string {
	var b strings.Builder
	b.WriteString("nats.conf directive ownership after takeover:\n")
	for _, e := range own.Entries {
		var note string
		switch e.Bucket {
		case InstallSafe:
			note = "preserved (install.sh)"
		case TetherGen:
			note = "REGENERATED by tether (cluster)"
		case OperatorAuth:
			note = "superseded by tether's generated authorization (identity read from it)"
		case TetherPassthrough:
			note = "preserved (tuning)"
		}
		fmt.Fprintf(&b, "  %-16s %s\n", e.Key, note)
	}
	return b.String()
}
