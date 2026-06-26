package main

import (
	"fmt"
	"io"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
)

// cluster_rotation.go (C7) — the GUIDED compromised-node credential-rotation printer + the persistent
// NOT-SAFE tracking alert. C7 is the security-sensitive phase: this is a PRINTER/CHECKLIST (idiom =
// forceSingleGuided/recoverGuided), NOT an automator. It NEVER generates or moves private key material
// (rejection #2) and it NEVER signals "safe" — it raises a persistent severe alert + a non-green banner
// the operator must explicitly clear AFTER an out-of-band reseed (rejection #5).
//
// The ONLY secret-touching helpers it calls are the sanctioned public/hash-only readers
// (derivePublicKey, readClusterPublicIdentities, AccountFingerprint, SecretsPreflight) — enforced by
// the allow-list token-scan guard TestC7RotationNoPrivateKeyExfilStatic.

// rotationTrackingKey is the SSOT for the per-node NOT-SAFE alert dedup key (manual:<label>).
func rotationTrackingKey(node string) string { return "manual:credrot:" + node }

// raiseCredRotationAlert raises the persistent severe NOT-SAFE tracking alert for a retired
// compromised node. Best-effort: a failure (e.g. mid-Propose leadership loss) does NOT fail the
// command — the non-green banner is the floor; the alert is durable persistence. Returns the error so
// the caller can surface it as a warning.
func raiseCredRotationAlert(socketPath, node string) error {
	resp, err := callAdmin(socketPath, adminsock.Request{
		Op:            adminsock.OpClusterAlertRaise,
		AlertKind:     cluster.AlertKindManual,
		AlertSeverity: cluster.AlertSeveritySevere,
		AlertLabel:    "credrot:" + node,
		// Secret-free: node-id + static text only.
		AlertMessage: "credentials NOT yet rotated after retiring compromised node " + node +
			" — the cluster is NOT SAFE against it; complete the rotation guide then `tether alert clear " +
			rotationTrackingKey(node) + "`",
	})
	if err != nil {
		return err
	}
	if resp.Error != "" {
		return clusterAdminError("alert raise", resp)
	}
	return nil
}

// printCredentialRotationGuide prints the read-only, SECRET-FREE rotation checklist. secretsDir is read
// for PUBLIC fingerprints (this host only) via the sanctioned helpers; nothing private is read or moved.
// raised reports whether the caller actually raised the persistent NOT-SAFE alert (M2: the advisory
// --compromised-only path does NOT raise it, so the "cluster reports a severe alert" / "alert clear"
// lines are suppressed there to avoid pointing the operator at a non-existent alert).
func printCredentialRotationGuide(w io.Writer, node, secretsDir, confPath string, raised bool) {
	p := func(format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }
	p("\n=== CREDENTIAL ROTATION GUIDE (compromised node %s) ===\n", node)
	p("RETIRE IS A TOPOLOGY CHANGE, NOT A TRUST REVOCATION. %s still holds account.nk + the cluster CA +\n", node)
	if raised {
		p("its route key until you rotate. The cluster reports NOT-SAFE (severe alert %s) until you finish\n", rotationTrackingKey(node))
		p("the steps below ON EVERY SURVIVING BROKER and clear the alert.\n\n")
	} else {
		p("its route key until you rotate. The cluster is NOT SAFE against it until you finish the steps\n")
		p("below ON EVERY SURVIVING BROKER. (Run with --require-credential-rotation to also raise a\n")
		p("persistent severe alert that tracks this until cleared.)\n\n")
	}

	// Advisory LOCAL public fingerprints (THIS host only — cannot prove the other voters' state).
	ids := readClusterPublicIdentities(secretsDir, confPath)
	p("Current credentials on THIS host (verify your NEW ones DIFFER — this is the local view only):\n")
	if ids.AccountIssuer != "" {
		p("  account issuer (public): %s\n", ids.AccountIssuer)
	}
	if ids.Note != "" {
		p("  %s\n", ids.Note)
	}
	if caFP, err := clusteroffline.AccountFingerprint(secretsDir); err == nil {
		p("  cluster-ca.pem sha256:    %s\n", caFP)
	}
	// (the per-host tunnel-cert fp is shown in Step B, derived per surviving broker, not here.)
	p("\n")

	p("STEP A — account.nk + cluster-CA + route leaves  (CRITICAL, OUT-OF-BAND, your own PKI):\n")
	p("  account.nk signs EVERY user JWT — it is the load-bearing shared secret.\n")
	p("  1. On a trusted host, regenerate account.nk (your `nk`), and re-issue the cluster CA + each\n")
	p("     surviving voter's route-cert.pem/route-key.pem with YOUR PKI.\n")
	p("  2. Distribute the new 0600 files to EACH surviving voter's secrets dir over YOUR OWN channel\n")
	p("     (scp/ssh). tether NEVER copies private keys for you (rejection #2).\n")
	p("  3. On each voter: `tether cluster reconcile nats` (or `reconcile nats --manual`) so the\n")
	p("     auth_callout issuer matches the new PUBLIC account key, then rolling-restart the broker.\n")
	p("  See docs/cluster-runbook.md §2.1 for the verbatim commands.\n\n")

	p("STEP B — tunnel-cert  (OPTIONAL defense-in-depth, online; does NOT mitigate %s's compromise):\n", node)
	p("  The retired node's tunnel key left with it. Rotating survivors is fleet hygiene only.\n")
	p("  Per surviving broker <self>: `tether cluster transfer-leader <self> --wait` then\n")
	p("    `tether cluster rotate-tunnel-cert <self> --cert-fp sha256:<new>` (rotates ITS OWN cert).\n\n")

	p("NOT rotated here (intentional):\n")
	p("  - broker.nk / node-ident.nk are PER-NODE: they depart with %s. broker.nk trust rides the\n", node)
	p("    account.nk chain you rotate in Step A; the raft identity is revoked by the retire (RemoveServer).\n")
	p("  - If %s hosted a proxy subscription, rotate that PSK separately (out of cluster scope).\n\n", node)

	p("CAVEAT: even after rotation, JWTs the OLD account.nk signed stay valid until their TTL expires\n")
	p("  (or you force-disconnect sessions). Do not assume instantaneous revocation.\n\n")

	if raised {
		p("WHEN DONE on every surviving broker (rotated + redistributed + reloaded):\n")
		p("  tether alert clear %s\n", rotationTrackingKey(node))
	} else {
		p("WHEN DONE: re-verify your NEW public fingerprints differ on every surviving broker.\n")
	}
}

// printNotSafeBanner is the non-green floor (rejection #5): no "done/safe/secure/complete" wording.
func printNotSafeBanner(w io.Writer, node string) {
	_, _ = fmt.Fprintf(w, "\nSECURITY: credentials are NOT yet rotated — %s can still authenticate. The cluster is NOT SAFE\n", node)
	_, _ = fmt.Fprintf(w, "  against this node until you complete the rotation above and clear %s.\n", rotationTrackingKey(node))
	_, _ = fmt.Fprintf(w, "  Track: `tether alert ls` / `tether cluster status`.\n")
}
