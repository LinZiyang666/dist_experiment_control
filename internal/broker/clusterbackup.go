package broker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
)

// clusterbackup.go — the B6 OPS#3 ONLINE backup handler. It runs on ANY node (leader OR
// follower — a backup is a read-only paged copy off the RO handle, no raft write), so it is
// dispatched BEFORE the leader-only gate. It produces the same self-describing
// { state.db, manifest.json } bundle the offline path does, built from the live node's RO
// handle. account_fp (advisory) is omitted on the online path (the load-bearing provenance
// anchor — self_cert_fp — is captured from the DB self row either way).
func (b *clusterAdminBackend) handleBackup(req adminsock.Request) adminsock.Response {
	if req.BackupPath == "" {
		return adminsock.Response{Op: req.Op, Error: "backup requires --out <dir>", Code: adminsock.CodeBadRequest}
	}
	node := b.admin.node
	selfID := node.SelfID()
	bundleDir := req.BackupPath

	// External-review F3: an online backup off a lagging/partitioned FOLLOWER yields a structurally
	// complete but possibly-STALE bundle (missing leader-committed state). Default to leader-only —
	// the freshest committed view — and refuse a follower unless the operator explicitly opts in.
	isLeader := node.IsLeader()
	_, leaderID := node.LeaderWithID()
	sourceRole := "follower"
	if isLeader {
		sourceRole = "leader"
	}
	if !isLeader && !req.AllowFollower {
		hint := "online backup must run on the leader (the freshest committed state)"
		if leaderID != "" {
			hint += "; current leader: " + leaderID
		}
		hint += ". Re-run there, or pass --allow-stale-follower to snapshot THIS node's possibly-stale local view."
		return adminsock.Response{Op: req.Op, Error: hint, Code: adminsock.CodeNotLeader}
	}

	// O_EXCL bundle dir (never clobber an existing bundle). Clean up on any later failure.
	if err := os.MkdirAll(filepath.Dir(bundleDir), 0o700); err != nil {
		return adminsock.Response{Op: req.Op, Error: fmt.Sprintf("prepare bundle parent: %v", err), Code: adminsock.CodeStoreError}
	}
	if err := os.Mkdir(bundleDir, 0o700); err != nil {
		return adminsock.Response{Op: req.Op, Error: fmt.Sprintf("create bundle dir %q (must not exist): %v", bundleDir, err), Code: adminsock.CodeBadRequest}
	}
	ok := false
	defer func() {
		if !ok {
			_ = os.RemoveAll(bundleDir)
		}
	}()

	statePath := filepath.Join(bundleDir, "state.db")
	manifestPath := filepath.Join(bundleDir, "manifest.json")
	if err := node.BackupDBTo(context.Background(), statePath); err != nil {
		return adminsock.Response{Op: req.Op, Error: fmt.Sprintf("backup state.db: %v", err), Code: adminsock.CodeStoreError}
	}
	if err := cluster.VerifyIntegrity(statePath); err != nil {
		return adminsock.Response{Op: req.Op, Error: fmt.Sprintf("backup verify: %v", err), Code: adminsock.CodeStoreError}
	}

	// Audit DI-MAJOR-3: build the manifest FROM THE COPY (state.db), not the LIVE RODB handle. The
	// paged backup snapshots at applied_index=X; the live DB may have committed to Y>X by now, so
	// reading identity/roster/applied_index off the live handle would overstate the bundle's true
	// cursor (DR tooling trusting manifest.applied_index would believe it more current than it is).
	copyRO, err := storage.OpenReadOnly("file:" + statePath)
	if err != nil {
		return adminsock.Response{Op: req.Op, Error: fmt.Sprintf("backup open copy: %v", err), Code: adminsock.CodeStoreError}
	}
	defer func() { _ = copyRO.Close() }()
	m, err := clusteroffline.ReadSelfIdentity(copyRO, selfID)
	if err != nil {
		return adminsock.Response{Op: req.Op, Error: fmt.Sprintf("backup manifest identity: %v", err), Code: adminsock.CodeStoreError}
	}
	m.Kind = clusteroffline.ManifestKindBackup
	m.Mode = clusteroffline.ManifestModeCluster
	m.CreatedAt = b.admin.now().UTC().Format(timeRFC3339Nano)
	m.ToolVersion = proto.ReleaseVersion
	// External-review F3: stamp the freshness provenance into the bundle so DR tooling can tell a
	// leader snapshot from a (possibly-stale) follower snapshot.
	m.SourceNodeID = selfID
	m.SourceRole = sourceRole
	m.LeaderID = leaderID
	roster, err := clusteroffline.ProjectRoster(copyRO)
	if err != nil {
		return adminsock.Response{Op: req.Op, Error: fmt.Sprintf("backup roster: %v", err), Code: adminsock.CodeStoreError}
	}
	m.Roster = roster
	if err := clusteroffline.WriteManifest(manifestPath, m); err != nil {
		return adminsock.Response{Op: req.Op, Error: fmt.Sprintf("backup manifest: %v", err), Code: adminsock.CodeStoreError}
	}

	st, _ := os.Stat(statePath)
	var bytes int64
	if st != nil {
		bytes = st.Size()
	}
	ok = true
	b.admin.logger.Info("cluster: online backup complete", "bundle", bundleDir, "self", selfID,
		"applied_index", m.AppliedIndex, "source_role", sourceRole)
	return adminsock.Response{Op: req.Op, OK: true, Backup: &adminsock.BackupResult{
		Path: bundleDir, Bytes: bytes, AppliedIndex: m.AppliedIndex, SelfID: selfID,
		SourceRole: sourceRole, LeaderID: leaderID,
	}}
}

// timeRFC3339Nano mirrors time.RFC3339Nano without importing time at the file top (the layout is
// a const so the manifest timestamps match the offline path byte-for-byte).
const timeRFC3339Nano = "2006-01-02T15:04:05.999999999Z07:00"
