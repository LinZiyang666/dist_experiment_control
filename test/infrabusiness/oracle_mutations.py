"""Hermetic on-disk oracle mutations; always restore source before checking green.

Run from the repository root with python3 test/infrabusiness/oracle_mutations.py.
Only this package is edited/tested; no live producer or installed binary runs.
"""
from pathlib import Path
import json
import subprocess
import sys

base = Path('test/infrabusiness')
mutations = []

def add(name, file, old, new, test):
    mutations.append((name, file, old, new, test))

def omit(name, file, fn, test):
    old = 'if err := '+fn+'; err != nil {\n\t\treturn err\n\t}'
    add(name, file, old, '// Mutation: omit the required evidence guard.', test)

Q = '^TestBusinessOracle'
omit('F3-attachment-scope', 'migration_test.go', 'checkAttachmentScopes(r)', Q+'RejectsReplayedAttachmentHops$')
add('F3-cross-hop-continuity', 'receipt_scope_test.go', 'exists && (prev.Returned != row.Old || prev.ReturnedRuntime != row.OldRuntime)', 'exists && false && (prev.Returned != row.Old || prev.ReturnedRuntime != row.OldRuntime)', Q+'BindsEveryCutProof$/old_generation_replayed$')
omit('F3-auth-scope', 'auth_test.go', 'checkAuthScopes(r)', Q+'RejectsForeignAuthSteps$')
add('F3-required-local-queue', 'receipt_scope_test.go', 'if len(seen) != len(required) {', 'if false && len(seen) != len(required) {', Q+'BindsEveryCutProof$/auth_missing_host_queue$')
omit('F4-raw-progress-dispatch', 'transfer_test.go', 'checkTransferProgress(r, originals)', Q+'ComputesRawDurableProgress$/frozen_sixty_seconds$')
add('F4-computed-stall', 'transfer_evidence_test.go', 'cp.AtNS-lastAdvance > int64(upgradebudget.TransferProgress)', 'false && cp.AtNS-lastAdvance > int64(upgradebudget.TransferProgress)', Q+'ComputesRawDurableProgress$/raw_stall_overrides_zero_summary$')
add('F4-per-hop-progress', 'transfer_evidence_test.go', '|| last.Durable <= before {', '|| false && last.Durable <= before {', Q+'ComputesRawDurableProgress$/no_per_hop_progress$')
add('F4-raw-summary-binding', 'transfer_evidence_test.go', 'if last.Durable != row.Durable || last.PrefixSHA256 != row.RawPrefixSHA256 {', 'if false && (last.Durable != row.Durable || last.PrefixSHA256 != row.RawPrefixSHA256) {', Q+'ComputesRawDurableProgress$/raw_summary_conflict$')
omit('F4-final-receipts', 'transfer_test.go', 'checkTransferFinals(r, originals)', Q+'RequiresFinalAndPhaseContent$/(missing_final|missing_terminal|duplicate_start|incomplete_history_scan|wrong_terminal_bytes|wrong_hash|wrong_bytes|final_consumer_missing|consumer_still_present)$')
omit('F4-phase-content', 'transfer_test.go', 'checkTransferPhases(r, originals)', Q+'RequiresFinalAndPhaseContent$/(missing_commit_lane|coverage_labels_only|phase_same_invocation|phase_did_not_cross_cut|rename_not_observed|parent_not_synced|terminal_not_delivered|terminal_bytes_changed|no_pending_ack|no_partial_write|no_synced_progress)$')
add('F5-quadrant-set', 'transfer_evidence_test.go', '|| quadrants[quadrant] ||', '|| false && quadrants[quadrant] ||', Q+'FreezesFourTransferQuadrants$/(all_small|all_large|duplicate_quadrant)$')
add('F5-frozen-hop-tuples', 'transfer_evidence_test.go', 'row.Direction != id.Direction || row.Session != id.Session || row.Bucket != id.Bucket ||', 'false && (row.Direction != id.Direction || row.Session != id.Session || row.Bucket != id.Bucket) ||', Q+'FreezesFourTransferQuadrants$/later_hop_changed_size_direction_assignment$')
add('F6-global-cid', 'receipt_scope_test.go', 'return a.ServerID == b.ServerID && a.CID == b.CID', 'return a.CID == b.CID', Q+'AcceptsDistinctServersWithEqualCID$')
add('F6-route-global-cid', 'migration_test.go', 'sameConnection(row.Old, row.New)', 'row.Old.CID == row.New.CID', Q+'AcceptsDistinctServersWithEqualCID$')
add('F6-reused-raw', 'migration_test.go', 'row.New.Raw.Cookie == row.Old.Raw.Cookie || row.Returned.Raw.Cookie == row.New.Raw.Cookie', 'false', Q+'RejectsReusedPhysicalConnections$/raw_socket$')
add('F6-route-accepted-process', 'migration_test.go', '|| row.New.Ingress != row.Target ||', '|| false && row.New.Ingress != row.Target ||', Q+'RejectsReusedPhysicalConnections$/(accepted_pid|accepted_exe)$')

add('F5-precut-seal', 'transfer_evidence_test.go', 'if len(r.Hops) == 0 || r.TransferSealNS <= 0 || r.TransferSealNS > r.Hops[0].StartNS || r.TransferSealSHA256 != contentDigest(sealed) {', 'if false && (len(r.Hops) == 0 || r.TransferSealNS <= 0 || r.TransferSealNS > r.Hops[0].StartNS || r.TransferSealSHA256 != contentDigest(sealed)) {', Q+'FreezesFourTransferQuadrants$/(sealed_after_cut|changed_seal)$')

selected = set(sys.argv[1:])
if selected - {row[0] for row in mutations}:
    raise RuntimeError('unknown mutation selector')
results = []
if selected:
    results = [row for row in json.loads((base/'mutation-receipts.json').read_text()) if row['name'] not in selected]
    mutations = [row for row in mutations if row[0] in selected]
for name, file, old, new, test in mutations:
    path = base/file
    original = path.read_text()
    if old not in original:
        raise RuntimeError('mutation site missing: '+name)
    cmd = ['go', 'test', '-race', '-tags', 'infrabusiness_integration', './test/infrabusiness', '-run', test, '-count=1', '-timeout', '120s']
    try:
        path.write_text(original.replace(old, new))
        red = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=150)
    finally:
        path.write_text(original)
    green = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=150)
    results.append(dict(name=name, file=file, old=old, new=new, command=cmd,
                        red_rc=red.returncode, red_output=red.stdout,
                        restored_rc=green.returncode, restored_output=green.stdout))
    (base/'mutation-receipts.json').write_text(json.dumps(results, indent=2)+'\n')
    print(name, 'red='+str(red.returncode), 'restored='+str(green.returncode), flush=True)
    if red.returncode != 1 or '--- FAIL:' not in red.stdout or green.returncode != 0:
        print(red.stdout, green.stdout, flush=True)
        raise RuntimeError('mutation did not produce assertion red / restored green: '+name)
