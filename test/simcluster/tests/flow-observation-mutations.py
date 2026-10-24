#!/usr/bin/env python3
"""Serial edit/run/restore controls for private observer receipts.

Go runtime mutations use overlays. Source gates and Python use on-disk edits;
no deployment, installed binary, mechanical ledger or other worktree is touched.
origin: hot-update S5 plan §8 / T26 / T48 / DRILL-OBSERVER.
"""
import argparse
import difflib
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile

REPO = Path(__file__).resolve().parents[3]
CONTROL = 'test/simcluster/tests/flow-fixture-test.py'
OBS = 'test/simcluster/drills/lib/flow_observation.py'
GATE = 'TestObservationHooksKeepExactProductBarriers'


def go(pkg, name):
    return ['go','test','-count=1','-timeout','40s','./'+pkg,'-run','^'+name+'$']


def py(name):
    return [sys.executable,'-I','-B',CONTROL,'ObservationCustodyTests.'+name]


# Each named prerequisite is independently removed; its recorded control must
# fail its assertion, rather than merely fail to compile or parse the mutation.
PY_GUARDS = [
 ('partial-count', 'product_partial_cipher_observer_unavailable', 'test_partial_requires_actual_short_write_suffix_and_before_cut'),
 ('partial-seal', 'partial_without_seal', 'test_partial_requires_actual_short_write_suffix_and_before_cut'),
 ('custody-seals', 'custody_unsealed', 'test_custody_rejects_unsealed_truncated_poisoned_and_gapped_streams'),
 ('custody-header', 'custody_header', 'test_custody_rejects_unsealed_truncated_poisoned_and_gapped_streams'),
 ('custody-poison', 'custody_broken', 'test_custody_rejects_unsealed_truncated_poisoned_and_gapped_streams'),
 ('custody-trailing', 'custody_trailing', 'test_custody_rejects_unsealed_truncated_poisoned_and_gapped_streams'),
 ('custody-sequence', 'custody_sequence_gap', 'test_custody_rejects_unsealed_truncated_poisoned_and_gapped_streams'),
 ('nonce-alias', 'nonce_reuse', 'test_actual_subkey_nonce_ledger_rejects_aliases_and_missing_attempts'),
 ('missing-attempts', 'product_seal_observer_unavailable', 'test_actual_subkey_nonce_ledger_rejects_aliases_and_missing_attempts'),
 ('revoke-dup', 'victim_dup_not_zero', 'test_revoke_requires_original_binding_revision_and_zero_dups'),
 ('fresh-grant', 'fresh_ss_grant_unavailable', 'test_revoke_requires_original_binding_revision_and_zero_dups'),
 ('domain-budget', 'domain_budget_exceeded', 'test_budget_and_default_reaper_require_live_domain_receipts'),
 ('domain-live', 'live_domain_observations_unavailable', 'test_budget_and_default_reaper_require_live_domain_receipts'),
 ('default-reaper', 'live_reaper_receipt_unavailable', 'test_budget_and_default_reaper_require_live_domain_receipts'),
 ('offset-base', 'offset_base_not_advanced', 'test_progress_requires_current_original_offset_base_nonce_and_retirement'),
 ('nonce-progress', 'nonce_not_advanced', 'test_progress_requires_current_original_offset_base_nonce_and_retirement'),
 ('retirement-progress', 'retirement_not_advanced', 'test_progress_requires_current_original_offset_base_nonce_and_retirement'),
 ('legacy-alpn', 'legacy_alpn_not_empty', 'test_legacy_requires_negotiated_empty_alpn_and_original_prefix'),
 ('legacy-prefix', 'legacy_prefix_unavailable', 'test_legacy_requires_negotiated_empty_alpn_and_original_prefix'),
 ('legacy-business-barrier', 'legacy_negotiated_after_business', 'test_legacy_negotiation_precedes_original_business'),
 ('legacy-header', 'legacy_flow_header', 'test_legacy_requires_negotiated_empty_alpn_and_original_prefix'),
 ('watchdog-permission', 'watchdog_permission_prior_unavailable', 'test_watchdog_requires_applied_failure_ready_and_automatic_current_cut'),
 ('watchdog-ready', 'watchdog_mark_ready_unavailable', 'test_watchdog_requires_applied_failure_ready_and_automatic_current_cut'),
 ('watchdog-deadline', 'watchdog_automatic_deadline_unavailable', 'test_watchdog_requires_applied_failure_ready_and_automatic_current_cut'),
 ('watchdog-current-cut', 'watchdog_second_current_cut_unavailable', 'test_watchdog_requires_applied_failure_ready_and_automatic_current_cut'),
 ('original-fd', 'original_flow_fd_changed', 'test_original_business_binding_uses_same_flow_and_actual_tls_session'),
 ('original-session', 'original_carrier_not_changed', 'test_original_business_binding_uses_same_flow_and_actual_tls_session'),
 ('legacy-original-carrier', 'legacy_original_carrier_changed', 'test_legacy_original_socket_keeps_one_real_carrier_binding'),
 ('broker-budget', 'broker_flow_budget_exceeded', 'test_broker_replay_domain_is_a_separate_node_budget_and_fd_census'),
 ('broker-kernel', 'broker_kernel_observation_unavailable', 'test_broker_replay_domain_is_a_separate_node_budget_and_fd_census'),
 ('payload-primitive', 'partial_payload_seal_binding', 'test_payload_partial_counts_one_recorded_primitive_for_original_cipher'),
]


def mutations():
    specs=[]
    source=(REPO/OBS).read_text()
    for name,criterion,test in PY_GUARDS:
        # Replace the condition, retaining the same branch and subsequent code.
        pattern=r"require\((.*?)\s*,\s*['\"]"+re.escape(criterion)+r"['\"]\)"
        matches=list(re.finditer(pattern,source,re.S))
        # Start at the last require() before this criterion; the permissive regex
        # must not consume unrelated earlier calls across a function boundary.
        tail=source.index("'"+criterion+"'")
        start=source.rfind('require(',0,tail)
        end=source.index(')',tail)+1
        old=source[start:end]
        new="require(True,'"+criterion+"')"
        specs.append((name,OBS,old,new,py(test),False))
    specs.extend([
      ('closed-actor-hidden-fds',OBS,"('ss-state','ss-binding','ss-closed') and len(e['data'])>=56","('ss-state','ss-binding','ss-closed') and e['actor'] not in {row['actor'] for row in self.events if row['kind']=='ss-closed'} and len(e['data'])>=56",py('test_closed_actor_duplicates_remain_in_live_domain_census'),False),
      ('victim-replay-dup',OBS,"if e['flow']==binding and len(e['data'])==32:","if False and e['flow']==binding and len(e['data'])==32:",py('test_revoke_requires_original_binding_revision_and_zero_dups'),False),
      ('default-reaper-dwell','internal/broker/proxy_reconcile.go','const proxyRehomeDwell = 3','const proxyRehomeDwell = 2',go('internal/drillobserve',GATE),False),
      ('keeper-private-mode',OBS,'os.chmod(path,0o660)','os.chmod(path,0o666)',py('test_keeper_requires_fresh_ack_and_preserves_sealed_archive_bytes'),False),
      ('installer-private-group','test/simcluster/drills/lib/flow_observer_service.py','root.chmod(0o2770)','root.chmod(0o2777)',py('test_installer_preserves_product_uid_and_resolves_private_operator_group'),False),
      ('keeper-fresh-ack',OBS,'if not records.broken:\n                            try:', 'if False:\n                            try:',py('test_keeper_requires_fresh_ack_and_preserves_sealed_archive_bytes'),False),
      ('keeper-archive',OBS,'self.compact()', 'pass',py('test_keeper_requires_fresh_ack_and_preserves_sealed_archive_bytes'),False),
      ('common-global-ledger','test/simcluster/drills/lib/hot_update_flow_probe.py',"require(len(pairs)==len(set(pairs)),'nonce_reuse')","require(True,'nonce_reuse')",py('test_common_judge_consumes_all_attempts_across_failed_writes_and_aliases'),False),
      ('common-original-loss','test/simcluster/drills/lib/flow_fixture_peer.py',"for row in document['flows']:","for row in []:",[sys.executable,'-I','-B',CONTROL,'PeerTests.test_common_judge_keeps_native_loss_and_refuses_client_only_crypto_proof'],False),
      ('client-payload-primitive','test/simcluster/drills/lib/flow_fixture_peer.py',"seal_calls=sum(\n                                                  call['nonce']==(self.tx-1).to_bytes(12,'little').hex() and\n                                                  call['subkey_sha256']==probe.digest(self.key) for call in self.calls)","seal_calls=0",[sys.executable,'-I','-B',CONTROL,'PeerTests.test_partial_cipher_receipt_uses_the_actual_writer_count'],False),
      ('polling-run-cancellation','internal/agent/agent.go','case <-ctx.Done():\n\t\t\treturn\n\t\tcase <-ticker.C:', 'case <-ctx.Done():\n\t\t\tcontinue\n\t\tcase <-ticker.C:',go('internal/agent','TestAgentCustodyPollingJoinsRunCancellation'),True),
      ('actual-subkey','internal/agent/ssproxy/observation.go','Digest: sha256.Sum256(subkey)', 'Digest: sha256.Sum256(actor[:])',go('internal/agent/ssproxy','TestSSObservationRecordsEveryAttemptBeforeSeal'),True),
      ('attempt-before-seal','internal/agent/ssproxy/crypto.go','observeSeal(a, nonce, len(plain))','_ = a',go('internal/agent/ssproxy','TestSSObservationRecordsEveryAttemptBeforeSeal'),True),
      ('seal-inert-nil-guard','internal/agent/ssproxy/observation.go','func observeSeal(a cipher.AEAD, nonce []byte, size int) {\n\tif drillobserve.Hook == nil {','func observeSeal(a cipher.AEAD, nonce []byte, size int) {\n\tif drillobserve.Hook != nil {',go('internal/drillobserve',GATE),False),
      ('failed-write-count','internal/agent/ssproxy/crypto.go','uint64(len(attempt)), uint64(n), uint64(len(attempt) - n)','uint64(len(attempt)), uint64(len(attempt)), uint64(len(attempt) - n)',go('internal/agent/ssproxy','TestSSObservationRetainsFailedShortWriteWithoutReseal'),True),
      ('failed-write-suffix','internal/agent/ssproxy/crypto.go','Data: attempt[n:]','Data: nil',go('internal/agent/ssproxy','TestSSObservationRetainsFailedShortWriteWithoutReseal'),True),
      ('payload-nonce','internal/agent/ssproxy/crypto.go','e.Nonce = observedPayloadNonce(aw.nonce, aw.exhausted)','copy(e.Nonce[:],aw.nonce)',go('internal/agent/ssproxy','TestSSObservationRetainsFailedShortWriteWithoutReseal'),True),
      ('timeout-normalization','internal/agent/ssproxy/actor.go','observation.Values[3] = 1','observation.Values[3] = 0',go('internal/agent/ssproxy','TestSSObservationSocketResultPrecedesTimeoutNormalization'),True),
      ('applied-grant','internal/agent/ssproxy/observation.go','if drillobserve.Hook != nil {\n\t\te := drillobserve.Event{Kind: "ss-grant"','if false {\n\t\te := drillobserve.Event{Kind: "ss-grant"',go('internal/agent/ssproxy','TestSSObservationGrantRevokeAndCloseFollowAppliedState'),True),
      ('tls-session','internal/tunnel/observation.go','sha256.Sum256(identity)','sha256.Sum256(identity[:0])',go('internal/tunnel','TestCarrierObservationUsesNegotiatedSessionAndOriginalPrefix'),True),
      ('original-prefix','internal/tunnel/flow_alpn.go','e.Data = *prefix','e.Data = nil',go('internal/tunnel','TestCarrierObservationUsesNegotiatedSessionAndOriginalPrefix'),True),
      ('original-tuple','internal/tunnel/observation.go','observedKernelAddress(tuple.Remote), observedKernelAddress(tuple.Local)','observedKernelAddress(tuple.Local), observedKernelAddress(tuple.Remote)',go('internal/tunnel','TestFlowSocketObservationBindsIndependentOriginalTuple'),True),
      ('flow-committed-offset','internal/flow/observation.go','e.Values[10] = r.rxApplied','e.Values[10] = 0',go('internal/flow','TestFlowObservationKeepsOriginalBindingAndCommittedOffsets'),True),
      ('flow-ack-reservation','internal/flow/observation.go','e.Values[12] = uint64(tx.Capacity)','e.Values[12] = tx.End-tx.Base',go('internal/flow','TestFlowObservationKeepsOriginalBindingAndCommittedOffsets'),True),
      ('runtime-reservation','internal/execsession/runtime.go','e.Values[2] += uint64(state.Capacity)','e.Values[2] += state.End-state.Base',go('internal/execsession','TestRuntimeObservationCountsLiveOwnedReservations'),True),
      ('spawnexec-private-env','internal/spawnexec/helper.go','return drillobserve.StripEnvironment(out)','_ = drillobserve.EnableEnv; return out',go('internal/spawnexec','TestHelperStripsObserverCustodyFromExplicitWorkloadEnvironment'),True),
      ('spawnsafe-private-env','internal/spawnsafe/command.go','return drillobserve.StripEnvironment(out)','return out',go('internal/spawnsafe','TestChildEnvironmentStripsBothObserverCustodyKeys'),True),
      ('startup-two-enablers','internal/drillobserve/custody_linux.go','if enable != "v1" {','if enable == "impossible" {',go('internal/drillobserve','TestCustodyRequiresBothStartupEnablers'),True),
      ('startup-sealed-config','internal/drillobserve/custody_linux.go','seals&custodySeals != custodySeals','seals&custodySeals < 0',go('internal/drillobserve','TestCustodyRequiresBothStartupEnablers'),True),
      ('oversize-poison','internal/drillobserve/custody_linux.go','c.broken.Store(true)','c.broken.Store(false)',go('internal/drillobserve','TestCustodyOverflowAndWriteFailurePoisonTheStream'),True),
      ('same-pid-custody','internal/drillobserve/custody_linux.go','return append(env, EnableEnv+"=v1", CustodyEnv+"="+strconv.Itoa(active.fd))','return append(env, CustodyEnv+"="+strconv.Itoa(active.fd))',go('internal/drillobserve','TestCustodySurvivesSamePIDExec'),True),
      ('queue-overflow-poison','internal/drillobserve/custody_linux.go','case c.queue <- pending{event: &e}:\n\tdefault:\n\t\tc.broken.Store(true)','case c.queue <- pending{event: &e}:\n\tdefault:\n\t\tc.broken.Store(false)',go('internal/drillobserve','TestCustodyOverflowAndWriteFailurePoisonTheStream'),True),
      ('write-failure-poison','internal/drillobserve/custody_linux.go','unix.MSG_DONTWAIT|unix.MSG_NOSIGNAL); err != nil {\n\t\tc.broken.Store(true)','unix.MSG_DONTWAIT|unix.MSG_NOSIGNAL); err != nil {\n\t\tc.broken.Store(false)',go('internal/drillobserve','TestCustodyOverflowAndWriteFailurePoisonTheStream'),True),
      ('exec-broken-custody','internal/drillobserve/custody_linux.go','if active.broken.Load() {','if false {',go('internal/drillobserve','TestCustodyOverflowAndWriteFailurePoisonTheStream'),True),
      ('fmt-secret','internal/drillobserve/custody.go','io.WriteString(w, e.String())','io.WriteString(w, string(e.Data))',go('internal/drillobserve',GATE),True),
      ('json-secret','internal/drillobserve/custody.go','[]byte(`{"redacted":true}`)','[]byte(fmt.Sprintf("%q",e.Data))',go('internal/drillobserve',GATE),True),
    ])
    gate=(REPO/'internal/drillobserve/custody_linux_test.go').read_text()
    rows=[]
    for line in gate.splitlines():
        if line.strip().startswith('{"internal/') and line.count('",')>=4:
            rows.append(json.loads('['+line.strip()[1:].removesuffix(',').removesuffix('}')+']'))
    for index,(path,start,before,hook,after) in enumerate(rows):
        text=(REPO/path).read_text();at=text.index(hook,text.index(start))
        if 'Kind:' in hook:
            old=hook;new=hook.replace('Kind:','Kind:').replace('"','"',1)
            kind=re.search(r'Kind: "([^"]+)"',hook).group(1)
            new=hook.replace('"'+kind+'"','"dropped-'+kind+'"')
        else:
            # Drop the actual invocation while leaving a valid empty nil guard.
            begin=text.rfind('\n',0,at)+1;end=text.index('\n',at)
            if 'observeGrantLocked' in hook:
                begin=text.index('\n',at)+1;end=text.index('\n',begin)
            old=text[text.index(start):end];new=text[text.index(start):begin]+'\t\t// Mutant: required receipt omitted.'
        specs.append(('barrier-'+str(index+1),path,old,new,go('internal/drillobserve',GATE),False))
    specs.append(('inert-nil-guard','internal/tunnel/observation.go','if drillobserve.Hook == nil {','if drillobserve.Hook != nil {',go('internal/drillobserve',GATE),False))
    return specs


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--receipts',required=True,type=Path)
    p.add_argument('--only',action='append',default=[])
    args=p.parse_args();args.receipts.mkdir(parents=True,exist_ok=True)
    outcomes=json.loads((args.receipts/'mutations.json').read_text()) if args.only and (args.receipts/'mutations.json').exists() else []
    for name,path,old,new,command,overlay in mutations():
        if args.only and name not in args.only:
            continue
        source=REPO/path;original=source.read_text()
        if old not in original:
            raise RuntimeError('mutation anchor unavailable: '+name)
        changed=original.replace(old,new,1 if name not in ('spawnsafe-private-env','keeper-archive') else -1)
        output=args.receipts/(name+'.log')
        with tempfile.TemporaryDirectory(prefix='s5-observation-mutation-') as tmp:
            try:
                if overlay:
                    replacement=Path(tmp)/source.name;replacement.write_text(changed)
                    manifest=Path(tmp)/'overlay.json';manifest.write_text(json.dumps({'Replace':{str(source):str(replacement)}}))
                    command=command[:2]+['-overlay',str(manifest)]+command[2:]
                else:
                    source.write_text(changed)
                result=subprocess.run(command,cwd=REPO,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=90)
                output.write_bytes(result.stdout)
                killed=result.returncode!=0 and b'FAIL' in result.stdout and b'build failed' not in result.stdout and b'SyntaxError' not in result.stdout
                outcomes=[row for row in outcomes if row['name']!=name]
                outcomes.append(dict(name=name,path=path,command=command,rc=result.returncode,killed=killed,mode='overlay' if overlay else 'on-disk'))
                (args.receipts/(name+'.diff')).write_text(''.join(difflib.unified_diff(original.splitlines(True),changed.splitlines(True),fromfile=path,tofile=path)))
                print(name,result.returncode,'RED' if killed else 'SURVIVED',flush=True)
            finally:
                if not overlay:
                    source.write_text(original)
                (args.receipts/'mutations.json').write_text(json.dumps(outcomes,indent=2)+'\n')
    return 0 if outcomes and all(row['killed'] for row in outcomes) else 1

if __name__=='__main__':
    sys.exit(main())
