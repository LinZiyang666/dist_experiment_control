#!/bin/sh
# origin: hot-update S8 plan §9.3 / §11.2 P28. Hermetic contract controls;
# synthetic observations below are oracle inputs, never deployment receipts.
set -eu
P28_SIM_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
export P28_SIM_ROOT
exec python3 -I -B "$P28_SIM_ROOT/drills/lib/upgrade_tx_supervisor.py" run --timeout-s 55 -- /usr/bin/python3 -I -B - "$@" <<'PYTEST'
import ast,copy,hashlib,importlib.util,json,os,pathlib,re,subprocess,sys,tempfile,time,unittest
os.dup2(1,2)
ROOT=pathlib.Path(os.environ['P28_SIM_ROOT'])
D39=ROOT/'drills/39-hot-update-legacy-n1.sh'
PTY=ROOT/'drills/lib/hot_update_pty.sh'
BROKER=ROOT/'drills/lib/hot_update_broker.sh'
spec=importlib.util.spec_from_file_location('supervisor',ROOT/'drills/lib/upgrade_tx_supervisor.py')
S=importlib.util.module_from_spec(spec); spec.loader.exec_module(S)
OLD,NEW='a'*64,'b'*64

def function(path,name):
    match=re.search(r'^'+re.escape(name)+r'\(\) \{\n.*?^}\n',path.read_text(),re.M|re.S)
    if match is None: raise AssertionError('missing function '+name)
    return match.group()
def shell(body):
    return S.run(['/bin/sh','-c',body],timeout_s=10)
def cases():
    raw=D39.read_text().split("cat <<'CASES39'\n")[1].split('\nCASES39')[0]
    return [line.split('\t') for line in raw.splitlines()]
def expected_arms():
    return ['X','Y','P','F','BR']+[f'Q{q}B{b}-{verb}' for b in '01' for q in ('00','01','10','11') for verb in ('exec','run','kill','expose','transfer')]+[
        'LB-new','LB-old','LI-defer','LI-interrupting','M-route','CC-idle','CC-late-create','CL-reject',
        'ABI-adjacent','ABI-too-far','ABI-unknown','GO-hash','CFG-nats-old','CFG-nats-new','CFG-caddy-old','CFG-caddy-new',
        # origin: hot-update S9 plan §7.1 / T80 — frozen S9 infra refusal arms on the same drill.
        'NI','CI']
def legacy_files(root):
    documents={'legacy-source.json':dict(commit='c'*40,tag='v0.6.0')}
    owner=dict(pid=100,start_ticks=1000,boot_id='hermetic-boot',running_sha=OLD)
    for role in ('agent','broker'):
        for phase in ('before','after'):
            documents[f'legacy-{role}-{phase}.json']={**owner,'running_sha':NEW if role=='agent' and phase=='after' else OLD}
    raw={'legacy-source.sha':OLD+'\n','legacy-target.sha':NEW+'\n','legacy-command.rc':'0\n',
         'legacy-ctl-before.sha':OLD+'  /opt/old/tether\n','legacy-ctl-after.sha':OLD+'  /opt/old/tether\n'}
    for name,row in documents.items(): (root/name).write_text(json.dumps(row))
    for name,value in raw.items(): (root/name).write_text(value)
    return documents

def python_body(path,tag):
    return path.read_text().split("<<'"+tag+"'\n")[1].split('\n'+tag)[0]

class Drill39Contracts(unittest.TestCase):
    # origin: S8 internal review rv-n1 F6
    def test_adjacent_exec_keeps_selected_tag_binary_and_receipt_together(self):
        path=ROOT/'drills/35-hot-update-exec-carrier.sh'; source=path.read_text()
        arm=source.split('\nC)\n',1)[1].split('\n;;',1)[0]
        gate=function(path,'_prev_die_gate')
        self.assertIn('sim_select_previous dynamic || setup_fail',arm)
        self.assertIn('PREV_TAG=$SIM_PREVIOUS_TAG',arm)
        self.assertLess(arm.index('sim_select_previous dynamic'),arm.index('PREV_TAG=$SIM_PREVIOUS_TAG'))
        self.assertLess(arm.index('PREV_TAG=$SIM_PREVIOUS_TAG'),arm.index('_prev_die_gate'))
        self.assertIn('sim_dynamic_prev_source_ok "$PREV_TAG" ||',gate)
        self.assertIn('sha256sum "$SIM_PREVIOUS_BINARY"',gate)
        self.assertIn('d cp "$SIM_PREVIOUS_BINARY" "$(ctr_name ctl1):$PREV_BIN"',gate)
        self.assertIn('d cp "$SIM_PREVIOUS_BINARY" "$(ctr_name agt1):$DST"',arm)
        receipt='"$EV/tether-dynamic-prev-source.json"'
        retain='cp "$SIM_PREVIOUS_SOURCE" '+receipt+' || setup_fail'
        self.assertIn(retain,arm)
        self.assertLess(arm.index('_prev_die_gate'),arm.index(retain))
        self.assertLess(arm.index(retain),arm.index('_leg_begin c1'))
        self.assertIn('cmp -s "$SIM_PREVIOUS_SOURCE" '+receipt,arm)
        self.assertIn('test -s '+receipt,arm)
        # The shared A/B setup cannot retain a fixed receipt before C selects
        # the adjacent release; that would silently label the wrong artifact.
        shared=source.split('case "${ARM:?}" in',1)[0].split('drill_begin ',1)[1]
        self.assertNotIn('tether-prev-source.json',shared)
        self.assertNotIn('tether-dynamic-prev-source.json',shared)
        for body in (gate,arm):
            self.assertNotIn('sim_prev_tag',body)
            self.assertNotIn('sim_fixed_prev_tag',body)
            self.assertNotIn('sim_prev_source_ok',body)
            self.assertNotIn('$HERE/vendor/tether-prev',body)

    # origin: S8 internal review rv-n1 F6
    def test_historical_pty_keeps_fixed_tag_artifact_and_receipt_together(self):
        path=ROOT/'drills/36-hot-update-pty.sh'
        supply=function(path,'_pty_previous_supply')
        self.assertIn('PREV_TAG=$(sim_fixed_prev_tag) || return 1',supply)
        self.assertIn('sim_prev_source_ok "$PREV_TAG" || return 1',supply)
        self.assertIn('d cp "$HERE/vendor/tether-prev"',supply)
        self.assertIn('sha256sum "$HERE/vendor/tether-prev"',supply)
        self.assertIn('cp "$HERE/vendor/tether-prev-source.json" "$PTY_EV/tether-prev-source.json"',supply)
        prepare=function(path,'_pty_prepare_fixture')
        self.assertIn('sim_prev_source_ok "$(sim_fixed_prev_tag)"',prepare)
        self.assertIn('upgrade_legacy_stack "$HERE/vendor/tether-prev" "$HERE/vendor/tether-prev-source.json"',prepare)
        for body in (supply,prepare):
            self.assertNotIn('sim_prev_tag',body)
            self.assertNotIn('sim_dynamic_prev',body)
            self.assertNotIn('sim_select_previous dynamic',body)

    # origin: S8 internal review rv-n1 F6
    def test_historical_process_keeps_fixed_tag_artifact_and_receipt_together(self):
        prepare=function(D39,'_h0_prepare'); gate=function(D39,'_prev_die_gate')
        # Both the old control-stack setup and the later exporter die-gate
        # must remain historical when an adjacent release newer than 0.6 lands.
        self.assertEqual(prepare.count('PREV_TAG=$(sim_fixed_prev_tag) || setup_fail'),2)
        self.assertIn('sim_prev_source_ok "$PREV_TAG"',prepare)
        self.assertIn('upgrade_legacy_stack "$HERE/vendor/tether-prev" "$HERE/vendor/tether-prev-source.json"',prepare)
        self.assertIn('cp "$HERE/vendor/tether-prev-source.json" "$H0_EVIDENCE/tether-prev-source.json"',prepare)
        self.assertIn('sim_prev_source_ok "$PREV_TAG" || return 1',gate)
        self.assertIn('[ "$PREV_TAG" = v0.6.0 ] || return 1',gate)
        self.assertIn('d cp "$HERE/vendor/tether-prev"',gate)
        self.assertIn('sha256sum "$HERE/vendor/tether-prev"',gate)
        for body in (prepare,gate):
            self.assertNotIn('sim_prev_tag',body)
            self.assertNotIn('sim_dynamic_prev',body)
            self.assertNotIn('sim_select_previous dynamic',body)

    def test_flat_manifest_and_frozen_argv_cover_every_cell(self):
        rows=cases(); arms=expected_arms(); self.assertEqual([r[0] for r in rows],arms)
        source=D39.read_text()
        self.assertEqual(re.findall(r'^# arms: (.*)$',source,re.M),[' '.join(arms)])
        for header,column in [('fixture',4),('grows',5),('worst',6)]:
            self.assertEqual(re.findall(r'^# '+header+r': (.*)$',source,re.M),[' '.join(r[0]+'='+r[column] for r in rows)])
        self.assertEqual(re.findall(r'^# forgoes: (.*)$',source,re.M),[' '.join(a+'=-' for a in arms)])
        body=source.split('case "${ARM:?}" in')[1].split('\nesac')[0]
        self.assertEqual(re.findall(r'^([A-Za-z0-9-]+)\)',body,re.M),['BR','F','P','X','Y']+arms[5:])
        self.assertIn('*) setup_fail',body); self.assertNotIn('exit ',body)
        self.assertEqual(source.count('case "${ARM:?}" in'),1)
        for arm,kind,native,cli,fixture,grows,worst in rows:
            self.assertGreaterEqual(int(worst),60)
            if kind=='historical': self.assertEqual(native,'3')
            if kind=='refusal': self.assertEqual(native,'0'); self.assertIn(cli,('64','75'))
            if kind=='non_hot': self.assertEqual((arm,native,cli),('LI-interrupting','4','74'))
        indexed={r[0]:r for r in rows}
        self.assertEqual(indexed['LI-interrupting'][1:4],['non_hot','4','74'])
        for arm in arms[5:45]: self.assertEqual(indexed[arm][1:3],['wire','0'])
        for arm in ('LB-new','LB-old','LI-defer','M-route','CL-reject','ABI-too-far','ABI-unknown'):
            self.assertEqual(indexed[arm][1:3],['refusal','0'])
        for arm in ('CC-idle','CC-late-create','ABI-adjacent'): self.assertEqual(indexed[arm][1:3],['hot','0'])
        argv=json.loads((ROOT.parent.parent/'test/hotupdate/testdata/upgrade_wire_argv.json').read_text())
        self.assertEqual({r['cell']+'-'+r['verb'] for r in argv},set(arms[5:45]))
        self.assertEqual(len(argv),40)

    def test_historical_delegates_select_old_control_stack_before_business(self):
        source=D39.read_text(); pty=(ROOT/'drills/36-hot-update-pty.sh').read_text(); flow=(ROOT/'drills/75-hot-update-flows.sh').read_text()
        self.assertIn('_pty_prepare_fixture "$PREV_BIN" old-old-old',source)
        self.assertIn('_flow_legacy_arm "$PREV_BIN" old-old-old',source)
        self.assertIn('. "$HERE/drills/37-hot-update-broker.sh"',source)
        self.assertLess(source.index('broker_legacy_arm grow_to_3'),source.index('\nX)\n    _h0_prepare'))
        self.assertIn('upgrade_legacy_ctl node upgrade',source)
        self.assertIn('"$H0_EXEC_BIN" exec',function(D39,'_launch_producer'))
        for path,name in [(D39,'_h0_prepare'),(ROOT/'drills/36-hot-update-pty.sh','_pty_prepare_fixture'),(ROOT/'drills/75-hot-update-flows.sh','_flow_prepare')]:
            body=function(path,name)
            self.assertLess(body.index('upgrade_legacy_stack'),body.index('"$SIM" session'))
        self.assertIn('upgrade_legacy_ctl node upgrade',function(ROOT/'drills/36-hot-update-pty.sh','_pty_upgrade'))
        self.assertIn('upgrade_legacy_ctl node upgrade',function(ROOT/'drills/75-hot-update-flows.sh','_flow_upgrade_start'))
        self.assertIn('upgrade_legacy_exec_required',function(D39,'_pty_legacy_exporter'))
        self.assertIn('upgrade_legacy_exec_required "$H0_EVIDENCE" agent || return 2',source)
        self.assertIn('upgrade_legacy_exec_required',function(ROOT/'drills/75-hot-update-flows.sh','_flow_legacy_arm'))
        self.assertIn('broker_legacy_exec_required',function(BROKER,'broker_legacy_verdict'))
        # S7's landed transfer lane is an unchanged dependency, not a new S8 oracle.
        self.assertIn('transfer_run T',function(BROKER,'broker_transfer_lane'))
        self.assertIn('transfer_cleanup',function(BROKER,'broker_transfer_lane'))

    def test_legacy_exporter_requires_actual_exec_and_all_old_roles(self):
        with tempfile.TemporaryDirectory(prefix='legacy-oracle-') as directory:
            root=pathlib.Path(directory)
            def judge():
                return shell(function(PTY,'upgrade_legacy_exec_required')+f'upgrade_legacy_exec_required "{root}" agent\n')['native_rc']
            docs=legacy_files(root); self.assertEqual(judge(),0)
            mutants=[('legacy-agent-after.json','running_sha',OLD),('legacy-agent-before.json','running_sha',NEW),
                ('legacy-agent-after.json','pid',101),('legacy-agent-after.json','start_ticks',1001),
                ('legacy-agent-after.json','boot_id','different-boot'),('legacy-broker-before.json','running_sha',NEW),
                ('legacy-broker-after.json','running_sha',NEW),('legacy-source.json','commit',''),('legacy-source.json','tag','new-as-old')]
            for name,key,value in mutants:
                with self.subTest(field=name+'.'+key):
                    legacy_files(root); row=copy.deepcopy(docs[name]);row[key]=value;(root/name).write_text(json.dumps(row))
                    self.assertEqual(judge(),2)
            for name,value in [('legacy-command.rc','75\n'),('legacy-command.rc','64\n'),('legacy-ctl-before.sha',NEW),('legacy-ctl-after.sha',NEW),('legacy-target.sha',OLD)]:
                legacy_files(root);(root/name).write_text(value);self.assertEqual(judge(),2,name)
            legacy_files(root); (root/'legacy-agent-after.json').unlink(); self.assertEqual(judge(),2)

    def test_missing_exec_cannot_reach_the_pty_loss_oracle(self):
        with tempfile.TemporaryDirectory(prefix='legacy-no-exec-') as directory:
            script=function(PTY,'upgrade_legacy_exec_required')+function(D39,'_pty_legacy_exporter')
            # This isolated driver control fails if even the first loss-oracle
            # call runs. It supplies no product implementation or live receipt.
            script+=f'PTY_EV="{directory}"\n_pty_continuity() {{ echo INVALID_ORACLE_REACHED; return 1; }}\n'
            script+='_pty_legacy_exporter\n'
            result=shell(script);self.assertEqual(result['native_rc'],2)
            self.assertNotIn('INVALID_ORACLE_REACHED',result['stdout'])
            self.assertFalse((pathlib.Path(directory)/'continuity.out').exists())

    def test_missing_exec_and_invalid_oracle_cannot_mint_product_red(self):
        with tempfile.TemporaryDirectory(prefix='legacy-classifier-') as directory:
            root=pathlib.Path(directory)
            base=function(PTY,'upgrade_legacy_exec_required')+function(PTY,'upgrade_legacy_assert')+function(PTY,'upgrade_legacy_observation')
            # Classifier unit only: emitting a synthetic matching diagnostic
            # must not run at all without the independent kernel evidence.
            prefix='_AS_SETUP=0; _AS_NC=0; _AS_NC_GUARD=0\nassert_bug() { echo CLASSIFIER_REACHED; }\n'
            invocation=f'upgrade_legacy_assert "{root}" agent desc owner signature /bin/sh -c "echo signature; exit 1"\nprintf "setup=%s\\n" "$_AS_SETUP"\n'
            result=shell(prefix+base+invocation)
            self.assertEqual(result['native_rc'],0)
            self.assertNotIn('CLASSIFIER_REACHED',result['stdout']); self.assertIn('setup=1',result['stdout'])
            legacy_files(root)
            invalid=invocation.replace('exit 1','exit 2')
            result=shell(prefix+base+invalid)
            self.assertNotIn('CLASSIFIER_REACHED',result['stdout']); self.assertIn('setup=1',result['stdout'])
            result=shell(prefix+base+invocation)
            self.assertIn('CLASSIFIER_REACHED',result['stdout']); self.assertIn('setup=0',result['stdout'])

    def test_broker_exec_proof_is_signed_pinned_and_independent(self):
        with tempfile.TemporaryDirectory(prefix='broker-exec-proof-') as directory:
            root=pathlib.Path(directory)
            pin=dict(source_sha='c'*40,binary_sha256=OLD)
            owner=dict(pid=200,start_ticks=2000,boot_id='oracle-broker')
            row=dict(command_rc=0,source_sha='c'*40,signed_command=True,old_running_sha=OLD,new_running_sha=NEW,
                     role='follower',topology='N3',run_unwind_observed=True,before=owner,after=owner)
            (root/'pin.json').write_text(json.dumps(pin))
            def judge(value):
                (root/'legacy-exec.json').write_text(json.dumps(value))
                return shell(f'BROKER_EV="{root}"; BROKER_LEGACY_PIN="{root}/pin.json"; BROKER_SHA={NEW}; BROKER_COMMAND_RC=0\n'+function(BROKER,'broker_legacy_exec_required')+'broker_legacy_exec_required\n')['native_rc']
            self.assertEqual(judge(row),0)
            for key,value in [('command_rc',75),('source_sha','d'*40),('signed_command',False),('old_running_sha',NEW),
                              ('new_running_sha',OLD),('role','leader'),('topology','N1'),('run_unwind_observed',False),('after',{**owner,'pid':201})]:
                changed={**row,key:value};self.assertEqual(judge(changed),2,key)

    def test_native_verdicts_are_never_translated_to_expected_success(self):
        for native in range(5):
            script=f'. "{ROOT}/lib/assert.sh"\nlog() {{ :; }}; ok() {{ :; }}; err() {{ :; }}\nINSTANCE=drill-hermetic-native\ndrill_begin native\n'
            result=shell(script+function(BROKER,'broker_native')+f'broker_native {native}\ndrill_end\n')
            self.assertEqual(result['native_rc'],native)
        body=python_body(BROKER,'PYCOMPAT')
        self.assertIn('native_rc=rc',body);self.assertIn('expected_native_rc=expected',body)
        self.assertIn("signature=result['signature']; rc=oracle_rc",body)
        self.assertIn("if statuses!=wanted: rc=1",body)
        self.assertIn("cleanup.get('empty') is not True",body)
        self.assertIn("require(commands[0]['product_argv']==primary",body)
        self.assertIn("require(commands[1]['product_argv']==companion",body)
        self.assertIn("require(fm['arm']==arm",body)
        self.assertIn("fixture_module.drain(root/'fixture',fm)",body)
        self.assertIn("fixture_module.stop(root/'fixture',fm)",body)
        self.assertIn("'--manifest',plan['fixture_manifest']",body)
        self.assertIn("result['native_rc']==oracle_rc",body)
        self.assertIn("result['command_rcs']==statuses",body)
        self.assertIn("required_checks=checks[arm]",body)
        self.assertIn("all(result['checks'].get(key) is True for key in required_checks)",body)

    def test_compatibility_prerequisites_fail_before_any_setup(self):
        script=function(D39,'upgrade_39_cases')+function(BROKER,'upgrade_39_compat')
        for arm in ('missing','Q00B0-exec','LI-interrupting','CFG-nats-old'):
            result=shell(script+f'SIM_UPGRADE_COMPAT_SUPPLIER=\nupgrade_39_compat {arm}\n')
            self.assertEqual(result['native_rc'],2)

    def test_late_create_runs_while_the_original_upgrade_is_fenced(self):
        tree=ast.parse(python_body(BROKER,'PYCOMPAT'))
        definitions=[node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name in ('require','invoke','run_commands')]
        for round in range(5):
            with tempfile.TemporaryDirectory(prefix='late-create-order-') as directory:
                root=pathlib.Path(directory)
                # Real, finite shell actors model only driver scheduling. No
                # supplier/product fake is used to certify a compatibility arm.
                first=['/bin/sh','-c','touch ready; while [ ! -e done ]; do sleep 0.01; done']
                late=['/bin/sh','-c','test -e fence || exit 9; touch done; exit 75']
                calls=[]
                def supply(op):
                    self.assertEqual(op,'await-fence'); calls.append(op)
                    until=time.monotonic()+3
                    while not (root/'ready').exists():
                        self.assertLess(time.monotonic(),until);time.sleep(.01)
                    (root/'fence').touch();return 0
                scope=dict(root=root,arm='CC-late-create',subprocess=subprocess,time=time,supply=supply)
                exec(compile(ast.Module(body=definitions,type_ignores=[]),str(BROKER),'exec'),scope)
                commands=[dict(argv=argv,cwd=str(root),timeout_s=5) for argv in (first,late)]
                self.assertEqual(scope['run_commands'](commands),[0,75])
                self.assertEqual(calls,['await-fence'])
                self.assertEqual((root/'command-0.rc').read_text(),'0\n')
                self.assertEqual((root/'command-1.rc').read_text(),'75\n')

    def test_fixture_errors_and_cleanup_keep_native_precedence(self):
        tree=ast.parse(python_body(BROKER,'PYCOMPAT'))
        definitions=[node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name in ('error_native','join_native')]
        scope={};exec(compile(ast.Module(body=definitions,type_ignores=[]),str(BROKER),'exec'),scope)
        priority=[1,2,3,4,0]
        for first in range(5):
            for second in range(5):
                self.assertEqual(scope['join_native'](first,second),min((first,second),key=priority.index))
        for code in (1,2,3,4,0,75,True,None):
            error=ValueError('synthetic error contract'); error.rc=code
            expected=code if type(code) is int and code in (1,2,3,4) else 2
            self.assertEqual(scope['error_native'](error),expected)

    def test_execution_provenance_rejects_metadata_only_or_wrong_quadrant(self):
        body=python_body(BROKER,'PYCOMPAT');tree=ast.parse(body)
        # Execute only the pure evidence predicates, never the driver or ports.
        definitions=[node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name in ('require','read','executions')]
        with tempfile.TemporaryDirectory(prefix='wire-execution-') as directory:
            root=pathlib.Path(directory);scope=dict(json=json,pathlib=pathlib,re=re,root=root,kind='wire',arm='Q00B0-exec')
            exec(compile(ast.Module(body=definitions,type_ignores=[]),str(BROKER),'exec'),scope)
            path='/opt/s8/old/tether'
            row=dict(schema_version=1,pid=101,start_ticks=1000,boot_id='hermetic',observed_at='2026-10-13',
                     running_sha256=OLD,copied_sha256=OLD,copied_path=path,proc_exe=path,argv=[path,'exec'],
                     artifact=dict(binary_sha256=OLD,commit='c'*40,tree_digest='d'*64,archive_sha256='e'*64,toolchain='go1.26.8',build_flags=['-trimpath'],dirty_diff_sha256='f'*64))
            roles={role:copy.deepcopy(row) for role in ('ctl','agent','broker')}
            selected=dict(cell='Q00B0',verb='exec',**{role+'_path':path for role in roles},**{role+'_sha':OLD for role in roles})
            manifest=root/'wire.json';manifest.write_text(json.dumps(dict(rows=[selected])))
            plan=dict(wire_manifest=str(manifest))
            def judge(rows):
                (root/'executions.json').write_text(json.dumps(rows)); return scope['executions'](plan)
            self.assertEqual(judge(roles),roles)
            for role,key,value in [('ctl','pid',0),('agent','running_sha256',NEW),('agent','copied_sha256',NEW),('broker','copied_path','/opt/s8/new/tether'),('ctl','observed_at','')]:
                broken=copy.deepcopy(roles);broken[role][key]=value
                with self.assertRaises(ValueError,msg=role+'.'+key): judge(broken)
            broken=copy.deepcopy(roles);del broken['broker']
            with self.assertRaises(ValueError): judge(broken)
            broken=copy.deepcopy(roles);del broken['agent']['artifact']['toolchain']
            with self.assertRaises(KeyError): judge(broken)

    def test_shared_supervisor_and_shell_syntax_cover_every_delegate(self):
        for name in ('39-hot-update-legacy-n1','36-hot-update-pty','37-hot-update-broker','75-hot-update-flows'):
            path=ROOT/'drills'/(name+'.sh');source=path.read_text()
            self.assertIn('exec "$SIM_UPGRADE_SUPERVISOR" -- /bin/sh -c',source)
            self.assertLess(source.index('SIM_HOT_UPDATE_OWNER'),source.index('. "$HERE/lib/docker.sh"'))
            self.assertEqual(S.run(['/bin/sh','-n',str(path)])['native_rc'],0)
        for path in (PTY,BROKER): self.assertEqual(S.run(['/bin/sh','-n',str(path)])['native_rc'],0)
        child=function(BROKER,'broker_child_run')
        self.assertIn('exec "$SIM_UPGRADE_SUPERVISOR" --log',child)
        self.assertNotIn('subprocess.Popen',child)
        # Five actual child/reap rounds through the shared external owner.
        for round in range(5):
            result=S.run(['/bin/sh','-c',f'exit {round}'])
            self.assertEqual(result['native_rc'],round)

if len(sys.argv)>1 and sys.argv[1]=='--expected-rows':
    # Append payload only; retain every original S7/S6/historical ledger row.
    for row in cases()[5:]:
        name='39-hot-update-legacy-n1.'+row[0]
        verdict={'0':'GREEN','4':'INCOMPLETE'}[row[2]]
        print('\t'.join([name,verdict,'0','-','-',name]))
elif len(sys.argv)>1 and sys.argv[1]=='--rows':
    for row in cases(): print('\t'.join(['39-hot-update-legacy-n1.'+row[0],*row[1:]]))
else:
    unittest.main(verbosity=2)
PYTEST
