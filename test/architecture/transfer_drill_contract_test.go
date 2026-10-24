package architecture

// Real inputs: on-disk driver/manifest/overlay sources. No product or processes
// are started here; live data, OFD and native judge evidence belong to P29/P32.
import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

var transferPositiveArms = strings.Fields("A A1 B C1 C2 C3 R1 R2 X W1 W2")
var transferControlArms = strings.Fields("M01 M02 M03 M04 M05 M06 M07 M08A M08B M08C M09 M10A M10B M11A M11B M11C M12A M12B M12C M13A M13B M13C M14")

func transferDrillSources(t *testing.T) map[string]string {
	t.Helper()
	sources := map[string]string{}
	for _, path := range []string{
		"drills/63-hot-update-transfer.sh", "drills/64-hot-update-transfer-controls.sh",
		"drills/37-hot-update-broker.sh", "drills/lib/hot_update_transfer.sh",
		"drills/lib/transfer_mutations.py", "drills/lib/hot_update_broker.sh",
		"drills/lib/hot_update_budget.sh", "expected-verdicts.tsv", "expected-verdicts-log.md", "README.md",
		"tests/hot-update-broker-ledger-test.sh", "tests/assert-identity.baseline.tsv",
	} {
		raw, err := os.ReadFile(filepath.Join(repoRoot(t), "test/simcluster", path))
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) == 0 {
			t.Fatalf("empty driver asset %s", path)
		}
		sources[path] = string(raw)
	}
	return sources
}

func transferRequireText(source string, needles ...string) error {
	for _, needle := range needles {
		if !strings.Contains(source, needle) {
			return fmt.Errorf("missing transfer contract %q", needle)
		}
	}
	return nil
}

func transferHeader(source, key string) (string, error) {
	pattern := regexp.MustCompile(`(?m)^# ` + regexp.QuoteMeta(key) + `: (.+)$`)
	matches := pattern.FindAllStringSubmatch(source, -1)
	if len(matches) != 1 {
		return "", fmt.Errorf("missing/duplicate %s header", key)
	}
	return matches[0][1], nil
}

func transferCheckArmManifest(source string, want []string, controls bool) error {
	arms, err := transferHeader(source, "arms")
	if err != nil {
		return err
	}
	if !slices.Equal(strings.Fields(arms), want) {
		return fmt.Errorf("missing child or control in positive manifest: %s", arms)
	}
	for _, key := range []string{"fixture", "grows", "worst", "forgoes"} {
		value, err := transferHeader(source, key)
		if err != nil {
			return err
		}
		fields := strings.Fields(value)
		if len(fields) != len(want) {
			return fmt.Errorf("%s does not cover every child", key)
		}
		for i, field := range fields {
			parts := strings.Split(field, "=")
			if len(parts) != 2 || parts[0] != want[i] {
				return fmt.Errorf("%s has unexpected child %s", key, field)
			}
			arm, got := want[i], parts[1]
			expected := "-"
			switch key {
			case "fixture":
				expected = "N1"
				if arm == "A" {
					expected = "install"
				}
				if arm == "B" {
					expected = "N3-live"
				}
			case "grows":
				expected = "0"
				if arm == "B" {
					expected = "2"
				}
			case "worst":
				expected = "1200"
				if strings.HasPrefix(arm, "C") {
					expected = "1000"
				}
				if arm == "X" || strings.HasPrefix(arm, "W") {
					expected = "1100"
				}
				if controls {
					expected = "900"
				}
				seconds, err := strconv.Atoi(got)
				if err != nil || seconds*2 > 2700 || seconds*2 < 1800 {
					return fmt.Errorf("invalid per-launch ceiling %s", field)
				}
			}
			if got != expected {
				return fmt.Errorf("wrong %s %s", key, field)
			}
		}
	}
	dispatch := regexp.MustCompile(`(?m)^    ([A-Za-z0-9]+)\) transfer_run ([A-Za-z0-9]+)(?: grow_to_3)? ;;$`).FindAllStringSubmatch(source, -1)
	if len(dispatch) != len(want) {
		return fmt.Errorf("required child has no dispatch")
	}
	for i, row := range dispatch {
		if row[1] != want[i] || row[2] != want[i] {
			return fmt.Errorf("wrong dispatched child")
		}
	}
	return transferRequireText(source, "drill_begin ", "transfer_cleanup\ntransfer_native \"$?\"\ndrill_end\nTRANSFER_DRILL_RC=$?\nexit \"$TRANSFER_DRILL_RC\"")
}

func transferCheckDriver(sources map[string]string) error {
	if err := transferCheckArmManifest(sources["drills/63-hot-update-transfer.sh"], transferPositiveArms, false); err != nil {
		return err
	}
	if err := transferCheckArmManifest(sources["drills/64-hot-update-transfer-controls.sh"], transferControlArms, true); err != nil {
		return err
	}
	checks := map[string][]string{
		"drills/63-hot-update-transfer.sh":          {"assert_setup 'positive manifest requires every leg and child'"},
		"drills/64-hot-update-transfer-controls.sh": {"assert_setup 'production control source build and runtime receipt are bound'"},
		"tests/assert-identity.baseline.tsv": {
			"63-hot-update-transfer._shared\tassert_setup\tpositive manifest requires every leg and child",
			"64-hot-update-transfer-controls._shared\tassert_setup\tproduction control source build and runtime receipt are bound",
		},
		"drills/lib/transfer_mutations.py": {
			"for leg in (1, 2):", "('forward', 30), ('rollback', 190)", "latest_s=110 if direction == 'forward' else 190",
			"['source-follower', 'original-leader', 'third-voter']", "plan.get('required_children') == (list(MUTATIONS) if plan['controls'] else POSITIVE)",
			"plan.get('steps') == requirements(arm, topology)", "row.get('complete') is False", "row.get('role') == role",
			"0 < row['durable_bytes'] < row['size']", "row['pending_responsibilities'] > 0", "66549440", "1073741824", "except CoverageError:",
			"'receiver' if name.endswith('push') else 'courier'", "'sender' if name.endswith('pull') else 'prepared'",
			"('agent', 'broker-store', 'broker-control')", "('A', 'B')", "('push', 'pull')",
		},
		"drills/lib/hot_update_transfer.sh": {
			"TRANSFER_PROCESS_SUPERVISOR", "drill_install_traps transfer_cleanup_all", "broker_cleanup || _tca_rc=2", "transfer_wait_window \"$TRANSFER_CELL\"", "transfer_fixture start", "transfer_fixture sample",
			"transfer_fixture drain\n    _tfin_drain=$?", "transfer_fixture judge\n    _tfin_rc=$?", "transfer_fixture stop", "TRANSFER_JUDGED=1",
			"node upgrade \"$_tac_nid\" --url \"$TRANSFER_URL\" --sha256 \"$TRANSFER_SHA\"",
			"broker upgrade reload", "--socket \"$_tbc_socket\"", "cluster upgrade --broker-only",
			"agent-forward) transfer_agent_cut B", "agent-rollback) transfer_agent_cut A",
			"broker-forward) transfer_broker_cut B", "broker-rollback) transfer_broker_cut A",
			"ss -K dst", "restart nats-server", "elapsed_s>=120", "kill -INT", "session rm",
			"safe-defer) transfer_safe_defer", "[ \"$_tsd_rc\" = 75 ]",
		},
		"drills/lib/hot_update_broker.sh":        {"transfer_run T", "TRANSFER_TOPOLOGY=$_btl_topology", "transfer_cleanup"},
		"drills/37-hot-update-broker.sh":         {"T=1200", "broker_transfer_lane quorum", "broker_transfer_lane single"},
		"drills/lib/hot_update_budget.sh":        {"HOT_UPDATE_TRANSFER_PROGRESS_S=10"},
		"tests/hot-update-broker-ledger-test.sh": {"transfer-capable-live-cut-and-legacy-defer", "historical transfer defer cannot sign S7 capable cut"},
	}
	for path, needles := range checks {
		if err := transferRequireText(sources[path], needles...); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	}
	lib := sources["drills/lib/hot_update_transfer.sh"]
	if strings.Index(lib, "transfer_fixture drain\n    _tfin_drain=$?") > strings.Index(lib, "transfer_fixture judge\n    _tfin_rc=$?") {
		return fmt.Errorf("judge before drain")
	}
	if strings.Contains(lib, "Pdeathsig") && !strings.Contains(lib, "No custom Pdeathsig") {
		return fmt.Errorf("private process supervisor")
	}
	for _, arm := range transferPositiveArms {
		key := "63-hot-update-transfer." + arm
		if err := transferRequireText(sources["expected-verdicts.tsv"], key+"\tGREEN\t0\t-\t-\t"+key+"\n"); err != nil {
			return err
		}
		if err := transferRequireText(sources["expected-verdicts-log.md"], "## "+key+"\n", "NOT_RUN"); err != nil {
			return err
		}
	}
	return nil
}

// origin: hot-update S7 plan §10.3 / §10.6 / T43.
// gate-control: TestTransferDrillManifestRequiresEveryLegAndChild
func TestTransferDrillManifestRequiresEveryLegAndChild(t *testing.T) {
	sources := transferDrillSources(t)
	if err := transferCheckDriver(sources); err != nil {
		t.Fatal(err)
	}
	// Controls use the actual loaded source. Equal child counts cannot hide a
	// replacement, and prepared/courier cannot stand in for nonempty data.
	for _, mutation := range []struct{ path, before, after string }{
		{"drills/63-hot-update-transfer.sh", "# arms: A A1 B C1", "# arms: A A1 B M01"},
		{"drills/63-hot-update-transfer.sh", " X W1 W2\n", " X W1\n"},
		{"drills/lib/transfer_mutations.py", "for leg in (1, 2):", "for leg in (1,):"},
		{"drills/lib/transfer_mutations.py", "row.get('complete') is False", "True"},
		{"drills/lib/transfer_mutations.py", "row.get('role') == role", "True"},
		{"drills/lib/transfer_mutations.py", "0 < row['durable_bytes'] < row['size']", "row['durable_bytes'] >= 0"},
		{"drills/lib/hot_update_transfer.sh", "transfer_fixture drain\n    _tfin_drain=$?", "transfer_fixture sample\n    _tfin_drain=$?"},
		{"drills/lib/hot_update_transfer.sh", "broker_cleanup || _tca_rc=2", ":"},
	} {
		t.Run(mutation.before, func(t *testing.T) {
			broken := make(map[string]string, len(sources))
			for path, raw := range sources {
				broken[path] = raw
			}
			if !strings.Contains(broken[mutation.path], mutation.before) {
				t.Fatal("control did not match source")
			}
			broken[mutation.path] = strings.Replace(broken[mutation.path], mutation.before, mutation.after, 1)
			if err := transferCheckDriver(broken); err == nil {
				t.Fatal("driver mutation survived")
			}
		})
	}
}

func transferCheckMutations(sources map[string]string) error {
	python := sources["drills/lib/transfer_mutations.py"]
	catalog := regexp.MustCompile(`(?m)^    '(M[0-9]{2}[ABC]?)': \('internal/`).FindAllStringSubmatch(python, -1)
	got := make([]string, 0, len(catalog))
	for _, row := range catalog {
		got = append(got, row[1])
	}
	if !slices.Equal(got, transferControlArms) {
		return fmt.Errorf("production mutation catalog is incomplete")
	}
	if err := transferRequireText(python,
		"source.count(before) == 1", "go_tokens(before) != go_tokens(after)", "declarations[-1] == function",
		"'not implemented (S7' not in source", "change.get('source_sha256') == sha(original.read_bytes())",
		"str(original) not in mapping", "mapping[name] = replacement", "kept.append('-overlay='+str(selector.resolve()))",
		"build.get('build_selector')", "runtime.get('build_selector') == flags", "process.get('running_sha') == running_sha",
		"witness.get('exercised') is True", "witness.get('native_rc') == native_rc == 1", "witness['elapsed_s'] >= 125", "120000000000",
		"parse_native(log, native_rc)", "sha(raw.read_bytes()) == witness.get(key.replace('_path', '_sha'))",
		"version = capable_reader", "hashes['A'] != hashes['B']",
	); err != nil {
		return err
	}
	if err := transferRequireText(sources["drills/lib/hot_update_transfer.sh"],
		"GOFLAGS=$(cat \"$TRANSFER_EV/mutation/goflags.txt\")", "    export GOFLAGS\n",
		"go build -trimpath=false -o \"$TRANSFER_EV/mutation/tether\"",
		"go build -trimpath -o \"$TRANSFER_EV/mutation/tether-original\"", "bind --manifest \"$TRANSFER_MANIFEST\"", "transfer_control_build \"$1\"",
		"--native-rc \"$_tfin_rc\"", "printf '%s\\n' \"$_tfin_rc\" > \"$TRANSFER_EV/judge.rc\"",
	); err != nil {
		return err
	}
	for _, arm := range append([]string{""}, transferControlArms...) {
		key := "64-hot-update-transfer-controls"
		if arm != "" {
			key += "." + arm
		}
		if err := transferRequireText(sources["expected-verdicts.tsv"], key+"\tASSERT-FAIL\t0\t-\t-\t"+key+"\n"); err != nil {
			return err
		}
		if err := transferRequireText(sources["expected-verdicts-log.md"], "## "+key+"\n"); err != nil {
			return err
		}
	}
	return nil
}

// origin: hot-update S7 plan §10.5 / T44.
// gate-control: TestTransferDrillControlsUseProductionMutations
func TestTransferDrillControlsUseProductionMutations(t *testing.T) {
	sources := transferDrillSources(t)
	if err := transferCheckMutations(sources); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []struct{ path, before, after string }{
		{"drills/lib/hot_update_transfer.sh", "    export GOFLAGS\n", ":"},
		{"drills/lib/transfer_mutations.py", "process.get('running_sha') == running_sha", "True"},
		{"drills/lib/transfer_mutations.py", "witness.get('native_rc') == native_rc == 1", "native_rc in (1, 2, 4)"},
		{"drills/lib/transfer_mutations.py", "witness.get('exercised') is True", "True"},
		{"drills/lib/transfer_mutations.py", "go_tokens(before) != go_tokens(after)", "True"},
		{"drills/lib/transfer_mutations.py", "mapping[name] = replacement", "pass"},
		{"drills/lib/transfer_mutations.py", "    'M14': ('internal/", "    'M15': ('internal/"},
		{"drills/lib/transfer_mutations.py", "version = capable_reader", "version = ignored_reader"},
		{"drills/lib/transfer_mutations.py", "hashes['A'] != hashes['B']", "True"},
	} {
		t.Run(mutation.before, func(t *testing.T) {
			broken := make(map[string]string, len(sources))
			for path, raw := range sources {
				broken[path] = raw
			}
			if !strings.Contains(broken[mutation.path], mutation.before) {
				t.Fatal("control did not match source")
			}
			broken[mutation.path] = strings.Replace(broken[mutation.path], mutation.before, mutation.after, 1)
			if err := transferCheckMutations(broken); err == nil {
				t.Fatal("mutation evidence guard survived")
			}
		})
	}
}

func transferPhysicalLineLimit(source string) error {
	if len(strings.Split(strings.TrimSuffix(source, "\n"), "\n")) > 700 {
		return fmt.Errorf("transfer script exceeds 700 physical lines")
	}
	return nil
}

// origin: S7 internal review rv-drill F9.
// gate-control: TestTransferDrillScriptPhysicalLineLimit
func TestTransferDrillScriptPhysicalLineLimit(t *testing.T) {
	root := filepath.Join(repoRoot(t), "test/simcluster")
	var paths []string
	for _, pattern := range []string{"drills/lib/transfer*.py", "tests/*transfer*.py", "drills/lib/hot_update_transfer.sh"} {
		matches, err := filepath.Glob(filepath.Join(root, pattern))
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, matches...)
	}
	if len(paths) < 10 {
		t.Fatal("empty transfer script inventory")
	}
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := transferPhysicalLineLimit(string(raw)); err != nil {
			t.Errorf("%s: %v", path, err)
		}
	}
	if err := transferPhysicalLineLimit(strings.Repeat("# comment\n", 700)); err != nil {
		t.Fatal(err)
	}
	if err := transferPhysicalLineLimit(strings.Repeat("# comment\n", 701)); err == nil {
		t.Fatal("701-line mutation survived")
	}
}

// origin: S7 internal review rv-drill F2.
func TestTransferObserverMatrixRequiresRolesResourcesAndHops(t *testing.T) {
	reviewTransferPython(t, `
value = tm.make_manifest(deployment(), 'B')
with tempfile.TemporaryDirectory() as directory:
    src, dst = Path(directory)/'driver.json', Path(directory)/'fixture.json'
    src.write_text(json.dumps(value))
    adapter.write_manifest(src,dst)
    for defect in ('hop','prepared','resource','generation','cohort'):
        broken=copy.deepcopy(value)
        cuts=broken['fixture']['cuts']
        if defect=='hop': cuts[:]=[c for c in cuts if c['hop']!='third-voter']
        elif defect=='prepared': next(c for c in cuts if c['role']=='prepared')['role']='sender'
        elif defect=='resource':
            resource=next(iter(broken['fixture']['transfers'][0]['required_resources']))
            broken['fixture']['transfers'][0]['required_resources'][resource]=[]
        elif defect=='generation': cuts[0]['generation']+=1
        else: cuts[0]['cohort']='foreign'
        src.write_text(json.dumps(broken))
        try: adapter.write_manifest(src,dst)
        except (ValueError,KeyError): pass
        else: raise AssertionError('accepted '+defect)
`)
}

// origin: S7 internal review rv-drill F7.
func TestTransferReaderRecoveryRescansAndRejectsEvidenceDamage(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		reviewTransferPython(t, `
h=reader_helpers()
for defect in ('recoverable','changed','expired','undeclared','sequence','purged','store','metadata'):
    with h.live_fixture() as (fixture,producer,wire,_,_):
        fixture.manifest['arm']='C3'
        fixture.scan()
        before={name:scan.cursor for name,scan in fixture.scans.items()}
        now=time.monotonic_ns()
        fixture.fault_interval=(now,now+5_000_000_000) if defect!='undeclared' else None
        fixture.reader_error(ConnectionRefusedError())
        if defect=='undeclared':
            assert fixture.keeper.setup_failures
            continue
        assert fixture.outage and not fixture.keeper.setup_failures
        assert before=={name:scan.cursor for name,scan in fixture.scans.items()}
        if defect=='changed':
            with wire.lock: wire.rows['history'][1]=('history.original',h.canonical('failed'))
        if defect=='expired': fixture.fault_interval=(now-10,now-1)
        if defect=='sequence': fixture.scans['history'].cursor+=100
        if defect=='purged':
            with wire.lock: wire.rows['history'].pop()
        if defect=='store':
            with wire.lock: wire.config['storage']='memory'
        if defect=='metadata':
            with wire.lock: wire.config=None
        try: fixture.scan()
        except (obs.SetupError,OSError,IndexError):
            assert defect!='recoverable'
        else:
            assert defect=='recoverable', 'accepted '+defect
            assert fixture.outage is None
            assert (fixture.keeper.evidence/'reader-recovery.json').exists()
`)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), beforeG)
	// Two descriptors allow the Go runtime's first netpoll initialization.
	testharness.AssertNoFDLeak(t, t.Name(), beforeFD, 2)
}

// origin: S7 internal review rv-drill F5.
func TestTransferRollbackCellsRequireOriginalInlineTasks(t *testing.T) {
	reviewTransferPython(t, `
for arm in ('R1','R2'):
    value=tm.make_manifest(deployment(arm),arm)
    adapter.validate_rollback_tasks(value)
    assert [step['leg'] for step in value['steps']]==sorted(step['leg'] for step in value['steps'])
    for defect in ('missing','tier','invocation','command','leg'):
        broken=copy.deepcopy(value)
        if defect=='missing': broken['rollback_tasks'].pop()
        elif defect=='tier': next(t for t in broken['rollback_tasks'] if t['tier']=='A')['transfer']['size']=66549440
        elif defect=='invocation': broken['rollback_tasks'][1]['transfer']['transfer_id']=broken['rollback_tasks'][0]['transfer']['transfer_id']
        elif defect=='command': broken['rollback_tasks'][0]['transfer']['launch']['argv']=['/isolated/empty']
        else: broken['steps'][0],broken['steps'][-1]=broken['steps'][-1],broken['steps'][0]
        try: tm.check_manifest(broken)
        except ValueError: pass
        else: raise AssertionError('accepted '+defect)
`)
}

// origin: S7 internal review rv-drill F5.
func TestTransferRollbackCellsRequireFailedImportAndFreshCurrentState(t *testing.T) {
	reviewTransferPython(t, `
from transfer_oracle import rollback_task_coverage
for arm in ('R1','R2'):
    for tier in ('A','B'):
        manifest=dict(arm=arm,rollback_tasks=[dict(cell='cell',tier=tier,transfer=dict(transfer_id='t',size=100))])
        def row(kind,when,**extra):
            return dict(kind=kind,boottime_ns=when,transfer_id='t',cut_id='cell',producer='agent',
                        pid=1,process_start=2,boot_id='boot',generation=2,**extra)
        events=[row('append',1,offset=0,length=10),row('control',2,active=True),
                row('import',3,outcome='failed'),row('rollback',4),row('append',5,offset=10,length=10),
                row('primitive_exit',6,outcome='landed')]
        assert not rollback_task_coverage(manifest,events)
        for defect in ('failure','rollback','identity','fresh','active'):
            if defect=='active' and arm=='R1': continue
            broken=copy.deepcopy(events)
            if defect=='failure': broken[2]['outcome']='success'
            elif defect=='rollback': broken[3]['cut_id']='foreign'
            elif defect=='identity': broken[3]['pid']=9
            elif defect=='fresh': broken[4]['boottime_ns']=3; broken[5]['boottime_ns']=3
            else: broken[1]['active']=False
            assert rollback_task_coverage(manifest,broken),(arm,tier,defect)
`)
}
