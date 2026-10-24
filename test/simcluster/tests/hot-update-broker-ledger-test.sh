#!/bin/sh
# Pure ledger/manifest/receipt checks. Never executes a drill or a product binary.
# origin: hot-update S6 plan §10.2 / §12.1 P33 / T58
set -eu
BROKER_LEDGER_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
python3 - "$BROKER_LEDGER_ROOT" "$@" <<'PY'
import hashlib
import json
from pathlib import Path
import re
import runpy
import subprocess
import sys
import tempfile

ROOT = Path(sys.argv[1])
D37, D39 = "37-hot-update-broker", "39-hot-update-legacy-n1"
OWNER = "hot-update-plan S6 legacy broker Run unwind; S8 legacy-to-capable first migration"
OLD_OWNERS = ["hot-update-plan H0 legacy exporter; S8 legacy-to-capable first migration",
              "hot-update-plan H1 legacy PTY exporter; S8 legacy-to-capable first migration",
              "hot-update-plan H2 legacy flow exporter; S8 legacy-to-capable first migration"]
HEADER = "candidate_sha arm claim status blocked_by legacy_owner marginal_seconds reader_digest receipt".split()
ARMS = ["37", "A", "A1", "B", "T", "R", "BR"]
CLAIMS = ["native-parent-join", "single-current-store-and-original-cohort", "sole-voter-write-budget",
          "quorum-maintenance-and-broker-scope", "transfer-capable-live-cut-and-legacy-defer",
          "held-salvage-and-active-current-rollback", "legacy-n3-run-unwind"]
BUDGETS = {"HOT_UPDATE_FREEZE_MS": 500, "HOT_UPDATE_REQUEST_S": 5, "HOT_UPDATE_PROBE_RATE": 10,
           "HOT_UPDATE_LIVE_FLOW_STALL_S": 5, "HOT_UPDATE_BROKER_MAINTENANCE_TTL_S": 8}
MANIFEST37 = {"arms": "A A1 B T R", "fixture": "A=install A1=N1 B=N3-live T=N3-live R=N1",
              "grows": "A=0 A1=0 B=2 T=2 R=0", "worst": "A=420 A1=420 B=720 T=1200 R=360",
              "forgoes": "A=- A1=- B=- T=- R=-"}
# origin: S8 internal review s8-hermetic F4. Pin the complete current union.
MANIFEST39 = runpy.run_path(str(ROOT / "tests/testdata/upgrade-legacy-manifest.py"))["MANIFEST39"]


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def rows(text, width):
    result = []
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        row = line.split("\t")
        require(len(row) == width and all(row), f"expected {width} nonempty columns")
        result.append(row)
    return result


def unique(table, key):
    found = [row for row in table if row[0] == key]
    require(len(found) == 1, f"missing or duplicate row {key}")
    return found[0]


def native_join(children):
    require(bool(children), "empty native children")
    rank = {"GREEN": 0, "INCOMPLETE": 1, "PRODUCT-RED": 2, "SETUP-RED": 3, "ASSERT-FAIL": 4}
    codes = {"GREEN": 0, "ASSERT-FAIL": 1, "SETUP-RED": 2, "PRODUCT-RED": 3, "INCOMPLETE": 4}
    for verdict, rc in children:
        require(verdict in codes and type(rc) is int and rc == codes[verdict], "translated native verdict/rc")
    verdict = max((v for v, _ in children), key=rank.__getitem__)
    return verdict, codes[verdict]


def manifest(source, expected):
    for name, value in expected.items():
        found = re.findall(r"^# " + name + r": (.*)$", source, re.M)
        require(found == [value], f"wrong/missing/duplicate manifest {name}")


def contained(base, name):
    require(isinstance(name, str) and name and name != "-", "missing receipt path")
    path = Path(name)
    require(not path.is_absolute() and ".." not in path.parts, "receipt path escapes archive")
    full = (base / path).resolve()
    require(full.is_relative_to(base.resolve()) and full.is_file(), "receipt artifact missing/escaped")
    return full


def check_receipt(row, root):
    candidate, arm, claim, status, blocked, owner, marginal, reader, receipt = row
    if status == "NOT_RUN":
        require(blocked != "-" and all(value == "-" for value in (candidate, marginal, reader, receipt)),
                "NOT_RUN must retain blockers and cannot claim measured evidence")
        return
    require(status in ("PASS", "FAIL", "INCOMPLETE"), "unknown receipt status")
    require(re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", candidate), "missing candidate SHA")
    require(re.fullmatch(r"[0-9a-f]{64}", reader), "missing SHA-bound reader digest")
    require(re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", marginal), "missing measured marginal cost")
    path = contained(root, receipt)
    evidence = json.loads(path.read_text())
    require(evidence.get("source_sha") == candidate and evidence.get("claim") == claim and
            evidence.get("test_or_arm") == arm and evidence.get("reader_digest") == reader,
            "receipt identity mismatch")
    for key in ("artifact_sha", "previous_sha", "mutation_digest"):
        require(re.fullmatch(r"[0-9a-f]{64}", evidence.get(key, "")), "missing " + key)
    for key in ("command", "log_mapping", "phase_intersections", "oracle_inputs"):
        require(bool(evidence.get(key)), "empty receipt " + key)
    for key in ("pid", "startticks"):
        require(type(evidence.get(key)) is int and evidence[key] > 0, "missing original process identity")
    require(type(evidence.get("wall_seconds")) in (int, float) and evidence["wall_seconds"] >= 0,
            "missing measured wall time")
    verdict, rc = evidence.get("verdict"), evidence.get("native_rc")
    native_join([(verdict, rc)])
    counters = evidence.get("counters")
    require(isinstance(counters, dict) and all(type(counters.get(key)) is int and counters[key] >= 0
            for key in ("assert_fail", "setup_red", "product_red", "not_covered", "nc_gap", "nc_guard", "pass")),
            "missing native counters")
    require(counters["not_covered"] == counters["nc_gap"] + counters["nc_guard"], "native gap counters disagree")
    counter_verdict = "GREEN"
    for key, label in [("assert_fail", "ASSERT-FAIL"), ("setup_red", "SETUP-RED"),
                       ("product_red", "PRODUCT-RED"), ("not_covered", "INCOMPLETE")]:
        if counters[key]:
            counter_verdict = label
            break
    require(verdict == counter_verdict and counters["pass"] > 0, "native counters do not support verdict/nonempty cohort")
    if status == "PASS":
        require(blocked == "-" and (verdict, rc) == (("PRODUCT-RED", 3) if arm == "BR" else ("GREEN", 0)),
                "PASS does not have the required native verdict")
    else:
        require(blocked != "-" and verdict != "GREEN" and
                (status != "INCOMPLETE" or (verdict, rc) == ("INCOMPLETE", 4)), "red receipt lost blocker/verdict")
    # Bind actual archived inputs, rather than accepting a same-SHA label with
    # missing stdout or a collector that quietly dropped its last sample.
    for key in ("stdout_path", "stderr_path"):
        contained(path.parent, evidence.get(key))
    for oracle in evidence["oracle_inputs"]:
        require(isinstance(oracle, dict), "oracle input needs path and SHA256")
        raw = contained(path.parent, oracle.get("path"))
        require(hashlib.sha256(raw.read_bytes()).hexdigest() == oracle.get("sha256"), "oracle input digest mismatch")


def check(tables, root, include_drills=False):
    verdicts = rows(tables["expected-verdicts.tsv"], 6)
    costs = rows(tables["drill-costs.tsv"], 4)
    ledger = rows(tables["hot-update-broker-ledger.tsv"], 9)
    require(ledger.pop(0) == HEADER, "broker ledger schema changed")
    # origin: hot-update S7 plan §10.6 / T43. The frozen S6 row is still
    # lead-owned. It can retain NOT_RUN while P35 migrates the key, but an old
    # defer receipt can never sign today's capable-transfer cut.
    for row in ledger:
        if row[1:3] == ["T", "transfer-defer-and-terminal-cut"]:
            require(row[3] == "NOT_RUN", "historical transfer defer cannot sign S7 capable cut")
            row[2] = CLAIMS[4]
    require(sorted((r[1], r[2]) for r in ledger) == sorted(zip(ARMS, CLAIMS)), "missing/duplicate/unowned broker claim")
    for row in ledger:
        require(row[5] == (OWNER if row[1] == "BR" else "-"), "broker legacy owner changed")
        check_receipt(row, root)
    measured = [row for row in ledger if row[3] != "NOT_RUN"]
    require(len({row[0] for row in measured}) <= 1, "broker claims mix candidate SHAs")
    by_arm = {row[1]: row for row in ledger}
    if by_arm["37"][3] == "PASS":
        require(all(by_arm[arm][3] == "PASS" for arm in ["A", "A1", "B", "T", "R"]),
                "37 parent signed before all native children passed")
    expected = [(D37 + suffix, "GREEN", "-") for suffix in ("", ".A", ".A1", ".B", ".T", ".R")]
    expected += [(D39 + ".BR", "PRODUCT-RED", OWNER)]
    for key, verdict, owner in expected:
        require(unique(verdicts, key) == [key, verdict, "0", "-", owner, key], f"changed native expectation {key}")
        require(tables["expected-verdicts-log.md"].count("## " + key + "\n") == 1, f"missing/duplicate note-ref {key}")
    parent = unique(verdicts, D39)
    require(parent == [D39, "PRODUCT-RED", "0", "-", "; ".join(OLD_OWNERS + [OWNER]), D39], "39 parent owner/join changed")
    for arm, verdict, owner in zip("XYPF", ["GREEN", "PRODUCT-RED", "PRODUCT-RED", "PRODUCT-RED"], ["-"] + OLD_OWNERS):
        key = D39 + "." + arm
        require(unique(verdicts, key) == [key, verdict, "0", "-", owner, key], "historical child changed")
    # origin: S8 internal review s8-hermetic F4. S7 37.T declares a full
    # capable single/N3 cut and legacy defer; 1200 s is still NOT_MEASURED.
    for arm, estimate in zip(["A", "A1", "B", "T", "R", "BR"], [180, 180, 360, 1200, 120, 90]):
        key = (D39 if arm == "BR" else D37) + "." + arm
        row = unique(costs, key)
        require(row[1].isdigit() and int(row[1]) > 0, "empty broker cost")
        seconds = int(row[1])
        require(row[2] == ("short" if seconds <= 180 else "medium" if seconds <= 500 else "long"), "wrong cost class")
        if "NOT_RUN" in row[3]:
            require(seconds == estimate and "estimate" in row[3] and "lead replace" in row[3], "estimate disguised as measurement")
        else:
            require("COMPLETE" in row[3] and "image " in row[3] and "estimate" not in row[3], "cost lost native receipt")
    assignments = {}
    for line in tables["drills/lib/hot_update_budget.sh"].splitlines():
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(r"(HOT_UPDATE_[A-Z0-9_]+)=([0-9]+)", line)
        require(match is not None, "budget must be a literal assignment")
        name, value = match.groups()
        require(name not in assignments, "duplicate budget")
        assignments[name] = int(value)
    for name, value in BUDGETS.items():
        require(assignments.get(name) == value, f"wrong/missing frozen budget {name}")
    require(native_join([("GREEN", 0)] * 5) == ("GREEN", 0), "37 parent join changed")
    require(native_join([("GREEN", 0), ("PRODUCT-RED", 3)]) == ("PRODUCT-RED", 3), "39 native red translated")
    loop = re.findall(r"^for t in (.*); do$", tables["tests/run-all.sh"], re.M)
    require(len(loop) == 1 and loop[0].split().count("hot-update-broker-ledger-test") == 1,
            "missing/duplicate broker ledger hermetic entry")
    if include_drills:
        manifest(tables["drills/37-hot-update-broker.sh"], MANIFEST37)
        manifest(tables["drills/39-hot-update-legacy-n1.sh"], MANIFEST39)
        run_all = tables["tests/run-all.sh"]
        loop = re.findall(r"^for t in (.*); do$", run_all, re.M)
        require(len(loop) == 1, "missing hermetic loop")
        for name in ("hot-update-broker-ledger-test", "hot-update-broker-probe-test", "hot-update-broker-drill-test"):
            require(loop[0].split().count(name) == 1 and (root / "tests" / (name + ".sh")).is_file(),
                    "missing broker hermetic entry " + name)
        require((root / "drills/lib/flow_fixture").is_file(), "missing live fixture wrapper")


def selftest(tables):
    check(tables, ROOT)
    killed = 0
    mutations = []
    # Delete every new child and cost separately; a cardinality-only gate
    # would miss swapping one arm for another with the same number of rows.
    for file, keys in [("expected-verdicts.tsv", [D37] + [D37 + "." + a for a in ["A", "A1", "B", "T", "R"]] + [D39 + ".BR"]),
                       ("drill-costs.tsv", [D37 + "." + a for a in ["A", "A1", "B", "T", "R"]] + [D39 + ".BR"])]:
        for key in keys:
            mutations.append((file, "\n".join(line for line in tables[file].splitlines() if not line.startswith(key + "\t"))))
    for arm in ARMS:
        file = "hot-update-broker-ledger.tsv"
        mutations.append((file, "\n".join(line for line in tables[file].splitlines() if not line.startswith("-\t" + arm + "\t"))))
    mutations += [("expected-verdicts.tsv", tables["expected-verdicts.tsv"].replace(D39 + ".BR\tPRODUCT-RED", D39 + ".BR\tGREEN")),
                  ("hot-update-broker-ledger.tsv", tables["hot-update-broker-ledger.tsv"].replace(OWNER, "wrong-owner")),
                  ("hot-update-broker-ledger.tsv", tables["hot-update-broker-ledger.tsv"].replace("\tNOT_RUN\t", "\tPASS\t", 1)),
                  ("tests/run-all.sh", tables["tests/run-all.sh"].replace("hot-update-broker-ledger-test", "", 1))]
    for name, value in BUDGETS.items():
        file = "drills/lib/hot_update_budget.sh"
        mutations.append((file, tables[file].replace(f"{name}={value}", f"{name}={value+1}")))
    for file, mutant in mutations:
        broken = dict(tables)
        broken[file] = mutant
        try:
            check(broken, ROOT)
        except (ValueError, OSError, json.JSONDecodeError):
            killed += 1
        else:
            raise ValueError("mutation survived: " + file)
    for expected in (MANIFEST37, MANIFEST39):
        source = "\n".join("# " + key + ": " + value for key, value in expected.items())
        manifest(source, expected)
        for key, value in expected.items():
            try:
                manifest(source.replace("# " + key + ": " + value, "# " + key + ": missing"), expected)
            except ValueError:
                killed += 1
            else:
                raise ValueError("manifest mutation survived " + key)
    legacy = "\n".join("# " + key + ": " + value for key, value in MANIFEST39.items())
    try:
        manifest(legacy.replace("BR=N3-live", "BR=N1"), MANIFEST39)
    except ValueError:
        killed += 1
    else:
        raise ValueError("legacy N1 fixture survived")
    for children in [[("PRODUCT-RED", 0)], [("INCOMPLETE", 3)], []]:
        try:
            native_join(children)
        except ValueError:
            killed += 1
        else:
            raise ValueError("translated native rc survived")
    # Synthetic parser controls certify the receipt guard, not a live arm.
    # The temporary archive is destroyed before this script returns.
    with tempfile.TemporaryDirectory(prefix="broker-ledger-controls-") as directory:
        base = Path(directory)
        (base / "stdout.raw").write_text("synthetic oracle\n")
        (base / "stderr.raw").write_text("")
        evidence = {"source_sha": "a" * 40, "artifact_sha": "b" * 64, "previous_sha": "c" * 64,
                    "mutation_digest": "d" * 64, "reader_digest": "e" * 64, "claim": CLAIMS[1], "test_or_arm": "A",
                    "command": "synthetic parser control", "counters": {"assert_fail": 0, "setup_red": 0, "product_red": 0, "not_covered": 0, "nc_gap": 0, "nc_guard": 0, "pass": 1}, "native_rc": 0, "verdict": "GREEN", "pid": 1,
                    "startticks": 1, "wall_seconds": 1, "phase_intersections": ["synthetic-cut"],
                    "log_mapping": {"synthetic": "stdout.raw"}, "stdout_path": "stdout.raw", "stderr_path": "stderr.raw",
                    "oracle_inputs": [{"path": "stdout.raw", "sha256": hashlib.sha256((base / "stdout.raw").read_bytes()).hexdigest()}]}
        receipt = base / "receipt.json"
        row = ["a" * 40, "A", CLAIMS[1], "PASS", "-", "-", "1", "e" * 64, "receipt.json"]
        receipt.write_text(json.dumps(evidence))
        check_receipt(row, base)
        for field in evidence:
            broken = dict(evidence)
            del broken[field]
            receipt.write_text(json.dumps(broken))
            try:
                check_receipt(row, base)
            except (ValueError, TypeError):
                killed += 1
            else:
                raise ValueError("missing receipt field survived: " + field)
        for counter in evidence["counters"]:
            broken = dict(evidence)
            broken["counters"] = dict(evidence["counters"])
            del broken["counters"][counter]
            receipt.write_text(json.dumps(broken))
            try:
                check_receipt(row, base)
            except ValueError:
                killed += 1
            else:
                raise ValueError("missing native counter survived: " + counter)
        receipt.write_text(json.dumps(evidence))
        for index, value in [(0, "f"*40), (1, "BR"), (2, "unknown"), (3, "UNKNOWN"), (4, "P35"),
                             (6, "estimate"), (7, "f"*64), (8, "../receipt.json"), (8, "/receipt.json")]:
            broken = list(row)
            broken[index] = value
            try:
                check_receipt(broken, base)
            except ValueError:
                killed += 1
            else:
                raise ValueError("receipt identity/escape mutation survived")
        # A native parent cannot be signed by deleting a child from its
        # evidence chain; all rows must describe the same candidate tree.
        synthetic = dict(tables)
        chain = rows(synthetic["hot-update-broker-ledger.tsv"], 9)
        for entry in chain[1:]:
            if entry[1] == "BR":
                continue
            entry[2] = CLAIMS[ARMS.index(entry[1])]
            entry[0], entry[3], entry[4], entry[6], entry[7], entry[8] = "a"*40, "PASS", "-", "1", "e"*64, entry[1]+".json"
            body = dict(evidence)
            body["claim"], body["test_or_arm"] = entry[2], entry[1]
            (base / entry[8]).write_text(json.dumps(body))
        synthetic["hot-update-broker-ledger.tsv"] = "\n".join("\t".join(entry) for entry in chain)
        check(synthetic, base)
        historical = [list(entry) for entry in chain]
        next(entry for entry in historical if entry[1] == "T")[2] = "transfer-defer-and-terminal-cut"
        broken = dict(synthetic)
        broken["hot-update-broker-ledger.tsv"] = "\n".join("\t".join(entry) for entry in historical)
        try:
            check(broken, base)
        except ValueError as error:
            require("historical transfer defer" in str(error), "historical T control missed its guard")
            killed += 1
        else:
            raise ValueError("historical transfer defer signed S7 capable cut")
        for arm in ["A", "A1", "B", "T", "R"]:
            broken = dict(synthetic)
            missing = [list(entry) for entry in chain]
            entry = next(entry for entry in missing if entry[1] == arm)
            entry[0], entry[3], entry[4], entry[6], entry[7], entry[8] = "-", "NOT_RUN", "P35", "-", "-", "-"
            broken["hot-update-broker-ledger.tsv"] = "\n".join("\t".join(entry) for entry in missing)
            try:
                check(broken, base)
            except ValueError as error:
                require("parent signed" in str(error), "child control failed for an unrelated reason")
                killed += 1
            else:
                raise ValueError("parent signed with a missing child")
        mixed = [list(entry) for entry in chain]
        entry = next(entry for entry in mixed if entry[1] == "A")
        entry[0] = "f"*40
        body = dict(evidence)
        body["source_sha"] = "f"*40
        (base / "A.json").write_text(json.dumps(body))
        broken = dict(synthetic)
        broken["hot-update-broker-ledger.tsv"] = "\n".join("\t".join(entry) for entry in mixed)
        try:
            check(broken, base)
        except ValueError as error:
            require("mix candidate" in str(error), "candidate control failed for an unrelated reason")
            killed += 1
        else:
            raise ValueError("mixed candidate claims survived")
        (base / "stdout.raw").write_text("tampered oracle\n")
        try:
            check_receipt(row, base)
        except ValueError:
            killed += 1
        else:
            raise ValueError("raw oracle tampering survived")
    # Exercise the repository's actual native verdict implementation, including
    # INCOMPLETE=4. Only the pure assertion library is sourced, never a drill.
    for counter, verdict, rc in [("_AS_PASS", "GREEN", 0), ("_AS_PRODUCT_RED", "PRODUCT-RED", 3),
                                 ("_AS_NC", "INCOMPLETE", 4), ("_AS_SETUP", "SETUP-RED", 2),
                                 ("_AS_FAIL", "ASSERT-FAIL", 1)]:
        body = '. "$1/lib/assert.sh"\nlog() { :; }; ok() { :; }; err() { :; }\nINSTANCE=drill-ledger-selftest\ndrill_begin ledger\n' + counter + '=1\ndrill_end\n'
        result = subprocess.run(["sh", "-c", body, "ledger-selftest", str(ROOT)], capture_output=True, text=True, timeout=5)
        require(result.returncode == rc and f"verdict={verdict} rc={rc} " in result.stderr, "actual native verdict contract changed")
    print(f"broker ledger controls: PASS ({killed} rejected mutations; actual native rc 0/1/2/3/4)")


files = ["expected-verdicts.tsv", "expected-verdicts-log.md", "drill-costs.tsv", "hot-update-broker-ledger.tsv",
         "drills/lib/hot_update_budget.sh", "tests/run-all.sh"]
mode = sys.argv[2:] or ["--require-drills"]
require(mode in (["--selftest"], ["--tables-only"], ["--require-drills"]), "unknown ledger check mode")
try:
    if mode == ["--require-drills"]:
        files += ["drills/37-hot-update-broker.sh", "drills/39-hot-update-legacy-n1.sh"]
    tables = {file: (ROOT / file).read_text() for file in files}
    if mode == ["--selftest"]:
        selftest(tables)
    else:
        check(tables, ROOT, mode == ["--require-drills"])
        print("broker ledger: PASS (declarations/receipt integrity; live claims retain their status)")
except (ValueError, TypeError, OSError, json.JSONDecodeError, subprocess.TimeoutExpired) as error:
    print("broker ledger: FAIL: " + str(error), file=sys.stderr)
    sys.exit(1)
PY
