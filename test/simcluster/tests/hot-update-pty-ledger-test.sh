#!/bin/sh
# hot-update-pty-ledger-test.sh — preserve S4's drill receipts.
# Count floors alone permit absent rows; launch costs otherwise have no completeness check.
# origin: hot-update S4 plan §9.3 / §9.4. Until the first real run it pinned the
# declared/NOT_RUN estimates; run 5 (2026-10-03, image 78f2c62d) was the first
# complete real run, so it now pins the measured costs, the real-run receipts and
# the lead's 39.P signature pin — a later edit cannot quietly turn them back into
# estimates or an unpinned boundary.
set -eu
HERE="$(cd "$(dirname "$0")" && pwd)"
python3 -I -B - "$HERE/.." <<'PY'
from pathlib import Path
import sys

root = Path(sys.argv[1])
def rows(name):
    return [line.split("\t") for line in (root/name).read_text().splitlines()
            if line and not line.startswith("#")]

def check(verdicts, costs, kept, log):
    def unique(table, key):
        found = [row for row in table if row[0] == key]
        assert len(found) == 1, f"{key}: expected exactly one ledger row, got {len(found)}"
        return found[0]
    h0 = "hot-update-plan H0 legacy exporter; S8 legacy-to-capable first migration"
    h1 = "hot-update-plan H1 legacy PTY exporter; S8 legacy-to-capable first migration"
    h2 = "hot-update-plan H2 legacy flow exporter; S8 legacy-to-capable first migration"
    # origin: hot-update S6 plan §10.2 / T58 — append the broker boundary while
    # retaining every S3/S4/S5 owner and receipt pin.
    h3 = "hot-update-plan S6 legacy broker Run unwind; S8 legacy-to-capable first migration"
    d36, d39 = "36-hot-update-pty", "39-hot-update-legacy-n1"
    for key in [d36, *(d36+"."+a for a in "AWCL"), d39+".P", d39]:
        owner = h0+"; "+h1+"; "+h2+"; "+h3 if key == d39 else h1 if key == d39+".P" else "-"
        verdict = "PRODUCT-RED" if key.startswith(d39) else "GREEN"
        assert unique(verdicts, key) == [key, verdict, "0", "-", owner, key], f"{key}: declaration changed"
    # origin: hot-update S5 plan §9.5 / GATES-FINAL-3. Fresh final-image
    # measurements supersede S4 run 5 costs, without changing its verdicts.
    # Receipt: s5-evidence/drills/RECEIPTS.md, final acceptance image 9d8a232c;
    # raw seconds: run-274/runner/36-hot-update-pty.{A,W,C,L}.secs and
    # run-273/runner/39-hot-update-legacy-n1.P.secs (serial -j1 quiet window).
    for key, secs in [(d36+".A",107), (d36+".W",218), (d36+".C",115), (d36+".L",201), (d39+".P",349)]:
        row = unique(costs, key)
        assert row[1:3] == [str(secs), "medium" if secs > 180 else "short"], f"{key}: measured cost changed"
        receipt = "run-273" if key == d39+".P" else "run-274"
        assert len(row) == 4 and "image 9d8a232c" in row[3] and "estimate" not in row[3] and "NOT_RUN" not in row[3], f"{key}: measured cost lost its run identity"
        assert receipt+"/runner/"+key+".secs" in row[3], f"{key}: measured cost lost its receipt reference"
    # origin: S8 internal review s8-hermetic F3. The exact pins follow the
    # merged ledger additions; no historical floor is reduced.
    for drill, counts in [(d36,{"":96,".A":0,".W":19,".C":22,".L":9,"._shared":46}),
                           (d39,{"":34,".X":6,".Y":6,".P":0,".F":1,"._shared":21})]:
        for suffix, count in counts.items():
            key = drill+suffix
            assert unique(kept,key) == [key,str(count)], f"{key}: kept-site receipt changed"
    for key in [d36, *(d36+"."+a for a in "AWCL"), d39+".P"]:
        marker = "## "+key+"\n"
        assert log.count(marker) == 1, f"{key}: missing or duplicate note-ref section"
        section = log.split(marker)[1].split("\n## ")[0]
        assert "declared / NOT_RUN" not in section, f"{key}: a real-run receipt was turned back into a declaration"
        if key == d39+".P":
            assert "Pinned 2026-10-03" in section and "real run 5 PRODUCT-RED match" in section and h1 in section, "39.P: reviewed pin receipt missing"
        elif key == d36:
            assert "First complete real run 2026-10-03" in section, f"{key}: first-run receipt missing"
        else:
            assert "real run 5 GREEN" in section, f"{key}: real-run receipt missing"

    # origin: hot-update S5 plan §9.4–§9.5. A new child cannot silently lose
    # its declaration/cost while the S4 receipt counts remain unchanged.
    d75 = "75-hot-update-flows"
    for suffix, count in {"":64,".A":10,".C":2,".R":2,".B":2,".W":6,".L":2,"._shared":40}.items():
        key = d75+suffix
        assert unique(kept, key) == [key, str(count)], f"{key}: missing S5 assertion floor"
    assert unique(kept, "lib/hot_update_flow_faults") == ["lib/hot_update_flow_faults", "1"], "shared flow classifier missing"
    units = [(d75+"."+arm, secs) for arm, secs in zip("ACRBWL", [180,240,180,240,300,150])]
    units.append((d39+".F", 300))
    for key in [d75, *(unit for unit, _ in units)]:
        owner = h2 if key == d39+".F" else "-"
        verdict = "PRODUCT-RED" if key == d39+".F" else "GREEN"
        assert unique(verdicts, key) == [key, verdict, "0", "-", owner, key], f"{key}: S5 declaration changed"
        marker = "## "+key+"\n"
        assert log.count(marker) == 1, f"{key}: missing or duplicate S5 note-ref"
        section = log.split(marker)[1].split("\n## ")[0]
        assert verdict in section and "nc_gap=0" in section, f"{key}: S5 note disagrees"
    for key, estimate in units:
        row = unique(costs, key)
        assert len(row) == 4 and row[1].isdigit() and int(row[1]) > 0, f"{key}: invalid S5 cost"
        secs = int(row[1])
        assert row[2] == ("short" if secs <= 180 else "medium" if secs <= 500 else "long"), f"{key}: cost class disagrees"
        if "NOT_RUN" in row[3]:
            assert row[1] == str(estimate) and "estimate" in row[3] and "lead replace" in row[3], f"{key}: undeclared cost placeholder"
        else:
            assert "COMPLETE" in row[3] and "image " in row[3] and "estimate" not in row[3], f"{key}: missing real cost identity"

verdicts = rows("expected-verdicts.tsv")
costs = rows("drill-costs.tsv")
kept = rows("tests/kept-sites.baseline.tsv")
log = (root/"expected-verdicts-log.md").read_text()
check(verdicts,costs,kept,log)
# Removing the appended owner must fail the same predicate that signs the table.
changed = [list(row) for row in verdicts]
parent = next(row for row in changed if row[0] == "39-hot-update-legacy-n1")
parent[4] = parent[4].replace("; hot-update-plan S6 legacy broker Run unwind; S8 legacy-to-capable first migration", "")
try:
    check(changed,costs,kept,log)
except AssertionError:
    pass
else:
    raise AssertionError("missing S6 legacy broker owner escaped")
# origin: hot-update S5 plan §9.5 / GATES-FINAL-3. Keep the measured
# seconds, artifact and raw receipt independently pinned for every S4 arm.
for row in costs:
    if row[0] not in {"36-hot-update-pty."+arm for arm in "AWCL"} | {"39-hot-update-legacy-n1.P"}:
        continue
    for field, value in [(1, str(int(row[1])+1)),
                         (3, row[3].replace("image 9d8a232c", "image 78f2c62d")),
                         (3, row[3].replace("/runner/", "/missing/"))]:
        changed = list(row)
        changed[field] = value
        mutated = [changed if other is row else other for other in costs]
        try:
            check(verdicts,mutated,kept,log)
        except AssertionError:
            continue
        raise AssertionError("changed measurement escaped: "+row[0]+" field "+str(field))
# The same check must reject each missing S4 receipt. These controls exercise the
# on-disk gate's input comparison, without invoking a drill or changing live data.
for table_index, table in enumerate((verdicts,costs,kept)):
    for row in table:
        if row[0].startswith("75-hot-update-flows") or row[0].startswith("36-hot-update-pty") or row[0] == "39-hot-update-legacy-n1.F" or row[0] in ("39-hot-update-legacy-n1", "39-hot-update-legacy-n1.P", "39-hot-update-legacy-n1._shared"):
            inputs = [verdicts,costs,kept]
            inputs[table_index] = [other for other in table if other is not row]
            try:
                check(*inputs,log)
            except AssertionError:
                continue
            raise AssertionError("missing row escaped: "+row[0])
# origin: hot-update S4 FX-D-MERGE. Keep an independently witnessed
# control from each concurrent writer. Deleting an expectation otherwise
# lowers r9d's proved count while leaving its exit status falsely green.
def check_controls(source):
    required = {
        "P16-SIM active generation": 'nv_pty_mutate "$PTY_EV/active.json" \'.generation=1\' _pty_current_state',
        "FX-D no-responder reason": '\'.reason="agent_no_responders"\'',
    }
    for label, statement in required.items():
        assert source.count(statement) == 1, f"{label}: missing or duplicate nonvacuity control"
    return required
source = (root/"tests/r9d-nonvacuity.sh").read_text()
for label, statement in check_controls(source).items():
    try:
        check_controls(source.replace(statement, ""))
    except AssertionError:
        continue
    raise AssertionError("removed expectation escaped: "+label)
print("hot-update PTY ledgers: PASS (real run 5 verdicts, final-image costs, 39.P pin and S5 declarations; measurement and row-deletion controls reject)")
PY
