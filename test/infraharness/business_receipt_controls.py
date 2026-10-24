#!/usr/bin/env python3
"""Byte-codec controls only. These fixture records never enter runLive."""
import copy
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import business_receipt as producer


def rejected(schema, value):
    try:
        producer.encode(schema, value)
    except ValueError:
        return
    raise AssertionError("canonical producer accepted an invalid typed receipt")


def main():
    repo, root = map(Path, sys.argv[1:])
    root.chmod(0o700)
    schema = json.loads(subprocess.check_output(["go", "run", "./test/infraharness/receiptschema", str(repo)],
                                              cwd=repo, timeout=60))
    original = producer.decode((root / "input.json").read_bytes())
    # Sorting all object keys must not change the actual Go declaration order.
    unordered = producer.decode(json.dumps(original, sort_keys=True).encode())
    (root / "canonical-control.json").write_bytes(producer.encode(schema, unordered))
    for defect in ("unknown", "missing", "int-overflow", "negative-uint", "float", "bool-as-int",
                   "fixed-array", "nested-scope", "unknown-audit-field", "base64"):
        value = copy.deepcopy(original)
        if defect == "unknown":
            value["Invented"] = True
        elif defect == "missing":
            del value["TransferSealNS"]
        elif defect == "int-overflow":
            value["TransferSealNS"] = 1 << 63
        elif defect == "negative-uint":
            value["Required"][0]["Cookie"] = -1
        elif defect == "float":
            value["Required"][0]["Cookie"] = 1.0
        elif defect == "bool-as-int":
            value["Required"][0]["Cookie"] = True
        elif defect == "fixed-array":
            value["Hops"][0]["Tx"] = [0]
        elif defect == "nested-scope":
            del value["Transfers"][0]["EvidenceDirection"]
        elif defect == "unknown-audit-field":
            value["Required"][0]["AuditIdentity"]["extra"] = "unknown"
        else:
            value["TransferFinals"][0]["History"]["Start"][0]["Bytes"] = "@@@"
        rejected(schema, value)
    for raw in (b'{"Version":2,"Version":2}', b'{"Version":NaN}'):
        try:
            producer.decode(raw)
        except ValueError:
            continue
        raise AssertionError("receipt decoder accepted duplicate/nonfinite fields")
    # Exercise real atomic publication under a control-only filename, then
    # remove it. The caller compares canonical-control through actual Go code.
    supply = {name: original[name] for name in ("SourceSHA256", "SchemaSHA256")}
    path = producer.publish(schema, original, supply, root)
    assert path.read_bytes() == (root / "input.json").read_bytes()
    try:
        producer.publish(schema, original, supply, root)
    except FileExistsError:
        pass
    else:
        raise AssertionError("producer overwrote an existing receipt")
    path.unlink()
    for name in supply:
        foreign = dict(supply, **{name: "f" * 64})
        try:
            producer.publish(schema, original, foreign, root)
        except ValueError:
            continue
        raise AssertionError("producer lost its source/schema binding")
    assert not (root / "business-receipt.json").exists()


if __name__ == "__main__":
    main()
