"""Bounded, append-only S8 evidence IO and closed schema validation.

This leaf holds no product ownership or completion decision. The collector and
companion verifier share the exact same errors, provenance and JSON predicates.
"""
import csv
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import sys
import tempfile
import time

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("upgrade_tx_supervisor", HERE / "upgrade_tx_supervisor.py")
supervisor = sys.modules.get(spec.name)
if supervisor is None:
    supervisor = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = supervisor
    spec.loader.exec_module(supervisor)
MAX_BYTES = 16 * 1024 * 1024
DOMAINS = set("control exec pty flow ss transfer_worker transfer_tracker broker_store broker_runtime authority upgrade".split())
PROFILE = dict(ssh=2, tcp=2, pty=2, exec=4, transfer=4, ss=1, remote_fs=1, control=1)
VERDICTS = {0: "GREEN", 1: "ASSERT-RED", 2: "SETUP-RED", 3: "PRODUCT-RED", 4: "INCOMPLETE"}


class EvidenceError(ValueError):
    def __init__(self, reason, rc=2):
        super().__init__(reason)
        self.rc = rc


def require(value, reason, rc=2):
    if not value:
        raise EvidenceError(reason, rc)


def boottime_ns():
    require(hasattr(time, "CLOCK_BOOTTIME"), "native_clock_adapter_required")
    return time.clock_gettime_ns(time.CLOCK_BOOTTIME)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate_json_field")
        result[key] = value
    return result


def decode(raw):
    require(len(raw) <= MAX_BYTES, "evidence_overflow")
    return json.loads(raw, object_pairs_hook=unique_object,
                      parse_constant=lambda _value: require(False, "nonfinite_json"))


def read(path):
    path = Path(path)
    require(path.is_file() and path.stat().st_size <= MAX_BYTES, "evidence_missing_or_oversize")
    return decode(path.read_bytes())


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def publish(path, value):
    # origin: hot-update S8 plan §9.0. Nonce evidence is append-only.
    # An atomic link refuses concurrent publication of the same nonce. A
    # check-then-rename can overwrite a rival observation after both pass.
    path = Path(path)
    fd, name = tempfile.mkstemp(prefix=".publish-", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream, sort_keys=True, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(name, path)
        except FileExistsError:
            raise EvidenceError("evidence_already_published", 1) from None
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        os.unlink(name)


def schema_check(value, schema=None, root=None, where="sample"):
    if schema is None:
        schema = read(HERE / "upgrade_tx_schema.json")
    root = root or schema
    if "$ref" in schema:
        return schema_check(value, root["$defs"][schema["$ref"].split("/")[-1]], root, where)
    expected = schema.get("type")
    types = dict(object=dict, array=list, string=str, integer=int, boolean=bool)
    if expected == "number":
        require(type(value) in (int, float) and math.isfinite(value), "schema_type_" + where)
    elif expected:
        require(type(value) is types[expected], "schema_type_" + where)
    if "const" in schema:
        require(type(value) is type(schema["const"]) and value == schema["const"], "schema_const_" + where)
    if "enum" in schema:
        require(any(type(value) is type(item) and value == item for item in schema["enum"]), "schema_enum_" + where)
    if expected == "object":
        require(set(schema["required"]) <= set(value), "schema_missing_" + where)
        require(set(value) <= set(schema["properties"]), "schema_unknown_" + where)
        for key, item in value.items():
            schema_check(item, schema["properties"][key], root, where + "." + key)
    elif expected == "array":
        require(schema.get("minItems", 0) <= len(value) <= schema["maxItems"], "schema_count_" + where)
        for item in value:
            schema_check(item, schema["items"], root, where)
    elif expected == "integer":
        require(schema["minimum"] <= value <= schema["maximum"], "schema_range_" + where)
    elif expected == "string":
        require(schema.get("minLength", 0) <= len(value) <= schema.get("maxLength", 256), "schema_length_" + where)
        if "pattern" in schema:
            require(re.fullmatch(schema["pattern"], value), "schema_pattern_" + where)


def index(rows):
    result = {(row["domain"], row["id"]): row for row in rows}
    require(len(rows) == len(result), "duplicate_resource_identity", 1)
    return result


def validate_originals(originals):
    require(set(originals) == {"version", "manifest_digest", "run_id", "domains", "resources", "original_set_digest"},
            "original_set_schema_invalid")
    body = {key: value for key, value in originals.items() if key != "original_set_digest"}
    require(originals.get("version") == 1 and digest(body) == originals.get("original_set_digest"),
            "original_set_seal_changed", 1)
    require(originals.get("resources"), "original_set_empty")
    index(originals["resources"])


def unknown_lane(manifest):
    # origin: hot-update S8 plan §8.2 / P26b. Durability failure and revocation
    # can both report unknown while the current runtime is still known. Only
    # a declared dead/unknown cell permits incomplete custody observations.
    arm = manifest["arm"]
    return arm == "C-owner-dead" or re.fullmatch(r"W\d{2}-F\d-(?:agent|single|sole-voter|N3)-D-unknown-(?:none|inside|outside)", arm) is not None


def schema_definition(value, name):
    schema = read(HERE / "upgrade_tx_schema.json")
    schema_check(value, schema["$defs"][name], schema, name)


def arm_contract(manifest):
    with (HERE / "upgrade_tx_arms.tsv").open() as stream:
        rows = list(csv.DictReader((line for line in stream if not line.startswith("#")), delimiter="\t"))
    rows = [row for row in rows if row["arm"] == manifest["arm"]]
    require(len(rows) == 1, "scenario_arm_not_declared")
    row = rows[0]
    require((row["expected_phase"], row["expected_outcome"], int(row["cli_rc"])) ==
            tuple(manifest["expect"][key] for key in ("phase", "outcome", "native_rc")), "scenario_expectation_changed")
    return row


def evidence_file(root, reference):
    # origin: hot-update S8 plan §9.0 / P26b. A companion may only reference
    # immutable files inside this run. Paths and symlinks cannot import another
    # run's receipts, and hashes cover bytes actually read, not a supplied label.
    schema_definition(reference, "evidence_file")
    path = Path(root) / reference["path"]
    require(not Path(reference["path"]).is_absolute() and ".." not in Path(reference["path"]).parts
            and path.resolve().is_relative_to(Path(root).resolve()), "scenario_evidence_path_foreign")
    require(path.is_file() and path.stat().st_size <= MAX_BYTES, "scenario_evidence_missing")
    raw = path.read_bytes()
    require(hashlib.sha256(raw).hexdigest() == reference["sha256"], "scenario_evidence_digest_changed")
    return decode(raw)


def scenario_result(manifest, originals, proofs, rc):
    result = dict(version=1, run_id=manifest["run_id"], arm=manifest["arm"], source_commit=manifest["source_commit"],
                  original_set_digest=originals["original_set_digest"], proofs=proofs, rc=rc)
    schema_definition(result, "scenario")
    return result
