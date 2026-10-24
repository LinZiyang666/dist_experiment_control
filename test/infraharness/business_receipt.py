#!/usr/bin/env python3
"""Canonical business receipt encoder for independently collected observations.

The schema is derived from the actual Go consumer by receiptschema, rather
than a second handwritten ABI. This codec neither creates missing observations
nor certifies continuity: P30's independent oracles still judge every live row.
origin: S9 internal review INFRA-SUITE-SUPPLIES F6 / hot-update S9 plan §9.1
"""
import base64
import binascii
import json
import os
from pathlib import Path
import re
import tempfile

MAX_BYTES = 4 << 20


def unique(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("duplicate business receipt field")
        result[name] = value
    return result


def decode(raw):
    if not 0 < len(raw) <= MAX_BYTES:
        raise ValueError("bounded business receipt required")
    return json.loads(raw, object_pairs_hook=unique,
                      parse_constant=lambda _: reject("nonfinite business value"))


def reject(reason):
    raise ValueError(reason)


def ordered(shape, value):
    kind = shape["Kind"]
    if kind == "struct":
        fields = shape.get("Fields", [])
        names = [field["Name"] for field in fields]
        if len(set(names)) != len(names) or not isinstance(value, dict) or set(value) != set(names):
            raise ValueError("missing or unknown business receipt field")
        return {field["Name"]: ordered(field["Type"], value[field["Name"]]) for field in fields}
    if kind in ("slice", "array"):
        if value is None and kind == "slice":
            return None
        if not isinstance(value, list) or kind == "array" and len(value) != shape["Length"]:
            raise ValueError("invalid business receipt array")
        return [ordered(shape["Item"], item) for item in value]
    if kind in ("int", "uint"):
        bits = shape["Bits"]
        lower, upper = (0, 1 << bits) if kind == "uint" else (-(1 << (bits - 1)), 1 << (bits - 1))
        if type(value) is not int or not lower <= value < upper:
            raise ValueError("business integer outside Go range")
        return value
    if kind == "bool":
        if type(value) is not bool:
            raise ValueError("business boolean required")
        return value
    if kind == "string":
        if not isinstance(value, str) or any(0xD800 <= ord(c) <= 0xDFFF for c in value):
            raise ValueError("business UTF-8 string required")
        return value
    if kind == "bytes":
        if value is None:
            return None
        try:
            if not isinstance(value, str) or base64.b64encode(base64.b64decode(value, validate=True)).decode() != value:
                raise ValueError("noncanonical business byte string")
        except binascii.Error as error:
            raise ValueError("noncanonical business byte string") from error
        return value
    raise ValueError("unsupported business receipt schema")


def encode(schema, record):
    normalized = ordered(schema, record)
    # Match encoding/json's default HTML and JavaScript line separator escaping.
    # Python integers preserve the entire Go uint64 range without float coercion.
    text = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    for character, escaped in (("&", "\\u0026"), ("<", "\\u003c"), (">", "\\u003e"),
                               ("\u2028", "\\u2028"), ("\u2029", "\\u2029")):
        text = text.replace(character, escaped)
    raw = text.encode("utf-8")
    if len(raw) > MAX_BYTES:
        raise ValueError("business receipt exceeds custody bound")
    return raw


def publish(schema, record, supply, evidence):
    """Publish once, after the live owner has closed its independent readers.

    The caller supplies observations; no default/empty positive rows are added.
    A failed write leaves no apparently complete receipt for a later runLive.
    """
    if record["Version"] != 2 or record["Scenario"] not in ("mixed", "route-reaper", "auth-replay", "four-transfer"):
        raise ValueError("business receipt version/scenario mismatch")
    for name in ("SourceSHA256", "SchemaSHA256"):
        if record[name] != supply[name] or re.fullmatch(r"[0-9a-f]{64}", record[name]) is None:
            raise ValueError("business receipt lost source/schema custody")
    if not record["Required"] or not record["Hops"] or not record["Slots"]:
        raise ValueError("empty original business evidence")
    raw = encode(schema, record)
    root = Path(evidence)
    if not root.is_absolute() or root.resolve() != root or root.stat().st_uid != os.geteuid() or root.stat().st_mode & 0o077:
        raise ValueError("private business evidence directory required")
    # link publishes a fully synced inode atomically and refuses an existing
    # name. A stale receipt/symlink is never overwritten, even on a racing run.
    destination = root / "business-receipt.json"
    fd, temporary = tempfile.mkstemp(prefix=".business-receipt-", dir=root)
    pending, linked = Path(temporary), False
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(raw)
            output.flush()
            os.fsync(output.fileno())
        os.link(pending, destination, follow_symlinks=False)
        linked = True
        fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except BaseException:
        if linked:
            destination.unlink(missing_ok=True)
        raise
    finally:
        pending.unlink(missing_ok=True)
    return destination
