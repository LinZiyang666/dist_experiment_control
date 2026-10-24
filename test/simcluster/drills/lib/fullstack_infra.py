#!/usr/bin/env python3
"""fullstack_infra.py — read-only S10 infrastructure receipts; no service or client repair authority.

S9 records retain their original field spelling and every physical identity.
These protocol checks feed P19/P01; they cannot promote hermetic evidence into
an E1 deployment receipt. P22/P25 own real collector lifecycle exercises.
"""
from functools import lru_cache
import hashlib
import ipaddress
import json
from pathlib import Path
import re
from urllib.parse import urlsplit


class EvidenceError(ValueError):
    """Malformed or incomplete input cannot certify a cut."""


class PendingEvidence(EvidenceError):
    """A missing upstream observation stays pending rather than becoming zero."""


class ContinuityError(EvidenceError):
    """Complete observations contradict a required continuity invariant."""


# origin: hot-update S10 plan §5.1 / §5.4 / §10.2 P15.
# Exact S9 DTO closure: a projection must not quietly discard a newly added
# field. P00/S9 owns changes to these shapes; this adapter preserves native JSON.
SHAPES = {
    "Process": "BootID:text PID:i32 StartTicks:u64 InvocationID:text ExeDevice:u64 ExeInode:u64 ExeSHA256:digest Cgroup:text",
    "Socket": "NetNS:u64 Device:u64 Inode:u64 Cookie:u64 Local:text Remote:text Owner:Process",
    "Attachment": "Participant:id Generation:u64 ServerID:text CID:u64 URL:text Raw:Socket Ingress:Process HealthGeneration:u64",
    "Store": "Device:u64 Inode:u64 MountID:u64 LockDevice:u64 LockInode:u64",
    "Artifact": "Kind:u8 Version:text GOOS:text GOARCH:text ArchiveSHA256:digest BinarySHA256:digest CatalogSHA256:digest",
    "Plan": "Tx:id Step:u32 Domain:id Component:u8 Action:u8 Candidate:Artifact Previous:Artifact Before:Process ConfigSHA256:digest Store:Store PolicyRevision:u64",
    "Census": "Complete:bool Generation:u64 Processes:Process[] Sockets:Socket[] LiveBusiness:u32 OldOwnerPending:u32 Digest:digest",
    "Status": "Tx:id Step:u32 Phase:u8 Revision:u64 Code:text Detail:text Before:Process Current:Process Census:Census Proof:digest",
    "AssetSeal": "Revision:u64 Accounts:digest Assets:digest Participants:digest Complete:bool",
    "TransferSeal": "Version:u16 SourceSHA256:digest SchemaSHA256:digest InvocationSet:digest ReceiptSet:digest ConsumerSet:digest Complete:bool",
    "Observation": "Version:u16 Tx:id Step:u32 Kind:text BootID:text BootNS:i64 Subject:id Generation:u64 Evidence:digest",
    "HostReceipt": "Tx:id Sequence:u64 Revision:u64 Op:u8 Applied:bool RequestDigest:digest Journal:digest Current:Process Census:Census Code:text",
    "Checkpoint": "Cohort:id Assets:AssetSeal Transfers:TransferSeal Proof:digest",
}
LIMIT = 4 * 1024 * 1024
ARCHIVE_LIMIT = 256 * 1024 * 1024


def require(ok, message, error=EvidenceError):
    if not ok:
        raise error(message)


def fields(value, names):
    require(type(value) is dict and set(value) == set(names.split()), "missing or unknown receipt field")


def integer(value, minimum=0, maximum=(1 << 64) - 1):
    require(type(value) is int and minimum <= value <= maximum, "invalid receipt integer")


def text(value, nonempty=False):
    require(type(value) is str and (value or not nonempty), "invalid receipt text")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeError as exc:
        raise EvidenceError("invalid receipt UTF-8") from exc


def byte_array(value, size, nonzero=False):
    require(type(value) is list and len(value) == size, "invalid S9 identity width")
    for item in value:
        integer(item, 0, 255)
    require(not nonzero or any(value), "unknown S9 identity")


def digest(value):
    require(type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value) is not None
            and value != "0" * 64, "invalid evidence digest")


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON field")
        result[key] = value
    return result


def decode(raw):
    require(type(raw) is bytes and 0 < len(raw) <= LIMIT, "infra receipt byte ceiling")
    try:
        return json.loads(raw.decode("utf-8", errors="strict"), object_pairs_hook=_pairs,
                          parse_constant=lambda _: require(False, "nonfinite JSON value"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise EvidenceError("invalid infra receipt JSON") from exc


def s9(value, kind):
    if kind in SHAPES:
        spec = [row.split(":") for row in SHAPES[kind].split()]
        fields(value, " ".join(name for name, _ in spec))
        return {name: s9(value[name], child) for name, child in spec}
    if kind.endswith("[]"):
        # Native S9 uses null for empty slices. Preserve that distinction in raw.
        require(value is None or type(value) is list, "invalid S9 collection")
        return None if value is None else [s9(item, kind[:-2]) for item in value]
    if kind in ("digest", "id"):
        byte_array(value, 32 if kind == "digest" else 16)
    elif kind == "text":
        text(value)
    elif kind == "bool":
        require(type(value) is bool, "invalid S9 boolean")
    else:
        bits = int(kind[1:])
        integer(value, -(1 << (bits - 1)) if kind[0] == "i" else 0,
                (1 << (bits - (kind[0] == "i"))) - 1)
    return value


def canonical_s9(value, kind):
    # Field order is S9's struct order. Sorting keys here would change its actual
    # census digest and let an adapter sign a different protocol than the owner.
    body = json.dumps(s9(value, kind), ensure_ascii=False, separators=(",", ":"))
    for char, escaped in (("<", r"\u003c"), (">", r"\u003e"), ("&", r"\u0026"),
                          ("\u2028", r"\u2028"), ("\u2029", r"\u2029")):
        body = body.replace(char, escaped)
    return body.encode()


def process(value):
    s9(value, "Process")
    require(value["BootID"] and value["PID"] > 0 and value["StartTicks"] > 0
            and re.fullmatch(r"[0-9a-f]{32}", value["InvocationID"]) is not None
            and value["ExeDevice"] > 0 and value["ExeInode"] > 0
            and any(value["ExeSHA256"]) and value["Cgroup"].startswith("/"), "unknown process incarnation")
    return value


def socket(value, connected=True):
    s9(value, "Socket")
    process(value["Owner"])
    for name in ("NetNS", "Device", "Inode", "Cookie"):
        integer(value[name], 1)
    for name in ("Local", "Remote"):
        if connected:
            endpoint(value[name])
        else:
            text(value[name], True)
    return value


def endpoint(value):
    text(value, True)
    try:
        if value.startswith("["):
            address, port = value[1:].split("]:")
        else:
            address, port = value.rsplit(":", 1)
        ip = ipaddress.ip_address(address)
        integer(int(port), 1, 65535)
        return (ip.ipv4_mapped if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped else ip, int(port))
    except (ValueError, TypeError) as exc:
        raise EvidenceError("invalid physical socket endpoint") from exc


def census(value):
    s9(value, "Census")
    require(value["Complete"] is True and value["Generation"] > 0 and value["Processes"],
            "pending complete kernel census", PendingEvidence)
    pids = []
    for row in value["Processes"]:
        process(row)
        pids.append(row["PID"])
    require(pids == sorted(set(pids)), "ambiguous process census")
    seen = set()
    for row in value["Sockets"] or []:
        socket(row, connected=False)
        require(row["Owner"] in value["Processes"], "socket owner outside census")
        key = (row["NetNS"], row["Cookie"], row["Owner"]["PID"])
        require(key not in seen, "duplicate accepted socket")
        seen.add(key)
    unsigned = dict(value, Digest=[0] * 32)
    expected = hashlib.sha256(canonical_s9(unsigned, "Census")).digest()
    require(bytes(value["Digest"]) == expected, "kernel census digest mismatch")
    return value


def ingress(raw, accepted, observed, expected, translation=None):
    socket(raw)
    socket(accepted)
    census(observed)
    process(expected)
    require(accepted["Owner"] == expected and expected in observed["Processes"]
            and accepted in (observed["Sockets"] or []), "accepted socket owner mismatch", ContinuityError)
    direct = (endpoint(raw["Local"]) == endpoint(accepted["Remote"])
              and endpoint(raw["Remote"]) == endpoint(accepted["Local"]))
    if translation is None:
        require(direct, "pending independent kernel NAT translation", PendingEvidence)
        return
    fields(translation, "netns original_local original_remote reply_local reply_remote route_generation rules_digest raw")
    raw_ref(translation["raw"])
    digest(translation["rules_digest"])
    integer(translation["route_generation"], 1)
    require(translation["netns"] == accepted["NetNS"] and type(translation["netns"]) is int
            and endpoint(translation["original_local"]) == endpoint(raw["Local"])
            and endpoint(translation["original_remote"]) == endpoint(raw["Remote"])
            and endpoint(translation["reply_local"]) == endpoint(accepted["Local"])
            and endpoint(translation["reply_remote"]) == endpoint(accepted["Remote"]),
            "kernel NAT translation does not bind this socket", ContinuityError)


# origin: hot-update S10 plan §5.4 / §8 M07N/M07C / §10.2 P15.
def physical(value):
    fields(value, "attachment accepted upstream server_accepted server_id cid census translation")
    a = s9(value["attachment"], "Attachment")
    byte_array(a["Participant"], 16, True)
    integer(a["Generation"], 1)
    integer(a["HealthGeneration"], 1)
    integer(a["CID"], 1)
    text(a["ServerID"], True)
    require(value["server_id"] == a["ServerID"] and type(value["cid"]) is int
            and value["cid"] == a["CID"], "NATS serverID/cid observation mismatch", ContinuityError)
    ingress(a["Raw"], value["accepted"], value["census"], a["Ingress"], value["translation"])
    u = parse_url(a["URL"])
    require(u.scheme in ("nats", "tls", "ws", "wss") and u.hostname and not u.username
            and not u.password and not u.query and not u.fragment, "invalid attachment URL")
    server = socket(value["server_accepted"])
    if u.scheme in ("ws", "wss"):
        require(value["upstream"] is not None, "pending Caddy to NATS socket", PendingEvidence)
        socket(value["upstream"])
        require(value["upstream"]["Owner"] == a["Ingress"], "upstream belongs to another Caddy", ContinuityError)
        ingress(value["upstream"], server, value["census"], server["Owner"])
    else:
        require(value["upstream"] is None and server == value["accepted"], "raw NATS connection path mismatch")
    return a


def raw_ref(value):
    fields(value, "path sha bytes schema")
    text(value["path"], True)
    text(value["schema"], True)
    digest(value["sha"])
    integer(value["bytes"], 1, LIMIT)


def parse_url(value):
    text(value, True)
    try:
        parsed = urlsplit(value)
        _ = parsed.port
        return parsed
    except ValueError as exc:
        raise EvidenceError("invalid receipt URL") from exc


def proof_probe(value, slot):
    fields(value, "id kind started_boot_ns finished_boot_ns attempts rc authenticated hinted url tls_name certificate_sha256 route_generation raw accepted census translation")
    fields(slot, "id kind scheduled_boot_ns deadline_boot_ns url")
    require(value["id"] == slot["id"] and value["kind"] == slot["kind"]
            and value["url"] == slot["url"], "canonical probe slot mismatch", ContinuityError)
    integer(slot["scheduled_boot_ns"], 1, (1 << 63) - 1)
    integer(slot["deadline_boot_ns"], slot["scheduled_boot_ns"], (1 << 63) - 1)
    integer(value["started_boot_ns"], slot["scheduled_boot_ns"], slot["deadline_boot_ns"])
    integer(value["finished_boot_ns"], value["started_boot_ns"], slot["deadline_boot_ns"])
    require(type(value["attempts"]) is int and value["attempts"] == 1
            and type(value["rc"]) is int and value["rc"] == 0,
            "canonical first attempt failed or retried", ContinuityError)
    require(type(value["hinted"]) is bool and type(value["authenticated"]) is bool, "invalid probe flags")
    integer(value["route_generation"], 1)
    socket(value["raw"])
    socket(value["accepted"])
    u = parse_url(value["url"])
    require(value["kind"] in ("tls", "wss", "ssh", "tcp"), "unknown probe kind")
    if value["kind"] in ("tls", "wss"):
        require(u.scheme == ("https" if value["kind"] == "tls" else "wss")
                and u.hostname and (u.port or 443) == 443 and not u.username and not u.password
                and not u.query and not u.fragment and value["tls_name"] == u.hostname,
                "fresh probe must use canonical 443", ContinuityError)
        digest(value["certificate_sha256"])
        # This destination check supplements the physical join. A pretty 443 URL
        # with a raw socket still dialled to 8443 is a distinct negative control.
        require(endpoint(value["raw"]["Remote"])[1] == 443, "probe socket bypassed canonical 443", ContinuityError)
    if value["kind"] == "wss":
        require(value["authenticated"] is True, "WSS was not authenticated", ContinuityError)
    ingress(value["raw"], value["accepted"], value["census"], value["accepted"]["Owner"], value["translation"])
    if value["translation"] is not None:
        require(value["translation"]["route_generation"] == value["route_generation"], "stale probe rule generation", ContinuityError)


def validate_h0(value, expected_urls, ready_boot_ns):
    fields(value, "schema initial_urls discovered_urls endpoints raw")
    require(value["schema"] == "fullstack-infra-h0-v1", "unknown H0 schema")
    raw_ref(value["raw"])
    require(type(value["initial_urls"]) is list and value["initial_urls"], "missing H0 initial routes", PendingEvidence)
    require(type(value["discovered_urls"]) is list, "missing discovered connect_urls", PendingEvidence)
    urls = value["initial_urls"] + value["discovered_urls"]
    require(all(type(u) is str for u in urls) and set(urls) == set(expected_urls), "H0 discovered route set mismatch")
    require(type(value["endpoints"]) is list, "missing H0 endpoint observations", PendingEvidence)
    seen = set()
    for row in value["endpoints"]:
        fields(row, "url trusted tls_name certificate_sha256 route_generation finished_boot_ns raw accepted census translation")
        require(row["url"] not in seen and row["url"] in urls, "duplicate or unexpected H0 route")
        seen.add(row["url"])
        u = parse_url(row["url"])
        require(u.scheme in ("tls", "wss") and u.hostname and not u.username and not u.password
                and not u.query and not u.fragment and row["tls_name"] == u.hostname
                and row["trusted"] is True, "untrusted H0 route", ContinuityError)
        digest(row["certificate_sha256"])
        integer(row["route_generation"], 1)
        integer(row["finished_boot_ns"], 1, ready_boot_ns - 1)
        ingress(row["raw"], row["accepted"], row["census"], row["accepted"]["Owner"], row["translation"])
    require(seen == set(urls), "pending discovered route trust/certificate/socket", PendingEvidence)


@lru_cache(maxsize=1)
def _p00_schema():
    path = Path(__file__).resolve().parents[3] / "hotupdate/acceptance/contract/schema.json"
    return json.loads(path.read_bytes(), object_pairs_hook=_pairs)["$defs"]


def p00(value, name):
    # Consume the frozen P00 schema. Do not maintain a permissive process/stamp
    # copy that could drop identity axes while the Go adapter still retains them.
    def check(item, spec):
        if "$ref" in spec:
            return check(item, _p00_schema()[spec["$ref"].split("/")[-1]])
        if "enum" in spec:
            require(item in spec["enum"], "unknown P00 vocabulary")
        kind = spec.get("type")
        if kind == "object":
            fields(item, " ".join(spec["properties"]))
            for key, child in item.items():
                check(child, spec["properties"][key])
        elif kind == "string":
            text(item, spec.get("minLength", 0) > 0)
            require("pattern" not in spec or re.fullmatch(spec["pattern"], item) is not None, "invalid P00 identity")
        elif kind == "integer":
            integer(item, spec.get("minimum", 0), spec.get("maximum", (1 << 64) - 1))
    check(value, _p00_schema()[name])
    return value


def _participant(value):
    byte_array(value, 16, True)
    return tuple(value)


def record(raw):
    value = decode(raw)
    fields(value, "schema tier stage stamp plan checkpoint bindings attachments physical status executing probes owners raw events signals outbound")
    require(value["schema"] == "fullstack-infra-v1" and value["tier"] in ("hermetic", "deploy")
            and value["stage"] in ("before", "outbound", "returned", "completed"), "unknown infra record vocabulary")
    for name, kind in (("plan", "Plan"), ("checkpoint", "Checkpoint"), ("status", "Status")):
        s9(value[name], kind)
    plan, cp, status = value["plan"], value["checkpoint"], value["status"]
    byte_array(plan["Tx"], 16, True)
    byte_array(plan["Domain"], 16, True)
    integer(plan["Step"], 1, (1 << 32) - 1)
    require(plan["Component"] in (1, 2) and plan["Action"] in (1, 2, 3, 4)
            and (plan["Action"] != 3 or plan["Component"] == 2), "unknown infra component/action")
    require(status["Phase"] in range(1, 12), "unknown S9 phase")
    require(status["Tx"] == plan["Tx"] and status["Step"] == plan["Step"]
            and status["Before"] == plan["Before"] and status["Revision"] > 0, "status belongs to another step")
    # Registry.Seal is a participant-only checkpoint. Its account/asset and S7
    # completion bits may be false; those owners retain their separate receipts.
    # Preserve the native bits instead of forging a full readiness seal here.
    require(cp["Assets"]["Revision"] > 0 and any(cp["Assets"]["Participants"])
            and any(cp["Cohort"]) and any(cp["Proof"]), "pending S9 cohort seal", PendingEvidence)
    census(status["Census"])
    process(value["executing"])
    require(value["executing"] == status["Current"], "disk/report SHA cannot replace executing image", ContinuityError)
    for name in ("bindings", "attachments", "physical", "probes", "owners", "raw", "events", "signals", "outbound"):
        require(type(value[name]) is list, "missing collector population", PendingEvidence)
    require(value["bindings"] and value["raw"], "pending original cohort or source raw", PendingEvidence)
    for ref in value["raw"]:
        raw_ref(ref)
    # Use the P00 stamp spelling without making a lossy alias for upstream S9.
    stamp = p00(value["stamp"], "stamp")
    require(stamp["process"]["boot_id"] == plan["Before"]["BootID"], "cross-host BOOTTIME cannot sign an infra window")
    fields(stamp, "process tx_id cut_id direction query_nonce scope_digest generation revision sequence at_boot_ns")
    require(stamp["tx_id"] == bytes(plan["Tx"]).hex() and stamp["direction"] == ("reverse" if plan["Action"] == 2 else "forward")
            and stamp["query_nonce"] and stamp["scope_digest"] and stamp["cut_id"], "wrong current stamp")
    for name in ("generation", "revision", "sequence", "at_boot_ns"):
        integer(stamp[name], 1, (1 << 63) - 1 if name == "at_boot_ns" else (1 << 64) - 1)
    require(stamp["revision"] == status["Revision"], "stamp status revision mismatch")
    attachments = {}
    for a in value["attachments"]:
        s9(a, "Attachment")
        key = _participant(a["Participant"])
        require(key not in attachments, "duplicate NC attachment")
        attachments[key] = a
    require(len(attachments) == len(value["attachments"]) and attachments, "duplicate or missing NC", PendingEvidence)
    observed = {}
    for row in value["physical"]:
        a = physical(row)
        key = _participant(a["Participant"])
        require(key not in observed and attachments.get(key) == a, "physical/NC set mismatch")
        observed[key] = row
    require(observed.keys() == attachments.keys(), "pending physical socket proof", PendingEvidence)
    bindings = set()
    for b in value["bindings"]:
        fields(b, "workload_id leg participant original")
        text(b["workload_id"], True)
        require(b["leg"] in ("flow", "sender", "receiver"), "unknown workload leg")
        key = (b["workload_id"], b["leg"])
        require(key not in bindings and _participant(b["participant"]) in observed, "missing or duplicate workload binding")
        bindings.add(key)
        p00(b["original"], "process")
    return value


# origin: hot-update S10 plan §5.4 / S9 L1/L2 / §10.2 P15.
def cut_order(before, after, topology):
    plan = before["plan"]
    events = after["events"]
    last = 0
    kinds = {"artifact_verified", "asset_sealed", "successor_installed", "attachment_changed", "auth_usable",
             "business_ready", "signal_applied", "old_exit", "storage_finalized", "startup_released",
             "return_verified", "owner_retired", "completed", "halted"}
    for event in events:
        s9(event, "Observation")
        require(event["Version"] == 1 and event["Tx"] == plan["Tx"] and event["Step"] == plan["Step"]
                and event["Subject"] == plan["Domain"] and event["Kind"] in kinds
                and event["BootID"] == plan["Before"]["BootID"] and any(event["Evidence"])
                and event["Generation"] > 0, "unknown or cross-step infra observation")
        integer(event["BootNS"], max(last, before["stamp"]["at_boot_ns"]), after["stamp"]["at_boot_ns"])
        last = event["BootNS"]
    required = {"return_verified", "completed"} if plan["Action"] == 3 else {"old_exit", "owner_retired", "return_verified", "completed"}
    require(required <= {event["Kind"] for event in events}, "pending S9 return/retirement observations", PendingEvidence)
    if plan["Action"] == 3:
        require(not after["outbound"], "reload has a binary migration ledger")
        return
    prior = {_participant(row["Participant"]): row for row in before["attachments"]}
    outbound = {}
    for row in after["outbound"]:
        a = physical(row)
        key = _participant(a["Participant"])
        require(key not in outbound and key in prior, "unexpected outbound NC")
        require(a["Generation"] > prior[key]["Generation"]
                and (a["ServerID"], a["CID"]) != (prior[key]["ServerID"], prior[key]["CID"])
                and (a["Raw"]["NetNS"], a["Raw"]["Cookie"]) != (prior[key]["Raw"]["NetNS"], prior[key]["Raw"]["Cookie"])
                and (a["Raw"]["NetNS"], a["Raw"]["Device"], a["Raw"]["Inode"]) !=
                    (prior[key]["Raw"]["NetNS"], prior[key]["Raw"]["Device"], prior[key]["Raw"]["Inode"]),
                "outbound URL did not move its socket", ContinuityError)
        outbound[key] = row
    require(outbound.keys() == prior.keys(), "pending complete outbound NC ledger", PendingEvidence)
    signals, sequence = [], 0
    for row in after["signals"]:
        fields(row, "receipt observed_boot_ns")
        receipt = s9(row["receipt"], "HostReceipt")
        require(receipt["Tx"] == plan["Tx"] and receipt["Op"] in (5, 6, 7) and receipt["Applied"] is True
                and receipt["Sequence"] > sequence and receipt["Revision"] > 0 and not receipt["Code"]
                and any(receipt["RequestDigest"]) and any(receipt["Journal"]), "unknown or duplicate signal receipt")
        sequence = receipt["Sequence"]
        integer(row["observed_boot_ns"], before["stamp"]["at_boot_ns"], after["stamp"]["at_boot_ns"])
        process(receipt["Current"])
        census(receipt["Census"])
        require(any(e["Kind"] == "signal_applied" and e["Evidence"] == receipt["RequestDigest"]
                    and e["BootNS"] == row["observed_boot_ns"] for e in events), "pending actual signal observation", PendingEvidence)
        signals.append(row)
    if plan["Component"] != 1:
        return
    ldm = [row for row in signals if row["receipt"]["Op"] == 5]
    if topology == "single":
        require(not ldm, "N1 must not use LDM", ContinuityError)
        return
    require(len(ldm) == 1 and ldm[0]["receipt"]["Current"] == plan["Before"], "pending exact old NATS LDM receipt", PendingEvidence)
    for row in outbound.values():
        require(not _same_birth(row["server_accepted"]["Owner"], plan["Before"]), "N3 outbound stayed on retiring NATS", ContinuityError)
        require(any(e["Kind"] == "attachment_changed" and e["Evidence"] == row["census"]["Digest"]
                    and e["BootNS"] < ldm[0]["observed_boot_ns"] for e in events),
                "N3 LDM preceded physical cohort migration", ContinuityError)


def _same_birth(a, b):
    return all(a[k] == b[k] for k in ("BootID", "PID", "StartTicks"))


# origin: hot-update S10 plan §5.4 / §10.2 P15 / WSS, URL, old-Caddy controls.
def validate_cut(before_raw, after_raw, required_bindings, required_owners, probe_slots, topology):
    before, after = record(before_raw), record(after_raw)
    require(before["stage"] == "before" and after["stage"] == "completed", "pending cut endpoints", PendingEvidence)
    require(topology in ("single", "quorum"), "unknown infra topology")
    plan = before["plan"]
    require(after["plan"] == plan and before["tier"] == after["tier"], "cross-plan/tier receipt substitution")
    a, b = before["stamp"], after["stamp"]
    require(all(a[k] == b[k] for k in ("tx_id", "cut_id", "direction", "scope_digest", "generation"))
            and a["query_nonce"] != b["query_nonce"] and b["sequence"] > a["sequence"]
            and b["revision"] > a["revision"] and b["at_boot_ns"] > a["at_boot_ns"], "replayed or stale cut stamp", ContinuityError)
    require(before["bindings"] == required_bindings == after["bindings"], "original workload binding changed", ContinuityError)
    families = {row["leg"] for row in required_bindings}
    require(families == {"flow", "sender", "receiver"}, "pending original flow and both JS legs", PendingEvidence)
    cut_order(before, after, topology)
    status = after["status"]
    require(status["Phase"] == 9 and not status["Code"] and any(status["Proof"])
            and status["Census"]["OldOwnerPending"] == 0, "infra did not complete retirement", ContinuityError)
    target = plan["Before"]
    if plan["Action"] == 3:
        require(after["executing"] == target, "Caddy reload changed process identity", ContinuityError)
    else:
        require(after["executing"]["ExeSHA256"] == plan["Candidate"]["BinarySHA256"]
                and not _same_birth(after["executing"], target), "binary cut kept old executing process", ContinuityError)
    for binding in required_bindings:
        participant = _participant(binding["participant"])
        old = next(row for row in before["physical"] if _participant(row["attachment"]["Participant"]) == participant)
        actual = old["attachment"]["Ingress"] if plan["Component"] == 2 else old["server_accepted"]["Owner"]
        require(actual == target, "original workload/JS leg did not traverse cut node", ContinuityError)
        new = next((row["attachment"] for row in after["physical"] if _participant(row["attachment"]["Participant"]) == participant), None)
        require(new is not None, "pending returned NC", PendingEvidence)
        returned_row = next(row for row in after["physical"] if row["attachment"] == new)
        serving = new["Ingress"] if plan["Component"] == 2 else returned_row["server_accepted"]["Owner"]
        require(serving == status["Current"], "returned cohort missed the current component", ContinuityError)
        prior = old["attachment"]
        # Both kernel incarnation and NATS session must change. URL labels, or
        # forwarded ACKs from an old cid, cannot certify the returned generation.
        if plan["Action"] == 3:
            require(new == prior, "reload changed an original NC", ContinuityError)
            continue
        require(new["Generation"] > prior["Generation"] and new["HealthGeneration"] >= prior["HealthGeneration"]
                and (new["ServerID"], new["CID"]) != (prior["ServerID"], prior["CID"])
                and (new["Raw"]["NetNS"], new["Raw"]["Cookie"]) != (prior["Raw"]["NetNS"], prior["Raw"]["Cookie"])
                and (new["Raw"]["NetNS"], new["Raw"]["Device"], new["Raw"]["Inode"]) != (prior["Raw"]["NetNS"], prior["Raw"]["Device"], prior["Raw"]["Inode"]),
                "URL changed without a physical NC move", ContinuityError)
    if plan["Action"] != 3:
        for row in after["outbound"]:
            outward = row["attachment"]
            returned = next(a for a in after["attachments"] if a["Participant"] == outward["Participant"])
            require(returned["Generation"] > outward["Generation"]
                    and (returned["ServerID"], returned["CID"]) != (outward["ServerID"], outward["CID"])
                    and (returned["Raw"]["NetNS"], returned["Raw"]["Cookie"]) != (outward["Raw"]["NetNS"], outward["Raw"]["Cookie"]),
                    "return kept the outbound temporary attachment", ContinuityError)
    require(type(required_owners) is list, "missing expected retiring owner roster")
    for row in required_owners:
        fields(row, "role process")
        process(row["process"])
    require(type(required_owners) is list and (plan["Action"] == 3 or
            any(row["process"] == target for row in required_owners)), "pending complete retiring owner roster", PendingEvidence)
    require(len({(row["role"], row["process"]["PID"], row["process"]["StartTicks"]) for row in required_owners})
            == len(required_owners), "duplicate retiring owner")
    require(len(after["owners"]) == len(required_owners), "pending retiring owner census", PendingEvidence)
    for expected, row in zip(required_owners, after["owners"]):
        fields(expected, "role process")
        fields(row, "role process exited source observed_boot_ns")
        require(expected["role"] in ("nats", "caddy", "temporary-caddy", "helper", "launcher")
                and row["role"] == expected["role"] and row["process"] == expected["process"], "wrong retiring owner")
        process(row["process"])
        integer(row["observed_boot_ns"], b["at_boot_ns"], (1 << 63) - 1)
        require(row["source"] == "held-pidfd" and row["exited"] is True, "old or temporary owner still alive", ContinuityError)
        require(not any(_same_birth(row["process"], p) for p in status["Census"]["Processes"]), "old owner remains in current census", ContinuityError)
    require(probe_slots and type(probe_slots) is list, "pending canonical probe schedule", PendingEvidence)
    expected = {slot["id"]: slot for slot in probe_slots}
    observed = {probe["id"]: probe for probe in after["probes"]}
    require(len(expected) == len(probe_slots) and len(observed) == len(after["probes"])
            and expected.keys() == observed.keys(), "pending canonical probe slot", PendingEvidence)
    require({slot["kind"] for slot in probe_slots} == {"tls", "wss", "ssh", "tcp"}, "pending canonical probe kind", PendingEvidence)
    for key, slot in expected.items():
        proof_probe(observed[key], slot)
    require(any(p["kind"] == "wss" and p["hinted"] is False for p in after["probes"]), "pending no-hint canonical WSS", PendingEvidence)
    require(all(a["at_boot_ns"] <= slot["scheduled_boot_ns"] < b["at_boot_ns"]
                for slot in probe_slots), "probe schedule missed the cut window", ContinuityError)
    return {"schema": "fullstack-infra-protocol-check-v1", "tier": before["tier"],
            "tx": bytes(plan["Tx"]).hex(), "step": plan["Step"], "physical_participants": len(before["physical"])}


class InfraCollector:
    """One bounded read-only archive; external P05 owns any source process.

    Inputs are immutable raw bytes. No command embedded in raw is interpreted.
    Close stops intake, Join returns its sticky failure; neither invents missing
    pidfd/canonical evidence. P19 still must run the actual evidence judge.
    """
    def __init__(self, root, tier):
        require(tier in ("hermetic", "deploy"), "unknown collector tier")
        self.root, self.tier = Path(root), tier
        self.closed, self.failure, self.refs = False, None, []
        self.total = 0

    def collect(self, raw):
        require(not self.closed, "collector is closed")
        if self.failure is not None:
            raise self.failure
        try:
            value = record(raw)
            require(value["tier"] == self.tier, "hermetic evidence cannot become deploy")
            require(self.total + len(raw) <= ARCHIVE_LIMIT, "infra archive ceiling")
            path = self.root / ("infra-%06d.json" % len(self.refs))
            with path.open("xb") as stream:
                stream.write(raw)
            ref = {"path": str(path), "sha": hashlib.sha256(raw).hexdigest(),
                   "bytes": len(raw), "schema": "fullstack-infra-v1"}
            self.refs.append(ref)
            self.total += len(raw)
            return ref
        except (EvidenceError, OSError) as exc:
            self.failure = self.failure or exc
            raise

    def close(self):
        self.closed = True

    def join(self):
        require(self.closed, "Close must precede Join")
        if self.failure is not None:
            raise self.failure
        return tuple(self.refs)
