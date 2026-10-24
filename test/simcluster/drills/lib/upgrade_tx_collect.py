#!/usr/bin/env python3
"""S8 public observation reader and independent interval/completion/A-6 oracle.

Samples are supplied by read-only product/S7 ports, never manufactured from
counts or phase logs. No collector opens a business socket, repairs a journal,
replays a request or changes the sealed original roster. All clocks are mapped
to this host's BOOTTIME before entering the public schema. Frozen Stamp.At
remains Unix nanoseconds and uses separate mapped Unix observation bounds. SS material remains
in the S7 keeper's sealed custody; only its public identity/decision proofs pass.
"""
import argparse
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import re
import shutil
import sys
import time

HERE = Path(__file__).resolve().parent
# Explicit file loading keeps the hermetic -I entry point independent of PATH
# and sys.path. Shared module identity also preserves the single error type.
# origin: hot-update S8 plan §11.1 / S7 plan §11.4 / T51
for name in ("upgrade_tx_evidence", "upgrade_tx_companion"):
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, HERE / (name + ".py"))
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)

from upgrade_tx_evidence import (
    DOMAINS, MAX_BYTES, PROFILE, VERDICTS, supervisor,
    EvidenceError, require, boottime_ns, unique_object, decode, read, digest, publish, schema_check, index, validate_originals, unknown_lane, schema_definition, arm_contract, evidence_file, scenario_result,
)
from upgrade_tx_companion import (
    mutation_tools, companion_selection, companion_command, companion_corpus, companion_sources, produce_companion, mutation_owner_binding, mutation_evidence, scenario_companion,
)

def original_identity(row):
    return {key: row[key] for key in ("id", "domain", "family", "origin_owner")} | {
        "ofd_identity": row["ofd"]["original_identity"], "reconnects": row["reconnects"],
        "file_identities": {item["role"]: item["proof"]["original_identity"] for item in row["files"]},
        "mount_identity": row["mount_identity"],
        "transfer_identity": {key: value for key, value in row["transfer"].items() if key not in {"durable_bytes", "known"}}}


def seal(sample, manifest_digest):
    schema_check(sample)
    validate_resources(sample)
    originals = sorted((original_identity(row) for row in sample["resources"]), key=lambda row: (row["domain"], row["id"]))
    body = dict(version=1, manifest_digest=manifest_digest, run_id=sample["run_id"],
                domains=sorted(sample["domains"]), resources=originals)
    return dict(**body, original_set_digest=digest(body))


def validate_safety(sample):
    # origin: hot-update S8 plan §8.1–§8.2. Unknown custody can sign only an
    # explicitly non-hot refusal. A fresh independent death/exec-trace proof,
    # not an absent PID or guessed zero, must rule out automatic reversal.
    proof = sample["safety_receipt"]
    require(proof["known"] and proof["complete"] and proof["original_owner_dead"]
            and not proof["current_owner_known"] and proof["reverse_execs"] == 0, "unknown_owner_safety_unproven", 1)
    require(all(proof[key] == sample[key] for key in ("nonce", "tx_id", "cut_id", "generation", "scope_digest"))
            and proof["owner"]["pid"] == sample["pid"] and proof["owner"]["start_ticks"] == sample["start_ticks"]
            and proof["owner"]["boot_id"] == sample["owner_boot_id"], "unknown_owner_safety_stale", 1)
    require(0 < proof["begin_ns"] <= proof["end_ns"] <= sample["observed_end_ns"]
            and proof["end_ns"] >= sample["observed_begin_ns"], "unknown_owner_safety_interval", 1)
    require(all(not sample[field]["known"] and not sample[field]["complete"]
                for field in ("control_receipt", "resources_receipt", "released_receipt")), "unknown_owner_claimed_completion", 1)
    if sample["phase"] == "abandoned":
        # origin: hot-update S8 plan §8.1 / P26b. Death alone cannot release
        # Current. Archive needs an independent, fresh all-custody census.
        archive = sample.get("archive_receipt")
        require(archive is not None, "archive_custody_evidence_missing", 4)
        schema_definition(archive, "archive_receipt")
        require(archive["known"] and archive["complete"] and archive["current_released"]
                and archive["remaining"] == [] and archive["phase"] == "abandoned" and archive["outcome"] == "unknown",
                "archive_custody_not_empty", 1)
        require(all(archive[key] == sample[key] for key in ("nonce", "tx_id", "cut_id", "generation", "scope_digest"))
                and archive["owner"] == proof["owner"] and archive["evidence_digest"] != proof["evidence_digest"]
                and sample["observed_begin_ns"] <= archive["at_ns"] <= sample["observed_end_ns"], "archive_custody_stale", 1)
        require(set(archive["resource_ids"]) == {row["id"] for row in sample["resources"]}
                and len(archive["resource_ids"]) == len(sample["resources"]), "archive_custody_roster_missing", 1)


def cold_boundary(root, manifest, sample, originals):
    # origin: hot-update S8 plan §5.2 / §8.1 / P26b. H deliberately crosses
    # an ordinary cold boot AFTER a proved current reverse and drained cohort.
    # Keep dead originals as tombstones; a new connection never inherits their
    # identity. The exemption starts at this exact observed boundary, not at H.
    path = Path(root) / "cold-boot.json"
    if not path.exists():
        return set()
    boundary = read(path)
    schema_definition(boundary, "cold_boundary")
    require(manifest["arm"] in {"H-root", "H-user", "H-no-systemd", "H-colocated", "H-sibling", "H-readonly"}
            and not manifest["expect"]["a6"], "cold_boundary_not_declared")
    require(all(boundary[key] == manifest[key] for key in ("run_id", "arm", "source_commit", "artifact_sha"))
            and boundary["original_set_digest"] == originals["original_set_digest"], "cold_boundary_foreign")
    require(boundary["end_ns"] > boundary["begin_ns"] > 0, "cold_boundary_interval_invalid")
    if sample["observed_end_ns"] < boundary["begin_ns"]:
        return set()
    require(sample["observed_begin_ns"] >= boundary["end_ns"], "cold_boundary_observation_overlaps", 4)
    before = read(Path(root) / "samples" / (boundary["before_nonce"] + ".json"))
    require(before["direction"] == "reverse" and before["phase"] == "rolled_back"
            and before["observed_end_ns"] < boundary["begin_ns"]
            and before["run_id"] == sample["run_id"] and before["original_set_digest"] == originals["original_set_digest"],
            "cold_boundary_reverse_missing", 4)
    for field, kind in (("control_receipt", "control_ready"), ("resources_receipt", "resources_migrated"),
                        ("released_receipt", "old_generation_released")):
        validate_receipt(before, field, kind)
    require(boundary["selected_sha"] == before["running_sha"] == before["effective_sha"]
            and boundary["owner_before"]["pid"] == before["pid"]
            and boundary["owner_before"]["start_ticks"] == before["start_ticks"]
            and boundary["owner_before"]["boot_id"] == before["owner_boot_id"]
            and any(boundary["owner_after"][key] != boundary["owner_before"][key] for key in ("boot_id", "pid", "start_ticks"))
            and boundary["owner_after"]["pid"] == sample["pid"]
            and boundary["owner_after"]["start_ticks"] == sample["start_ticks"]
            and boundary["owner_after"]["boot_id"] == sample["owner_boot_id"], "cold_boundary_owner_or_selector_changed", 1)
    closed = [evidence_file(root, ref) for ref in boundary["drains"]]
    require(len(closed) == 2, "cold_boundary_drains_missing")
    for row in closed:
        validate_drain(row, originals)
    require(before["observed_end_ns"] <= closed[0]["at_ns"] < closed[1]["at_ns"] < boundary["begin_ns"]
            and closed[0]["nonce"] != closed[1]["nonce"] and closed[1]["at_ns"] - closed[0]["at_ns"] >= 2_000_000_000
            and closed[0]["terminals"] == closed[1]["terminals"] and closed[0]["history"] == closed[1]["history"],
            "cold_boundary_cohort_not_settled", 1)
    retired = {(row["domain"], row["id"]) for row in originals["resources"]}
    require(set(boundary["retired_ids"]) == {key[1] for key in retired}
            and len(boundary["retired_ids"]) == len(retired), "cold_boundary_original_missing", 1)
    current = index(sample["resources"])
    require(retired <= set(current) and all(not current[key]["known"] and current[key]["custody"] == "retired"
                                          and current[key]["state"] == "terminated" for key in retired),
            "cold_boundary_original_recreated", 3)
    return retired


def validate_resources(sample, allow_unknown=False, retired=frozenset()):
    # origin: hot-update S8 plan §9.0. A count/FD number cannot replace the
    # original source/tmp/parent OFD and the product keeper's closed census.
    unknown = allow_unknown and sample["phase"] in {"recovery_required", "abandoned"} and sample["outcome"] == "unknown"
    if unknown:
        validate_safety(sample)
    else:
        require(sample["known"] and sample["complete"], "observation_unknown")
        require(sample["admission_complete"], "admission_observer_unavailable")
    require(len(set(sample["domains"])) == len(sample["domains"]), "duplicate_domain", 1)
    rows = index(sample["resources"])
    require({domain for domain, _rid in rows} == set(sample["domains"]), "domain_census_incomplete", 1)
    for key, row in rows.items():
        if key in retired:
            continue  # Independently drained before the explicit H cold boot.
        if unknown and not row["known"]:
            continue  # Metadata retains the original ID; no continuity is claimed.
        require(row["known"] and row["current_owner"]["pid"] > 0 and row["origin_owner"]["start_ticks"] > 0,
                "resource_identity_unknown")
        ofd = row["ofd"]
        require(ofd["known"], "ofd_unknown")
        if row["domain"] in {"exec", "pty", "flow", "ss", "transfer_worker"}:
            require(ofd["method"] in {"kcmp", "kernel_cookie", "verified_native"}, "kernel_ofd_proof_missing")
            require(ofd["same"] and ofd["identity"] == ofd["original_identity"], "original_ofd_reopened", 3)
        files = row["files"]
        if row["family"] in {"transfer", "remote_fs"}:
            require({item["role"] for item in files} == {"source", "tmp", "parent"} and len(files) == 3,
                    "source_tmp_parent_ofd_missing")
        for item in files:
            proof = item["proof"]
            require(proof["known"] and proof["same"] and proof["method"] in {"kcmp", "kernel_cookie", "verified_native"}
                    and proof["identity"] == proof["original_identity"], "source_tmp_parent_ofd_reopened", 3)
        transfer = row["transfer"]
        if row["family"] == "transfer":
            require(transfer["applicable"] and transfer["known"] and transfer["tier"] == "B"
                    and transfer["direction"] in {"push", "pull"} and transfer["size_bytes"] > 0
                    and 0 < transfer["original_start_ns"] < transfer["budget_deadline_ns"]
                    and transfer["durable_bytes"] <= transfer["size_bytes"], "original_tier_b_transfer_unknown")
        else:
            require(not transfer["applicable"], "unexpected_transfer_identity", 1)


def validate_transfer_budget(sample, budget):
    # origin: hot-update S8 plan §10. Check actual starts and product rate/
    # budget seals, rather than trusting a size formula in fixture metadata.
    transfers = [row["transfer"] for row in sample["resources"] if row["family"] == "transfer"]
    require(len(transfers) == 4 and len({row["business_id"] for row in transfers}) == 4
            and {row["direction"] for row in transfers} == {"push", "pull"}, "four_original_transfer_identities_missing")
    remaining_ns = (budget["cut_budget_s"] + 2 * budget["drain_budget_s"] + budget["margin_s"]) * 1_000_000_000
    for row in transfers:
        require(row["size_bytes"] >= budget["size_bytes"] and row["rate_bytes_s"] == budget["rate_bytes_s"]
                and row["budget_deadline_ns"] - sample["observed_end_ns"] >= remaining_ns,
                "actual_original_transfer_budget_too_short")


def validate_sample(sample, originals, nonce, independent_admissions=None, allow_unknown=False, retired=frozenset()):
    schema_check(sample)
    validate_originals(originals)
    validate_resources(sample, allow_unknown, retired)
    require(sample["nonce"] == nonce, "stale_observation_nonce", 1)
    require(sample["observed_begin_ns"] <= sample["observed_end_ns"]
            and sample["observed_begin_unix_ns"] <= sample["observed_end_unix_ns"], "observation_interval_invalid", 1)
    require(sample["run_id"] == originals["run_id"] and sample["original_set_digest"] == originals["original_set_digest"],
            "original_set_digest_changed", 1)
    # origin: hot-update S8 plan §9.0 / M20. Compare to the sealed arm-start
    # roster, never to a newly calculated digest of the surviving subset.
    current, initial = index(sample["resources"]), index(originals["resources"])
    require(set(initial) <= set(current), "original_resource_missing", 1)
    require(set(originals["domains"]) <= set(sample["domains"]), "original_domain_missing", 1)
    for key, original in initial.items():
        require(original_identity(current[key]) == original, "original_identity_or_reconnect_changed", 3)
    admissions = index(sample["admissions"])
    require(set(admissions) <= set(current), "new_admission_swallowed", 1)
    require(set(current) - set(initial) <= set(admissions), "new_resource_without_admission", 1)
    if independent_admissions is not None:
        require(index(independent_admissions) == admissions, "independent_admission_census_mismatch", 1)
    for key, admission in admissions.items():
        resource = current[key]
        require(admission["generation"] == resource["generation"] and admission["owner"] == resource["current_owner"],
                "admission_owner_mismatch", 1)


def validate_receipt(sample, field, kind, retired=frozenset()):
    receipt = sample[field]
    require(receipt["known"] and receipt["complete"], "receipt_unknown_" + kind, 1)
    stamp, census = receipt["stamp"], receipt["census"]
    handle = stamp["attempt"]
    # origin: hot-update S8 plan §4.2 / M07. Receipt kind and cut/generation
    # binding are independent; a reverse cannot recycle a forward proof.
    require(receipt["kind"] == kind, "receipt_kind_mismatch_" + kind, 1)
    require(all(handle[name] == sample[name] for name in ("tx_id", "cut_id", "generation")),
            "receipt_attempt_mismatch_" + kind, 1)
    require(stamp["nonce"] == sample["nonce"] and stamp["scope_digest"] == sample["scope_digest"],
            "receipt_freshness_mismatch_" + kind, 1)
    require(stamp["sequence"] == sample["sequence"] and stamp["producer"]["pid"] == sample["pid"]
            and stamp["producer"]["start_ticks"] == sample["start_ticks"]
            and stamp["producer"]["boot_id"] == sample["owner_boot_id"], "receipt_producer_mismatch_" + kind, 1)
    require(sample["observed_begin_unix_ns"] <= stamp["at"] <= sample["observed_end_unix_ns"],
            "receipt_timestamp_stale_" + kind, 1)
    require(receipt["running_sha"] == sample["running_sha"] == sample["target_sha"],
            "receipt_running_image_mismatch_" + kind, 3)
    require(census["known"] and census["complete"] and census["stamp"] == stamp
            and census["expected_digest"] == sample["original_set_digest"], "receipt_census_unknown_" + kind, 1)
    expected = index(sample["resources"])
    expected = {key: value for key, value in expected.items() if key not in retired}
    require(expected, "receipt_live_census_empty", 1)
    actual = index(census["resources"])
    require(set(expected) == set(actual), "receipt_resource_census_mismatch_" + kind, 1)
    for key, row in expected.items():
        require(actual[key]["owner"] == row["current_owner"] and actual[key]["generation"] == row["generation"],
                "receipt_resource_owner_mismatch_" + kind, 1)
    require(census["admitting"] == 0, "completion_admission_not_sealed_" + kind, 1)
    if kind == "old_generation_released":
        require(census["old_owners"] == [], "old_owner_not_released", 3)


def validate_hit(sample, case):
    hit = sample["hit"]
    require(hit["known"], "fault_not_hit", 4)
    target = case["target"]
    # A candidate can have another PID, and a later current reverse has another
    # CutID. The immutable injection target, not the latest reporter, binds hit.
    require(hit["cut_id"] == target["cut_id"] and hit["owner"] == target["owner"]
            and hit["resource_id"] == target["resource_id"] and hit["end_ns"] <= sample["observed_end_ns"],
            "fault_owner_or_cut_mismatch", 1)
    # origin: hot-update S8 plan §8.2 / M19. Both the real syscall interval and
    # the injected environment interval must contain the hit in this exact cut.
    require(0 < case["armed_ns"] <= hit["begin_ns"] <= hit["end_ns"] <= case["released_ns"]
            and 0 < hit["window_begin_ns"] <= hit["begin_ns"] <= hit["end_ns"] <= hit["window_end_ns"],
            "fault_hit_outside_interval", 1)
    require(sample["hit_begin_ns"] == hit["begin_ns"] and sample["hit_end_ns"] == hit["end_ns"]
            and sample["syscall"] == hit["syscall"] and sample["errno"] == hit["errno"], "fault_hit_projection_mismatch", 1)
    require(hit["mechanism"] == case["mechanism"] and hit["syscall"] == case["syscall"]
            and hit["errno"] == case["errno"], "fault_mechanism_not_observed", 4)
    require(hit["resource_id"] in {row["id"] for row in sample["resources"]}, "fault_resource_not_in_scope", 1)


def validate_a6(sample, originals, observations=None):
    # origin: hot-update S8 plan §10 / A-6. These frozen values are checked
    # against upgradebudget by P32; failed/slow samples remain in the denominator.
    counts = {family: sum(row["family"] == family for row in originals["resources"]) for family in PROFILE}
    require(counts == PROFILE, "a6_profile_incomplete")
    metrics = sample["metrics"]
    require(metrics["node_replay_bytes"] <= 224 * 1024 * 1024, "a6_replay_headroom_exceeded", 3)
    freezes = metrics["freezes"]
    require(len(freezes) >= 3 and len({row["cut_id"] for row in freezes}) == len(freezes), "a6_three_cuts_missing", 4)
    for row in freezes:
        require(set(row["domains"]) == set(originals["domains"]), "a6_freeze_domain_missing", 1)
        require(0 < row["last_pump_ready_ns"] - row["earliest_fence_ns"] <= 500_000_000,
                "a6_full_freeze_exceeded", 3)
        require(row["source_sha"] != row["target_sha"], "a6_cut_did_not_change_running_image", 3)
        if observations is not None:
            # origin: hot-update S8 plan §10. Measured durations alone do not
            # prove that three actual image cuts completed on these owners.
            matches = [item for item in observations if item["cut_id"] == row["cut_id"]
                       and item["tx_id"] == row["tx_id"] and item["generation"] == row["generation"]
                       and item["source_sha"] == row["source_sha"] and item["target_sha"] == row["target_sha"]
                       and item["pid"] == row["owner"]["pid"] and item["start_ticks"] == row["owner"]["start_ticks"]
                       and item["owner_boot_id"] == row["owner"]["boot_id"]
                       and item["phase"] in {"completed", "rolled_back"}]
            require(matches, "a6_measured_cut_has_no_completed_observation", 4)
            for field, kind in (("control_receipt", "control_ready"), ("resources_receipt", "resources_migrated"),
                                ("released_receipt", "old_generation_released")):
                validate_receipt(matches[-1], field, kind)
    calibrations = metrics["calibrations"]
    require({(row["round"], row["load"]) for row in calibrations} == {(round, load) for round in (1, 2) for load in ("idle", "full")}
            and len(calibrations) == 4, "a6_calibration_rounds_missing", 4)
    for row in calibrations:
        require(row["end_ns"] - row["start_ns"] >= 300_000_000_000
                and row["end_ns"] < min(cut["earliest_fence_ns"] for cut in freezes), "a6_calibration_not_precut", 4)
    start, end = metrics["schedule_start_ns"], metrics["schedule_end_ns"]
    require(end > start, "a6_schedule_empty", 4)
    expected = (end - start + 99_999_999) // 100_000_000
    requests = metrics["requests"]
    require(metrics["expected_slots"] == expected == len(requests)
            and {row["slot"] for row in requests} == set(range(expected)), "a6_scheduled_slot_missing", 1)
    require(len({row["call_id"] for row in requests}) == expected
            and {row["kind"] for row in requests} == {"read", "write", "shortcmd"}, "a6_request_identity_missing", 1)
    for row in requests:
        require(row["scheduled_ns"] == start + row["slot"] * 100_000_000
                and row["scheduled_ns"] <= row["run_e_ns"] <= row["result_ns"], "a6_request_clock_or_slot_changed", 1)
        require(row["native_rc"] == 0 and row["result_ns"] - row["run_e_ns"] <= 5_000_000_000,
                "a6_request_latency_or_result_failed", 3)
        require(row["durable_effects"] == (1 if row["kind"] in {"write", "shortcmd"} else 0), "a6_duplicate_or_missing_effect", 3)
    require(start <= min(row["earliest_fence_ns"] for row in freezes)
            and max(row["last_pump_ready_ns"] for row in freezes) < end, "a6_load_does_not_span_cuts", 4)
    for field, families, budget in (("flow_progress", {"ssh", "tcp", "ss"}, 5_000_000_000),
                                     ("transfer_progress", {"transfer"}, 10_000_000_000)):
        wanted = {row["id"] for row in originals["resources"] if row["family"] in families}
        rows = metrics[field]
        require({row["resource_id"] for row in rows} == wanted, "a6_original_progress_missing", 1)
        for resource_id in wanted:
            points = sorted((row for row in rows if row["resource_id"] == resource_id), key=lambda row: row["at_ns"])
            require(len(points) >= 2 and start <= points[0]["at_ns"] <= start + budget
                    and end - budget <= points[-1]["at_ns"] <= end, "a6_progress_live_window_expired", 4)
            for previous, current in zip(points, points[1:]):
                require(0 < current["at_ns"] - previous["at_ns"] <= budget
                        and current["sequence"] > previous["sequence"]
                        and current["durable_bytes"] > previous["durable_bytes"], "a6_original_progress_stalled", 3)
            for cut in freezes:
                require(any(cut["last_pump_ready_ns"] < point["at_ns"] <= cut["last_pump_ready_ns"] + budget for point in points),
                        "a6_postcut_progress_missing", 4)


def validate_drain(row, originals, allow_unknown=False, settled=False):
    required = {"version", "nonce", "run_id", "original_set_digest", "at_ns", "cursor", "known", "complete",
                "joined", "writers_retired", "pending_gc", "late_results", "terminals", "history"}
    require(set(row) == required and type(row["version"]) is int and row["version"] == 1
            and isinstance(row["nonce"], str) and re.fullmatch("[0-9a-f]{32}", row["nonce"]), "drain_schema_invalid")
    require(row["known"] is True and row["complete"] is True
            and ((allow_unknown and not settled) or (row["joined"] is True and row["writers_retired"] is True)), "drain_ack_or_join_missing")
    require(row["run_id"] == originals["run_id"] and row["original_set_digest"] == originals["original_set_digest"], "drain_scope_mismatch", 1)
    require(type(row["at_ns"]) is int and row["at_ns"] > 0 and type(row["cursor"]) is int and row["cursor"] >= 0,
            "drain_clock_or_cursor_invalid")
    if not allow_unknown or settled:
        require(row["pending_gc"] == [] and row["late_results"] == [], "drain_unsettled_responsibility", 3)
    wanted = {item["id"] for item in originals["resources"] if item["family"] in {"exec", "pty", "transfer", "remote_fs"}}
    require(isinstance(row["terminals"], list) and {item["id"] for item in row["terminals"]} == wanted
            and len(row["terminals"]) == len(wanted), "drain_terminal_inventory_missing", 1)
    for terminal in row["terminals"]:
        require(set(terminal) == {"id", "start_count", "terminal_count", "result_digest"}, "drain_terminal_schema")
        require(type(terminal["start_count"]) is int and type(terminal["terminal_count"]) is int
                and ((0 <= terminal["start_count"] <= 1 and 0 <= terminal["terminal_count"] <= 1) if allow_unknown
                     else terminal["start_count"] == terminal["terminal_count"] == 1), "drain_duplicate_or_missing_terminal", 3)
        require(re.fullmatch("[0-9a-f]{64}", terminal["result_digest"]), "drain_result_digest_missing")
    transfers = {item["id"] for item in originals["resources"] if item["family"] == "transfer"}
    require(isinstance(row["history"], list) and {item["id"] for item in row["history"]} == transfers
            and len(row["history"]) == len(transfers), "drain_history_inventory_missing", 1)
    for history in row["history"]:
        require(set(history) == {"id", "physical_records", "canonical_start", "canonical_terminal", "content_digest"}, "drain_history_schema")
        require(all(type(history[key]) is int for key in ("physical_records", "canonical_start", "canonical_terminal"))
                and ((0 <= history["canonical_start"] <= 1 and 0 <= history["canonical_terminal"] <= 1
                      and history["physical_records"] == history["canonical_start"] + history["canonical_terminal"]) if allow_unknown
                     else history["physical_records"] == 2 and history["canonical_start"] == history["canonical_terminal"] == 1),
                "drain_history_not_exact", 3)
        require(re.fullmatch("[0-9a-f]{64}", history["content_digest"]), "drain_history_digest_missing")


def judge_observations(root, manifest):
    root = Path(root)
    originals = read(root / "originals.json")
    validate_originals(originals)
    require(digest(manifest) == originals["manifest_digest"], "manifest_changed_after_start", 1)
    declared_ids = {resource_id for actor in manifest["actors"] for resource_id in actor["resource_ids"]}
    require(declared_ids <= {row["id"] for row in originals["resources"]}
            and set(originals["domains"]) == set(manifest["domains"]), "manifest_original_roster_changed", 1)
    samples = [read(path) for path in sorted((root / "samples").glob("*.json"))]
    require(samples, "fresh_observation_missing", 4)
    samples.sort(key=lambda row: row["sequence"])
    allow_unknown = unknown_lane(manifest)
    previous = None
    for sample in samples:
        retired = cold_boundary(root, manifest, sample, originals)
        admissions = read(root / "admissions" / (sample["nonce"] + ".json"))
        require(admissions["nonce"] == sample["nonce"] and admissions["run_id"] == sample["run_id"]
                and admissions["sequence"] == sample["sequence"]
                and ((allow_unknown and not sample["safety_receipt"]["current_owner_known"])
                     or (admissions["known"] is True and admissions["complete"] is True)), "independent_admission_evidence_stale", 1)
        validate_sample(sample, originals, sample["nonce"], admissions["resources"], allow_unknown, retired)
        for key in ("source_commit", "artifact_sha", "run_id", "arm", "role", "topology"):
            require(sample[key] == manifest[key], "observation_provenance_changed", 1)
        if previous:
            # A fresh read-only query need not admit a new resource. Nonce and
            # observation time prove freshness; the admission sequence may stay
            # equal while it must never regress.
            require(sample["sequence"] >= previous["sequence"] and sample["observed_begin_ns"] >= previous["observed_end_ns"],
                    "observation_replayed_or_reordered", 1)
            old, new = index(previous["resources"]), index(sample["resources"])
            for key, row in old.items():
                require(key in new and new[key]["sequence"] >= row["sequence"], "resource_progress_regressed", 3)
        previous = sample
    latest = samples[-1]
    expected = manifest["expect"]
    require(all(latest[key] == expected[key] for key in ("phase", "outcome", "native_rc")), "native_result_mismatch", 3)
    if expected["completion"]:
        require(latest["running_sha"] == latest["effective_sha"] == latest["target_sha"], "running_image_not_selected", 3)
        for field, kind in (("control_receipt", "control_ready"), ("resources_receipt", "resources_migrated"),
                            ("released_receipt", "old_generation_released")):
            validate_receipt(latest, field, kind, retired)
        require(len({latest[field]["evidence_digest"] for field in ("control_receipt", "resources_receipt", "released_receipt")}) == 3,
                "completion_receipts_not_independent", 1)
    if expected["hit_required"]:
        case = read(root / "faults" / (manifest["case_id"] + ".json"))
        require(case["released_ns"] > 0, "fault_not_released", 4)
        require(any(row["hit"]["known"] for row in samples), "fault_not_hit", 4)
        for sample in samples:
            if sample["hit"]["known"]:
                validate_hit(sample, case)
    if expected["a6"]:
        validate_transfer_budget(read(root / "start-observation.json"), manifest["transfer_budget"])
        validate_a6(latest, originals, samples)
    drains = read(root / "drains.json")
    require(isinstance(drains, list) and len(drains) == 2, "two_independent_drains_missing")
    for row in drains:
        validate_drain(row, originals, allow_unknown, settled=latest["phase"] == "abandoned")
    require(drains[0]["nonce"] != drains[1]["nonce"] and drains[1]["at_ns"] - drains[0]["at_ns"] >= 2_000_000_000,
            "drain_not_fresh_or_quiet", 1)
    require(drains[1]["cursor"] >= drains[0]["cursor"] and drains[0]["terminals"] == drains[1]["terminals"]
            and drains[0]["history"] == drains[1]["history"], "late_terminal_or_history_changed", 3)
    require(drains[0]["at_ns"] >= latest["observed_end_ns"], "drain_precedes_final_sample", 1)
    return dict(rc=0, verdict="GREEN", non_hot=expected["outcome"] == "unknown" or bool(retired), run_id=manifest["run_id"], original_set_digest=originals["original_set_digest"], samples=len(samples))


def judge(root, manifest):
    # origin: hot-update S8 plan §9.0 / P26b. Emit scenario only after the
    # required evidence is checked. Missing/foreign companions cannot acquire
    # a proof list copied from the arm table and masquerade as a real verdict.
    originals = read(Path(root) / "originals.json")
    validate_originals(originals)
    contract = arm_contract(manifest)
    companion = None
    if contract["family"] != "A":
        companion = scenario_companion(root, manifest, originals, contract)
    try:
        result = judge_observations(root, manifest)
    except EvidenceError as error:
        if companion is None or not companion["rc"] or error.rc not in {1, 3}:
            raise
        result = dict(rc=error.rc, verdict=VERDICTS[error.rc], reason=str(error),
                      non_hot=unknown_lane(manifest) or contract["family"] == "H",
                      run_id=manifest["run_id"], original_set_digest=originals["original_set_digest"])
    if companion is not None:
        # The Go mutant oracle may catch a defect outside the observation
        # oracle's scope. Preserve both native results using the runner's red
        # precedence; never turn the actual mutant red into a GREEN control.
        result["observation_rc"] = result["rc"]
        result["rc"] = next((rc for rc in (1, 2, 3, 4) if rc in (result["rc"], companion["rc"])), 0)
        result["verdict"] = VERDICTS[result["rc"]]
        proofs = companion["proofs"]
    else:
        require(manifest["expect"]["a6"] and manifest["expect"]["completion"], "scenario_a6_oracle_disabled")
        proofs = ["full_profile", "two_300s_calibrations", "three_real_cuts", "fixed_a6_limits"]
    if contract["family"] == "H":
        require(result["non_hot"], "scenario_cold_boundary_missing", 4)
    result["scenario"] = scenario_result(manifest, originals, proofs, result["rc"])
    schema_definition(result, "judgment")
    return result


def observed_command(root, command, payload):
    result = supervisor.call(root, dict(op="run", spec=command, input=payload))
    require(result["native_rc"] == 0, "observer_native_failure")
    return decode(result["stdout"])


def sample(root, case_id, nonce):
    root = Path(root)
    require(re.fullmatch("[0-9a-f]{32}", nonce), "query_nonce_invalid")
    manifest = read(root / "manifest.json")
    require(case_id == manifest["case_id"], "case_not_declared")
    originals = read(root / "originals.json")
    request = dict(version=1, op="sample", nonce=nonce, run_id=manifest["run_id"], case=case_id,
                   original_set_digest=originals["original_set_digest"])
    begin = boottime_ns()
    begin_unix = time.time_ns()
    row = observed_command(root, manifest["ports"]["observe"], request)
    admissions = observed_command(root, manifest["ports"]["admissions"], request)
    end = boottime_ns()
    end_unix = time.time_ns()
    require(set(admissions) == {"version", "nonce", "run_id", "sequence", "known", "complete", "resources"}
            and type(admissions["version"]) is int and admissions["version"] == 1
            and ((unknown_lane(manifest) and not row["safety_receipt"]["current_owner_known"])
                 or (admissions["known"] is True and admissions["complete"] is True)),
            "independent_admission_observer_unavailable")
    require(admissions["nonce"] == nonce and admissions["run_id"] == manifest["run_id"]
            and admissions["sequence"] == row["sequence"], "admission_observer_stale", 1)
    retired = cold_boundary(root, manifest, row, originals)
    validate_sample(row, originals, nonce, admissions["resources"], unknown_lane(manifest), retired)
    require(begin <= row["observed_begin_ns"] <= row["observed_end_ns"] <= end
            and begin_unix <= row["observed_begin_unix_ns"] <= row["observed_end_unix_ns"] <= end_unix, "sample_clock_not_fresh", 1)
    publish(root / "admissions" / (nonce + ".json"), admissions)
    publish(root / "samples" / (nonce + ".json"), row)
    return row


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("sample",))
    parser.add_argument("--case", required=True)
    parser.add_argument("--nonce", required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    row = sample(args.evidence, args.case, args.nonce)
    print(json.dumps(dict(rc=0, verdict="GREEN", nonce=row["nonce"], sequence=row["sequence"],
                          original_set_digest=row["original_set_digest"]), sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except EvidenceError as error:
        print(json.dumps(dict(rc=error.rc, verdict=VERDICTS[error.rc], reason=str(error))))
        sys.exit(error.rc)
    except (supervisor.SupervisionError, OSError, ValueError, KeyError, TypeError):
        print('{"rc":2,"verdict":"SETUP-RED","reason":"observation_unavailable"}')
        sys.exit(2)
