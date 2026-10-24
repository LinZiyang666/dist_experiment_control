#!/usr/bin/env python3
"""On-disk collector mutations; restore each source before the next control.

origin: hot-update S5 plan §8 / T48. Run serially with source gates and Go
compilation. Receipts include the exact command, native rc, source digest,
mutation diff and unittest output. No product/container command is run.
"""
import argparse
import difflib
import hashlib
import json
from pathlib import Path
import subprocess
import sys

SIM = Path(__file__).resolve().parents[1]
REPO = SIM.parents[1]
LIB = SIM / "drills/lib"
CONTROL = SIM / "tests/flow-fixture-test.py"

MUTATIONS = [
    ("capacity-double-reverses-peer-tuple", "observation", "'\\x00'.join(pending)", "'\\x00'.join([pending[0],pending[2],pending[1]])", "CapacityPhaseTests.test_capacity_observation_requires_original_active_and_settled_owners"),
    ("open-prior-wrong-kernel-inode", "syn", "pending = [event for event in pending if event['values'][6] in syn]", "pending = pending", "OpenSynFaultTests.test_local_prior_requires_same_pending_inode_and_no_target_accept"),
    ("open-prior-already-accepted", "syn", "len(pending) != 1 or target['flows'].get('opening')", "len(pending) != 1", "OpenSynFaultTests.test_local_prior_requires_same_pending_inode_and_no_target_accept"),
    ("discard-delivered-terminal-marker", "peer", "if value.get('state') == 'rolled_back':", "if False:", "PeerTests.test_terminal_marker_is_retained_before_product_removes_it"),
    ("accept-unrelated-rollback-transaction", "fixture", "marker.get('upgrade_id') == pending['upgrade_id']", "True", "CollectorTests.test_watchdog_redispatch_does_not_prove_completed_rollback"),
    ("accept-old-rollback-generation", "fixture", "marker.get('generation', 0) > pending['generation']", "True", "CollectorTests.test_watchdog_redispatch_does_not_prove_completed_rollback"),
    ("use-later-open-census", "observation", "if local_prior.exists():", "if False:", "ObservationCustodyTests.test_local_open_prior_matches_native_sealed_event_before_release"),
    ("trust-unmatched-open-prior", "observation", "event['at'] == prior['at'] and", "True and", "ObservationCustodyTests.test_local_open_prior_matches_native_sealed_event_before_release"),
    ("observe-wrong-broker-owner", "observation", "broker_flow_domain(records, kernel, node)", "broker_flow_domain(records, kernel, 'brk1')", "ObservationCustodyTests.test_broker_receipt_follows_actual_allocation_owner"),
    ("read-reaper-from-idle-broker", "observation", "records.reaper(reaper_sources)", "records.reaper()", "ObservationCustodyTests.test_broker_receipt_follows_actual_allocation_owner"),
    ("watchdog-unpersisted-boot", "syn", "marker.get('boot_count', 0) < 1", "False", "OpenSynFaultTests.test_first_commit_fault_requires_actual_candidate_and_bounded_syn"),
    ("watchdog-root-process", "syn", "uid == 0 or binary.stat().st_uid != uid", "False", "OpenSynFaultTests.test_first_commit_fault_requires_actual_candidate_and_bounded_syn"),
    ("watchdog-wrong-running-image", "syn", "hashlib.file_digest(image, 'sha256').hexdigest() != marker['new_sha']", "False", "OpenSynFaultTests.test_first_commit_fault_requires_actual_candidate_and_bounded_syn"),
    ("watchdog-permissions-still-writable", "syn", "subprocess.call(['runuser', '-u', 'sim', '--', 'test', '-w', str(binary)]) != 1", "False", "OpenSynFaultTests.test_first_commit_fault_requires_actual_candidate_and_bounded_syn"),
    ("watchdog-expired-syn-budget", "syn", "time.monotonic() - started >= 2", "False", "OpenSynFaultTests.test_first_commit_fault_requires_actual_candidate_and_bounded_syn"),
    ("pending-open-copies-full-ledgers", "peer", "if request.get('accepts_only'):", "if False:", "PeerTests.test_open_accept_census_avoids_unrelated_byte_ledgers"),
    ("complete-artifact-before-open-prior", "gate", "while not (gate / 'release').exists():", "while False:", "ArtifactGateTests.test_artifact_withholds_completion_until_original_open_prior"),
    ("drop-final-artifact-byte", "gate", "destination.write(source.read(1))", "source.read(1)", "ArtifactGateTests.test_artifact_withholds_completion_until_original_open_prior"),
    ("hide-peer-socket-failure-class", "peer", "ConnectionRefusedError: 'connection_refused'", "ConnectionRefusedError: 'peer_observation_failed'", "PeerTests.test_peer_errors_keep_public_class_and_discard_private_detail"),
    ("publish-private-custody-on-failure", "peer", "dict(kind=e['kind'], at=e['at'], values=e['values'])", "dict(e)", "PeerTests.test_failed_observation_publishes_only_public_control_fields"),
    ("discard-short-flow-echo", "peer", "stream.sendall(expected)", "stream.sendall(bytes(len(expected)))", "PeerTests.test_short_flow_closes_both_directions_after_exact_echo"),
    ("reuse-preexisting-native-syn-cut", "syn", "records.events[baseline:]", "records.events", "OpenSynFaultTests.test_queued_original_syn_releases_only_at_a_new_native_cut"),
    ("sign-syn-release-without-original-packet", "syn", "if not pending:", "if False:", "OpenSynFaultTests.test_native_cut_without_original_syn_cannot_sign_fault_release"),
    ("repeat-barrier-for-tiny-transfer-pages", "observation", 'end=min(cursor+TRANSFER_FDS,len(records.frames))', 'end=min(cursor+16,len(records.frames))', "ObservationCustodyTests.test_keeper_transfer_pages_preserve_all_frames_with_bounded_round_trips"),
    ("truncate-bounded-transfer-rights", "observation", 'socket.CMSG_SPACE(TRANSFER_FDS * 4)', 'socket.CMSG_SPACE(16 * 4)', "ObservationCustodyTests.test_keeper_transfer_pages_preserve_all_frames_with_bounded_round_trips"),
    ("half-close-completion-follows-eof", "peer", 'if self.end is None:\n                    self.end = time.monotonic()', 'self.end = time.monotonic()', "PeerTests.test_half_close_keeps_tail_completion_before_original_eof"),
    ("prepare-recovery-fakes-accepted-permit", "tunnel", 'if err := exchangeResume(*attachment, req, deadline); err == nil {', 'if err := exchangeResume(*attachment, req, deadline); err == nil || err != nil {', "LogCustodyTests.test_product_messages_pin_exact_barrier_positions_and_public_fields"),
    ("eagerly-drain-finite-tails", "peer", 'time.sleep(delay)', 'pass', "PeerTests.test_finite_tail_pacing_keeps_original_bytes_and_bounded_gaps"),
    ("extend-finite-window-during-census", "peer", "end=getattr(self.peer,'observation_end',None) or now", 'end=now', "PeerTests.test_live_peer_keeps_original_socket_through_finish_and_never_redials"),
    ("release-cipher-capacity-in-whole-poll-frames", "peer", 'read_quantum=128 if spec.get("burst") else 0', 'read_quantum=1024 if spec.get("burst") else 0', "PeerTests.test_original_cipher_receiver_advances_in_small_tcp_quanta"),
    ("omit-original-receiver-pressure-window", "peer", 'time.sleep(3)', 'pass', "PeerTests.test_receiver_pressure_is_bounded_and_released_for_finite_drain"),
    ("retain-pressure-during-finite-drain", "peer", 'if self.spec.get("burst") and not self.peer.finish.is_set():', 'if self.spec.get("burst"):', "PeerTests.test_receiver_pressure_is_bounded_and_released_for_finite_drain"),
    ("retain-cipher-read-quantum-during-drain", "peer", 'self.crypto.read_quantum = 0', 'pass', "PeerTests.test_receiver_pressure_is_bounded_and_released_for_finite_drain"),
    ("flatten-checkpoint-product-criterion", "fixture", 'raise failure if failure is not None else SetupError("required_product_observation_unavailable")', 'raise SetupError("required_product_observation_unavailable")', "CollectorTests.test_checkpoint_retains_failed_product_criterion"),
    ("omit-restored-stderr-events", "fixture", 'if stream not in ("agent_slog", "agent_panic"):', 'if stream != "agent_slog":', "CollectorTests.test_restored_carrier_stderr_barrier_keeps_stable_event_identity"),
    ("omit-default-handler-message", "fixture", "r'(tunnel: flow carrier resumed|tunnel: flow handoff prepared)(?: |$)'", "r'(unreachable native message)(?: |$)'", "CollectorTests.test_restored_carrier_stderr_barrier_keeps_stable_event_identity"),
    ("shift-consumed-event-identities", "fixture", 'self.events.extend(rows[len(prior):])', 'self.events = rows', "CollectorTests.test_restored_carrier_stderr_barrier_keeps_stable_event_identity"),
    ("revoke-on-predecessor-reconnect", "fixture", 'valid = valid and record.get("mode") == 4 and record.get("owner_generation", 0) > 1', 'pass', "CollectorTests.test_revoke_resume_cannot_reuse_initial_business_binding"),
    ("omit-original-tcp-segment-contract", "peer", 'stream.setsockopt(socket.IPPROTO_TCP, socket.TCP_MAXSEG, 1200)', 'pass', "PeerTests.test_ss_tcp_backpressure_records_original_cipher_suffix"),
    ("flatten-public-expose-code", "fixture", 'boundary += "_"+code[1].decode().replace(":", "_")', 'pass', "CollectorTests.test_native_boundary_status_is_preserved"),
    ('peer-flattens-observer-criterion', 'peer', 'criterion=str(error), public_observations=', 'criterion="peer_observation_failed", public_observations=', 'PeerTests.test_peer_retains_failed_observer_criterion'),
    ("collector-flattens-observer-criterion", "fixture", 'raise SetupError("product_receipt_"+criterion)', 'raise SetupError("remote_observation_failed")', "CollectorTests.test_failed_product_receipt_keeps_public_criterion"),
    ("collector-reflects-private-error-text", "fixture", 'require(isinstance(criterion, str) and re.fullmatch(r"[a-z][a-z_0-9]*", criterion), "remote_observation_failed")', 'pass', "CollectorTests.test_failed_product_receipt_keeps_public_criterion"),
    ("log-cursors-serialize", "fixture", 'with ThreadPoolExecutor(max_workers=4) as workers:\n            tasks =', 'with ThreadPoolExecutor(max_workers=1) as workers:\n            tasks =', "CollectorTests.test_four_log_cursors_share_one_observation_window"),
    ("checkpoint-censuses-serialize", "fixture", 'with ThreadPoolExecutor(max_workers=4) as workers:\n            observations =', 'with ThreadPoolExecutor(max_workers=1) as workers:\n            observations =', "CollectorTests.test_checkpoint_censuses_share_one_observation_window"),
    ("omit-cipher-receiver-quantum", "peer", 'min(length-len(chunks), quantum) if quantum else length-len(chunks)', 'length-len(chunks)', "PeerTests.test_cipher_receiver_frees_tcp_capacity_in_declared_quanta"),
    ("omit-cipher-receiver-pressure", "peer", 'time.sleep(.001)', 'pass', "PeerTests.test_cipher_receiver_frees_tcp_capacity_in_declared_quanta"),
    ("cleanup-omits-remote-request-finish", "fixture", 'self.request_mark("finish")\n                    if self.request_job.wait(timeout=20) != 0:', 'pass\n                    if self.request_job.wait(timeout=20) != 0:', "CollectorTests.test_cleanup_reaps_requests_before_transport_shutdown"),
    ("cleanup-omits-request-reap", "fixture", 'if self.request_job.wait(timeout=20) != 0:', 'if False:', "CollectorTests.test_cleanup_reaps_requests_before_transport_shutdown"),
    ("partial-readiness-needs-only-one-original", "observation", 'partial_ready=bool(ready) and all(ready)', 'partial_ready=bool(ready) and any(ready)', "ObservationCustodyTests.test_partial_readiness_requires_every_original_and_both_writers"),
    ("partial-readiness-omits-client-write", "observation", "require(calls and crypto.partials, 'partial_cipher_prior_unavailable')", "require(calls, 'partial_cipher_prior_unavailable')", "ObservationCustodyTests.test_partial_readiness_requires_every_original_and_both_writers"),
    ("partial-readiness-omits-product-primitive", "observation", "payload_partial(records, calls, calls[0]['actor'])", "pass", "ObservationCustodyTests.test_partial_readiness_requires_every_original_and_both_writers"),
    ("omit-pending-raw-fd-observation", "flow", 'if drillobserve.Hook != nil {\n\t\t\tobserveRecordLocked(r)\n\t\t}', 'if drillobserve.Hook != nil {\n\t\t\t_ = r.raw\n\t\t}', "LogCustodyTests.test_product_messages_pin_exact_barrier_positions_and_public_fields"),
    ("open-syn-fault-uses-peer-nic", "fixture", 'open=dict(ip="127.0.0.1", port=open_port)', 'open=dict(ip=ip, port=open_port)', "CollectorTests.test_open_syn_fault_targets_actual_expose_loopback_address"),
    ("omit-finite-burst-frame-batching", "peer", '20 <= seq < 532 and seq % 4 != 3', 'False', "PeerTests.test_ss_burst_batches_declared_frames_and_keeps_small_finite_tail"),
    ("skip-baseline-partial-prior", "fixture", 'complete = prior.get("partial_ready") is True', 'complete = True', "CollectorTests.test_baseline_waits_for_original_client_and_product_partial_writes"),
    ("unbatch-client-cipher-writes", "peer", 'wire.extend(self.seal(struct.pack("!H", len(chunk))) + self.seal(chunk))', 'self.stream.sendall(self.seal(struct.pack("!H", len(chunk))) + self.seal(chunk))', "PeerTests.test_ss_tcp_backpressure_records_original_cipher_suffix"),
    ("single-packet-large-checkpoint-request", "peer", 'for offset in range(0, len(message), 32768):\n            stream.sendmsg([message[offset:offset+32768]], control if offset == 0 else [])', 'stream.sendmsg([message], control)', "PeerTests.test_peer_accepts_large_checkpoint_request_and_one_sealed_descriptor"),
    ("hide-native-client-short-write", "peer", 'self.stream.send(wire[cursor:], socket.MSG_DONTWAIT) if isinstance(self.stream, socket.socket) else self.stream.send(wire[cursor:])', 'self.stream.send(wire[cursor:])', "PeerTests.test_ss_backpressure_records_real_short_write_without_reseal"),
    ("native-cipher-wrong-counter", "probe", 'struct.pack("<I", 1)+nonce', 'struct.pack("<I", 0)+nonce', "FlowProbeTests.test_native_reference_chacha_matches_scalar_reference"),
    ("enable-before-complete-initial-keyset", "fixture", 'subscriptions = {}', 'self.transport.run("ctl", "proxy", "on", "--yes")\n        subscriptions = {}', "CollectorTests.test_initial_listener_is_enabled_after_complete_subscription_creation"),
    ("long-collector-bind-path", "fixture", 'server.bind(address)', 'server.bind(str(args.root / "fixture.sock"))', "CollectorTests.test_collector_socket_accepts_long_evidence_directory"),
    ("long-collector-connect-path", "fixture", 'connection.connect(address)', 'connection.connect(str(root / "fixture.sock"))', "CollectorTests.test_collector_socket_accepts_long_evidence_directory"),
    ("single-packet-large-ledger", "peer", 'for offset in range(0, len(response), 32768):\n                        connection.sendall(response[offset:offset+32768])', 'connection.sendall(response)', "PeerTests.test_peer_snapshot_fragments_large_ledger_without_losing_keeper"),
    ("swallow-native-command-status", "fixture", 'require(value.returncode in allowed, "transport_native_rc_"+str(value.returncode)+"_"+boundary)', 'require(True, "transport_native_rc_"+str(value.returncode)+"_"+boundary)', "CollectorTests.test_native_boundary_status_is_preserved"),
    ("skip-proxy-ready-observation", "fixture", 'status = self.transport.json("ctl", "proxy", "status", "--json")', 'return', "CollectorTests.test_subscription_supply_waits_for_actual_proxy_ready"),
    ("accept-disabled-proxy-ready", "fixture", 'status.get("enabled") is True and any(', 'any(', "CollectorTests.test_subscription_supply_waits_for_actual_proxy_ready"),
    ("accept-other-node-proxy-ready", "fixture", 'row.get("nid") == "agt1" and row.get("ready") is True', 'row.get("ready") is True', "CollectorTests.test_subscription_supply_waits_for_actual_proxy_ready"),
    ("omit-native-command-identity", "fixture", '"_"+boundary)', ')', "CollectorTests.test_native_boundary_status_is_preserved"),
    ("drop-remote-judge-native-status", "fixture", 'require(command == "judge" or result.get("rc", 0) == 0, "remote_observation_failed")\n        return result', 'require(command == "judge" or result.get("rc", 0) == 0, "remote_observation_failed")\n        return dict(result, rc=0)', "CollectorTests.test_remote_judge_native_failure_is_preserved"),
    ("accept-failed-peer-snapshot", "fixture", 'require(command == "judge" or result.get("rc", 0) == 0, "remote_observation_failed")', 'require(True, "remote_observation_failed")', "CollectorTests.test_remote_failed_snapshot_cannot_become_success"),
    ("substring-barrier-diagnostic", "fixture", 'valid = record["msg"] == EVENTS[event]', 'valid = EVENTS[event] in record["msg"]', "CollectorTests.test_barrier_is_exact_and_consumed_once"),
    ("reuse-consumed-barrier", "fixture", 'if (event, seq) in self.consumed:', 'if False:', "CollectorTests.test_barrier_is_exact_and_consumed_once"),
    ("omit-barrier-generations", "fixture", 'require(not valid or type(record.get("generation" if event == "final-cut" else "carrier_generation")) is int,', 'require(True,', "CollectorTests.test_barrier_requires_generation_fields"),
    ("reuse-old-carrier-restoration", "fixture", 'valid = seq >= minimum_seq', 'valid = True', "CollectorTests.test_carrier_rebuild_cannot_reuse_old_resume"),
    ("revoke-at-initial-business-resume", "fixture", 'valid = seq >= minimum_seq', 'valid = True', "CollectorTests.test_revoke_resume_cannot_reuse_initial_business_binding"),
    ("omit-independent-cut-observer-marks", "fixture", "marks = {node:self.remote_call(node,'mark') for node in ('ctl1','agt1')}", 'marks = {}', "CollectorTests.test_background_cut_observer_marks_real_barrier_and_records_read_failure"),
    ("ignore-cut-observer-read-failure", "fixture", "self.observer_errors.append('cut_observation_failed')", 'pass', "CollectorTests.test_background_cut_observer_marks_real_barrier_and_records_read_failure"),
    ("boot-without-persisted-count", "fixture", 'marker["boot_count"] > 0', 'marker["boot_count"] >= 0', "CollectorTests.test_candidate_boot_requires_running_image_and_persisted_count"),
    ("boot-with-wrong-running-sha", "fixture", 'observation["running_sha"] == marker["new_sha"]', 'True', "CollectorTests.test_candidate_boot_requires_running_image_and_persisted_count"),
    ('redispatch-is-rollback-success', 'fixture', "marker.get('state') == 'rolled_back'", 'True', 'CollectorTests.test_watchdog_redispatch_does_not_prove_completed_rollback'),
    ("accept-agent-pid-reuse", "fixture", 'require(observation["identity"] == before["identity"], "agent_identity_changed")', 'require(True, "agent_identity_changed")', "CollectorTests.test_checkpoint_rejects_pid_reuse"),
    ("accept-unchanged-hop-sha", "fixture", 'observation["running_sha"] != reference["agent"]["running_sha"]', 'True', "CollectorTests.test_hop_requires_new_running_sha_and_committed_marker"),
    ("accept-pending-hop-marker", "fixture", 'observation["marker"]["state"] == "committed"', 'True', "CollectorTests.test_hop_requires_new_running_sha_and_committed_marker"),
    ("legacy-refusal-can-replace-image", "fixture", 'observation["running_sha"] == before["running_sha"] and observation["marker"] == before["marker"]', 'True', "CollectorTests.test_refusal_retains_original_marker_and_image"),
    ("empty-sql-is-evidence", "fixture", 'isinstance(rows, list) and rows, "sql_allocation_receipt_empty"', 'isinstance(rows, list), "sql_allocation_receipt_empty"', "CollectorTests.test_empty_sql_is_missing_evidence"),
    ('subscription-token-in-argv', 'fixture', '"input", "brk1", "curl", "-fsS", "--max-time", "6", "--config", "-",\n                data=(\'url = "http://127.0.0.1:8090/sub/\'+token+\'"\\n\').encode()', '"exec", "brk1", "curl", "-fsS", "--max-time", "6", "http://127.0.0.1:8090/sub/"+token', 'CollectorTests.test_all_profiles_declare_finite_original_cohorts_before_start'),
    ("diagnostic-mints-legacy-exec", "fixture", 'row["msg"] == "agent: upgrade COMMITTED (registered as the new binary)"', 'True', "CollectorTests.test_legacy_exec_uses_durable_successor_commit_without_requiring_old_image_log"),
    ("omit-live-census-before-cleanup", "fixture", 'peer.publish(self.root / "cleanup-agent.json", self.kernel())', 'pass', "CollectorTests.test_cleanup_saves_live_observations_before_closing_original_owners"),
    ("overwrite-capture-on-second-cleanup", "fixture", 'if self.phase == "cleaned":', 'if False:', "CollectorTests.test_cleanup_saves_live_observations_before_closing_original_owners"),
    ("ignore-independent-aead-selftest", "fixture", 'require(control.returncode == 0, "independent_oracle_controls_failed")', 'require(True, "independent_oracle_controls_failed")', "CollectorTests.test_selftest_requires_both_independent_oracle_control_processes"),
    ("ignore-independent-request-selftest", "fixture", 'require(control.returncode == 0, "request_oracle_controls_failed")', 'require(True, "request_oracle_controls_failed")', "CollectorTests.test_selftest_requires_both_independent_oracle_control_processes"),
    ("discard-request-drain-native-status", "fixture", 'require(self.request_job.wait(timeout=20) == 0, "request_collector_native_failure")', 'require(True, "request_collector_native_failure")', "CollectorTests.test_finish_drains_both_endpoints_concurrently_and_reaps_requests"),
    ("accept-request-evidence-from-other-cohort", "fixture", 'require(metadata["nonce"] == self.nonce, "request_cohort_nonce_changed")', 'require(True, "request_cohort_nonce_changed")', "CollectorTests.test_request_receipt_rejects_incomplete_evidence_and_another_cohort"),
    ("ignore-request-side-effects", "fixture", 'mixed.accepted(summary, "upgrade", 10)', 'True', "CollectorTests.test_request_receipt_reuses_independent_judge_and_keeps_entire_schedule"),
    ("allow-over-budget-new-requests", "fixture", 'and summary["over_5s"] == 0', '', "CollectorTests.test_request_receipt_reuses_independent_judge_and_keeps_entire_schedule"),
    ("permit-in-window-request-refusals", "fixture", 'and summary["refusals"] == 0', '', "CollectorTests.test_request_receipt_reuses_independent_judge_and_keeps_entire_schedule"),
    ("empty-baseline-receive-ledger", "fixture", 'len(doc["flows"][name]["received"]) >= 20', 'len(doc["flows"][name]["received"]) >= 0', "CollectorTests.test_baseline_requires_twenty_independent_samples_in_every_direction"),
    ("infer-product-partial-from-syn-sent", "fixture", 'raise SetupError("product_partial_cipher_observer_unavailable")', 'pass', "CollectorTests.test_original_pending_open_never_synthesizes_partial_cipher_write"),
    ('silence-missing-product-priors', 'fixture', 'raise failure if failure is not None else SetupError("required_product_observation_unavailable")', 'pass', 'CollectorTests.test_unavailable_private_priors_are_explicit_setup_failures'),
    ("omit-observed-freeze-calibration", "fixture", 'values.append(int(match.group(3))/1000)', 'pass', "LogCustodyTests.test_freeze_uses_existing_activation_message_and_keeps_calibration_overage"),
    ("freeze-milliseconds-as-centiseconds", "fixture", 'int(match.group(3))/1000', 'int(match.group(3))/100', "LogCustodyTests.test_freeze_uses_existing_activation_message_and_keeps_calibration_overage"),
    ("incomplete-collection-is-native-success", "fixture", 'return dict(rc=2, verdict="SETUP-RED", criterion="required_product_observation_unavailable")', 'return dict(rc=0, verdict="GREEN")', "CollectorTests.test_four_log_collection_cannot_bless_rotated_or_unreadable_stream"),
    ("ignore-log-read-failure", "fixture", 'meta.get("read_ok") is True and type(meta.get("inode")) is int', 'type(meta.get("inode")) is int', "LogCustodyTests.test_log_identity_prefix_length_and_read_status_are_independent"),
    ("ignore-log-length", "fixture", 'and meta.get("length") == len(raw)', '', "LogCustodyTests.test_log_identity_prefix_length_and_read_status_are_independent"),
    ("ignore-log-inode", "fixture", 'require(before_meta["inode"] == after_meta["inode"], "log_rotated")', 'require(True, "log_rotated")', "LogCustodyTests.test_log_identity_prefix_length_and_read_status_are_independent"),
    ("ignore-log-prefix", "fixture", 'require(after.startswith(before), "log_prefix_lost")', 'require(True, "log_prefix_lost")', "LogCustodyTests.test_log_identity_prefix_length_and_read_status_are_independent"),
    ("ignore-component-identity", "fixture", '[(p["path"], p["dev"], p["inode"]) for p in new], "log_component_rotated")', '[(p["path"], p["dev"], p["inode"]) for p in old], "log_component_rotated")', "LogCustodyTests.test_components_and_journal_cursors_cannot_rotate"),
    ("ignore-journal-prefix", "fixture", 'and new[:len(old)] == old,', ',', "LogCustodyTests.test_components_and_journal_cursors_cannot_rotate"),
    ("skip-memfd-seals", "peer", 'fcntl.fcntl(fd, fcntl.F_ADD_SEALS, fcntl.F_SEAL_SEAL | fcntl.F_SEAL_SHRINK |\n                    fcntl.F_SEAL_GROW | fcntl.F_SEAL_WRITE)', 'pass', "PeerTests.test_sealed_custody_survives_judge_subprocess_and_rejects_plain_fd"),
    ("omit-private-target-from-sealed-supply", "peer", "+host+target+master)", "+host+master)", "PeerTests.test_private_target_and_master_share_sealed_supply_descriptor"),
    ("accept-incomplete-private-target", "peer", "if len(target) != 6:", "if False:", "PeerTests.test_private_target_and_master_share_sealed_supply_descriptor"),
    ("ss-target-in-json-control-bundle", "fixture", 'spec.update(key=key_name, host=key["host"], port=key["port"])', 'spec.update(key=key_name, host=key["host"], port=key["port"], target_host=ip, target_port=port)', "CollectorTests.test_all_profiles_declare_finite_original_cohorts_before_start"),
    ("omit-failed-seal-attempt", "peer", 'self.calls.append(dict(subkey_sha256=probe.digest(self.key), nonce=nonce.hex()))', 'pass', "PeerTests.test_client_records_actual_seals_before_write_failure"),
    ("reset-client-nonce", "peer", 'self.tx += 1', 'self.tx = 0', "PeerTests.test_client_records_actual_seals_before_write_failure"),
    ("omit-actual-partial-write-count", "peer", 'if 0 < n < attempt:', 'if False:', "PeerTests.test_partial_cipher_receipt_uses_the_actual_writer_count"),
    ('ciphertext-mints-product-seal-proof', 'peer', "if not hasattr(peer,'observations'):", "if not hasattr(peer,'observations'):\n            return dict(rc=0, verdict='GREEN')", 'PeerTests.test_common_judge_keeps_native_loss_and_refuses_client_only_crypto_proof'),
    ("allow-ssh-process-redial", "peer", 'if name in self.flows:\n            raise ValueError("application redial refused")', 'if False:\n            raise ValueError("application redial refused")', "PeerTests.test_ssh_process_boundary_keeps_one_process_socket_and_channel"),
    ("retry-ssh-password", "peer", "'NumberOfPasswordPrompts=1'", "'NumberOfPasswordPrompts=2'", "PeerTests.test_ssh_ancestor_walk_stops_at_pid_one"),
    ("omit-ssh-remote-channel", "peer", "'stdio','--root',remote", "'kernel','--root',remote", "PeerTests.test_ssh_ancestor_walk_stops_at_pid_one"),
    ("omit-explicit-adapter-session", "transport", 'upgradecfg_allow_agent agt1 flow_fixture_transport_online lab', 'upgradecfg_allow_agent agt1 flow_fixture_transport_online', "TransportShellTests.test_deploy_adapter_uses_only_fake_processes_and_preserves_native_status"),
    ("run-client-peer-as-root", "transport", 'dexec -u sim "$_fft_node" -- "$@"', 'dexec "$_fft_node" -- "$@"', "TransportShellTests.test_deploy_adapter_uses_only_fake_processes_and_preserves_native_status"),
    ("drop-private-input-pipe", "transport", 'd exec -i "$(ctr_name "$_fft_node")" "$@"', 'd exec "$(ctr_name "$_fft_node")" "$@"', "TransportShellTests.test_deploy_adapter_uses_only_fake_processes_and_preserves_native_status"),
    ("omit-explicit-private-sink-policy", "transport", 'agent_provision_yaml agt1 lab nats://brk1:4222 proxyprivate', 'agent_provision_yaml agt1 lab nats://brk1:4222 open', "TransportShellTests.test_deploy_adapter_uses_only_fake_processes_and_preserves_native_status"),
    ("bypass-shared-log-mapping", "transport", 'sim_log_snapshot "$2" "$3" "$4" "$5"', 'true', "TransportShellTests.test_deploy_adapter_uses_only_fake_processes_and_preserves_native_status"),
    ("stop-receiving-at-declared-final-byte", "peer", '# Continue observing the original socket. Returning\n                            # here would hide duplicated trailing frames/RST.', 'return', "PeerTests.test_receiver_observes_duplicate_tail_after_declared_total"),
    ("omit-prepare-message", "tunnel", 'c.logger.Info("tunnel: flow handoff prepared", "owner_generation", req.owner,\n\t\t\t\t"carrier_generation", a.gen, "generation", nextOwner)', '', "LogCustodyTests.test_product_messages_pin_exact_barrier_positions_and_public_fields"),
    ("omit-resume-message", "tunnel", 'c.logger.Info("tunnel: flow carrier resumed", "owner_generation", resp.owner,\n\t\t"carrier_generation", resp.carrier, "mode", uint8(req.mode))', '', "LogCustodyTests.test_product_messages_pin_exact_barrier_positions_and_public_fields"),
    ("omit-final-cut-message", "agent", 'a.cfg.Logger.Info("agent: image final cut", "generation", cut.generation)', '', "LogCustodyTests.test_product_messages_pin_exact_barrier_positions_and_public_fields"),
    ("omit-resume-carrier-generation", "tunnel", '"carrier_generation", resp.carrier, "mode", uint8(req.mode)', '"mode", uint8(req.mode)', "LogCustodyTests.test_product_messages_pin_exact_barrier_positions_and_public_fields"),
]


# origin: hot-update S5 plan lead C1 / §3.4 / DRILL-RUN
MUTATIONS += [('capacity-normal-before-recovery', 'fixture', 'self.args.profile != "capacity-32" or getattr(self,"capacity_complete",False)', 'True', 'CollectorTests.test_all_profiles_declare_finite_original_cohorts_before_start'), ('capacity-full-count', 'fixture', 'full.get("active_flows") == 32', 'True', 'CapacityPhaseTests.test_two_phases_require_native_capacity_release_and_same_cluster'), ('capacity-full-budget', 'fixture', 'full.get("reservation") == 256<<20', 'True', 'CapacityPhaseTests.test_two_phases_require_native_capacity_release_and_same_cluster'), ('capacity-exec-type', 'fixture', 'row.get("error_type") == "workload.ErrBackpressure"', 'True', 'CapacityPhaseTests.test_two_phases_require_native_capacity_release_and_same_cluster'), ('capacity-exec-effect', 'fixture', 'self.remote_call("agt1", "capacity-exec-ledger")["lines"] == []', 'True', 'CapacityPhaseTests.test_two_phases_require_native_capacity_release_and_same_cluster'), ('capacity-flow-count', 'fixture', 'refused.get("flow_refusals",0) >= 1', 'True', 'CapacityPhaseTests.test_two_phases_require_native_capacity_release_and_same_cluster'), ('capacity-flow-type', 'fixture', 'refused.get("reason") == 10', 'True', 'CapacityPhaseTests.test_two_phases_require_native_capacity_release_and_same_cluster'), ('capacity-release-budget', 'fixture', 'released.get("reservation") == 0', 'True', 'CapacityPhaseTests.test_two_phases_require_native_capacity_release_and_same_cluster'), ('capacity-release-count', 'fixture', 'released.get("settled_originals") == 32', 'True', 'CapacityPhaseTests.test_two_phases_require_native_capacity_release_and_same_cluster'), ('capacity-recovery-success', 'fixture', 'success.get("success") is True', 'True', 'CapacityPhaseTests.test_two_phases_require_native_capacity_release_and_same_cluster'), ('capacity-source-identity', 'fixture', 'after["running_sha"] == before["running_sha"]', 'True', 'CapacityPhaseTests.test_two_phases_require_native_capacity_release_and_same_cluster'), ('capacity-allocation', 'fixture', 'self.sql("capacity-after") == allocation', 'True', 'CapacityPhaseTests.test_two_phases_require_native_capacity_release_and_same_cluster'), ('capacity-final-cohort', 'fixture', 'len(specs) == 32', 'True', 'CapacityPhaseTests.test_saturation_requires_complete_original_ledgers_and_natural_fin'), ('capacity-final-fin', 'fixture', 'row.get("fin_at") is not None', 'True', 'CapacityPhaseTests.test_saturation_requires_complete_original_ledgers_and_natural_fin'), ('capacity-final-accept', 'fixture', 'row["target_accepts"] == 1', 'True', 'CapacityPhaseTests.test_saturation_requires_complete_original_ledgers_and_natural_fin'), ('capacity-final-identity', 'fixture', 'probe.check_identity(before[role]["flows"][name]["identities"]+row["identities"], True)', 'pass', 'CapacityPhaseTests.test_saturation_requires_complete_original_ledgers_and_natural_fin'), ('capacity-fin-order', 'fixture', 'row["fin_at"] >= max(item["at"] for item in row["sent"])', 'True', 'CapacityPhaseTests.test_saturation_requires_complete_original_ledgers_and_natural_fin'), ('capacity-final-bytes', 'fixture', 'sent == received and len(sent) == wanted["bytes"] and probe.digest(sent) == wanted["sha256"]', 'True', 'CapacityPhaseTests.test_saturation_requires_complete_original_ledgers_and_natural_fin'), ('capacity-final-receiver', 'fixture', 'len(ledger) == len(parsed)', 'True', 'CapacityPhaseTests.test_saturation_requires_complete_original_ledgers_and_natural_fin'), ('capacity-final-eof', 'fixture', 'eof >= max(row["at"] for row in ledger)', 'True', 'CapacityPhaseTests.test_saturation_requires_complete_original_ledgers_and_natural_fin'), ('capacity-native-exec-rc', 'peer', 'result.returncode == 70 and result.stdout == b""', 'result.stdout == b""', 'CapacityPhaseTests.test_capacity_exec_counts_only_the_exact_native_error_and_recovery'), ('capacity-native-exec-effect', 'peer', 'and result.stdout == b"" and result.stderr.strip()', 'and result.stderr.strip()', 'CapacityPhaseTests.test_capacity_exec_counts_only_the_exact_native_error_and_recovery'), ('capacity-terminal-fd', 'peer', 'if (stat.st_dev,stat.st_ino,stat.st_mode & 0o170000) != (original["dev"],original["ino"],original["type"]):', 'if False:', 'CapacityPhaseTests.test_saturation_fin_keeps_complete_tail_on_original_tcp'), ('capacity-binding-count', 'observation', 'len(originals) == 32 and len(set(originals.values())) == 32', 'True', 'CapacityPhaseTests.test_capacity_observation_requires_original_active_and_settled_owners'), ('capacity-native-reason', 'observation', "e['values'][:3] == [10,256<<20,256<<20]", "e['values'][1:3] == [256<<20,256<<20]", 'CapacityPhaseTests.test_capacity_observation_requires_original_active_and_settled_owners'), ('capacity-native-used', 'observation', "e['values'][:3] == [10,256<<20,256<<20]", "e['values'][0] == 10", 'CapacityPhaseTests.test_capacity_observation_requires_original_active_and_settled_owners'), ('capacity-original-reserved', 'observation', "e['values'][3] == 3 and e['values'][12] == 8<<20 and e['values'][14:16] == [1,1]", "e['values'][3] == 3 and e['values'][14:16] == [1,1]", 'CapacityPhaseTests.test_capacity_observation_requires_original_active_and_settled_owners'), ('capacity-original-active', 'observation', "e['values'][3] == 3 and e['values'][12] == 8<<20 and e['values'][14:16] == [1,1]", "e['values'][3] == 3 and e['values'][12] == 8<<20", 'CapacityPhaseTests.test_capacity_observation_requires_original_active_and_settled_owners'), ('capacity-original-settled', 'observation', "e['values'][3] == 5 and e['values'][12] == 0 and e['values'][14:16] == [0,0]", "e['values'][3] == 5 and e['values'][14:16] == [0,0]", 'CapacityPhaseTests.test_capacity_observation_requires_original_active_and_settled_owners')]
MUTATIONS += [("capacity-normal-headroom", "observation", "if request.get('profile') == 'capacity-32' and label in ('baseline','hop-1'):", "if False:", "CapacityPhaseTests.test_normal_profile_keeps_shared_budget_headroom")]
MUTATIONS += [('capacity-final-sender', 'fixture', 'len(sender_ledger) == len(parsed)', 'True', 'CapacityPhaseTests.test_saturation_requires_complete_original_ledgers_and_natural_fin')]

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipts',type=Path,required=True)
    parser.add_argument('--only',action='append',default=[],help='Explicit mutation names; omitted runs the entire set.')
    args=parser.parse_args()
    args.receipts.mkdir(parents=True,exist_ok=True,mode=0o700)
    paths=dict(gate=LIB/'flow_artifact_gate.py',fixture=LIB/'flow_fixture.py',peer=LIB/'flow_fixture_peer.py',probe=LIB/'hot_update_flow_probe.py',flow=REPO/'internal/flow/pump.go',observation=LIB/'flow_observation.py',syn=LIB/'flow_open_fault.py',
               transport=LIB/'flow_fixture_transport.sh',
               tunnel=REPO/'internal/tunnel/flow_client.go',agent=REPO/'internal/agent/upgrade_image.go')
    rows=[]
    cases=list(MUTATIONS)
    # Moving an intact message is independently wrong: deletion controls
    # alone would permit a payload-free receipt at an earlier, failed barrier.
    for name,owner,start,end,record,anchor in (
        ('prepare-message-before-accept','tunnel','func (c *Client) PrepareHandoff','func channelErrors',
         'c.logger.Info("tunnel: flow handoff prepared", "owner_generation", req.owner,\n\t\t\t\t"carrier_generation", a.gen, "generation", nextOwner)',
         'err := prepareClientEndpoint(c, e, &a, req, deadline)'),
        ('resume-message-before-bind','tunnel','func attachClientFlow','func (c *Client) PrepareHandoff',
         'c.logger.Info("tunnel: flow carrier resumed", "owner_generation", resp.owner,\n\t\t"carrier_generation", resp.carrier, "mode", uint8(req.mode))',
         'if err := bindClientFlowResume(st, e, resp, a); err != nil {'),
        ('final-cut-message-before-last-validation','agent','func prepareImageCut',None,
         'a.cfg.Logger.Info("agent: image final cut", "generation", cut.generation)',
         'if check != nil {')):
        source=paths[owner].read_text()
        first=source.index(start); last=source.index(end,first) if end else len(source)
        old=source[first:last]
        changed=old.replace(record,'',1).replace(anchor,record+'\n\t'+anchor,1)
        cases.append((name,owner,old,changed,'LogCustodyTests.test_product_messages_pin_exact_barrier_positions_and_public_fields'))
    source = paths['flow'].read_text()
    first, last = source.index('func runPumps'), source.index('func stopped')
    old = source[first:last]
    hook = 'if drillobserve.Hook != nil {\n\t\t\tobserveRecordLocked(r)\n\t\t}'
    changed = old.replace(hook, '', 1).replace('\tr.openStatus = 2', hook+'\n\tr.openStatus = 2', 1)
    cases.append(('pending-observation-after-so-error', 'flow', old, changed,
                  'LogCustodyTests.test_product_messages_pin_exact_barrier_positions_and_public_fields'))
    cases.append(('finish-endpoints-sequentially','fixture',
        "with ThreadPoolExecutor(max_workers=2) as workers:",
        "with ThreadPoolExecutor(max_workers=1) as workers:",
        'CollectorTests.test_finish_drains_both_endpoints_concurrently_and_reaps_requests'))
    if args.only:
        if set(args.only)-{row[0] for row in cases}:
            raise SystemExit('unknown mutation selector')
        cases=[row for row in cases if row[0] in args.only]
    for name,owner,old,new,test in cases:
        path=paths[owner]; original=path.read_text()
        if original.count(old)!=1:
            raise SystemExit('mutation anchor is not unique: '+name)
        changed=original.replace(old,new,1)
        control = SIM/'tests/hot-update-flow-probe-test.py' if owner == 'probe' else CONTROL
        command=[sys.executable,'-I','-B',str(control),test]
        try:
            path.write_text(changed)
            result=subprocess.run(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=90)
        finally:
            path.write_text(original)
        (args.receipts/(name+'.log')).write_text(result.stdout)
        (args.receipts/(name+'.diff')).write_text(''.join(difflib.unified_diff(original.splitlines(True),changed.splitlines(True),fromfile=str(path),tofile=str(path))))
        rows.append(dict(mutation=name,test=test,command=command,rc=result.returncode,
                         assertion_failure='FAIL:' in result.stdout or 'AssertionError' in result.stdout,
                         mutation_sha256=hashlib.sha256(changed.encode()).hexdigest()))
        (args.receipts/'mutations.json').write_text(json.dumps(rows,indent=2)+'\n')
        print(name+': rc='+str(result.returncode),flush=True)
        if result.returncode!=1:
            return 1
    return 0


if __name__=='__main__':
    sys.exit(main())
