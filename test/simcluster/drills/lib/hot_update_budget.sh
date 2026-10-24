# drills/lib/hot_update_budget.sh — the hot-update acceptance budgets, deploy-tier half.
# POSIX sh, assignments only (no functions, no side effects): sourced by the hot-update drills and probes.
# The Go half is internal/upgradebudget; test/architecture/upgrade_budget_pin_test.go reconciles every
# value below against its constant, so a drill can neither quote a stale number nor quietly widen one.
# Never edit a value here to make a failed sample pass: the plan text changes first (hot-update-plan §7).
# origin: docs/reviews/hot-update-agent-bootstrap-plan.md §6.

# LocalFreezeWindow: old image's pumps paused → new image's pumps usable (recorded, calibration only in S3).
HOT_UPDATE_FREEZE_MS=500
# RequestLatency: every new control request end to end during an upgrade.
HOT_UPDATE_REQUEST_S=5
# ProbeRatePerSecond: the mixed control probe's request rate.
HOT_UPDATE_PROBE_RATE=10
# ComponentHandover: the agent's register deadline while an upgrade is pending.
HOT_UPDATE_REGISTER_DEADLINE_S=120
# SafePointWait: the cut's bounded wait for an in-flight creation.
HOT_UPDATE_SAFE_POINT_S=5

# origin: hot-update S4 plan §2.3 / §10. Capacity and recovery are separate from authority.
# PTYInputBytes: accepted input awaiting actual master writes.
HOT_UPDATE_PTY_INPUT_BYTES=262144
# PTYBatchBytes: one bounded input/output protocol batch.
HOT_UPDATE_PTY_BATCH_BYTES=32768
# PTYRecovery: one absolute client recovery window, including register watchdog rollback.
HOT_UPDATE_PTY_RECOVERY_S=180
# PTYRecoveryOpportunity: one opportunity per expired keepalive, preserved across images.
HOT_UPDATE_PTY_RECOVERY_OPPORTUNITY_S=5
# LiveFlowStall: longest allowed interruption of progress during a carrier switch.
HOT_UPDATE_LIVE_FLOW_STALL_S=5

# Fixed maintenance expiry, including the admission budget and proof margin.
# origin: hot-update S6 plan §7.1 / §13
HOT_UPDATE_BROKER_MAINTENANCE_TTL_S=8

# Healthy transfer durable progress, separate from the flow stall criterion.
# origin: hot-update S7 plan §1.2 / §10.3 / T43
HOT_UPDATE_TRANSFER_PROGRESS_S=10

# Privileged IPC budgets mirror upgradebudget/infra.go; the pin gate checks both.
# origin: S7 internal review GATES-PROD F3 / hot-update S9 plan §10.2 / T83
HOT_UPDATE_INFRA_READ_S=2
HOT_UPDATE_INFRA_ACTION_S=10
HOT_UPDATE_INFRA_WORKER_S=15
HOT_UPDATE_INFRA_MAX_WORKERS=4
