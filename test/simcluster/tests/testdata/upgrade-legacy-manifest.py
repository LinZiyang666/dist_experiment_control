"""Frozen legacy, S8 wire/policy and S9 migration manifest expectations.

This is independent contract data, never read back from the drill under test.
origin: S8 internal review s8-hermetic F4–F6; S8 plan §9.0 / §9.3.
"""
historical = ["X", "Y", "P", "F", "BR"]
wire = [cell + "-" + verb for broker in "01" for ctl_agent in ("00", "01", "10", "11")
        for cell in ["Q" + ctl_agent + "B" + broker]
        for verb in ("exec", "run", "kill", "expose", "transfer")]
policy = ["LB-new", "LB-old", "LI-defer", "LI-interrupting", "M-route",
          "CC-idle", "CC-late-create", "CL-reject", "ABI-adjacent", "ABI-too-far",
          "ABI-unknown", "GO-hash", "CFG-nats-old", "CFG-nats-new", "CFG-caddy-old", "CFG-caddy-new"]
# S9's additive NI/CI arms preserve every S8 cell and historical boundary.
arms = historical + wire + policy + ["NI", "CI"]
fixtures = {arm: "N1" for arm in arms}
fixtures.update(BR="N3-live", **{"M-route": "N3-live"})
fixtures.update({arm: "install" for arm in policy if arm.startswith("CFG-")})
growth = {arm: 0 for arm in arms}; growth.update(BR=2, **{"M-route": 2})
costs = dict(zip(historical, (300, 420, 300, 420, 300)))
costs.update({arm: dict(exec=60, run=90, kill=90, expose=90, transfer=120)[arm.rsplit("-", 1)[1]] for arm in wire})
costs.update(dict(zip(policy, (120, 120, 120, 120, 240, 240, 240, 120, 240, 120, 120, 120, 60, 60, 60, 60))))
costs.update(NI=300, CI=300)
MANIFEST39 = {"arms": " ".join(arms),
              "fixture": " ".join(f"{arm}={fixtures[arm]}" for arm in arms),
              "grows": " ".join(f"{arm}={growth[arm]}" for arm in arms),
              "worst": " ".join(f"{arm}={costs[arm]}" for arm in arms),
              "forgoes": " ".join(f"{arm}=-" for arm in arms)}
