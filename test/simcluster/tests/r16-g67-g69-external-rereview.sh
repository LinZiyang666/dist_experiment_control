#!/usr/bin/env bash
set -euo pipefail

here=$(cd "$(dirname "$0")/.." && pwd)
drill="$here/drills/96-mid-flight-chaos.sh"
verdicts="$here/expected-verdicts.tsv"
# The prose histories moved out of the TSV into expected-verdicts-log.md when the table was split into a
# strict machine half. This guard reads PROSE, so it follows the prose.
verdicts_log="$here/expected-verdicts-log.md"

failed=0
terminal_red_line=$(grep -nF 'product_red "#57 an in-flight tier-B transfer' "$drill" | cut -d: -f1)
recovery_start_line=$(grep -nE 'A1f-pre bring brk2 back|A2a bring brk2 back' "$drill" | head -1 | cut -d: -f1)
if [ -n "$terminal_red_line" ] && [ -n "$recovery_start_line" ] &&
	[ "$terminal_red_line" -lt "$recovery_start_line" ]; then
	echo "FAIL: drill 96 declares the finalize-on-recovery defect before it restarts the crashed home broker; it never tests the R16 recovery finalizer" >&2
	failed=1
fi
if grep -nF 'xfer_cross_home_reap_age=5s' "$drill"; then
	echo "FAIL: drill 96 still judges a short window as if the now-forbidden 5s cross-home age floor were loaded" >&2
	failed=1
fi
current_96_note=$(awk '/^## 96-mid-flight-chaos$/{f=1;next} /^## /{f=0} f' "$verdicts_log" | sed 's/ | R16.*//')
if [ -z "$current_96_note" ]; then
	echo "FAIL: no '## 96-mid-flight-chaos' section in $verdicts_log — this guard would pass vacuously" >&2
	failed=1
fi
if printf '%s\n' "$current_96_note" | grep -nF 'xfer_reap_interval + the new xfer_cross_home_reap_age'; then
	echo "FAIL: expected-verdicts still claims both #58 knobs were compressed after production config began rejecting the age-floor compression" >&2
	failed=1
fi
cross_home_gap_count=$(grep -cF 'not_covered "#58 cross-home GC reap (deploy-tier observation)"' "$drill" || true)
if [ "$cross_home_gap_count" -ne 1 ]; then
	echo "FAIL: drill 96 must record the structural #58 cross-home coverage gap exactly once; found $cross_home_gap_count copies" >&2
	failed=1
fi

exit "$failed"
