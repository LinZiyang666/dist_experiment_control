#!/bin/sh
# contention-registry-check.sh — two-way reconciliation of contention-sensors.tsv with the drill tree.
# POSIX sh, no docker, sub-second.
#
#     cd test/simcluster && sh tests/contention-registry-check.sh
#     REGISTRY=<tsv> DRILLS=<dir> sh tests/contention-registry-check.sh     # the self-test's sandbox
#
# RULES
#   C1  every row has 4 tab-separated fields; sensor ids are [A-Za-z0-9-]+ and unique (no '#': that is
#       the file's comment marker, so a gotcha number is written bare, e.g. 70-grow-timing)
#   C2  regime ∈ {default, live-grow, none-after-split, H-dependent}
#   C3  every unit in `units` resolves: `<drill>` is drills/<drill>.sh, `<drill>.<arm>` is an arm of
#       that drill's manifest (lib/manifest.sh); a regime other than none-after-split needs >= 1 unit,
#       and none-after-split must list none (a sensor with units is not "none")
#   C4  every `# forgoes:` value in every arm drill is `-` or `<id> [<id>…][: free text]` — every id
#       before the first `:` must be a registered sensor; a sensor a drill claims to forgo must be one
#       this registry knows, or the claim is unaccountable
#   C5  no sensor is registered twice under different ids by the same evidence line (a cheap duplicate
#       catch; identical evidence text)
# origin: simcluster-speed plan §5.5 (X26); accel plan C1 acceptance #6 (owed since 2026-07-24).
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
SIM_ROOT="$(cd "$HERE/.." && pwd)"
REGISTRY="${REGISTRY:-$SIM_ROOT/contention-sensors.tsv}"
DRILLS="${DRILLS:-$SIM_ROOT/drills}"
. "$SIM_ROOT/lib/manifest.sh"
FAIL=0
bad() { printf 'contention-registry-check: %s\n' "$1" >&2; FAIL=$((FAIL + 1)); }
[ -f "$REGISTRY" ] || { bad "missing $REGISTRY"; exit 1; }
[ -d "$DRILLS" ] || { bad "missing $DRILLS"; exit 1; }

rows() { grep -v '^#' "$REGISTRY" | grep -v '^[[:space:]]*$'; }
ids=$(rows | cut -f1)

# C1 / C2 / C3
while IFS= read -r line; do
    [ -n "$line" ] || continue
    nf=$(printf '%s\n' "$line" | awk -F'\t' '{print NF}')
    id=$(printf '%s\n' "$line" | cut -f1)
    if [ "$nf" -ne 4 ]; then bad "C1 row '$id' has $nf fields, want 4"; continue; fi
    case "$id" in ''|*[!A-Za-z0-9-]*) bad "C1 sensor id '$id' is not [A-Za-z0-9-]+ (a gotcha number is written without '#', the comment marker)" ;; esac
    regime=$(printf '%s\n' "$line" | cut -f2)
    units=$(printf '%s\n' "$line" | cut -f3)
    case "$regime" in
        default|live-grow|H-dependent)
            [ -n "$units" ] && [ "$units" != "-" ] || bad "C3 sensor '$id' ($regime) lists no unit that samples it" ;;
        none-after-split)
            [ -z "$units" ] || [ "$units" = "-" ] || bad "C3 sensor '$id' is none-after-split but lists units: $units" ;;
        *) bad "C2 sensor '$id' regime '$regime' is not one of default|live-grow|none-after-split|H-dependent" ;;
    esac
    if [ "$units" != "-" ]; then
        for u in $units; do
            drill=${u%%.*}
            f="$DRILLS/$drill.sh"
            if [ ! -f "$f" ]; then bad "C3 sensor '$id' names unit '$u' but drills/$drill.sh does not exist"; continue; fi
            case "$u" in
                *.*)
                    arm=${u#*.}
                    manifest_has "$f" || { bad "C3 sensor '$id' names unit '$u' but $drill has no arm manifest"; continue; }
                    printf '%s\n' $(manifest_arms "$f") | grep -qx -- "$arm" || bad "C3 sensor '$id' names unit '$u' but $arm is not an arm of $drill"
                    ;;
                *)
                    # A split drill is no longer a unit: the row must say WHICH arm still samples the sensor,
                    # or the split silently keeps a registry line alive that no unit backs (the exact
                    # go-dark this file exists to make visible).
                    manifest_has "$f" && bad "C3 sensor '$id' names '$u' as a whole drill but it is arm-split ($(manifest_arms "$f")) — name the arm(s) that sample it"
                    ;;
            esac
        done
    fi
done <<EOF
$(rows)
EOF
dups=$(printf '%s\n' "$ids" | sort | uniq -d)
[ -z "$dups" ] || bad "C1 duplicate sensor id(s): $(printf '%s' "$dups" | tr '\n' ' ')"
edups=$(rows | cut -f4 | sort | uniq -d)
[ -z "$edups" ] || bad "C5 two sensors share an evidence line: $(printf '%s' "$edups" | head -1)"

# C4
for f in "$DRILLS"/*.sh; do
    [ -e "$f" ] || continue
    manifest_has "$f" || continue
    d=$(basename "$f" .sh)
    for a in $(manifest_arms "$f"); do
        fg=$(manifest_value "$f" forgoes "$a")
        [ -n "$fg" ] && [ "$fg" != "-" ] || continue
        idlist=${fg%%:*}   # ids sit before the first ':'; free text after it is not parsed
        for sid in $idlist; do
            printf '%s\n' "$ids" | grep -qx -- "$sid" || printf 'C4 %s arm %s forgoes "%s", not a registered sensor\n' "$d" "$a" "$sid"
        done > "${TMPDIR:-/tmp}/crc.$$"
        while IFS= read -r m; do [ -n "$m" ] && bad "$m"; done < "${TMPDIR:-/tmp}/crc.$$"
        rm -f "${TMPDIR:-/tmp}/crc.$$"
    done
done

if [ "$FAIL" != 0 ]; then
    printf 'contention-registry-check: %s problem(s)\n' "$FAIL" >&2
    exit 1
fi
printf 'contention-registry-check: OK (%s sensor(s) registered, every forgoes accounted for)\n' "$(printf '%s\n' "$ids" | grep -c .)"
