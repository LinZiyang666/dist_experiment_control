#!/bin/sh
set -eu

HERE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
target="$HERE/drills/41-shrink-to-standalone.sh"

# Shell functions are not inherited by `sh -c`. With the leading `!`, "command
# not found" becomes success, so this exact shape is a permanently-green
# precondition rather than a check that the conf is clustered.
if awk '!/^[[:space:]]*#/ { print NR ":" $0 }' "$target" |
	grep -E 'sh -c ["'\'']! _no_cluster_block["'\'']'; then
	echo "FAIL: drill 41 executes _no_cluster_block in a child shell where the function is undefined; leading ! turns command-not-found into success" >&2
	exit 1
fi

echo "PASS"
