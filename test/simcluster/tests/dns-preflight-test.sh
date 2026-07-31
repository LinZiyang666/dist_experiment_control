#!/bin/sh
# Hermetic contract for the host fake-DNS preflight. The real host resolver is
# deliberately excluded: this test owns both NXDOMAIN and fabricated-answer
# outcomes through a PATH-local getent.
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
SIMROOT="$(cd "$HERE/.." && pwd)"
RT=$(mktemp -d)
trap 'rm -rf "$RT"' EXIT
mkdir -p "$RT/bin"

cat >"$RT/bin/getent" <<'EOF'
#!/bin/sh
case "${FAKE_GETENT_MODE:-}" in
  nxdomain) exit 2 ;;
  fake)
    printf '198.18.0.42     %s.lan\n' "$2"
    exit 0
    ;;
  *) exit 9 ;;
esac
EOF
chmod +x "$RT/bin/getent"

run_preflight() {
    PATH="$RT/bin:/usr/bin:/bin" FAKE_GETENT_MODE="$1" \
        bash -c 'set -euo pipefail; . "$1"; assert_host_dns_says_no' \
        bash "$SIMROOT/lib/docker.sh"
}

if ! run_preflight nxdomain >"$RT/nxdomain.out" 2>&1; then
    echo "FAIL: honest NXDOMAIN must pass under set -euo pipefail" >&2
    cat "$RT/nxdomain.out" >&2
    exit 1
fi

if run_preflight fake >"$RT/fake.out" 2>&1; then
    echo "FAIL: a fabricated host answer must be refused" >&2
    exit 1
fi
grep -q 'SIM-PREFLIGHT-FAIL' "$RT/fake.out" || {
    echo "FAIL: fake-DNS refusal lost its stable diagnostic" >&2
    cat "$RT/fake.out" >&2
    exit 1
}

if ! PATH="$RT/bin:/usr/bin:/bin" FAKE_GETENT_MODE=fake SIM_ALLOW_FAKE_DNS=1 \
    bash -c 'set -euo pipefail; . "$1"; assert_host_dns_says_no' \
    bash "$SIMROOT/lib/docker.sh" >"$RT/override.out" 2>&1; then
    echo "FAIL: explicit SIM_ALLOW_FAKE_DNS=1 override did not bypass the preflight" >&2
    cat "$RT/override.out" >&2
    exit 1
fi

echo "dns preflight: PASS"
