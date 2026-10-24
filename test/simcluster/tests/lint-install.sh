#!/bin/sh
# lint-install.sh — STATIC guard against command substitution inside install.sh heredocs (defect P9).
#
# WHY THIS EXISTS. install.sh runs as ROOT and generates config/unit files from heredocs. An UNQUOTED
# heredoc (`cat <<EOF`) makes its whole body shell-active: `$X` expands AND a backtick or $(…) is
# EXECUTED. That is fine for the intended `$bin`/`$etc` interpolations, but it means ordinary English
# prose in a generated comment is code. P9 was exactly that: the nats-server unit carried
#
#     # G4 §B / #23: Restart=always ... The `cluster add` grow cutover restarts nats-server ...
#
# so every broker install ran `cluster add` as root (printing "cluster: not found") and wrote a unit
# file with those two words silently deleted — the documented rationale for Restart=always became
# unreadable on the real machine. Nothing failed loudly, which is why a human review had missed it
# across several releases and why this has to be a static rule instead of vigilance.
#
# THE RULE. For every UNQUOTED heredoc, the body must contain no backtick and no $(…). Variable
# expansion is deliberately NOT banned: it is the entire reason those heredocs are unquoted. But
# command substitution has no legitimate use in a generated config file — anything it could produce
# should be computed into a variable before the heredoc, where it is reviewable and testable.
# QUOTED heredocs (`cat <<'EOF'`) are inert by construction, so their bodies are not scanned.
#
# The fix for a violation is one of:
#   * prose that only LOOKS like code  -> use single quotes:  'cluster add'
#   * a backtick that must be emitted  -> escape it:          \`
#   * a body that expands nothing      -> quote the heredoc:  <<'EOF'
#
# Run:  sh tests/lint-install.sh
set -eu
HERE="$(cd "$(dirname "$0")" && pwd)"
# tests/ -> simcluster/ -> test/ -> repo root. Keep this resolved rather than relying on cwd, since
# the drill runner invokes the lints from several different directories.
# origin: hot-update S8 plan §5.2 / P20 — explicit file input permits hermetic
# positive/negative controls without executing the root installer being scanned.
[ "$#" -le 1 ] || { echo "usage: lint-install.sh [install-script]" >&2; exit 2; }
TARGET="${1:-$(cd "$HERE/../../.." && pwd)/scripts/install.sh}"

[ -f "$TARGET" ] || { echo "lint-install: cannot find $TARGET" >&2; exit 1; }
# Standalone invocations use the same adapter as run-all. The private directory
# owns only the launcher build, never an installer output or a production path.
# origin: hot-update S9 plan §9.5 / P37 / T88
LINT_SUPERVISOR_DIR=
trap '[ -z "$LINT_SUPERVISOR_DIR" ] || rm -rf "$LINT_SUPERVISOR_DIR"' EXIT
if [ -z "${SIM_UPGRADE_SUPERVISOR:-}" ]; then
    . "$HERE/../lib/supervisor.sh"
    LINT_SUPERVISOR_DIR=$(mktemp -d)
    SIM_UPGRADE_SUPERVISOR=$(sim_supervisor_build "$(cd "$HERE/../../.." && pwd)" "$LINT_SUPERVISOR_DIR")
    export SIM_UPGRADE_SUPERVISOR
fi
[ -x "$SIM_UPGRADE_SUPERVISOR" ] || { echo "lint-install: shared supervisor is required" >&2; exit 2; }
# Syntax checking is read-only: never source or execute the root installer.
"$SIM_UPGRADE_SUPERVISOR" -- sh -n "$TARGET"
"$SIM_UPGRADE_SUPERVISOR" -- python3 -I -B - "$TARGET" <<'PYINSTALL'
from collections import Counter
from pathlib import Path
import re
import sys


# Exact source identities, including the two outputs of the shared drop-in
# writer. Counts alone let removal of a helper template hide behind a banner.
# origin: hot-update S9 plan §1 / §6.4 / P37 / T88
EXPECTED_TARGETS = {
    ('install_agent', '$SESSION_DIR/agent.yaml'),
    ('write_journald_dropin', '$_dropin'),
    ('write_infra_files', '$INFRA_ETC_DIR/templates/tether-infra-helper.socket'),
    ('write_infra_files', '$INFRA_ETC_DIR/templates/tether-infra-helper@.service'),
    ('write_infra_files', '$INFRA_ETC_DIR/templates/nats-server.service.d/90-tether-infra.conf'),
    ('write_infra_files', '$INFRA_ETC_DIR/templates/caddy.service.d/90-tether-infra.conf'),
    ('install_broker', '$ETC_DIR/broker.yaml'),
    ('install_broker', '$ETC_DIR/Caddyfile'),
    ('write_systemd_units', '$etc/nats.d/nats.conf'),
    ('write_systemd_units', '$sysd/nats-server.service'),
    ('write_systemd_units', '$sysd/caddy.service'),
    ('write_broker_systemd_unit', '$sysd/tether-broker.service'),
}
EXPECTED_STDOUT = Counter({
    'usage': 1, 'install_agent': 1, 'install_ctl': 2,
    'install_broker': 5, 'hot_update_retrofit': 1,
})


def backtick(source, offset):
    offset += 1
    while offset < len(source) and source[offset] != '`':
        if source.startswith('<<', offset):
            raise ValueError('nested heredoc backtick is unsupported')
        offset += 2 if source[offset] == '\\' else 1
    if offset >= len(source):
        raise ValueError('unfinished backtick expansion')
    return offset + 1


def expansion(source, offset):
    depth = 1
    arithmetic = source.startswith('$((', offset)
    offset += 2
    while offset < len(source) and depth:
        # Arithmetic shifts are harmless, but a nested command substitution
        # re-enters shell syntax and must not conceal another heredoc.
        # origin: hot-update S9 plan §12 P37 / T88 / M34
        if source.startswith('$(', offset):
            offset = expansion(source, offset)
            continue
        if not arithmetic and source.startswith('<<', offset):
            raise ValueError('nested heredoc expansion is unsupported')
        char = source[offset]
        if char in "'\"`":
            _, _, offset = word(source, offset)
            continue
        if char == '\\':
            offset += 2
            continue
        depth += (char == '(') - (char == ')')
        offset += 1
    if depth:
        raise ValueError('unfinished shell expansion')
    return offset


def word(source, offset):
    """Read one shell word; only quotes in THIS word quote a delimiter.

    Shell-active expansions outside a document are opaque to this lint. A
    nested heredoc in an expansion is rejected rather than silently unscanned.
    This is a bounded installer scanner, not a replacement shell interpreter.
    """
    start, result, quoted = offset, '', False
    while offset < len(source) and source[offset] not in ' \t\n;|&<>()':
        char = source[offset]
        if char == '\\':
            offset += 1
            if offset == len(source):
                raise ValueError('unfinished shell escape')
            if source[offset] != '\n':
                # Line continuation is removed before tokenization; it does
                # not quote the delimiter or make the document body inert.
                # origin: hot-update S9 plan §12 P37 / T88 / M34
                quoted = True
                result += source[offset]
            offset += 1
        elif char == '`':
            inner = offset
            offset = backtick(source, offset)
            # Backticks are expansion syntax, not delimiter quote removal.
            # Keeping the spelling makes such delimiters explicitly unsupported.
            result += source[inner:offset]
        elif char in "'\"":
            quote = char
            quoted = True
            offset += 1
            inner = offset
            while offset < len(source) and source[offset] != quote:
                if quote != "'" and source.startswith('$(', offset):
                    # Nested command substitutions can contain quotes of their
                    # own. Parse the whole expansion, rather than letting an
                    # inner quote expose literal << bytes as a fake opener.
                    offset = expansion(source, offset)
                    continue
                if quote == '"' and source[offset] == '`':
                    offset = backtick(source, offset)
                    continue
                if quote != "'" and source[offset] == '\\':
                    offset += 1
                    if offset == len(source):
                        raise ValueError('unfinished quoted escape')
                offset += 1
            if offset == len(source):
                raise ValueError('unfinished shell quote')
            result += source[inner:offset]
            offset += 1
        elif source.startswith('$(', offset):
            inner = offset
            offset = expansion(source, offset)
            result += source[inner:offset]
        else:
            result += char
            offset += 1
    if offset == start:
        raise ValueError('missing heredoc delimiter')
    return result, quoted, offset


def substitutions(body):
    """Apply unquoted-heredoc escape rules before looking for substitutions.

    Quotes in a BODY are ordinary bytes. Backslash only protects backslash,
    dollar, backtick and newline; an even number still leaves the next dollar
    active. Remove line continuations before detecting a split dollar-paren.
    """
    cooked, offset = [], 0
    while offset < len(body):
        char = body[offset]
        if char == '\\' and offset + 1 < len(body) and body[offset + 1] in '\\$`\n':
            if body[offset + 1] != '\n':
                cooked.append(None)  # Protected byte cannot begin a substitution.
            offset += 2
        else:
            cooked.append(char)
            offset += 1
    return any(char == '`' or (char == '$' and cooked[i + 1:i + 2] == ['('])
               for i, char in enumerate(cooked))


def scan(source):
    documents, pending, tokens = [], [], []
    owner, target, offset = '', None, 0
    while offset < len(source):
        char = source[offset]
        if char in ' \t':
            offset += 1
            continue
        if char == '#':
            end = source.find('\n', offset)
            offset = len(source) if end < 0 else end
            continue
        if char == '\n':
            if len(tokens) >= 4 and tokens[1:4] == ['(', ')', '{']:
                owner = tokens[0]
                target = None
            elif tokens == ['}']:
                if target is not None:
                    raise ValueError('template target has no scanned heredoc')
                owner, target = '', None
            elif tokens[:1] == ['config_dest']:
                if len(tokens) != 2 or target is not None:
                    raise ValueError('ambiguous template target')
                target = tokens[1]
            offset += 1
            for document in pending:
                document['owner'] = owner
                if '$CONFIG_DEST' in tokens:
                    if target is None:
                        raise ValueError('template heredoc has no config_dest')
                    document['target'] = target
                    target = None
                else:
                    if '>' in tokens or '>>' in tokens:
                        raise ValueError('unregistered direct heredoc output')
                    document['target'] = '$_dropin' if owner == 'write_journald_dropin' else 'stdout'
                body = []
                while offset < len(source):
                    end = source.find('\n', offset)
                    end = len(source) if end < 0 else end + 1
                    line = source[offset:end]
                    offset = end
                    compared = line.lstrip('\t') if document['strip_tabs'] else line
                    # The shell joins active backslash-newline before testing
                    # the delimiter. A physical EOF after a continued body
                    # line is still body, and cannot conceal the next command.
                    while not document['quoted'] and compared.endswith('\n'):
                        tail = compared[:-1]
                        escapes = len(tail) - len(tail.rstrip('\\'))
                        if escapes % 2 == 0 or offset == len(source):
                            break
                        next_end = source.find('\n', offset)
                        next_end = len(source) if next_end < 0 else next_end + 1
                        following = source[offset:next_end]
                        offset = next_end
                        line += following
                        compared = compared[:-2] + (following.lstrip('\t') if document['strip_tabs'] else following)
                    if compared.removesuffix('\n') == document['delimiter']:
                        break
                    body.append(line)
                else:
                    raise ValueError('unterminated heredoc: ' + document['delimiter'])
                document['body'] = ''.join(body)
                documents.append(document)
            pending, tokens = [], []
            continue
        if source.startswith('<<', offset):
            line = source.count('\n', 0, offset) + 1
            offset += 2
            strip_tabs = source[offset:offset + 1] == '-'
            offset += int(strip_tabs)
            while source[offset:offset + 1] in (' ', '\t'):
                offset += 1
            delimiter, quoted, offset = word(source, offset)
            if re.fullmatch(r'[A-Za-z_][A-Za-z_0-9.-]*', delimiter) is None:
                raise ValueError('unsupported heredoc delimiter')
            pending.append(dict(line=line, delimiter=delimiter, quoted=quoted, strip_tabs=strip_tabs))
            continue
        if char in ';|&<>()' or (char in '{}' and not tokens):
            end = offset + (2 if source[offset:offset + 2] == '>>' else 1)
            tokens.append(source[offset:end])
            offset = end
        else:
            value, _, offset = word(source, offset)
            tokens.append(value)
    if pending:
        raise ValueError('heredoc opener without a body')
    return documents


def check(source):
    documents = scan(source)
    targets, stdout = Counter(), Counter()
    for document in documents:
        if not document['quoted'] and substitutions(document['body']):
            raise ValueError('command substitution in unquoted heredoc at line ' + str(document['line']))
        target = document['target']
        if target == 'stdout':
            stdout[document['owner']] += 1
        elif target == '$INFRA_ETC_DIR/templates/$_wif_unit.service.d/90-tether-infra.conf':
            # The source writer is deliberately shared; both concrete outputs
            # belong to the targetset, and the loop must still enumerate both.
            if re.findall(r'^    for _wif_component in (.*); do$', source, re.M) != ['nats caddy']:
                raise ValueError('infra drop-in target loop changed')
            if len(re.findall(r'^        if \[ "\$_wif_component" = nats \]; then _wif_unit=nats-server;.*else _wif_unit=caddy;', source, re.M)) != 1:
                raise ValueError('infra drop-in target mapping changed')
            for unit in ('nats-server', 'caddy'):
                targets[(document['owner'], target.replace('$_wif_unit', unit))] += 1
        else:
            targets[(document['owner'], target)] += 1
    if targets != Counter({identity: 1 for identity in EXPECTED_TARGETS}):
        raise ValueError('exact template targetset mismatch: ' + repr(targets))
    if stdout != EXPECTED_STDOUT:
        raise ValueError('exact banner heredoc set mismatch: ' + repr(stdout))
    if len(re.findall(r'^        } > "\$_dropin"$', source, re.M)) != 1:
        raise ValueError('journald template output changed')
    return documents


def main():
    try:
        documents = check(Path(sys.argv[1]).read_text())
    except (OSError, UnicodeError, ValueError) as error:
        print('lint-install: ' + str(error), file=sys.stderr)
        return 1
    print('lint-install: OK (%d heredocs; exact template targetset; no active command substitution)' % len(documents))
    return 0


if __name__ == '__main__':
    sys.exit(main())
PYINSTALL
