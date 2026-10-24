#!/usr/bin/env python3
"""Reject captured predicates that export shell state to their caller.

origin: hot-update S4 plan §9.1–§9.3 / DRILL-SUBSHELL.
This is a source heuristic, not a shell interpreter: literal function calls,
assignments, variable reads and standalone shell state builtins are tracked.
Quoted remote programs and heredoc payloads are data, not host-side jobs.
Functions reached through a poll or wrapper are included. Scratch variables
written by every reader, and jobs started and joined entirely within one
predicate, do not export state. Dynamic eval/function names are outside scope.
"""
from dataclasses import dataclass
from pathlib import Path
import re
import shlex
import sys


@dataclass
class Source:
    path: Path
    text: str
    code: str
    reads: str


def shell_source(path):
    text = path.read_text()
    # Preserve offsets/lines while removing non-shell heredoc programs.
    lines = text.splitlines(keepends=True)
    delimiters = []
    for i, line in enumerate(lines):
        if delimiters:
            stripped = line.lstrip('\t').rstrip('\r\n')
            lines[i] = ''.join('\n' if c == '\n' else ' ' for c in line)
            if stripped == delimiters[0]:
                delimiters.pop(0)
            continue
        if not line.lstrip().startswith('#'):
            delimiters.extend(m.group(2) for m in re.finditer(
                r"<<-?\s*(['\"]?)([A-Za-z_][A-Za-z_0-9]*)\1", line))
    shell = ''.join(lines)
    code, reads = list(shell), list(shell)
    i = 0
    while i < len(shell):
        char = shell[i]
        if char == '#':
            end = shell.find('\n', i)
            end = len(shell) if end < 0 else end
            for j in range(i, end):
                code[j] = reads[j] = ' '
            i = end
        elif char in "'\"`":
            quote, start = char, i
            i += 1
            while i < len(shell):
                if shell[i] == '\\' and quote != "'":
                    i += 2
                elif shell[i] == quote:
                    i += 1
                    break
                else:
                    i += 1
            for j in range(start + 1, min(i - 1, len(shell))):
                if shell[j] != '\n':
                    code[j] = ' '
                    if quote == "'":
                        reads[j] = ' '
        elif char == '\\':
            i += 2
        else:
            i += 1
    return Source(path, text, ''.join(code), ''.join(reads))


@dataclass
class Function:
    source: Source
    start: int
    end: int
    code: str
    reads: str
    writes: set


def assignments(code):
    # Track literal assignments at command positions and loop iterator writes.
    names = set(re.findall(
        r'(?:^|[;\n]|(?<!&)&(?!&)|\bthen\s|\bdo\s|\belse\s)\s*([A-Za-z_][A-Za-z_0-9]*)=', code))
    names.update(re.findall(r'\bfor\s+([A-Za-z_][A-Za-z_0-9]*)\s+in\b', code))
    return names


def references(text):
    return set(re.findall(r'(?<!\\)\$\{?([A-Za-z_][A-Za-z_0-9]*)', text))


def functions(source):
    result = {}
    for match in re.finditer(r'^\s*([A-Za-z_][A-Za-z_0-9]*)\s*\(\s*\)\s*\{', source.code, re.M):
        start, depth, end = match.end(), 1, match.end()
        for brace in re.finditer(r'[{}]', source.code[start:]):
            depth += 1 if brace.group() == '{' else -1
            if depth == 0:
                end = start + brace.end()
                break
        code, reads = source.code[start:end-1], source.reads[start:end-1]
        result[match.group(1)] = Function(source, match.start(), end, code, reads, assignments(code))
    return result


def closure(name, defined):
    seen, todo = set(), [name]
    while todo:
        current = todo.pop()
        if current not in defined or current in seen:
            continue
        seen.add(current)
        # Masked quotes keep remote shell command names out of the call graph.
        code = defined[current].code
        calls = set(re.findall(
            r'(?:^|[;\n&|(){}]|\b(?:then|else|do|if|elif|while|until)\s+)\s*(?:!\s+)?([A-Za-z_][A-Za-z_0-9]*)\b', code))
        calls.update(re.findall(r'\b(?:poll_until|poll_until_fixed)\s+[^\n;]*?--\s+([A-Za-z_][A-Za-z_0-9]*)', code))
        todo.extend(calls & defined.keys())
    return seen


def predicates(source):
    # Join continuations so assert_bug's extra metadata and multiline polls
    # retain their true predicate. The original first line is the diagnostic.
    offset = 0
    for line in source.text.splitlines(keepends=True):
        start = offset
        offset += len(line)
        match = re.search(r'(?:^|[;{}]\s*|\bthen\s+|\bdo\s+)\s*(assert_[a-z_]+)\s+', line)
        if not match or source.code[start + match.start(1):start + match.end(1)] != match.group(1):
            continue
        tail = source.text[start + match.start(1):]
        command = ''
        words = None
        for part in tail.splitlines(keepends=True):
            command += part
            if part.rstrip('\n').endswith('\\'):
                continue
            try:
                words = shlex.split(command.replace('\\\n', ' '), comments=True)
                break
            except ValueError:
                continue
        if words is None:
            raise ValueError(f'{source.path}:{source.text.count(chr(10), 0, start)+1}: cannot inspect assertion')
        skip = {'assert_setup': 2, 'assert_ok': 2, 'assert_refuses': 3, 'assert_bug': 4}.get(words[0])
        if skip is None or len(words) <= skip:
            continue
        predicate = words[skip]
        if predicate in ('poll_until', 'poll_until_fixed') and '--' in words[skip:]:
            predicate = words[words.index('--', skip)+1]
        yield source.text.count('\n', 0, start)+1, predicate


def inspect(drill, libraries):
    source = shell_source(drill)
    defined = {}
    for library in libraries:
        defined.update(functions(library))
    # 39.P imports 36's functions before invoking the shared fixture. Include
    # literal sourced drivers too, so wrapping an imported function cannot
    # hide its state changes from this drill's assertion sites.
    visited = set()
    def import_driver(driver):
        if driver.path in visited:
            return
        visited.add(driver.path)
        for relative in re.findall(r'^\s*\.\s+"\$HERE/drills/([A-Za-z0-9_./-]+\.sh)"', driver.text, re.M):
            if not relative.startswith('lib/'):
                import_driver(shell_source(driver.path.parent / relative))
        defined.update(functions(driver))
    import_driver(source)
    own = functions(source)
    defined.update(own)
    # Main-body reads are a consumer independent of any predicate function.
    main_reads = list(source.reads)
    for function in own.values():
        main_reads[function.start:function.end] = ' ' * (function.end-function.start)
    main_code = list(source.code)
    for function in own.values():
        main_code[function.start:function.end] = ' ' * (function.end-function.start)
    consumers = {'<main>': references(''.join(main_reads)) - set(re.findall(r'\bfor\s+([A-Za-z_][A-Za-z_0-9]*)\s+in\b', ''.join(main_code)))}
    consumers.update({name: references(fn.reads) - fn.writes for name, fn in defined.items()})
    findings = []
    for line, predicate in predicates(source):
        reached = closure(predicate, defined)
        reasons = set()
        for name in reached:
            fn = defined[name]
            for variable in fn.writes:
                if any(variable in refs for reader, refs in consumers.items() if reader not in reached):
                    reasons.add(f'{name} assigns externally consumed {variable}')
            # Waiting for a job established outside this closure must happen
            # in that job's parent. A captured shell cannot wait for siblings.
            for job in re.findall(r'\bwait\s+[\"\s]*\$\{?([A-Za-z_][A-Za-z_0-9]*)', fn.reads):
                if not any(job in defined[writer].writes for writer in reached):
                    reasons.add(f'{name} waits for caller-owned {job}')
            # Only command-position builtins count; remote exec/cd/trap/source
            # inside quoted programs cannot change the drill shell.
            for builtin in re.findall(r'(?:^|[;\n])\s*(\.|source|cd|trap|export|exec)\s+', fn.code):
                reasons.add(f'{name} changes shell state with {builtin}')
            # A host job carrying its PID out of the capture loses its parent.
            if re.search(r'(?<![&>])&(?![&>])', fn.code):
                for variable in fn.writes:
                    if re.search(r'\b' + re.escape(variable) + r'=\$!', fn.code):
                        if any(variable in refs for reader, refs in consumers.items() if reader not in reached):
                            reasons.add(f'{name} exports background job {variable}')
        if reasons:
            findings.append(f'{drill}:{line}: assert-shell-state: {predicate}: ' + '; '.join(sorted(reasons)))
    return findings


def scan(root):
    libraries = [shell_source(path) for path in sorted((root / 'lib').glob('*.sh'))]
    findings = []
    for drill in sorted(root.glob('*.sh')):
        findings.extend(inspect(drill, libraries))
    return findings


if __name__ == '__main__':
    paths = [Path(arg) for arg in sys.argv[1:]]
    if not paths:
        paths = [Path(__file__).resolve().parent.parent / 'drills']
    try:
        findings = []
        for path in paths:
            if path.is_dir():
                findings.extend(scan(path))
            else:
                libraries = [shell_source(lib) for lib in sorted((path.parent / 'lib').glob('*.sh'))]
                findings.extend(inspect(path, libraries))
    except (OSError, ValueError) as error:
        print(f'assert-shell-state: inspection failed: {error}', file=sys.stderr)
        raise SystemExit(1)
    print('\n'.join(findings) if findings else 'assert-shell-state: OK')
    raise SystemExit(bool(findings))
