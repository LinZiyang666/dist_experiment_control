#!/usr/bin/env python3
"""Prove the legacy assertion forwarding ABI before tracing its callers.

origin: S8 internal review s8-hermetic F10.
A parameterized claim can be attributed only when the description's argument
and the one primitive receiving it are known. Changed/dynamic forwarding stays
BLIND; neither a helper's name nor its presence authorizes hiding a claim.
"""
from pathlib import Path
import re
import sys

path = Path(sys.argv[1])
source = path.read_text() if path.is_file() else ""
match = re.search(r'^upgrade_legacy_assert\(\) \{\n(.*?)^}', source, re.M | re.S)
valid = False
if match:
    body = "\n".join(line for line in match[1].splitlines() if not line.lstrip().startswith("#"))
    binding = '_ula_root=$1; _ula_target=$2; _ula_desc=$3; _ula_owner=$4; _ula_signature=$5; shift 5'
    claim = 'assert_bug "$_ula_desc" "$_ula_owner" "$_ula_signature" upgrade_legacy_observation "$_ula_rc" "$_ula_root/legacy-oracle.raw"'
    # There is exactly one claim and exactly one binding of its description.
    # Reject other primitives too: one caller must not erase a second claim.
    valid = (body.count(binding) == 1 and body.count(claim) == 1
             and len(re.findall(r'\b_ula_desc=', body)) == 1
             and re.findall(r'\b(?:assert_ok|assert_refuses|assert_setup|assert_bug|product_red|not_covered)\b', body) == ['assert_bug'])
if valid:
    print(source.count('\n', 0, match.start()) + 1, source.count('\n', 0, match.end()) + 1)
else:
    print('0 0')
