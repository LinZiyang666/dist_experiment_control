# S1 External Re-review Round 3 Tasklist

Scope: narrow closure review of round-2 R2-F1/F2/F3 and the appended developer response. No product or
drill behavior changed; prior live 60/61 evidence remains applicable.

- [x] Rebuild current unstaged/untracked scope and verify no out-of-scope product/drill changes were introduced.
- [x] Verify `s1-plan.md` now describes re-authentication plus the first independent node read, explicitly denying
  login snapshot semantics, consistently with implementation/README/inventory.
- [x] Run `pty-run.py` negative controls for no args, missing delimiter, delimiter-with-empty-command, plus a normal
  command; require clean rc=2 for all invalid forms and rc=0 without traceback for the normal form.
- [x] Verify round-1 verification text is corrected rather than silently rewritten.
- [x] Verify provenance wording distinguishes durable raw outputs from unavailable workflow metadata, and independently
  count the committed reviewer/verifier/synth output sections.
- [x] Run Python syntax, command-tree focused test, and targeted whitespace checks.
- [x] Write a round-3 report beginning with Pass or Fail, with closure evidence and release recommendation.
- [x] Stage all files with `git add -A`; verify no unstaged/untracked delivery files and `git diff --cached --check` passes.
