package determinism

// legacy_process_named_list.go — the DRAINING LEDGER for TestNoNewProcessNamedTestFiles (B6).
//
// IT IS EMPTY, AND THAT IS THE POINT.
//
// It was generated mechanically with 158 entries at the moment the freeze gate landed — every test file
// in the tree whose name described a development-process event rather than the unit under test
// (p13_external_review_round6_test.go, g4_external_review_fixes_test.go, b6_skew_test.go,
// codex_allgreen_external_review_test.go, …). All 158 were then renamed with `git mv` in the same batch,
// which made every entry stale, which failed the reverse assertion, which is how the ledger drained.
//
// That sequence is deliberate, and it is the reconciliation between two audit reports that disagreed:
// S1 §B6 said do the renames; S3 said "止血 first — freeze new names, put the backlog in a golden
// allow-list, and treat the renames as optional cleanup, NOT a gate precondition". Both are satisfied by
// ORDERING rather than by picking a side: the freeze landed first, so nothing new could be added while
// the renames were in flight, and each rename then tightened the gate by removing its own exemption.
//
// ON THE NUMBER 158
// -----------------
// Four figures appear across the audit: S1 says 141/499, L07's header says 134 while its own body says
// 123, S3 says 155, and one of this batch's drafting agents counted 165. None publishes its regex, so
// none is reproducible and they cannot all describe the same tree. 158 is what processNamedPattern in
// test_naming_test.go actually matched, and that pattern lives in the source precisely so a reader can
// disagree with it exactly rather than approximately.
//
// WHAT THE RENAMES DID NOT DO
// ---------------------------
// No test function was renamed, deleted or moved between packages: the (directory, function-name)
// identity multiset went from 2304 entries to 2333, and every one of the 29 additions is a test written
// in this same batch for B5/B7/B8/B9. Directories were not touched either — test/pN and test/dN are the
// e2e-matrix contract, referenced as STRING LITERALS from all_phases_test.go and AST-parsed by
// test/e2e/parallel/split.go, so renaming one would break the only full-matrix gate.
//
// KEEPING IT EMPTY
// ----------------
// Nothing should ever be added here again. A new test file named after a review round is not an exemption
// request — it is the thing CLAUDE.md §3 step 5b forbids. The reviewer's finding belongs in a
// `// origin:` line above the test function, where it survives a rename and stays greppable by the next
// person who changes that unit.
const legacyProcessNamedList = ``
