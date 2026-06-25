package clusteroffline

// export_test.go exposes internal test hooks to the external clusteroffline_test package (the
// standard Go idiom — compiled ONLY under `go test`, never in the production binary).

// SetRestoreAfterMigrateHook installs (or clears, with nil) the post-migration test seam so an
// external test can corrupt the migrated staging DB and prove the post-migration verify refuses to
// install it (Stage-C BLOCKER-2).
func SetRestoreAfterMigrateHook(h func(stagePath string)) { restoreAfterMigrateHook = h }
