package auth

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"testing"
)

// origin: acl_reconcile_external_review_test.go (renamed in B6)
//
// TestExternalReviewSubscriptionExtractorRejectsDeadDeclarations proves that a
// subject declaration alone is not evidence that the broker subscribes to it.
func TestExternalReviewSubscriptionExtractorRejectsDeadDeclarations(t *testing.T) {
	const src = `package sample
const SubjectPrefix = "tether.v2"
const DeadSubject = SubjectPrefix + ".ctrl.by.*.session.*.dead.req"
`
	f, err := parser.ParseFile(token.NewFileSet(), "sample.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := subjectLiterals(f); len(got) != 0 {
		t.Fatalf("extractor reported %v as live subscriptions, but the sample contains no Subscribe call", got)
	}
}

// TestExternalReviewSubscriptionExtractorRejectsDeadKeyedFields checks the
// other declaration-only shape. A field named subj is not evidence of a NATS
// subscription unless the surrounding value is proven to be a subscription
// table; ordinary config/state structs may use the same generic field name.
func TestExternalReviewSubscriptionExtractorRejectsDeadKeyedFields(t *testing.T) {
	const src = `package sample
const SubjectPrefix = "tether.v2"
type config struct { subj string }
var dead = config{subj: SubjectPrefix + ".ctrl.by.*.session.*.dead.req"}
`
	f, err := parser.ParseFile(token.NewFileSet(), "sample.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := subjectLiterals(f); len(got) != 0 {
		t.Fatalf("extractor reported %v as live subscriptions, but the keyed field is ordinary dead config", got)
	}
}

func TestExternalReviewDynamicSubscriptionConcatIsUnresolved(t *testing.T) {
	root := t.TempDir()
	src := `package broker
func register(nc interface{ Subscribe(string, func()) }, suffix string) {
	nc.Subscribe(SubjectPrefix + suffix, func(){})
}`
	if err := os.WriteFile(filepath.Join(root, "dynamic.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	got := unresolvedSubscriptionSites(t, root)
	if len(got) != 1 {
		t.Fatalf("dynamic concatenation must be declared, got sites %v", got)
	}
}
