package upgradetx

import "testing"

// origin: S8 internal review cluster-roll F4 F7
func TestParentCompletionRetainsIndependentChildCustody(t *testing.T) {
	a, o := completionFixture(DirectionForward)
	child := a.Handle.Key
	a.Handle.Key = Key{Component: ComponentBroker, DomainID: "parent", ConfiguredNID: "coordinator@roll", ConfigPath: "/fixture/broker.yaml"}
	for _, receipt := range []*Receipt{&a.Control, &a.Resources, &a.Released} {
		receipt.Stamp.Attempt = a.Handle
		receipt.Census.Stamp = receipt.Stamp
		receipt.Census.Resources[0].Owner = sourceOwner()
		receipt.Census.Resources[0].Generation = 9
	}
	o.Census.Stamp.Attempt = a.Handle
	o.Census.Resources[0].Owner = sourceOwner()
	o.Census.Resources[0].Generation = 9
	o.Current = cloneAttempt(a)
	if err := Completed(a, o); err != nil {
		t.Fatal("coordinator incorrectly claimed the child's ownership:", err)
	}
	a.Handle.Key = child
	for _, receipt := range []*Receipt{&a.Control, &a.Resources, &a.Released} {
		receipt.Stamp.Attempt = a.Handle
		receipt.Census.Stamp = receipt.Stamp
	}
	o.Census.Stamp.Attempt = a.Handle
	o.Current = cloneAttempt(a)
	if Completed(a, o) == nil {
		t.Fatal("component receipt was allowed to borrow another owner's live generation")
	}
}
