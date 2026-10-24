package xferimage

import (
	"reflect"
	"strconv"
	"testing"

	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// origin: S7 internal review rv-jsnats F3 / s7-p9jsn2 Requests 2
func TestWorkerConsumerCleanupCodecClosesDetachedState(t *testing.T) {
	state := workerFixture("empty")
	state.ConsumerCleanup = []x.ConsumerCleanupState{{OwnerID: "owner", Deletes: []x.ConsumerDelete{{Stream: "OBJ_bucket", StreamCreated: "2026-10-13T12:00:00Z", Consumer: "consumer"}}}}
	encoded, err := EncodeWorker(state)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeWorker(encoded)
	if err != nil || !reflect.DeepEqual(state, decoded) {
		t.Fatalf("codec lost detached deletion: %v", err)
	}
	clone := CloneWorkerState(state)
	clone.ConsumerCleanup[0].Deletes[0].Consumer = "changed"
	if state.ConsumerCleanup[0].Deletes[0].Consumer != "consumer" {
		t.Fatal("clone aliases original deletion evidence")
	}
	cases := []struct {
		name   string
		mutate func(*x.WorkerState)
	}{
		{"empty owner", func(s *x.WorkerState) { s.ConsumerCleanup[0].OwnerID = "" }},
		{"duplicate owner", func(s *x.WorkerState) { s.ConsumerCleanup = append(s.ConsumerCleanup, s.ConsumerCleanup[0]) }},
		{"duplicate consumer", func(s *x.WorkerState) {
			s.ConsumerCleanup = append(s.ConsumerCleanup, x.ConsumerCleanupState{OwnerID: "second", Deletes: s.ConsumerCleanup[0].Deletes})
		}},
		{"foreign stream", func(s *x.WorkerState) { s.ConsumerCleanup[0].Deletes[0].Stream = "history-session" }},
		{"missing incarnation", func(s *x.WorkerState) { s.ConsumerCleanup[0].Deletes[0].StreamCreated = "" }},
		{"noncanonical incarnation", func(s *x.WorkerState) { s.ConsumerCleanup[0].Deletes[0].StreamCreated = "2026-10-13T12:00:00+00:00" }},
		{"invalid consumer", func(s *x.WorkerState) { s.ConsumerCleanup[0].Deletes[0].Consumer = "consumer/foreign" }},
		{"dotted bucket", func(s *x.WorkerState) { s.ConsumerCleanup[0].Deletes[0].Stream = "OBJ_bucket.foreign" }},
		{"dotted consumer", func(s *x.WorkerState) { s.ConsumerCleanup[0].Deletes[0].Consumer = "consumer.foreign" }},
		{"dashed owner", func(s *x.WorkerState) { s.ConsumerCleanup[0].OwnerID = "owner-foreign" }},
		{"total deletion bound", func(s *x.WorkerState) {
			for i := 0; i < 1024; i++ {
				s.ConsumerCleanup[0].Deletes = append(s.ConsumerCleanup[0].Deletes, x.ConsumerDelete{Stream: "OBJ_bucket", StreamCreated: "2026-10-13T12:00:00Z", Consumer: "consumer" + strconv.Itoa(i)})
			}
		}},
		{"owner bound", func(s *x.WorkerState) {
			for i := 0; i < 1024; i++ {
				s.ConsumerCleanup = append(s.ConsumerCleanup, x.ConsumerCleanupState{OwnerID: "owner" + strconv.Itoa(i)})
			}
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			s := CloneWorkerState(state)
			test.mutate(&s)
			if _, err := EncodeWorker(s); err == nil {
				t.Fatal("invalid consumer closure was exportable")
			}
		})
	}
}

// origin: S7 internal review rv-lifecycle F5
func TestWorkerImageRejectsDuplicateAndDanglingRevocations(t *testing.T) {
	for _, mutate := range []func(*x.WorkerState){
		func(s *x.WorkerState) { s.Revocations = append(s.Revocations, s.Revocations[0]) },
		func(s *x.WorkerState) { s.Revocations[0].Identity.EntryNonce = "retired" },
	} {
		s := workerFixture("courier")
		mutate(&s)
		if _, err := EncodeWorker(s); err == nil {
			t.Fatal("unbounded or dangling revocation passed image validation")
		}
	}
}
