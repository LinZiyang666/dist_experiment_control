package main

import (
	"os"
	"strings"
	"testing"
)

func TestD9ExternalReviewNatsconfTakeoverRendersPeerSet(t *testing.T) {
	src, err := os.ReadFile("cluster_natsconf.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(src), "Peers:         []natscluster.Broker{self}") {
		t.Fatalf("takeover-natsconf renders only the local broker, so growth cannot produce a routed NATS mesh")
	}
}
