package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// origin: d9_external_review_test.go (renamed in B6) — docs/reviews/d9-external-review.md
func TestD9ExternalReviewNatsconfTakeoverRendersPeerSet(t *testing.T) {
	src, err := os.ReadFile("cluster_natsconf.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(src), "Peers:         []natscluster.Broker{self}") {
		t.Fatalf("takeover-natsconf renders only the local broker, so growth cannot produce a routed NATS mesh")
	}
}

// origin: batch B2 independent external review F1
func TestManualTakeoverAcceptsExplicitlyDisabledJetStream(t *testing.T) {
	for _, disabled := range []string{"false", "disabled"} {
		t.Run(disabled, func(t *testing.T) {
			confPath := filepath.Join(t.TempDir(), "nats.conf")
			conf := "server_name: \"solo\"\nlisten: \"127.0.0.1:4222\"\njetstream: " + disabled + "\n"
			if err := os.WriteFile(confPath, []byte(conf), 0o600); err != nil {
				t.Fatal(err)
			}

			cmd := &cobra.Command{}
			cmd.SetOut(&bytes.Buffer{})
			var stderr bytes.Buffer
			cmd.SetErr(&stderr)
			err := runNatsconfTakeover(cmd, &natsconfTakeoverFlags{
				confPath:         confPath,
				secretsDir:       "/etc/tether/secrets",
				serverName:       "solo",
				accountIssuer:    "AACCOUNT",
				brokerNkey:       "UBROKER",
				routeURL:         "nats://127.0.0.1:6222",
				clusterListen:    "0.0.0.0:6222",
				plan:             true,
				skipDryRun:       true,
				allowPartialMesh: true,
			})
			if err != nil {
				t.Fatalf("manual takeover rejected `jetstream: %s`: %v; explicit disablement must not be "+
					"treated as an enabled JetStream block missing store_dir", disabled, err)
			}
			if got := stderr.String(); strings.Contains(got, "STANDALONE-JS") ||
				strings.Contains(got, "RESET the JS store") {
				t.Fatalf("manual takeover accepted `jetstream: %s` but then emitted the destructive "+
					"standalone-JS reset warning:\n%s\nAn explicitly disabled subsystem has no active "+
					"standalone JS meta to migrate; telling the operator to remove its dormant store is "+
					"the same raw-presence/enablement confusion at the next call site.", disabled, got)
			}
		})
	}
}
