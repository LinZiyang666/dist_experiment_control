package clusterharness

import (
	"context"
	"errors"
	"fmt"
	"time"

	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// Listening client ports do not prove that routed JetStream metadata is usable.
// Keep cold infrastructure setup separate from the product's readiness window:
// one complete route/metadata census and real read-only API replies through all
// ingress servers are required before any serve process can be launched.
// origin: hot-update S6 plan §5 / §10.1 / HARDGATE-B request 3
func waitRoutedJetStream(ctx context.Context, servers []*server.Server, connections []*nats.Conn) error {
	if len(servers) < 3 || len(connections) != len(servers) {
		return errors.New("quorum infrastructure witness requires every ingress server")
	}
	names := make(map[string]bool, len(servers))
	clients := make([]nats.JetStreamContext, len(servers))
	for i, s := range servers {
		if s == nil || connections[i] == nil || names[s.Name()] {
			return errors.New("quorum infrastructure witness has an ambiguous server set")
		}
		names[s.Name()] = true
		js, err := connections[i].JetStream()
		if err != nil {
			return err
		}
		clients[i] = js
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	stage := "route and metadata census"
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("quorum infrastructure readiness (%s): %w", stage, err)
		}
		ready, leaders := true, 0
		for _, s := range servers {
			if s.NumRoutes() < len(servers)-1 || !s.JetStreamIsClustered() || !s.JetStreamIsCurrent() {
				ready = false
				continue // a standalone JS leader has no metadata cluster to enumerate
			}
			if s.JetStreamIsLeader() {
				leaders++
				peers := s.JetStreamClusterPeers()
				seen := make(map[string]bool, len(peers))
				for _, peer := range peers {
					if !names[peer] || seen[peer] {
						ready = false
					}
					seen[peer] = true
				}
				ready = ready && len(seen) == len(names)
			}
		}
		ready = ready && leaders == 1
		stage = "route and metadata census"
		if ready {
			// Each short RPC inherits the same infrastructure deadline. A
			// timeout is absence of a witness, never readiness or a new budget.
			for i, js := range clients {
				probeCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				info, err := js.AccountInfo(nats.Context(probeCtx))
				cancel()
				if err != nil || info == nil {
					stage = "AccountInfo through " + servers[i].Name()
					ready = false
					break
				}
			}
		}
		if ready && ctx.Err() == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("quorum infrastructure readiness (%s): %w", stage, ctx.Err())
		case <-ticker.C:
		}
	}
}
