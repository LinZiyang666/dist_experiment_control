package caddyconfig

import (
	"context"
	"errors"
	"net/http"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// Load checks current state before sending, then reads the actual applied
// config on the same socket. An ambiguous write/reply poisons this binding;
// neither HTTP retries nor an unconditionally replayed POST can change a new
// invocation after the old peer has exited.
// origin: hot-update S9 plan §5.1 / T19
func (a *Admin) Load(ctx context.Context, cfg *Config) (c.Digest, error) {
	if ctx == nil {
		return c.Digest{}, ErrAdmin
	}
	ctx, cancel := context.WithTimeout(ctx, upgradebudget.RequestLatency)
	defer cancel()
	a.mu.Lock()
	defer a.mu.Unlock()
	if cfg == nil || cfg.adminSocket != a.socket {
		return c.Digest{}, ErrAdmin
	}
	current, err := a.current(ctx)
	if err != nil {
		return c.Digest{}, err
	}
	if current == cfg.digest {
		return current, nil
	}
	_, err = a.exchange(ctx, http.MethodPost, "/load", cfg.data)
	if err != nil && !errors.Is(err, ErrOutcomeUnknown) {
		return c.Digest{}, err
	}
	if err == nil {
		current, err = a.current(ctx)
	}
	if err != nil {
		current, err = reloadReadback(ctx, a)
		if err != nil {
			return c.Digest{}, ErrOutcomeUnknown
		}
	}
	if current != cfg.digest {
		return c.Digest{}, ErrOutcomeUnknown
	}
	return current, nil
}

// Stock reload can close the old HTTP connection after applying the config.
// Poison that binding and reconnect for a read only, within Load's original
// deadline. OpenAdmin checks the original full Process on the new socket: a
// successor at the same pathname cannot supply evidence for this invocation.
// The caller holds a.mu; no retry here can replay /load or authorize /stop.
// origin: S9 internal review INFRA-SUITE-SUPPLIES F1 / hot-update S9 plan §5.1 / T19
func reloadReadback(ctx context.Context, a *Admin) (c.Digest, error) {
	_ = a.close()
	readback, err := OpenAdmin(ctx, a.socket, a.expected, a.uid, a.verify)
	if err != nil {
		return c.Digest{}, ErrOutcomeUnknown
	}
	defer func() { _ = readback.Close() }()
	return readback.Current(ctx)
}
