package xferserve

import (
	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/natsconf"
	"log/slog"
	"os"
	"sync"
)

// LoadedSystemPrincipals is the production SystemPrincipals source: the managed
// $SYS block of the NATS config this broker actually runs with. It re-reads the
// file only when its identity changes, so a reload that adds or rotates the
// infra principals is recognised without restarting the broker, while an
// unchanged file costs one stat per proven CONNECT. Any read/validation error
// returns no principal: SYS admission fails closed and every ordinary role is
// unaffected. An empty path (in-process brokers) yields the nil seam.
// origin: hot-update S9 plan §3.3 / P12 / T27; S7 lead integration R8
func LoadedSystemPrincipals(confPath string, logger *slog.Logger) func() []authcallout.SystemPrincipal {
	if confPath == "" {
		return nil
	}
	var mu sync.Mutex
	var seen os.FileInfo
	var cached []authcallout.SystemPrincipal
	return func() []authcallout.SystemPrincipal {
		mu.Lock()
		defer mu.Unlock()
		info, err := os.Stat(confPath)
		if err == nil && seen != nil && os.SameFile(seen, info) && info.ModTime().Equal(seen.ModTime()) && info.Size() == seen.Size() {
			return clonePrincipals(cached)
		}
		// A refused version is remembered too: every CONNECT re-parsing and
		// logging the same broken file would turn one bad edit into a flood.
		cached = nil
		var descriptors []natsconf.SystemPrincipalDescriptor
		if err == nil {
			descriptors, err = natsconf.LoadedSystemPrincipalDescriptors(confPath)
		}
		if err != nil {
			if logger != nil {
				logger.Warn("authcallout: managed $SYS principals unavailable; SYS admission fails closed", "conf", confPath, "err", err)
			}
			seen = info // nil when the stat itself failed: the next CONNECT retries
			return nil
		}
		for _, d := range descriptors {
			cached = append(cached, authcallout.SystemPrincipal{Nkey: d.Nkey, Publish: append([]string(nil), d.Publish...), Subscribe: append([]string(nil), d.Subscribe...)})
		}
		seen = info
		return clonePrincipals(cached)
	}
}

// clonePrincipals hands the handler fresh slices; it cannot widen the cache.
func clonePrincipals(in []authcallout.SystemPrincipal) []authcallout.SystemPrincipal {
	out := make([]authcallout.SystemPrincipal, 0, len(in))
	for _, p := range in {
		out = append(out, authcallout.SystemPrincipal{Nkey: p.Nkey, Publish: append([]string(nil), p.Publish...), Subscribe: append([]string(nil), p.Subscribe...)})
	}
	return out
}
