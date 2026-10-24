package agent

import "github.com/LinZiyang666/tether/internal/agentstate"

// Preserve the CLI and agent cache API while disk operations live in the persistence leaf.
// origin: hot-update S5 plan §10 / AGENT-SLIM-S5.
type rosterCacheStore = agentstate.CacheStore
