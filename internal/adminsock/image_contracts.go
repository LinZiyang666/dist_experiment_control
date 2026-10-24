// origin: hot-update S6 plan §2.1 / §12.1 P00
package adminsock

import (
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/listenerimage"
)

type StartInheritedFunc = func(*Server, listenerimage.Keeper, controlbudget.Deadline) error
type FenceImageFunc = func(*Server, controlbudget.Deadline) error
type JoinImageFunc = func(*Server, controlbudget.Deadline) error
type DetachImageFunc = func(*Server) (listenerimage.Keeper, error)

// Reload deferrals share the existing transient code without a new wire epoch.
// origin: hot-update S6 plan §9.1
const CodeUpgradeDeferred = "upgrade_deferred"
