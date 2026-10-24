// origin: hot-update S6 plan §2.1 / §12.1 P00
package httplisten

import (
	"net/http"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/listenerimage"
)

// Fence and Join cover accepted handlers, not only the Serve goroutine.
type ImageOwner interface {
	Fence(controlbudget.Deadline) error
	Join(controlbudget.Deadline) error
	Resume() error
	Stop(controlbudget.Deadline) error
}
type StartImageFunc = func(listenerimage.Keeper, http.Handler, string, controlbudget.Deadline) (ImageOwner, error)
