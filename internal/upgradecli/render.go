package upgradecli

import (
	"io"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// ExitCode uses the shared transaction error classification.
func ExitCode(err error) int { return upgradetx.ExitCode(err) }

// Render uses the shared transaction View presentation.
func Render(w io.Writer, view upgradetx.View, asJSON bool) error {
	return upgradetx.Render(w, view, asJSON)
}
