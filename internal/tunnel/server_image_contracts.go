// origin: hot-update S6 plan §2.1 / §12.1 P00
package tunnel

import (
	"crypto/tls"
	"log/slog"
	"os"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/sealedstate"
)

type ServerImage struct {
	Schema          string                     `json:"schema"`
	SchemaVersion   uint32                     `json:"schema_version"`
	Generation      uint64                     `json:"generation"`
	IssuerBootID    string                     `json:"issuer_boot_id"`
	InstanceID      string                     `json:"instance_id"`
	OwnerGeneration uint64                     `json:"owner_generation"`
	ReplayLimit     int64                      `json:"replay_limit"`
	ReplayUsed      int64                      `json:"replay_used"`
	Listeners       []listenerimage.Descriptor `json:"listeners"`
	StateResource   string                     `json:"state_resource"`
	Endpoints       []EndpointImage            `json:"endpoints"`
	Flows           flow.Image                 `json:"flows"`
}

type ServerImportConfig struct {
	PublicHost  string
	Lookup      TokenLookup
	Certificate *tls.Certificate
	Flow        FlowConfig
	Logger      *slog.Logger
}

type FileSource interface {
	TakeFile(string) (*os.File, error)
}
type Salvage struct{ Resources []handoff.Resource }

// P06 supplies the runtime safe point; P07 alone supplies the private codec.
type serverStateSnapshot struct {
	Records   []sealedstate.Record
	Resources []handoff.Resource
}
type serverRuntimeCut interface {
	snapshot() (serverStateSnapshot, error)
	flowExport() (flow.Image, *sealedstate.Snapshot, error)
	thaw(controlbudget.Deadline) error
	discard() error
}

// ServerRuntimeCut exposes only the type identity for the isolated compile stub.
// Its private methods remain implementable only by the tunnel runtime owner.
type ServerRuntimeCut = serverRuntimeCut
type FreezeServerRuntimeFunc = func(*Server, controlbudget.Deadline) (serverRuntimeCut, error)

type ServerFrozen interface {
	Export() (ServerImage, []handoff.Resource, error)
	Thaw(controlbudget.Deadline) error
	Discard() error
}
type ServerImported interface {
	Server() *Server
	Activate(controlbudget.Deadline) error
	DetachPristine() (Salvage, error)
}
type FreezeServerFunc = func(*Server, controlbudget.Deadline) (ServerFrozen, error)
type ImportServerFunc = func(ServerImportConfig, ServerImage, FileSource) (ServerImported, Salvage, error)
