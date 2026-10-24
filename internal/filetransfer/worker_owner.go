package filetransfer

import (
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/xferagent"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferworker"
	"github.com/nats-io/nats.go"
)

// worker_owner.go owns the continuation worker's lifetime beside the legacy
// byte handlers: construction (production runtime, injected or imported),
// adoption by an image restore, link refusal without a runtime, terminal stop
// and the advertised transfer version.
// origin: hot-update S7 plan §8.1 / P20 / P21

// WorkerDependencies keeps the leaf constructor injectable without making an
// unimplemented contract stub reachable from production. A nil constructor
// selects the production composition (xferagent); Legacy keeps an agent on
// the cap0 path and never advertises transfer-v1. Imported, when set, supplies
// the worker restored by a same-PID image cut instead of a fresh owner.
// origin: hot-update S7 plan §8.1 / P20 / P21
type WorkerDependencies struct {
	New      xferworker.Factory
	Config   xferworker.Config
	Legacy   bool
	Imported func(*xferagent.Runtime) (xferworker.Manager, error)
	// Installed observes the constructed or imported owner exactly once, so the
	// agent can register it as the image owner before any cut can run.
	Installed func(*Handler)
}
type WorkerOwner struct {
	once    sync.Once
	handler *Handler
	loaded  atomic.Pointer[Handler]
	// importing, while an agent image restore runs, replaces NewWorker with
	// the sealed worker's import (see Adopt).
	importing func(*xferagent.Runtime, xferworker.Config) (xferworker.Manager, error)
}

// AdoptWorker keeps legacy callbacks inside the handler owner while the image
// importer receives only the ctx-free adoption operation.
// origin: S7 internal review GATES-PROD F1
func (d WorkerDependencies) AdoptWorker(o *WorkerOwner, build func() *Handler, adopt func(*xferagent.Runtime, xferworker.Config) (xferworker.Manager, error)) (*Handler, error) {
	return o.Adopt(d, build, adopt)
}

// Loaded is the completely built handler, or nil before first use. An image
// cut asks it for the bound worker instead of keeping a second owner reference.
func (o *WorkerOwner) Loaded() *Handler { return o.loaded.Load() }

func LoadWorkerHandler(o *WorkerOwner, deps WorkerDependencies, build func() *Handler) *Handler {
	o.once.Do(func() {
		h := build()
		o.handler = h
		if deps.New == nil && !deps.Legacy {
			// The production worker: real transport on the agent subtree, real
			// sender/receiver/courier factories, real dedicated data attachments.
			// origin: hot-update S7 plan §4 / §8.1 / P20
			rt, err := xferagent.New(xferagent.Host{SID: h.SID, NID: h.NID, Instance: h.Instance, JetStream: h.JetStream, Roots: h.Roots, Mode: h.Mode, Logger: h.Logger, Admit: h.AdmitCreation})
			if err != nil {
				h.WorkerError = err
				return
			}
			h.Runtime = rt
			deps.New, deps.Config = xferworker.NewWorker, rt.WorkerConfig()
		}
		if deps.New != nil {
			if h.NID != nil && h.Instance != nil {
				deps.Config.Identity.SID, deps.Config.Identity.NID, deps.Config.Identity.AgentInstance = h.SID, h.NID(), h.Instance()
			}
			if h.Runtime == nil {
				// Explicit factories retain the INTEG-G host ports; the default
				// production runtime already supplied its signed transport and actors.
				// origin: hot-update S7 plan §8.1 / P20
				h.ports = newWorkerPorts(h)
				h.ports.fillConfig(&deps.Config)
			}
			if o.importing != nil {
				h.Worker, h.WorkerError = o.importing(h.Runtime, deps.Config)
			} else if deps.Imported != nil {
				h.Worker, h.WorkerError = deps.Imported(h.Runtime)
			} else {
				h.Worker, h.WorkerError = deps.New(deps.Config)
			}
			if h.Worker == nil && h.WorkerError == nil {
				h.WorkerError = errors.New("transfer worker constructor returned no owner")
			}
		}
		if h.Runtime != nil {
			h.Runtime.Bind(h.Worker, func() string { return WorkerVersion(h) })
		}
		if deps.Installed != nil && h.Worker != nil && h.WorkerError == nil {
			deps.Installed(h)
		}
		o.loaded.Store(h)
	})
	return o.handler
}

// Adopt builds the handler exactly once around a worker supplied by adopt (an
// agent image restore's sealed worker) instead of a fresh one, before the
// image activates and before any handler admission. An owner whose handler
// was already built refuses: its worker is the current owner, not the image's.
// origin: hot-update S7 plan §8.1–§8.2 / P21 — agent image cut continuity
func (o *WorkerOwner) Adopt(deps WorkerDependencies, build func() *Handler, adopt func(*xferagent.Runtime, xferworker.Config) (xferworker.Manager, error)) (*Handler, error) {
	if deps.New != nil || deps.Legacy || adopt == nil {
		return nil, errors.New(xfercontract.CodeOwnerRecovering + ": no production transfer runtime to adopt into")
	}
	ran := false
	o.importing = func(rt *xferagent.Runtime, cfg xferworker.Config) (xferworker.Manager, error) {
		ran = true
		return adopt(rt, cfg)
	}
	h := LoadWorkerHandler(o, deps, build)
	o.importing = nil
	if !ran {
		cause := errors.New(xfercontract.CodeOwnerRecovering + ": transfer handler was built before the image import")
		if h != nil && h.WorkerError != nil {
			cause = errors.Join(cause, h.WorkerError)
		}
		return nil, cause
	}
	return h, h.WorkerError
}

// refuseLink answers a broker link control on an agent without the production
// runtime: a probe gets the explicit cap0 answer, anything else a refusal. It
// never runs the legacy commit path for a link envelope.
// origin: hot-update S7 plan §6.3 / §7 / P20
func refuseLink(nc *nats.Conn, msg *nats.Msg, instance func() string) bool {
	var carrier proto.TransferCommitReq
	if json.Unmarshal(msg.Data, &carrier) != nil || carrier.Link == nil {
		return false
	}
	if msg.Reply == "" {
		return true
	}
	result := proto.TransferLinkResult{}
	if instance != nil {
		result.InstanceID = instance()
	}
	if carrier.Link.Op != proto.TransferLinkProbe {
		result.Code = xfercontract.CodeOwnerRecovering
	}
	body, err := json.Marshal(proto.TransferCommitResp{OK: result.Code == "", Code: result.Code, Link: &result})
	if err == nil {
		_ = nc.Publish(msg.Reply, body)
	}
	return true
}

// Stop joins the original owner and retains it on failure for another attempt.
// Setting closed before the call blocks late prepares even when Stop times out.
// Ordinary NATS teardown deliberately does not invoke this terminal operation.
// origin: hot-update S7 plan §6.4 / §11.2 / P20
func StopWorker(h *Handler) error {
	h.closed.Store(true)
	h.stopMu.Lock()
	defer h.stopMu.Unlock()
	if h.Worker == nil || h.stopped {
		return nil
	}
	d, err := controlbudget.After(5 * time.Second)
	if err == nil {
		err = h.Worker.Stop(d)
	}
	h.stopped = err == nil
	return err
}
func WorkerVersion(h *Handler) string {
	if h.Worker != nil && h.WorkerError == nil && !h.closed.Load() {
		return xfercontract.Version
	}
	return ""
}

// Only the normal REGISTER installs capability; the inert farewell bypasses it.
// The envelope is additive while P01 supplies the corresponding proto field.
// origin: hot-update S7 plan §6.2 / §8.4 / P20
func MarshalWorkerRegister(h *Handler, req proto.NodeRegisterReq) ([]byte, error) {
	return json.Marshal(struct {
		proto.NodeRegisterReq
		TransferVersion string `json:"transfer_version,omitempty"`
	}{req, WorkerVersion(h)})
}
