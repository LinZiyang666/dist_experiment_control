package drillobserve

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve/infrastructure"
	"github.com/LinZiyang666/tether/internal/drillobserve/receipt"
)

const RuntimeSampleKind = receipt.RuntimeSampleKind
const RuntimeUnknownCounters = receipt.RuntimeUnknownCounters

var ErrRuntimeSample = receipt.ErrRuntimeSample

type RuntimeBinding = receipt.RuntimeBinding
type RuntimeCounters = receipt.RuntimeCounters

type runtimeRequest struct {
	nonce [12]byte
	scope [32]byte
	at    uint64
}

func EncodeRuntimeSample(c RuntimeCounters) (Event, error) { return receipt.EncodeRuntimeSample(c) }
func DecodeRuntimeSample(e Event) (RuntimeCounters, error) { return receipt.DecodeRuntimeSample(e) }
func RuntimeSampleRequest(nonce [12]byte, scope [32]byte) ([]byte, error) {
	return receipt.RuntimeSampleRequest(nonce, scope)
}
func ValidateRuntimeSampleACK(data []byte, sample RuntimeCounters, sealedSequence uint64) error {
	return receipt.ValidateRuntimeSampleACK(data, sample, sealedSequence)
}

func EmitRuntimeSample(c RuntimeCounters) error {
	if Hook == nil {
		return nil
	}
	e, err := EncodeRuntimeSample(c)
	if err != nil {
		poisonRuntimeSamples()
		return err
	}
	Hook(e)
	return nil
}

// Custody polling shares Run's existing exit cancellation. It does not acquire
// proxy/carrier locks or introduce a root that can outlive the agent.
// origin: hot-update S5 plan §9.1 / DRILL-OBSERVER
// origin: hot-update S10 plan §5.3 / P06
func RunRuntimeSamples(done <-chan struct{}, b RuntimeBinding, observeDomains func()) {
	if Hook == nil {
		return
	}
	select {
	case <-done:
		return
	default:
	}
	base, err := runtimeSampleIdentity(b)
	if err != nil {
		poisonRuntimeSamples()
		return
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if observeDomains != nil {
				observeDomains()
			}
			c := base
			c.CutID = runtimeCutIdentity(c) // origin: S8 internal review activation-and-route-migration F4
			if base.Sequence == math.MaxUint64 {
				poisonRuntimeSamples()
				return
			}
			base.Sequence++
			c.Sequence = base.Sequence
			if request, ok := takeRuntimeRequest(); ok {
				c.Nonce, c.ScopeDigest = request.nonce, hex.EncodeToString(request.scope[:])
			} else {
				if _, err := rand.Read(c.Nonce[:]); err != nil {
					poisonRuntimeSamples()
					return
				}
				scope := sha256.Sum256([]byte(b.Domain + ":" + b.InstanceID))
				c.ScopeDigest = hex.EncodeToString(scope[:])
			}
			// Take the challenge before measuring. A request arriving between
			// measurement and mailbox receive would otherwise poison a valid tick
			// because its BOOTTIME preceded that request.
			// origin: hot-update S10 plan §5.3 / P06
			if err := fillRuntimeSample(&c); err != nil {
				poisonRuntimeSamples()
				return
			}
			if EmitRuntimeSample(c) != nil {
				return
			}
		}
	}
}

func decodeRuntimeRequest(data []byte) (runtimeRequest, error) {
	nonce, scope, err := receipt.DecodeRuntimeRequest(data)
	return runtimeRequest{nonce: nonce, scope: scope}, err
}

func runtimeToken(value string, empty bool) bool { return receipt.RuntimeToken(value, empty) }

// The custody root retains its API; infrastructure observation has its own leaf.
// origin: S10 internal review MAKETEST-GATES F8
const InfrastructureAttachmentKind = infrastructure.InfrastructureAttachmentKind
const InfrastructurePrefixKind = infrastructure.InfrastructurePrefixKind
const maxInfrastructureSources = infrastructure.MaxSources
const MaxInfrastructureReceipt = infrastructure.MaxInfrastructureReceipt

var ErrInfrastructureObservation = infrastructure.ErrInfrastructureObservation
var infrastructureSources = &infrastructure.Registry{Rows: make(map[[16]byte]func(int64) error)}

// The adapter erases named deadline types without importing their owners.
// This preserves callers' exact absolute clock values across the neutral port.
// origin: S10 internal review ARCH-REDS R2
func RegisterInfrastructure[D ~int64](id [16]byte, sample func(D) error) {
	var neutral func(int64) error
	if sample != nil {
		neutral = func(d int64) error { return sample(D(d)) }
	}
	infrastructureSources.Register(id, neutral, Hook != nil)
}
func UnregisterInfrastructure(id [16]byte) {
	infrastructureSources.Unregister(id)
}
func SampleInfrastructure[D ~int64](d D) error {
	return infrastructureSources.Sample(int64(d), infrastructurePlatformSupported(), Hook != nil, transferNow)
}
func RecordInfrastructure(kind string, id [16]byte, step uint32, data []byte) error {
	return infrastructure.RecordInfrastructure(kind, id, step, data, Hook)
}
func RecordAttachment(a receipt.AttachmentProjection) error {
	return infrastructure.RecordAttachment(a, Hook)
}
func ProcessFDCount() int {
	return infrastructure.ProcessFDCount()
}
