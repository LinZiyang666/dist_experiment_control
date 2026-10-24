package xferbridge

import (
	"sort"

	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/storage"
)

// Projection contains current process lineage and control decisions. Transfer
// refs are preserved from the leaf export, never rebuilt from a cold census.
// origin: hot-update S7 plan §8.2 / P24
type Projection struct {
	Generation                                                          uint64
	InstanceID, BootID, ExporterSHA256, CandidateSHA256, PreviousSHA256 string
	ConfigDigest, CutID                                                 string
	Args                                                                []string
	Maintenance                                                         []brokermaint.LocalLease
	HomeHistory                                                         []byte
	// Extra are borrowed resources that travel outside the broker private
	// state: only the sealed S8 upgrade-cut-v1 adjunct (brokerimage admits no
	// other unexported resource). They are never recorded as broker-owned.
	// origin: hot-update S8 plan §6.1 / §11.2 P17; S8 BROKER-EXPORTER R-11
	Extra []handoff.Resource
}

func WrapCut(cut brokerimage.Cut, projection func() (Projection, error)) (*Cut, error) {
	if cut == nil || projection == nil {
		return nil, ErrOwner
	}
	return &Cut{Cut: cut, projection: projection}, nil
}

// Cut supplies process lineage and the broker codec over the leaf's
// final closed-store graph. Its sealed private state records actual file IDs;
// broker metadata never contains ticket keys or data-plane plaintext.
// origin: hot-update S6 plan §2.2 / §3.2 / rv-cluster-2
type Cut struct {
	brokerimage.Cut
	projection func() (Projection, error)
	state      *sealedstate.Snapshot
}

func (c *Cut) Export(seal storage.StoreSeal) (brokerimage.Bundle, []handoff.Resource, error) {
	bundle, resources, err := c.Cut.Export(seal)
	if err != nil {
		return bundle, resources, err
	}
	return c.project(bundle, resources)
}

func (c *Cut) ImageCut() brokerimage.Cut { return c.Cut }

func (c *Cut) Prepare(_ brokerimage.Bundle, _ []handoff.Resource) (*handoff.Prepared, error) {
	return brokerimage.PrepareFinal(c.Cut, func(bundle brokerimage.Bundle, resources []handoff.Resource) (brokerimage.Bundle, []handoff.Resource, error) {
		bundle, resources, err := c.project(bundle, resources)
		// PrepareFinal transfers the final broker memfd to the leaf cut. This
		// wrapper owns only preliminary snapshots; two closers risk FD reuse.
		if err == nil {
			c.state = nil
		}
		return bundle, resources, err
	})
}

func (c *Cut) Thaw(d controlbudget.Deadline) error {
	if c.state != nil {
		if err := c.state.Close(); err != nil {
			return err
		}
		c.state = nil
	}
	return c.Cut.Thaw(d)
}

func (c *Cut) project(bundle brokerimage.Bundle, resources []handoff.Resource) (brokerimage.Bundle, []handoff.Resource, error) {
	o, err := c.projection()
	if err != nil {
		return bundle, resources, err
	}
	bundle.Generation = o.Generation
	bundle.InstanceID, bundle.BootID = o.InstanceID, o.BootID
	bundle.ExporterSHA256, bundle.CandidateSHA256, bundle.PreviousSHA256 = o.ExporterSHA256, o.CandidateSHA256, o.PreviousSHA256
	bundle.ConfigDigest, bundle.Args = o.ConfigDigest, o.Args
	bundle.StateResource = "broker-state." + o.CutID
	if !hasDomain(bundle.RequiredDomains, "flow-v1") {
		bundle.RequiredDomains = append(bundle.RequiredDomains, "flow-v1")
	}
	sort.Strings(bundle.RequiredDomains)
	bundle.Maintenance = o.Maintenance
	private := brokerimage.PrivateState{Runtime: brokerimage.RuntimeState{Role: bundle.Role, Mode: bundle.Mode,
		Args: bundle.Args, ConfigDigest: bundle.ConfigDigest, Generation: bundle.Generation, BootID: bundle.BootID,
		InstanceID: bundle.InstanceID, ExporterSHA256: bundle.ExporterSHA256, CandidateSHA256: bundle.CandidateSHA256,
		PreviousSHA256: bundle.PreviousSHA256, RequiredDomains: bundle.RequiredDomains, CutID: o.CutID}, Store: bundle.Seal,
		Control: brokerimage.ControlState{AdmissionGeneration: bundle.Generation, RequiredHandlers: []string{"business"},
			RequiredLoops: []string{}, Maintenance: bundle.Maintenance, HomeBaselines: []brokerimage.HomeBaseline{}}, Resources: []brokerimage.ResourceOwnership{}}
	private.Control.HomeHistory = o.HomeHistory
	for _, res := range resources {
		for name, file := range res.Files {
			identity, err := handoff.ResourceFileIdentity(file)
			if err != nil {
				return bundle, resources, err
			}
			purpose := "runtime-file"
			if res.ID == bundle.LockResource {
				purpose = "store-lock"
			}
			private.Resources = append(private.Resources, brokerimage.ResourceOwnership{ID: res.ID, Name: name, Kind: res.Kind,
				Data: res.Data, Identity: identity, Purpose: purpose, Owner: "broker", CutID: o.CutID})
		}
	}
	sort.Slice(private.Resources, func(i, j int) bool {
		return private.Resources[i].ID+"/"+private.Resources[i].Name < private.Resources[j].ID+"/"+private.Resources[j].Name
	})
	data, err := brokerimage.EncodePrivateState(private)
	if err != nil {
		return bundle, resources, err
	}
	state, err := sealedstate.New(bundle.StateResource, data)
	if err != nil {
		return bundle, resources, err
	}
	if c.state != nil {
		if err := c.state.Close(); err != nil {
			_ = state.Close()
			return bundle, resources, err
		}
	}
	c.state = state
	// The adjunct joins after the ownership records: Boot counts its one file
	// outside the private closure (brokerimage.Boot / prepareExtras).
	return bundle, append(append(resources, state.Resources...), o.Extra...), nil
}

// Discard releases only the projection memfd after leaf relinquishment.
// origin: hot-update S7 plan §8.2 / P24
func (c *Cut) Discard() error {
	if err := c.Cut.Discard(); err != nil {
		return err
	}
	err := c.state.Close()
	c.state = nil
	return err
}

func hasDomain(domains []string, domain string) bool {
	for _, value := range domains {
		if value == domain {
			return true
		}
	}
	return false
}
