package infraroot

import (
	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"github.com/LinZiyang666/tether/internal/infrapriv"
	"github.com/LinZiyang666/tether/internal/ingressswitch"
)

// installedRecord is public, root-owned configuration. It contains neither
// seeds nor caller-selected paths. Mutable proofs live in separately hashed
// root records and are revalidated at each effect boundary.
// origin: S9 internal review rv-infra F01
type installedRecord struct {
	Version uint16
	Purpose string
	Scope   infrahost.CensusScope
	Units   []infrahost.UnitPolicy
}

type installation struct {
	files       *installedFiles
	manager     infrahost.UnitManager
	ingress     ingressswitch.Backend
	uid, gid    uint32
	boot        string
	census      func(infrahost.CensusScope, c.Deadline) (c.Census, error)
	process     func(int32, c.Deadline) (c.Process, error)
	before      func(c.Deadline) ([]c.Process, error)
	initial     func(c.Deadline) (infrahost.HostState, error)
	mainProcess func(infrahost.UnitTarget, c.Deadline) (c.Process, error)
}

func (i *installation) policy() (infrapriv.Policy, error) {
	data, err := i.files.Read(stateDirectory+"/policy.json", c.MaxRequestBytes)
	if err != nil {
		return infrapriv.Policy{}, err
	}
	policy, err := infrapriv.DecodePolicy(data)
	if err != nil || policy.HostBootID != i.boot || policy.ServiceUID != i.uid || policy.ServiceGID != i.gid {
		return infrapriv.Policy{}, infrapriv.ErrPolicyStale
	}
	return policy, nil
}

func installedBudget(d c.Deadline, stop c.Stop) error {
	select {
	case <-stop:
		return infrapriv.ErrBudget
	default:
	}
	_, err := controlbudget.Remaining(d)
	return err
}
