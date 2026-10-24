package brokerimage

import (
	"errors"
	"os"
	"sync"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusterio"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/storage"
)

// clusterStore is the one owner of the current Node and the original lock OFD.
// It cannot manufacture a close receipt or open a second pool after a partial
// shutdown. The broker invokes PrepareImageShutdown and Node.Shutdown in Run;
// CloseForImage only collects their completed receipt.
// origin: hot-update S6 plan §3.2 / T10
type clusterStore struct {
	mu     sync.Mutex
	node   *cluster.Node
	lock   *cluster.DataDirLock
	config cluster.ProductionConfig
	closed bool
}

func NewClusterStore(node *cluster.Node, lock *cluster.DataDirLock, config cluster.ProductionConfig) (StoreOwner, error) {
	if node == nil || lock == nil {
		return nil, errors.New("brokerimage: cluster store requires node and original lock")
	}
	resource, err := lock.Export()
	if err != nil {
		return nil, err
	}
	if resource.Files["lock"] == nil {
		return nil, errors.New("brokerimage: cluster lock resource missing")
	}
	return &clusterStore{node: node, lock: lock, config: config}, nil
}

// NewClusterStoreForImage takes the inherited lock before the first store or
// transport side effect. Constructor errors retain a non-nil owner of the lock
// and every returned partial Node; a nil Node means construction opened none.
// Adoption refusal leaves file with the caller's salvage registry.
// origin: hot-update S6 plan §2.1 / §3.2 / T10
func NewClusterStoreForImage(config cluster.ProductionConfig, stamp storage.StoreStamp, file *os.File, deadline controlbudget.Deadline) (StoreOwner, error) {
	if _, err := clusterio.Remaining(deadline); err != nil {
		return nil, err
	}
	if config.DataDir != stamp.Identity.DataDir || config.DBPath != stamp.Identity.Path {
		return nil, errors.New("brokerimage: current store and lock binding mismatch")
	}
	lock, err := cluster.AdoptDataDirLock(config.DataDir, file)
	if err != nil {
		return nil, err
	}
	owner := &clusterStore{lock: lock, config: config}
	owner.node, err = cluster.NewProductionForImage(config, stamp)
	return owner, err
}

func (s *clusterStore) Quiesce(deadline controlbudget.Deadline) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("brokerimage: cluster store owner retired")
	}
	return cluster.QuiesceImageStore(s.node, deadline)
}

// ResumeClusterStore reopens admission after a reversible control/flow freeze
// failure, before PrepareImageShutdown has fenced the Raft transport.
func ResumeClusterStore(owner StoreOwner) error {
	s, ok := owner.(*clusterStore)
	if !ok {
		return errors.New("brokerimage: expected cluster store owner")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("brokerimage: cluster store owner retired")
	}
	return cluster.ResumeImageStore(s.node)
}

func (s *clusterStore) CloseForImage(deadline controlbudget.Deadline) (storage.StoreSeal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return storage.StoreSeal{}, errors.New("brokerimage: cluster store owner retired")
	}
	if _, err := clusterio.Remaining(deadline); err != nil {
		return storage.StoreSeal{}, err
	}
	return cluster.ClosedImageStore(s.node)
}

func (s *clusterStore) ReopenCurrent(stamp storage.StoreStamp, deadline controlbudget.Deadline) (StoreOwner, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("brokerimage: cluster store owner retired")
	}
	if _, err := clusterio.Remaining(deadline); err != nil {
		return nil, err
	}
	seal, err := cluster.ClosedImageStore(s.node)
	if err != nil {
		return nil, err
	}
	if stamp.Identity != seal.Stamp.Identity || stamp.AppliedIndex != seal.Stamp.AppliedIndex || stamp.AppliedTerm != seal.Stamp.AppliedTerm || stamp.RaftLastIndex != seal.Stamp.RaftLastIndex || stamp.RaftConfigIndex != seal.Stamp.RaftConfigIndex || stamp.RaftConfigDigest != seal.Stamp.RaftConfigDigest || stamp.Schema.Digest != seal.Stamp.Schema.Digest || stamp.Schema.ObjectDigest != seal.Stamp.Schema.ObjectDigest || stamp.Journal != seal.Stamp.Journal || stamp.Synchronous != seal.Stamp.Synchronous {
		return nil, errors.New("brokerimage: reopen stamp differs from final store receipt")
	}
	node, err := cluster.NewProductionForImage(s.config, stamp)
	if err != nil {
		if node != nil {
			s.node = node
		} // partial constructor owner stays reachable
		return s, err
	}
	// Transfer terminal responsibility to a distinct generation. The old slot
	// can retire this wrapper without unlocking the new generation's original OFD.
	current := &clusterStore{node: node, lock: s.lock, config: s.config}
	s.lock = nil
	s.closed = true
	return current, nil
}

func (s *clusterStore) Stamp() (storage.StoreStamp, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return storage.StoreStamp{}, errors.New("brokerimage: cluster store owner retired")
	}
	if seal, err := cluster.ClosedImageStore(s.node); err == nil {
		return seal.Stamp, nil
	}
	return cluster.ImageStoreStamp(s.node)
}

func (s *clusterStore) LockResource() (handoff.Resource, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return handoff.Resource{}, errors.New("brokerimage: cluster store owner retired")
	}
	return s.lock.Export()
}

// StopClusterStore is the terminal path. A failed Node close retains the flock;
// releasing it would let an offline writer race an unknown live pool.
func StopClusterStore(owner StoreOwner) error {
	s, ok := owner.(*clusterStore)
	if !ok {
		return errors.New("brokerimage: expected cluster store owner")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if s.node != nil {
		if err := s.node.Shutdown(); err != nil {
			return err
		}
	}
	if err := s.lock.Release(); err != nil {
		return err
	}
	s.closed = true
	return nil
}

// ClusterStoreNode exposes the current generation to Run without retaining an
// old Node in a defer or a control-root closure.
func ClusterStoreNode(owner StoreOwner) (*cluster.Node, error) {
	s, ok := owner.(*clusterStore)
	if !ok {
		return nil, errors.New("brokerimage: expected cluster store owner")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("brokerimage: cluster store owner retired")
	}
	return s.node, nil
}

var _ StoreOwner = (*clusterStore)(nil)
