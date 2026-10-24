package infraroot

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"github.com/LinZiyang666/tether/internal/infralaunch"
	"github.com/LinZiyang666/tether/internal/infrapriv"
	"github.com/LinZiyang666/tether/internal/infraroot/installfs"
)

// Offer joins the authenticated query with the same durable start intent used
// by the worker. A guessed transaction or local public descriptor cannot supply
// a permit, renew its original deadline, or select a different unit executable.
// origin: S9 internal review rv-infra F03
func (r *installedRecords) Offer(q infrapriv.LaunchQuery, peer c.Peer, d c.Deadline, stop c.Stop) (_ c.HostRequest, resultErr error) {
	if peer.PID != q.Expected.PID || peer.StartTicks != q.Expected.StartTicks || peer.BootID != q.Expected.BootID {
		return c.HostRequest{}, infrapriv.ErrPermission
	}
	if err := r.validateLauncher(q.Expected, q.Component, d, stop); err != nil {
		return c.HostRequest{}, err
	}
	state, head, err := r.root.journal.Read(d)
	if err != nil {
		return c.HostRequest{}, err
	}
	if state.Permit == (c.Digest{}) || state.LaunchClaimed || state.LaunchCandidate.Kind != c.ArtifactKind(q.Component) {
		return c.HostRequest{}, infrapriv.ErrBusy
	}
	left, err := controlbudget.Remaining(state.LaunchDeadline)
	if err != nil {
		return c.HostRequest{}, err
	}
	request := c.HostRequest{Version: 1, Op: c.LaunchClaim, Component: q.Component, Tx: state.Tx, Step: state.Step, Domain: r.policy.Domain, Sequence: state.Sequence + 1, PolicyRevision: r.policy.Revision, KeyID: q.Identity.KeyID, HostBootID: r.policy.HostBootID, Nonce: q.Nonce, Expected: q.Expected, Candidate: state.LaunchCandidate, Config: state.LaunchConfig, Payload: state.Permit, ExpectedJournal: head, RemainingNS: min(q.RemainingNS, uint64(left))}
	directory, locks, err := r.launchDirectories(request, d)
	if err != nil {
		return c.HostRequest{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, directory.Close()) }()
	defer func() { resultErr = errors.Join(resultErr, locks.Close()) }()
	request.Store, err = infralaunch.StoreWithLock(directory, locks, request.Component)
	return request, err
}

func (r *installedRecords) validateLauncher(expected c.Process, component c.Component, d c.Deadline, stop c.Stop) (resultErr error) {
	current, err := r.observe(expected, d)
	if err != nil {
		return err
	}
	target := infrahost.UnitNATS
	if component == c.Caddy {
		target = infrahost.UnitCaddy
	}
	main, err := r.installation.mainProcess(target, d)
	if err != nil || main != current {
		return infrapriv.ErrPermission
	}
	unit, err := r.installation.manager.Read(target, d, stop)
	if err != nil {
		return err
	}
	approved := false
	for _, policy := range r.record.Units {
		if policy.Target == target && !policy.Legacy && infrahost.ValidateEffectiveUnit(unit, policy) == nil {
			approved = true
		}
	}
	if !approved {
		return infrapriv.ErrPermission
	}
	executable, err := r.installation.files.Open("/usr/local/libexec/tether-infra-launch", 0)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, executable.Close()) }()
	digest, err := installfs.HashOpened(executable)
	if err != nil || digest != expected.ExeSHA256 {
		return infrapriv.ErrPermission
	}
	return nil
}

func (r *installedRecords) launchDirectories(request c.HostRequest, d c.Deadline) (*os.File, *os.File, error) {
	if request.Component == c.Caddy {
		view, err := infrahost.CurrentACMEView(r.root.storage, request.Tx, request.Domain, d)
		if err != nil {
			return nil, nil, err
		}
		locks, err := r.installation.files.Directory(stateDirectory+"/acme", false, 0, 0)
		if err != nil {
			return nil, nil, errors.Join(err, view.Directory.Close())
		}
		return view.Directory, locks, nil
	}
	directory, err := r.installation.serviceDirectory()
	if err != nil {
		return nil, nil, err
	}
	locks, err := r.installation.files.Directory(stateDirectory+"/locks", false, 0, 0)
	if err != nil {
		return nil, nil, errors.Join(err, directory.Close())
	}
	return directory, locks, nil
}

func (i *installation) serviceDirectory() (*os.File, error) {
	return i.files.ServiceDirectory("/var/lib/tether/jetstream", i.uid)
}

func (r *installedRecords) selection(request c.HostRequest, d c.Deadline, stop c.Stop) (infralaunch.Selection, error) {
	state, head, err := r.root.journal.Read(d)
	if err != nil {
		return infralaunch.Selection{}, err
	}
	if state.Tx != request.Tx || state.Step != request.Step || state.Permit != request.Payload || state.LaunchCandidate != request.Candidate || state.LaunchConfig != request.Config {
		return infralaunch.Selection{}, infrapriv.ErrPermission
	}
	if err := installedBudget(min(d, state.LaunchDeadline), stop); err != nil {
		return infralaunch.Selection{}, err
	}
	executable, err := r.installation.files.Open(stateDirectory+"/binary-"+digestName(request.Candidate.BinarySHA256), 0755)
	if err != nil {
		return infralaunch.Selection{}, err
	}
	r.hold(executable)
	config, err := r.installation.files.Open(stateDirectory+"/configs/"+digestName(request.Config), 0400)
	if err != nil {
		return infralaunch.Selection{}, err
	}
	r.hold(config)
	directory, locks, err := r.launchDirectories(request, d)
	if err != nil {
		return infralaunch.Selection{}, err
	}
	r.hold(directory)
	r.hold(locks)
	store, err := infralaunch.StoreWithLock(directory, locks, request.Component)
	if err != nil || store != request.Store {
		return infralaunch.Selection{}, infrapriv.ErrPermission
	}
	selection := infralaunch.Selection{Executable: executable, Config: config, Directory: directory, Locks: locks, Revision: state.Sequence, Journal: head}
	if request.Op == c.LaunchAck {
		if infralaunch.HasClaimCustody(r.root.launch, request) {
			return selection, nil
		}
		original := request
		original.Op = c.LaunchClaim
		event, err := r.root.journal.OperationRecord(original, d)
		if err != nil || event.Phase != infrahost.EffectApplied {
			return infralaunch.Selection{}, infrapriv.ErrPermission
		}
		var guard *infralaunch.Lock
		if request.Component == c.Caddy {
			guard, err = infralaunch.RecoverACMEWriter(directory, locks, request.Expected, d)
		} else {
			guard, err = infralaunch.RecoverStoreLock(directory, locks, request.Expected, d)
		}
		if err != nil {
			return infralaunch.Selection{}, err
		}
		r.hold(guard)
		selection.Guard, selection.Original, selection.Receipt = guard, &event.Request, &event.Receipt
	} else if request.Component == c.Caddy {
		view, err := infrahost.CurrentACMEView(r.root.storage, request.Tx, request.Domain, d)
		if err != nil {
			return infralaunch.Selection{}, err
		}
		if err := view.Directory.Close(); err != nil {
			return infralaunch.Selection{}, err
		}
		writer, err := infrahost.ClaimACMEWriter(r.root.storage, request.Tx, request.Domain, view.Revision, d, stop)
		if err != nil {
			return infralaunch.Selection{}, err
		}
		guard, err := infralaunch.AdoptACMEWriter(writer.Directory, locks, writer.Lease)
		err = errors.Join(err, writer.Directory.Close(), writer.Lease.Close())
		if err != nil {
			if guard != nil {
				err = errors.Join(err, guard.Close())
			}
			return infralaunch.Selection{}, err
		}
		r.hold(guard)
		selection.Guard = guard
	}
	return selection, nil
}

func (r *installedRecords) storeCensus(store c.Store, d c.Deadline, stop c.Stop) (_ c.Census, resultErr error) {
	if err := installedBudget(d, stop); err != nil {
		return c.Census{}, err
	}
	census, err := r.Inspect(r.policy.Domain, d)
	if err != nil || !census.Complete {
		return c.Census{}, c.ErrIncomplete
	}
	launcher, err := r.installation.files.Open("/usr/local/libexec/tether-infra-launch", 0)
	if err != nil {
		return c.Census{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, launcher.Close()) }()
	digest, err := installfs.HashOpened(launcher)
	if err != nil {
		return c.Census{}, err
	}
	nats, err := r.installation.serviceDirectory()
	if err != nil {
		return c.Census{}, err
	}
	natsStore, err := infralaunch.StoreIdentity(nats)
	err = errors.Join(err, nats.Close())
	if err != nil {
		return c.Census{}, err
	}
	group := "/system.slice/caddy.service"
	if store.Device == natsStore.Device && store.Inode == natsStore.Inode {
		group = "/system.slice/nats-server.service"
	}
	var owners []c.Process
	for _, process := range census.Processes {
		if process.Cgroup != group {
			continue
		}
		if process.ExeSHA256 == digest {
			continue
		}
		// Conservative coverage includes all managed stock owners, even aliases or
		// unknown configuration. A successful flock cannot erase a legacy owner.
		owners = append(owners, process)
	}
	var sockets []c.Socket
	for _, socket := range census.Sockets {
		if socket.Owner.Cgroup == group {
			sockets = append(sockets, socket)
		}
	}
	census.Processes, census.Sockets = owners, sockets
	census.LiveBusiness, census.OldOwnerPending = uint32(len(sockets)), uint32(len(sockets))
	census.Digest = c.Digest{}
	data, _ := json.Marshal(census)
	census.Digest = sha256.Sum256(data)
	return census, nil
}
