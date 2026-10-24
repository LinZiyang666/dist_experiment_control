package infralaunch

import (
	"crypto/rand"
	"errors"
	"os"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrapriv"
)

// LaunchClient has the same authenticated endpoint and HostRequest framing as
// ordinary clients. Query is read-only; Claim alone conveys the five fixed roles.
// origin: S9 internal review rv-infra F03
type LaunchClient interface {
	QueryLaunch(infrapriv.LaunchQuery, c.Deadline, c.Stop) (c.HostRequest, error)
	Exchange(c.HostRequest, []*os.File, c.Deadline, c.Stop) (c.HostReceipt, []*os.File, error)
	SubmitLaunchAck(c.HostRequest, c.Deadline, c.Stop) (func(), error)
}

// Run uses the fixed management endpoint. The component selects one of
// two unit policies, never an arbitrary unit, filename, argv or action.
func Run(component c.Component, d c.Deadline, stop c.Stop) (resultErr error) {
	if err := supported(); err != nil {
		return err
	}
	gid := uint32(os.Getgid())
	identity, err := infrapriv.LoadLaunchIdentity("/etc/tether/infra/credentials", gid)
	if err != nil {
		return err
	}
	key, err := infrapriv.LoadCredential("/etc/tether/infra/credentials", infrapriv.LocalLaunch, gid)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, key.Close()) }()
	client, err := infrapriv.NewClient(gid)
	if err != nil {
		return err
	}
	prepared, finish, err := AcquireLaunch(client, identity, key, component, d, stop)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, prepared.Close()) }()
	defer finish()
	return prepared.Exec(d, stop)
}

// AcquireLaunch sends Ack intent before returning to the sole exec call. It
// never treats successful transmission as an Ack receipt: the helper observes
// the candidate executable and inherited OFD after this launcher disappears.
// An exec failure closes the pending socket and all received references.
// origin: S9 internal review rv-infra F03 / F04
func AcquireLaunch(client LaunchClient, identity infrapriv.LaunchIdentity, key *infrapriv.Credential, component c.Component, d c.Deadline, stop c.Stop) (*Prepared, func(), error) {
	if err := admission(d, stop); err != nil {
		return nil, nil, err
	}
	self, err := CurrentProcess(os.Getenv("INVOCATION_ID"))
	if err != nil {
		return nil, nil, err
	}
	started, err := controlbudget.Now()
	if err != nil {
		return nil, nil, err
	}
	remaining, err := controlbudget.Remaining(d)
	if err != nil {
		return nil, nil, err
	}
	q := infrapriv.LaunchQuery{Identity: identity, Component: component, Expected: self, RemainingNS: uint64(min(remaining, infrapriv.ActionBudget))}
	if _, err := rand.Read(q.Nonce[:]); err != nil {
		return nil, nil, ErrLaunchRejected
	}
	q, err = infrapriv.SignLaunchQuery(q, key)
	if err != nil {
		return nil, nil, err
	}
	request, err := client.QueryLaunch(q, d, stop)
	if err != nil {
		return nil, nil, err
	}
	d = min(d, c.Deadline(started+int64(request.RemainingNS)))
	data, err := c.SigningBytes(request)
	if err != nil {
		return nil, nil, err
	}
	request.Signature, err = key.Sign(data)
	if err != nil {
		return nil, nil, err
	}
	receipt, files, err := client.Exchange(request, nil, d, stop)
	if err != nil {
		return nil, nil, err
	}
	prepared, err := AdoptPrepared(request, files, d)
	if err != nil {
		return nil, nil, err
	}
	ack := request
	ack.Op, ack.Sequence, ack.ExpectedJournal, ack.BeforeCensus = c.LaunchAck, request.Sequence+1, receipt.Journal, receipt.Census.Digest
	ack.Expected, err = prepared.Successor()
	if err == nil {
		_, err = rand.Read(ack.Nonce[:])
	}
	if err == nil {
		data, err = c.SigningBytes(ack)
	}
	if err == nil {
		ack.Signature, err = key.Sign(data)
	}
	if err != nil {
		return nil, nil, errors.Join(err, prepared.Close())
	}
	finish, err := client.SubmitLaunchAck(ack, d, stop)
	if err != nil {
		return nil, nil, errors.Join(err, prepared.Close())
	}
	return prepared, finish, nil
}
