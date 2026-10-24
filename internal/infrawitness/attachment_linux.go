//go:build linux

package infrawitness

import (
	"net"
	"os"
	"syscall"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"github.com/nats-io/nats.go"
)

// ReadSocket stays inside RawConn.Control: fd close/reuse cannot splice the
// inode, cookie and tuple of different connections into one attachment.
// Unknown process provenance or a missing accepted socket refuses the sample.
// origin: hot-update S9 plan §2.3 / T06; NC-EXPORTS physical witness handoff
func observe(w *Witness, nc *nats.Conn, raw net.Conn, participant c.ID, generation uint64, d c.Deadline) (c.Attachment, error) {
	if w == nil || w.ingress == nil || nc == nil || !nc.IsConnected() || participant == (c.ID{}) || generation == 0 {
		return c.Attachment{}, c.ErrIncomplete
	}
	conn, ok := raw.(syscall.Conn)
	if !ok {
		return c.Attachment{}, c.ErrUnsupported
	}
	owner, err := infrahost.ReadProcess(int32(os.Getpid()), d)
	if err != nil {
		return c.Attachment{}, err
	}
	fd, err := conn.SyscallConn()
	if err != nil {
		return c.Attachment{}, err
	}
	var socket c.Socket
	var readErr error
	err = fd.Control(func(fd uintptr) { socket, readErr = infrahost.ReadSocket(int(fd), owner, d) })
	if err != nil || readErr != nil {
		return c.Attachment{}, c.ErrIncomplete
	}
	cid, err := nc.GetClientID()
	if err != nil {
		return c.Attachment{}, err
	}
	server, url := nc.ConnectedServerId(), nc.ConnectedUrl()
	accepted, census, health, err := w.ingress(socket, server, cid, d)
	if err != nil {
		return c.Attachment{}, err
	}
	if health == 0 || infrahost.VerifyIngressProof(socket, accepted, accepted.Owner, census) != nil {
		return c.Attachment{}, c.ErrIncomplete
	}
	// Re-read both identities after the independent observation. A reconnect or
	// same-PID exec in the observation window cannot borrow stale evidence.
	var after c.Socket
	err = fd.Control(func(fd uintptr) { after, readErr = infrahost.ReadSocket(int(fd), owner, d) })
	currentCID, cidErr := nc.GetClientID()
	if err != nil || readErr != nil || socket != after || cidErr != nil || currentCID != cid ||
		!nc.IsConnected() || server != nc.ConnectedServerId() || url != nc.ConnectedUrl() ||
		socket.Local != raw.LocalAddr().String() || socket.Remote != raw.RemoteAddr().String() {
		return c.Attachment{}, c.ErrIncomplete
	}
	if _, err = controlbudget.Remaining(d); err != nil {
		return c.Attachment{}, err
	}
	return c.Attachment{Participant: participant, Generation: generation, ServerID: server, CID: cid, URL: url,
		Raw: socket, Ingress: accepted.Owner, HealthGeneration: health}, nil
}
