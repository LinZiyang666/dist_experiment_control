//go:build linux

package infrapriv

import (
	"context"
	"encoding/json"
	"errors"
	"net"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// LoadLaunchIdentity exposes no private journal state or signing seed. Its
// ownership checks are the same as the launch credential's fixed loader.
func LoadLaunchIdentity(directory string, gid uint32) (_ LaunchIdentity, resultErr error) {
	dir, err := openTrustedDirectory(directory)
	if err != nil {
		return LaunchIdentity{}, ErrNotEnabled
	}
	defer func() { resultErr = errors.Join(resultErr, dir.Close()) }()
	data, err := readOwnedFile(dir, "launch.json", 0, gid, 0640, c.MaxRequestBytes)
	if err != nil {
		return LaunchIdentity{}, err
	}
	var id LaunchIdentity
	if json.Unmarshal(data, &id) != nil || id.Version != 1 || id.ServiceGID != gid {
		return LaunchIdentity{}, ErrPolicy
	}
	canonical, _ := json.Marshal(id)
	if string(data) != string(canonical) {
		return LaunchIdentity{}, ErrPolicy
	}
	return id, nil
}

func (client *Client) launchConnection(boot string, d c.Deadline, stop c.Stop) (*net.UnixConn, func(), error) {
	if client == nil {
		return nil, nil, ErrIPC
	}
	if err := checkBudget(d, stop); err != nil {
		return nil, nil, err
	}
	if client.checkPath {
		if err := verifySocketPath(client.path, client.gid); err != nil {
			return nil, nil, err
		}
	}
	remaining, err := controlbudget.Remaining(d)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), remaining) // ctx-none: launch API carries deadline/stop.
	done, joined := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-stop:
			cancel()
		case <-done:
		case <-ctx.Done():
		}
	}()
	connection, err := (&net.Dialer{}).DialContext(ctx, "unix", client.path)
	close(done)
	cancel()
	<-joined
	if err != nil {
		return nil, nil, ErrIPC
	}
	conn, ok := connection.(*net.UnixConn)
	if !ok {
		return nil, nil, errors.Join(ErrIPC, connection.Close())
	}
	peer, err := PeerCredentials(conn)
	if err != nil || peer.UID != client.serverUID || peer.BootID != boot {
		return nil, nil, errors.Join(ErrPermission, conn.Close())
	}
	_, join := interruptConn(conn, d, stop)
	cleanup := func() {
		join()
		conn.Close() //nolint:errcheck // The no-result cancellation callback only disposes of an already completed or failed socket.
	}
	return conn, cleanup, nil
}

// QueryLaunch obtains a read-only request template over the same authenticated
// endpoint as ordinary Host operations. It is not authority to execute anything.
// origin: S9 internal review rv-infra F03
func (client *Client) QueryLaunch(q LaunchQuery, d c.Deadline, stop c.Stop) (c.HostRequest, error) {
	data, err := json.Marshal(q)
	if err != nil {
		return c.HostRequest{}, ErrIPC
	}
	if _, err := decodeLaunchQuery(data); err != nil {
		return c.HostRequest{}, err
	}
	conn, closeConn, err := client.launchConnection(q.Identity.HostBootID, d, stop)
	if err != nil {
		return c.HostRequest{}, err
	}
	defer closeConn()
	if err := setWriteDeadline(conn, d); err != nil {
		return c.HostRequest{}, err
	}
	if err := writeFrame(conn, LaunchQueryPurpose, data); err != nil {
		return c.HostRequest{}, err
	}
	if err := conn.CloseWrite(); err != nil {
		return c.HostRequest{}, err
	}
	if err := setReadDeadline(conn, d); err != nil {
		return c.HostRequest{}, err
	}
	data, err = readFrame(conn, LaunchOfferPurpose)
	if err != nil {
		return c.HostRequest{}, err
	}
	r, err := c.DecodeRequest(data)
	if err != nil || !offerMatches(r, q) {
		return c.HostRequest{}, ErrPermission
	}
	return r, nil
}

// SubmitLaunchAck writes the signed observation intent before exec and returns
// without waiting for its completion. Only root's post-exec observation can
// commit it; the socket remains CLOEXEC and is closed atomically by exec.
// The cleanup must be called if exec fails, joining the cancellation worker.
// origin: S9 internal review rv-infra F03 / F04
func (client *Client) SubmitLaunchAck(r c.HostRequest, d c.Deadline, stop c.Stop) (func(), error) {
	if r.Op != c.LaunchAck || len(r.Signature) != 64 {
		return nil, ErrPermission
	}
	data, err := c.EncodeRequest(r)
	if err != nil {
		return nil, err
	}
	conn, closeConn, err := client.launchConnection(r.HostBootID, d, stop)
	if err != nil {
		return nil, err
	}
	if err := setWriteDeadline(conn, d); err != nil {
		closeConn()
		return nil, err
	}
	if err := writeFrame(conn, RequestPurpose, data); err != nil {
		closeConn()
		return nil, err
	}
	if err := conn.CloseWrite(); err != nil {
		closeConn()
		return nil, err
	}
	return closeConn, nil
}
