//go:build linux

package clusterharness

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/port"
)

// The connecting OPEN has no pre-cut target bytes. Its independent oracle is
// the original bound agent socket, a zero-to-one target Accept census, and one
// payload delivered on the original client/broker sockets after readiness.
// origin: hot-update S6 plan §6.3 / §10.1 / T48
type cohortOpeningReceipt struct {
	Phase         string               `json:"phase"`
	AgentPID      int                  `json:"agent_pid"`
	AgentFD       int                  `json:"agent_fd"`
	AgentSocket   cohortSocketIdentity `json:"agent_socket"`
	ClientSocket  cohortSocketIdentity `json:"client_socket"`
	BrokerSocket  cohortSocketIdentity `json:"broker_socket"`
	ClientPID     int                  `json:"client_pid"`
	ClientFD      int                  `json:"client_fd"`
	TargetAccepts uint32               `json:"target_accepts"`
	Opened        int64                `json:"opened_boot_ns"`
	Started       int64                `json:"started_boot_ns"`
	Finished      int64                `json:"finished_boot_ns"`
	PayloadSHA    string               `json:"payload_sha256"`
}
type cohortOpening struct {
	c               *productCohort
	listener        net.Listener
	allocation      port.Allocation
	peer, target    net.Conn
	done            chan error
	accepted        chan net.Conn
	targetDone      chan struct{}
	started, joined bool
	receipt         cohortOpeningReceipt
}

const cohortOpeningPayload = "tether-broker-original-connecting-open-v1"

func (c *productCohort) setupOpening(db *sql.DB) error {
	o := &cohortOpening{c: c, done: make(chan error, 1), accepted: make(chan net.Conn, 1), targetDone: make(chan struct{})}
	c.opening = o
	var err error
	o.listener, err = net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return err
	}
	public, err := strconv.Atoi(strings.Split(productFixtureAddress(c.f), ":")[1])
	if err != nil {
		return err
	}
	localPort, err := brokerFixtureTCPPort(o.listener.Addr())
	if err != nil {
		return err
	}
	alloc, err := port.Allocate(db, "product", "agent", "cohort-opening", localPort, public, "owner", false, &port.Config{BandLow: public, BandHigh: public})
	if err != nil {
		return err
	}
	o.allocation = *alloc
	c.f.allocations = append(c.f.allocations, *alloc)
	data, err := json.Marshal(map[string]int{"port": localPort})
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(c.agentHome, "cohort-opening.json"), data, 0600); err != nil {
		return err
	}
	go func() {
		defer close(o.targetDone)
		conn, err := o.listener.Accept()
		if err != nil {
			return
		}
		o.accepted <- conn
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		value := make([]byte, len(cohortOpeningPayload))
		if _, err := io.ReadFull(conn, value); err == nil && bytes.Equal(value, []byte(cohortOpeningPayload)) {
			_, _ = conn.Write(value)
		}
		<-c.cleanup
		_ = conn.Close()
	}()
	return nil
}
func (o *cohortOpening) start() error {
	var err error
	o.receipt.Opened, err = controlbudget.Now()
	if err != nil {
		return err
	}
	o.peer, err = net.DialTimeout("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(o.allocation.Port)), time.Second)
	if err != nil {
		return err
	}
	o.receipt.ClientPID = os.Getpid()
	o.receipt.ClientFD, err = cohortFD(o.peer)
	if err != nil {
		return err
	}
	o.receipt.ClientSocket, err = cohortSocketAt(os.Getpid(), o.receipt.ClientFD)
	if err != nil {
		return err
	}
	until := time.Now().Add(time.Second)
	for time.Now().Before(until) {
		data, err := os.ReadFile(filepath.Join(o.c.agentHome, "cohort-opening-held.json"))
		if err == nil {
			var marker struct {
				FD    int  `json:"fd"`
				PID   int  `json:"pid"`
				Bound bool `json:"bound"`
			}
			if err = json.Unmarshal(data, &marker); err != nil {
				return err
			}
			if !marker.Bound || marker.PID != o.c.agent.Process.Pid {
				return errors.New("connecting OPEN lacks original bound agent socket")
			}
			o.receipt.AgentPID, o.receipt.AgentFD = marker.PID, marker.FD
			o.receipt.AgentSocket, err = cohortSocketAt(marker.PID, marker.FD)
			if err != nil {
				return err
			}
			clientPort, err := brokerFixtureTCPPort(o.peer.LocalAddr())
			if err != nil {
				return err
			}
			fd, err := cohortRemoteFD(o.c.f.cmd.Process.Pid, clientPort, o.allocation.Port)
			if err != nil {
				return err
			}
			o.receipt.BrokerSocket, err = cohortSocketAt(o.c.f.cmd.Process.Pid, fd)
			if err != nil {
				return err
			}
			select {
			case conn := <-o.accepted:
				o.target = conn
				return errors.New("connecting OPEN target accepted before release")
			default:
			}
			o.receipt.Phase = "connecting"
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return errors.New("real agent did not retain a connecting OPEN")
}

// Connection establishment and the held OPEN precede the cut. Start actual
// application I/O at the closed-store barrier, with its own original five-second
// budget; observation work never enters that operation's elapsed time.
// origin: hot-update S6 plan §6.3 / T48
func (o *cohortOpening) beginWork() {
	if o.started {
		return
	}
	_ = o.peer.SetDeadline(time.Now().Add(5 * time.Second))
	o.started = true
	o.receipt.Started, _ = controlbudget.Now()
	go func() {
		_, err := io.WriteString(o.peer, cohortOpeningPayload)
		reply := make([]byte, len(cohortOpeningPayload))
		if err == nil {
			_, err = io.ReadFull(o.peer, reply)
		}
		if err == nil && !bytes.Equal(reply, []byte(cohortOpeningPayload)) {
			err = errors.New("connecting OPEN payload differs")
		}
		o.receipt.Finished, _ = controlbudget.Now()
		o.done <- err
	}()
}
func (o *cohortOpening) release() error {
	return os.WriteFile(filepath.Join(o.c.agentHome, "cohort-opening-release"), []byte("ready"), 0600)
}
func (o *cohortOpening) after() error {
	err := <-o.done
	o.joined = true
	if err != nil {
		return fmt.Errorf("connecting OPEN business: %w (start=%d finish=%d freeze=%d ready=%d)", err, o.receipt.Started, o.receipt.Finished, o.c.cut, o.c.ready)
	}
	if o.receipt.Finished-o.receipt.Started > int64(5*time.Second) {
		return errors.New("connecting OPEN exceeded absolute business budget")
	}
	select {
	case o.target = <-o.accepted:
		o.receipt.TargetAccepts = 1
	default:
		return errors.New("connecting OPEN has no actual target Accept")
	}
	agent, err := cohortSocketAt(o.receipt.AgentPID, o.receipt.AgentFD)
	if err != nil {
		return err
	}
	client, err := cohortSocketAt(o.receipt.ClientPID, o.receipt.ClientFD)
	if err != nil {
		return err
	}
	clientPort, err := brokerFixtureTCPPort(o.peer.LocalAddr())
	if err != nil {
		return err
	}
	fd, err := cohortRemoteFD(o.c.f.cmd.Process.Pid, clientPort, o.allocation.Port)
	if err != nil {
		return err
	}
	broker, err := cohortSocketAt(o.c.f.cmd.Process.Pid, fd)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(agent, o.receipt.AgentSocket) || !reflect.DeepEqual(client, o.receipt.ClientSocket) || !reflect.DeepEqual(broker, o.receipt.BrokerSocket) {
		return errors.New("connecting OPEN replaced an original socket")
	}
	o.receipt.Phase = "connected"
	o.receipt.PayloadSHA = storeFixtureDigest([]byte(cohortOpeningPayload))
	return nil
}
func (o *cohortOpening) close() {
	if o == nil {
		return
	}
	if o.peer != nil {
		_ = o.peer.Close()
	}
	if o.listener != nil {
		_ = o.listener.Close()
		<-o.targetDone
	}
	if o.target != nil {
		_ = o.target.Close()
	}
	if o.started && !o.joined {
		<-o.done
		o.joined = true
	}
}
