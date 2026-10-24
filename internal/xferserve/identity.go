// Package xferserve composes the production broker-side continuation graph:
// the durable origin and issuer, the broker→agent link, the receiver/object
// retirement and census ports, and the authenticated agent/carrier ingress.
// The broker package only supplies its current backend through Host and
// installs the result; every decision lives in the transfer leaves.
// origin: hot-update S7 plan §5 / §6.3 / §8.2 / P22
package xferserve

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/LinZiyang666/tether/internal/procrequest"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nkeys"
	"github.com/nats-io/nuid"
)

// Identity is one broker incarnation's retained continuation authority.
// Root is private to this Origin: ledger rows with a different origin are never
// mixed into its census (the tracker refuses them as a terminal conflict).
type Identity struct {
	Origin x.Origin
	Issuer nkeys.KeyPair
	Root   string
}

type identityFile struct {
	BrokerID   string `json:"broker_id"`
	InstanceID string `json:"instance_id"`
	BootID     string `json:"boot_id"`
	Generation uint64 `json:"generation"`
	IssuerSeed string `json:"issuer_seed"`
}

// LoadIdentity returns the durable origin for base. Within one kernel boot a
// cold broker restart (or a same-PID image exec) keeps the same origin, issuer
// and ledger root, so retained entries resume and old permits still verify.
// After a reboot the boot-relative deadlines of old rows are no longer
// comparable: a new origin with its own root starts, and the previous root is
// reported as stranded rather than adopted under a forged boot identity.
// origin: hot-update S7 plan §1.2 / §5.1 / §6.1 / §9
func LoadIdentity(base, brokerID string) (Identity, []string, error) {
	if base == "" || brokerID == "" {
		return Identity{}, nil, errors.New("xferserve: continuation state root and broker identity are required")
	}
	boot, err := procrequest.CurrentBootID()
	if err != nil || boot == "" {
		return Identity{}, nil, errors.Join(errors.New("xferserve: boot identity unavailable"), err)
	}
	if err = os.MkdirAll(base, 0o700); err != nil {
		return Identity{}, nil, err
	}
	path := filepath.Join(base, "identity.json")
	var stored identityFile
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if json.Unmarshal(data, &stored) != nil || stored.InstanceID == "" || stored.IssuerSeed == "" {
			return Identity{}, nil, fmt.Errorf("xferserve: %s is unreadable; refusing to replace retained authority", path)
		}
		if stored.BootID != boot || stored.BrokerID != brokerID {
			stored = identityFile{}
		}
	case !os.IsNotExist(err):
		return Identity{}, nil, err
	}
	if stored.InstanceID == "" {
		issuer, err := nkeys.CreateAccount()
		if err != nil {
			return Identity{}, nil, err
		}
		seed, err := issuer.Seed()
		if err != nil {
			return Identity{}, nil, err
		}
		stored = identityFile{BrokerID: brokerID, InstanceID: nuid.Next(), BootID: boot, Generation: 1, IssuerSeed: string(seed)}
		if err = writeDurable(path, stored); err != nil {
			return Identity{}, nil, err
		}
	}
	issuer, err := nkeys.FromSeed([]byte(stored.IssuerSeed))
	if err != nil {
		return Identity{}, nil, fmt.Errorf("xferserve: retained issuer is invalid: %w", err)
	}
	root := filepath.Join(base, stored.InstanceID)
	if err = os.MkdirAll(root, 0o700); err != nil {
		return Identity{}, nil, err
	}
	origin := x.Origin{BrokerID: stored.BrokerID, InstanceID: stored.InstanceID, BootID: stored.BootID, Generation: stored.Generation}
	// Every retained root must remain visible on subsequent same-boot starts.
	// Logging the old root only on the reboot that created it loses protection
	// on the next restart, falsely turning an unread origin into absence.
	// origin: hot-update S7 plan §5.3 / §6.4 / T29
	entries, err := os.ReadDir(base)
	if err != nil {
		return Identity{}, nil, err
	}
	var stranded []string
	for _, entry := range entries {
		if entry.IsDir() && entry.Name() != stored.InstanceID {
			stranded = append(stranded, filepath.Join(base, entry.Name()))
		}
	}
	return Identity{Origin: origin, Issuer: issuer, Root: root}, stranded, nil
}

// writeDurable installs the identity with tmp→Sync→rename→directory Sync, so a
// crash never leaves a torn file that would force a second origin.
func writeDurable(path string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".identity-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer func() { _ = os.Remove(name) }()
	if _, err = tmp.Write(data); err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Chmod(name, 0o600); err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}

// configWriterFile records the retired bucket-config writer of this origin.
type configWriterFile struct {
	Generation uint64              `json:"generation"`
	State      x.ConfigWriterState `json:"state"`
}

// loadConfigWriter returns the joined, closed state of this origin's previous
// bucket-config writer and the generation its successor must exceed. With no
// record the origin has never retired a writer: generation one bootstraps.
// origin: hot-update S7 plan §2.1 / T05 — writer succession across a broker cut
func loadConfigWriter(root string) (*x.ConfigWriterState, uint64, error) {
	data, err := os.ReadFile(filepath.Join(root, "config-writer.json"))
	if os.IsNotExist(err) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	var stored configWriterFile
	if err = json.Unmarshal(data, &stored); err != nil || stored.Generation == 0 {
		return nil, 0, fmt.Errorf("xferserve: retired config writer record is unreadable; refusing a second writer: %v", err)
	}
	return &stored.State, max(stored.Generation, stored.State.Admission.Generation), nil
}

// RetireConfigWriter durably records the local coordinator's retirement
// proof after the graph stop joined it. Its sent-unknown attempts are the
// production ConfigAttempts: a successor is refused config authority until
// each is resolved (xferbucket.NewAuthority), never silently re-armed.
func (a Assembly) RetireConfigWriter(d x.Deadline) error {
	if a.Local == nil || a.Root == "" {
		return errors.New(x.CodeOwnerRecovering + ": no config writer to retire")
	}
	state, err := a.Local.Freeze(d)
	if err != nil {
		return err
	}
	return writeDurable(filepath.Join(a.Root, "config-writer.json"), configWriterFile{Generation: a.Generation, State: state})
}
