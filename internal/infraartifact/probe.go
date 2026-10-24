package infraartifact

import (
	"strings"

	"github.com/LinZiyang666/tether/internal/infracontract"
)

// Supervisor is the sole version-command boundary. The implementation must
// stage the verified bytes in an isolated directory and use shared process
// supervision before Start, with a nonroot service identity and a clean
// environment (no live admin/ACME state). It must join the entire process tree.
// This interface does not admit an arbitrary path, argv, URL or environment.
// origin: hot-update S9 plan §3.1 / T03
type Supervisor interface {
	Version(*Verified, infracontract.Deadline, infracontract.Stop) (VersionReport, error)
}

// VersionReport is private evidence, never a public status/log payload.
type VersionReport struct {
	Artifact      infracontract.Artifact
	RunningSHA256 infracontract.Digest
	UID           uint32
	Isolated      bool
	Joined        bool
	Output        string
}

// CompatibilityRequest binds the current-state check to exact component bytes,
// schema and canonical store. A version string or an empty-store smoke cannot
// authorize a rollback. The adapter owns its supervised real-binary exercise.
type CompatibilityRequest struct {
	Candidate infracontract.Artifact
	Previous  infracontract.Artifact
	Schema    infracontract.Digest
	Store     infracontract.Store
}

type CompatibilityReport struct {
	Version      uint16
	Request      CompatibilityRequest
	CurrentState infracontract.Digest
	Written      bool
	PreviousRead bool
	Reupgraded   bool
	Complete     bool
}

// CompatibilityProbe must capture the nonempty current-state digest before the
// previous-version read and compare stream/consumer/OBJ/receipt/audit or Caddy
// config/schema/ACME state as applicable. P28/P29 independently sign that IO;
// this package validates the report binding, not a producer's truthfulness.
type CompatibilityProbe interface {
	Check(CompatibilityRequest, *Verified, *Verified, infracontract.Deadline) (CompatibilityReport, error)
}

func (c *Catalog) probeVersion(v *Verified, d infracontract.Deadline, stop infracontract.Stop) error {
	if err := admission(d, stop); err != nil {
		return err
	}
	if c.config.Supervisor == nil {
		return infracontract.ErrIncomplete
	}
	report, err := c.config.Supervisor.Version(v, d, stop)
	// Do not propagate a subprocess error that may include raw config/secrets.
	if err != nil {
		return ErrArtifactInvalid
	}
	if err := admission(d, stop); err != nil {
		return err
	}
	if report.Artifact != v.artifact || report.RunningSHA256 != v.artifact.BinarySHA256 || report.UID == 0 || !report.Isolated || !report.Joined || len(report.Output) > 4096 {
		return ErrArtifactInvalid
	}
	fields := strings.Fields(strings.TrimSpace(report.Output))
	switch v.artifact.Kind {
	case infracontract.NATSBinary:
		if len(fields) != 2 || fields[0] != "nats-server:" || fields[1] != "v"+v.artifact.Version {
			return ErrArtifactInvalid
		}
	case infracontract.CaddyBinary:
		if len(fields) == 0 || fields[0] != "v"+v.artifact.Version {
			return ErrArtifactInvalid
		}
	case infracontract.HelperBinary, infracontract.LauncherBinary:
		return infracontract.ErrUnsupported
	default:
		return ErrArtifactInvalid
	}
	return nil
}

func (c *Catalog) Compatible(candidate, previous infracontract.Artifact, schema infracontract.Digest, store infracontract.Store, d infracontract.Deadline) error {
	if err := admission(d, nil); err != nil {
		return err
	}
	if _, err := c.entry(candidate); err != nil {
		return err
	}
	if _, err := c.entry(previous); err != nil {
		return err
	}
	// origin: hot-update S9 plan §3.1 / T04; same-version or foreign previous
	// evidence is not the adjacent-generation current-state readback contract.
	if candidate.Kind != previous.Kind || candidate.GOOS != previous.GOOS || candidate.GOARCH != previous.GOARCH || candidate.Version == previous.Version || candidate.BinarySHA256 == previous.BinarySHA256 || schema == (infracontract.Digest{}) || store.Device == 0 || store.Inode == 0 || store.MountID == 0 || store.LockDevice == 0 || store.LockInode == 0 {
		return ErrIncompatible
	}
	if c.config.Compatibility == nil {
		return infracontract.ErrIncomplete
	}
	current, err := c.ReadVerified(candidate, d, nil)
	if err != nil {
		return err
	}
	old, err := c.ReadVerified(previous, d, nil)
	if err != nil {
		return err
	}
	if err := c.probeVersion(current, d, nil); err != nil {
		return err
	}
	if err := c.probeVersion(old, d, nil); err != nil {
		return err
	}
	request := CompatibilityRequest{candidate, previous, schema, store}
	report, err := c.config.Compatibility.Check(request, current, old, d)
	if err != nil {
		return ErrIncompatible
	}
	if err := admission(d, nil); err != nil {
		return err
	}
	if report.Version != 1 || report.Request != request || report.CurrentState == (infracontract.Digest{}) || !report.Written || !report.PreviousRead || !report.Reupgraded || !report.Complete {
		return ErrIncompatible
	}
	return nil
}
