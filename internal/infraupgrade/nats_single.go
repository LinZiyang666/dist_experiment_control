package infraupgrade

import c "github.com/LinZiyang666/tether/internal/infracontract"

// Standalone describes JetStream, not the tether voter count. The readiness
// producer must attest a complete Topology/Groups inventory; only then does the
// absence of a meta group distinguish standalone. An incomplete clustered view
// must never be returned as Complete with an empty MetaLeader.
// origin: hot-update S9 plan §3.2 / §4.3 / T30
func natsStandalone(ready c.Readiness) bool { return ready.MetaLeader == "" }

func (d *natsDriver) readReady(r *natsRun, after bool) (c.Readiness, error) {
	// The producer qualifies default FileStorage persistence and standalone JS.
	// It must refuse MemoryStorage/AsyncPersistMode rather than changing config
	// or adding a hidden restart to make the old installation eligible.
	// After cut, an ordinary authenticated RPC is part of readiness; this is why
	// finish first completes the read-only pre-repair check before calling here.
	if err := natsCheck(r); err != nil {
		return c.Readiness{}, err
	}
	ready, err := d.deps.Ready.Read(r.plan, natsActionDeadline(r), r.stop)
	if err != nil {
		return c.Readiness{}, err
	}
	if !ready.Complete || ready.Topology == (c.Digest{}) || ready.Groups == (c.Digest{}) ||
		!natsAssetComplete(ready.Seal) || ready.Auth == (c.Digest{}) || ready.WSS == (c.Digest{}) ||
		!natsProcessComplete(ready.Running) {
		return c.Readiness{}, c.ErrIncomplete
	}
	if !after && ready.Running != r.plan.Before {
		return c.Readiness{}, c.ErrIncomplete
	}
	if after && (ready.Running.ExeSHA256 != r.plan.Candidate.BinarySHA256 ||
		natsSameIncarnation(ready.Running, r.plan.Before) || ready.Running.BootID != r.plan.Before.BootID ||
		ready.Seal != r.cohort.Assets || natsStandalone(ready) != natsStandalone(r.ready)) {
		return c.Readiness{}, c.ErrIncomplete
	}
	return ready, nil
}

// Persist the selector and the startup fence before any signal. Failures from
// this point retain the domain: even a lost fence reply can change restart
// behavior. Only an exact root receipt can resolve that uncertainty.
// origin: hot-update S9 plan §4.2 / §4.3 / §7.2 / T29 / T30
func (d *natsDriver) cutover(r *natsRun) error {
	if err := d.phase(r, c.Cutting); err != nil {
		return err
	}
	r.cut = true
	if !r.fenced {
		if _, err := d.host(r, c.FenceStart, r.cohort.Proof); err != nil {
			return err
		}
		r.fenced = true
	}
	if !r.promoted {
		if _, err := d.host(r, c.Promote, r.plan.Candidate.CatalogSHA256); err != nil {
			return err
		}
		r.promoted = true
	}
	// N1 and sole-voter-with-standalone-JS use the same stop path. No sibling,
	// configuration reload, hidden preliminary restart, or LDM is introduced.
	if !natsStandalone(r.ready) && !r.ldmApplied {
		if _, err := d.host(r, c.SignalLDM, r.cohort.Proof); err != nil {
			return err
		}
		r.ldmApplied = true
	}
	if !natsStandalone(r.ready) && !r.stopApplied && !r.oldExited {
		// LDM shuts down local JS; this probe must read surviving groups. It
		// cannot use a cached pre-LDM health result to authorize the final stop.
		survivor, err := d.deps.Ready.Read(r.plan, natsActionDeadline(r), r.stop)
		if err != nil {
			return err
		}
		if !survivor.Complete || survivor.MetaLeader == "" || survivor.Groups == (c.Digest{}) ||
			survivor.Auth == (c.Digest{}) || survivor.WSS == (c.Digest{}) || survivor.Seal != r.cohort.Assets {
			return c.ErrIncomplete
		}
	}
	if !r.oldExited {
		// LDM can exit by itself. An exact, complete census lets us skip stop in
		// that case. A still-live incarnation is signaled once; if it remains,
		// recovery queries its exit rather than signaling the current unit owner.
		census, err := d.inspect(r)
		if err != nil {
			return err
		}
		if natsOwnsOld(census, r.plan.Before) && !r.stopApplied {
			if _, err := d.host(r, c.SignalStop, r.cohort.Proof); err != nil {
				return err
			}
			r.stopApplied = true
			census, err = d.inspect(r)
			if err != nil {
				return err
			}
		}
		if natsOwnsOld(census, r.plan.Before) || census.OldOwnerPending != 0 {
			return natsError("infra_recovery_required", nil)
		}
		r.oldExited = true
		if err := d.record(r, "old_exit", census.Digest); err != nil {
			return err
		}
	}
	if !r.startReleased {
		// ReleaseStart opens the already-promoted launcher path in the same store.
		// The root backend, not this coordinator, retains the lifetime OFD and
		// enforces the systemd restart/fence contract across worker death.
		if _, err := d.host(r, c.ReleaseStart, r.cohort.Proof); err != nil {
			return err
		}
		r.startReleased = true
		if err := d.record(r, "startup_released", r.journal); err != nil {
			return err
		}
	}
	return d.phase(r, c.Verifying)
}

func (d *natsDriver) finish(r *natsRun) error {
	if !r.verified {
		// The read-only original ACK oracle precedes even ordinary readiness:
		// ready may need business RPCs whose producers are still behind this gate.
		// origin: hot-update S9 plan §4.3 / T30
		if !r.ackVerified {
			if err := d.deps.Recovery.VerifyBeforeReplay(r.recovery, natsActionDeadline(r), r.stop); err != nil {
				return err
			}
			r.ackVerified = true
			if err := d.save(r); err != nil {
				return err
			}
		}
		if !r.rebound {
			if err := d.deps.Transfers.Rebind(r.transfers, natsActionDeadline(r)); err != nil {
				return err
			}
			r.rebound = true
			if err := d.save(r); err != nil {
				return err
			}
		}
		if err := d.deps.Recovery.Release(r.recovery, natsActionDeadline(r)); err != nil {
			return err
		}
		r.recoveryHeld = false
		r.verified = true
	}
	ready, err := d.readReady(r, true)
	if err != nil {
		return err
	}
	r.status.Current = ready.Running
	if err := d.record(r, "auth_usable", ready.Auth); err != nil {
		return err
	}
	if err := d.record(r, "business_ready", ready.Groups); err != nil {
		return err
	}
	if !r.returned {
		// N1's outbound intent cannot bind a future CID before the process and
		// ordinary auth actually exist. Reuse the original prepare/nonce here.
		if natsStandalone(r.ready) {
			if err := d.move(r, c.RouteOutbound); err != nil {
				return err
			}
		}
		if err := d.phase(r, c.Returning); err != nil {
			return err
		}
		if err := d.install(r, c.RouteReturn); err != nil {
			return err
		}
		if err := d.move(r, c.RouteReturn); err != nil {
			return err
		}
		r.returned = true
		if err := d.record(r, "return_verified", r.census.Digest); err != nil {
			return err
		}
	}
	if err := d.phase(r, c.Retiring); err != nil {
		return err
	}
	finalReady, err := d.readReady(r, true)
	if err != nil {
		return err
	}
	if finalReady.Running != ready.Running {
		return c.ErrIncomplete
	}
	receipt, err := d.host(r, c.Inspect, c.Digest{})
	if err != nil {
		return err
	}
	census := receipt.Census
	// A new process running the original SHA is a legal rollback. Compare the
	// before incarnation, and include socket owners and pending responsibilities.
	// origin: hot-update S9 plan §2.3 / T31
	if natsOwnsOld(census, r.plan.Before) || census.OldOwnerPending != 0 ||
		!natsHasProcess(census, ready.Running) || receipt.Current != ready.Running {
		return natsError("infra_recovery_required", nil)
	}
	if err := d.release(r); err != nil {
		return err
	}
	if err := d.record(r, "owner_retired", census.Digest); err != nil {
		return err
	}
	// Root's journal is the CAS proof, never a locally invented "done" digest.
	// A failed S8 publication leaves the run retained for explicit recovery.
	if err := d.phase(r, c.Completed); err != nil {
		return err
	}
	return d.record(r, "completed", r.journal)
}

// PID/start ticks identify lifetime, while SHA identifies the mapped executable.
// A launcher can exec in place; it still belongs to the old process incarnation
// until the server actually exits. Conversely, a rollback may legitimately map
// the original SHA in a different process and must not be rejected for that.
// origin: hot-update S9 plan §2.3 / T31
func natsSameIncarnation(a, b c.Process) bool {
	return a.BootID == b.BootID && a.PID == b.PID && a.StartTicks == b.StartTicks
}

func natsOwnsOld(census c.Census, before c.Process) bool {
	for _, process := range census.Processes {
		if natsSameIncarnation(process, before) {
			return true
		}
	}
	for _, socket := range census.Sockets {
		if natsSameIncarnation(socket.Owner, before) {
			return true
		}
	}
	return false
}
