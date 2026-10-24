package ssstate

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"reflect"

	"github.com/LinZiyang666/tether/internal/proxyimage"
	"github.com/LinZiyang666/tether/internal/sealedstate"
)

func proxyCutRecords(c ProxyCut) ([]sealedstate.Record, error) {
	p := c.State
	if !validProxyState(p) || c.HomeEpoch < 0 || (p.AllocationPort != 0 && c.HomeEpoch != p.AllocationEpoch) || c.LocalPort < 0 || c.LocalPort > 65535 || len(c.Token) > 4096 || len(c.HomeAddr) > 4096 ||
		c.DigestApplied != (p.KeysetDigest != ([32]byte{})) ||
		p.AllocationPort != 0 && p.AllocationTokenHash != fmt.Sprintf("%x", sha256.Sum256([]byte(c.Token))) {
		return nil, ErrStateUnrepresentable
	}
	id := [16]byte{1}
	a, timer := proxyControlRecords(p, id)
	phase := uint64(1) // Pending in the frozen authority wire range.
	if c.DigestApplied {
		phase = 2 // Authorized.
	}
	if !p.Enabled {
		phase = 3 // Revoked.
	}
	a.put("phase", uint64(phase))
	endpoint := newPrivateRecord(sealedstate.KindEndpoint)
	endpoint.putBytes("id", id[:])
	endpoint.putBytes("allocation_sid", []byte(p.AllocationSID))
	endpoint.putBytes("allocation_nid", []byte(p.AllocationNID))
	endpoint.put("allocation_port", uint64(p.AllocationPort))
	endpoint.putBytes("allocation_token_hash", []byte(p.AllocationTokenHash))
	endpoint.put("allocation_epoch", uint64(c.HomeEpoch))
	endpoint.putBytes("register_token", []byte(c.Token))
	endpoint.putBytes("broker_addr", []byte(c.HomeAddr))
	endpoint.put("target_port", uint64(c.LocalPort))
	endpoint.put("owner_gen", 1)
	endpoint.put("closed_allocation", boolState(!p.Enabled))
	endpoint.put("ready_latch", boolState(c.TunnelUp))
	endpoint.put("ready_latch_deadline_boot_ns", uint64(p.PlannedDeadlineBootNS))
	// Binding represents a listener that must be reestablished; Detached
	// represents a retained allocation. No broker listener/fence is imported.
	endpoint.put("phase", 3)
	if c.NeedsReestablish {
		endpoint.put("phase", 1)
	}
	return []sealedstate.Record{a.clone(), endpoint.clone(), timer.clone()}, nil
}

// EncodeProxyCut never emits secrets into JSON or Resource.Data. Callers seal
// the returned bytes immediately, then clear that temporary buffer.
func EncodeProxyCut(c ProxyCut) ([]byte, error) {
	records, err := proxyCutRecords(c)
	if err != nil {
		return nil, err
	}
	data, err := encodeState(records)
	return data, err
}

// DecodeProxyCut performs exact closure through the canonical builder, so
// every required but inapplicable broker field is validated as zero/empty.
func DecodeProxyCut(data []byte) (ProxyCut, error) {
	// Keep both legacy versions readable through the image owner after moving
	// the parser out of Agent; new cuts still use the declared descriptors.
	// origin: hot-update S5 plan §7.2 / L12 — integration budget extraction.
	if bytes.HasPrefix(data, []byte("TPRX\x00")) {
		c, err := proxyimage.DecodeLegacyCut(data)
		if err != nil {
			return ProxyCut{}, ErrStateUnrepresentable
		}
		return c, nil
	}
	var c ProxyCut
	records, err := decodeState(data)
	if err != nil || len(records) != 3 {
		return c, ErrStateUnrepresentable
	}
	var a, endpoint, timer privateRecord
	for _, rec := range records {
		p := wrapPrivateRecord(rec)
		switch rec.Kind {
		case sealedstate.KindAuthority:
			a = p
		case sealedstate.KindEndpoint:
			endpoint = p
		case sealedstate.KindTimer:
			timer = p
		case sealedstate.KindBinding, sealedstate.KindFlow, sealedstate.KindMaterial, sealedstate.KindPermit, sealedstate.KindReceipt,
			sealedstate.KindRetirement, sealedstate.KindSSActor, sealedstate.KindSSServer, sealedstate.KindSaltRing:
			return c, ErrStateUnrepresentable
		}
	}
	if a.fields == nil || endpoint.fields == nil || timer.fields == nil {
		return c, ErrStateUnrepresentable
	}
	c.State = readProxyControl(a, timer)
	c.HomeEpoch = int64(endpoint.number("allocation_epoch"))
	c.LocalPort, c.Token, c.HomeAddr = int(endpoint.number("target_port")), string(endpoint.bytes("register_token")), string(endpoint.bytes("broker_addr"))
	c.DigestApplied, c.TunnelUp, c.NeedsReestablish = c.State.KeysetDigest != ([32]byte{}), endpoint.flag("ready_latch"), endpoint.number("phase") == 1
	want, err := proxyCutRecords(c)
	if err != nil || !reflect.DeepEqual(records, want) {
		return ProxyCut{}, ErrStateUnrepresentable
	}
	return c, nil
}
