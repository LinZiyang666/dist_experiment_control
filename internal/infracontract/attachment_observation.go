package infracontract

import (
	"encoding/hex"
	"encoding/json"
	"net/url"
)

// The typed owner projects public bytes before they cross into custody. The
// observer consumes this structural port without importing infrastructure DTOs.
// origin: S10 internal review ARCH-REDS R2
// Public custody uses P32's exact frozen field names. Digest arrays become
// canonical lowercase hex, including each socket's nested process owner.
// These are public identity receipts only; options, signers, auth tokens and
// private route state are never marshaled. Refuse credential-bearing URLs.
func (a Attachment) InfrastructureObservation() ([16]byte, []byte, error) {
	u, err := url.Parse(a.URL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || !publicAttachmentComplete(a) {
		return [16]byte{}, nil, ErrIncomplete
	}
	type process struct {
		Process
		ExeSHA256 string
	}
	project := func(p Process) process { return process{p, hex.EncodeToString(p.ExeSHA256[:])} }
	type socket struct {
		Socket
		Owner process
	}
	row := struct {
		Participant      string
		Generation       uint64
		ServerID         string
		CID              uint64
		URL              string
		Raw              socket
		Ingress          process
		HealthGeneration uint64
	}{hex.EncodeToString(a.Participant[:]), a.Generation, a.ServerID, a.CID, a.URL,
		socket{a.Raw, project(a.Raw.Owner)}, project(a.Ingress), a.HealthGeneration}
	data, err := json.Marshal(row)
	if err != nil {
		return [16]byte{}, nil, ErrIncomplete
	}
	return a.Participant, data, nil
}

func publicAttachmentComplete(a Attachment) bool {
	known := func(p Process) bool {
		return p.BootID != "" && p.PID > 0 && p.StartTicks > 0 && p.InvocationID != "" && p.ExeDevice > 0 && p.ExeInode > 0 && p.ExeSHA256 != (Digest{}) && p.Cgroup != ""
	}
	return a.Participant != (ID{}) && a.Generation > 0 && a.CID > 0 && a.ServerID != "" && a.HealthGeneration > 0 &&
		a.Raw.NetNS > 0 && a.Raw.Device > 0 && a.Raw.Inode > 0 && a.Raw.Cookie > 0 && a.Raw.Local != "" && a.Raw.Remote != "" && known(a.Raw.Owner) && known(a.Ingress)
}
