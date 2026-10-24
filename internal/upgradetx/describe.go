package upgradetx

// Description is a running component's own statement of what a Prepare must
// name. The frozen PrepareRequest carries the exact journal Key, the running
// source bytes and release, and the original scope; a ctl can know none of
// them (DomainID, UID and ConfigPath are installation facts, and only the
// target can enumerate its retained work), so the target describes itself.
// It confers no authority: Prepare is still issued and checked by the target's
// sole journal writer, and Start re-proves the source against /proc.
// Nonce echoes the caller's fresh request nonce so a cached or replayed
// description cannot answer a later request. The DTO lives in this core so
// the frozen wire contract can carry it without importing a control-plane
// port (upgradewire/contracts.go may import only upgradetx, upgradeartifact
// and proto).
// origin: hot-update S8 plan §2.1 / §3.1 / §7.1; S8 FIX-DESCRIBE (P19 request R1)
type Description struct {
	Key           Key    `json:"key"`
	Nonce         string `json:"nonce"`
	At            int64  `json:"at"`
	RunningSHA    string `json:"running_sha"`
	SourceRelease string `json:"source_release"`
	Scope         Scope  `json:"scope"`
	// Candidate is answered only by a broker: the frozen broker first call
	// names a version and SHA, never a local file, so the broker describes
	// the candidate staged at its registered canonical path. An agent's
	// candidate is downloaded and probed by the ctl itself.
	Candidate *CandidateDescription `json:"candidate,omitempty"`
}

// CandidateDescription binds the reader declarations of one probed executable
// to its SHA. CapabilityDigest is the value the grant must carry; the root
// stage recomputes it from the published bytes before any receipt.
type CandidateDescription struct {
	SHA              string `json:"sha"`
	Release          string `json:"release"`
	CapabilityDigest string `json:"capability_digest"`
}
