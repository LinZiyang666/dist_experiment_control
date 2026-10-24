// Package sealedstate carries private cut records in sealed memfd resources.
// It is below flow, tunnel and SS owners: no carrier or caller context owns its lifetime.
// origin: hot-update S5 plan §2.1 / §6.6 / §12.1
package sealedstate

import (
	"errors"
	"os"

	"github.com/LinZiyang666/tether/internal/handoff"
)

const Version = 1
const MaxStateBytes = 32 << 20

var (
	ErrNotImplemented = errors.New("sealedstate: not implemented (S5 P5)")
	ErrInvalid        = errors.New("sealedstate: invalid private state")
	ErrCapacity       = errors.New("sealedstate: capacity exceeded")
	ErrUnsupported    = errors.New("sealedstate: platform unsupported")
)

// Resources borrow original business FDs; Owned contains only cut-owned sealed FDs.
// Re-export replaces Owned only after the replacement has passed validation.
type Snapshot struct {
	Resources []handoff.Resource
	Owned     []*os.File
}

// Field and Record are private binary codec inputs, never public JSON image fields.
// U64 also carries nonnegative i64 and bool. Bytes carries all other field types.
// The exact primitive shape is the cross-owner boundary; do not add runtime pointers.
type Field struct {
	Name  string
	Type  uint8
	U64   uint64
	Bytes []byte
}
type Record struct {
	Kind   string
	Fields []Field
}
type CutState struct {
	Records   []Record
	Resources []handoff.Resource
}

// Aliases preserve the frozen Field.Type uint8 and Record.Kind string signatures.
type FieldType = uint8
type Kind = string

const (
	FieldU64            FieldType = 1
	FieldNonnegativeI64 FieldType = 2
	FieldBool           FieldType = 3
	FieldBytes          FieldType = 4
	FieldUTF8           FieldType = 5
	FieldID16           FieldType = 6
	FieldNonce12        FieldType = 7
	FieldSalt32         FieldType = 8
)
const (
	KindAuthority  Kind = "authority"
	KindBinding    Kind = "binding"
	KindEndpoint   Kind = "endpoint"
	KindFlow       Kind = "flow"
	KindMaterial   Kind = "material"
	KindPermit     Kind = "permit"
	KindReceipt    Kind = "receipt"
	KindRetirement Kind = "retirement"
	KindSaltRing   Kind = "salt-ring"
	KindSSActor    Kind = "ss-actor"
	KindSSServer   Kind = "ss-server"
	KindTimer      Kind = "timer"
)

// Width is exact when nonzero. Variable-width buffers are bounded by MaxBytes.
// Required includes empty buffers/false decisions: omission must never reset a nonce,
// cursor, SO_ERROR receipt or irreversible revoke decision on restore.
type FieldDescriptor struct {
	Name     string
	Type     uint8
	Width    int
	MaxBytes int
	Required bool
}
type RecordDescriptor struct {
	Kind   string
	Fields []FieldDescriptor
}

// RecordDescriptors is sorted by kind and then ASCII field name. Names themselves
// are the binary field IDs (length + name + type + value), not positional numbers.
// The codec and runtime snapshots must consume this table; there is no second schema.
// Every kind has one required id, even when its natural key is an endpoint/material.
// origin: hot-update S5 plan §3 / §6.1 / §12.1
var RecordDescriptors = []RecordDescriptor{
	{Kind: KindAuthority, Fields: []FieldDescriptor{
		{Name: "allocation_epoch", Type: FieldNonnegativeI64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "allocation_nid", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "allocation_port", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "allocation_sid", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "allocation_token_hash", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "applied_revision", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "deadline_boot_ns", Type: FieldNonnegativeI64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "dispatch_sequence", Type: FieldNonnegativeI64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "enabled", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "epoch", Type: FieldNonnegativeI64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "generation", Type: FieldNonnegativeI64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "keys", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "keyset_digest", Type: FieldBytes, Width: 32, MaxBytes: 32, Required: true},
		{Name: "last_applied_sequence", Type: FieldNonnegativeI64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "phase", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "revoked", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
	}},
	{Kind: KindBinding, Fields: []FieldDescriptor{
		{Name: "conn_id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "consumed", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "deadline_boot_ns", Type: FieldNonnegativeI64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "flow_id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "tuple", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
	}},
	{Kind: KindEndpoint, Fields: []FieldDescriptor{
		{Name: "allocation_epoch", Type: FieldNonnegativeI64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "allocation_nid", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "allocation_port", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "allocation_sid", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "allocation_token_hash", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "boot_id", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "broker_addr", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "carrier_gen", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "cleanup_pending", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "closed_allocation", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "default_broker_addr", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "fence_allocation_gen", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "fence_port_gen", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "fence_session_gen", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "first_detach_deadline_boot_ns", Type: FieldNonnegativeI64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "flow_ids", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "forgotten", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "instance_id", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "listener_ref", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "owner_gen", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "pending_raw_refs", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "pending_resume", Type: FieldBytes, Width: 0, MaxBytes: 32 << 10, Required: true},
		{Name: "permit_id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "phase", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "pin_current", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "pin_previous", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "pin_valid_until_unix_ns", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "ready_latch", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "ready_latch_deadline_boot_ns", Type: FieldNonnegativeI64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "receipt_ids", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "register_token", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "state_ref", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "target_port", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "ticket", Type: FieldBytes, Width: 0, MaxBytes: 512, Required: true},
	}},
	{Kind: KindFlow, Fields: []FieldDescriptor{
		{Name: "bind_nonce", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "carrier_gen", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "cleanup_pending", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "close_mode", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "close_reason", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "close_receipt", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "connect_deadline_boot_ns", Type: FieldNonnegativeI64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "connect_so_error", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "connect_so_error_consumed", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "connect_state", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "endpoint_id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "fin_flags", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "final_applied", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "final_end", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "intent", Type: FieldBytes, Width: 32, MaxBytes: 32, Required: true},
		{Name: "open_reason", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "open_seq", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "open_status", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "owner_gen", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "phase", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "raw_dev", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "raw_ino", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "raw_read_error", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "raw_read_pending", Type: FieldBytes, Width: 0, MaxBytes: 32 << 10, Required: true},
		{Name: "raw_ref", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "raw_tuple", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "raw_type", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "replay_ref", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "revoked", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "rx_applied", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "rx_close_write", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "rx_fin", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "rx_fin_receipt", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "rx_pending", Type: FieldBytes, Width: 0, MaxBytes: 32 << 10, Required: true},
		{Name: "rx_received", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "target_port", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "tx_base", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "tx_end", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "tx_fin", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "tx_fin_receipt", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "tx_pending", Type: FieldBytes, Width: 0, MaxBytes: 32 << 10, Required: true},
		{Name: "tx_pending_error", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "tx_source_ended", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "writer_error", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
	}},
	{Kind: KindMaterial, Fields: []FieldDescriptor{
		{Name: "active", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "fingerprint", Type: FieldBytes, Width: 32, MaxBytes: 32, Required: true},
		{Name: "id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "key_ids", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "live_rx_salts", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "live_tx_salts", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "master", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "version", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
	}},
	{Kind: KindPermit, Fields: []FieldDescriptor{
		{Name: "consumed", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "cut_id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "deadline_boot_ns", Type: FieldNonnegativeI64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "endpoint_id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "image_digest", Type: FieldBytes, Width: 32, MaxBytes: 32, Required: true},
		{Name: "next_owner_gen", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "owner_gen", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "purpose", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "request_id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "resources_digest", Type: FieldBytes, Width: 32, MaxBytes: 32, Required: true},
		{Name: "revoked", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "ticket", Type: FieldBytes, Width: 0, MaxBytes: 512, Required: true},
	}},
	{Kind: KindReceipt, Fields: []FieldDescriptor{
		{Name: "carrier_gen", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "close_mode", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "cut_id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "endpoint_id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "fin_flags", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "final_applied", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "final_end", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "flow_id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "grant_digest", Type: FieldBytes, Width: 32, MaxBytes: 32, Required: true},
		{Name: "id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "image_digest", Type: FieldBytes, Width: 32, MaxBytes: 32, Required: true},
		{Name: "intent", Type: FieldBytes, Width: 32, MaxBytes: 32, Required: true},
		{Name: "next_owner_gen", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "open_status", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "owner_gen", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "reason", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "request_id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "revoked", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "ticket", Type: FieldBytes, Width: 0, MaxBytes: 512, Required: true},
		{Name: "verdict", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
	}},
	{Kind: KindRetirement, Fields: []FieldDescriptor{
		{Name: "endpoint_id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "holes", Type: FieldBytes, Width: 0, MaxBytes: 1024 * 8, Required: true},
		{Name: "id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "next_open_seq", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "retired_through", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "terminal_metadata", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
	}},
	{Kind: KindSaltRing, Fields: []FieldDescriptor{
		{Name: "count", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "fingerprint", Type: FieldBytes, Width: 32, MaxBytes: 32, Required: true},
		{Name: "head", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "material_id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "material_version", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "salts", Type: FieldBytes, Width: 0, MaxBytes: 8192 * 32, Required: true},
	}},
	{Kind: KindSSActor, Fields: []FieldDescriptor{
		{Name: "authority_deadline_boot_ns", Type: FieldNonnegativeI64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "authority_phase", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "authorized_key_id", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "candidate_index", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "cleanup_pending", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "conn_id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "connect_deadline_boot_ns", Type: FieldNonnegativeI64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "connect_so_error", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "connect_so_error_consumed", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "connect_state", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "dns_decided", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "dns_error", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "fired", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "first_payload_suffix", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "flow_id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "handshake_deadline_boot_ns", Type: FieldNonnegativeI64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "loopback_dev", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "loopback_ino", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "loopback_ref", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "loopback_tuple", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "loopback_type", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "material_fingerprint", Type: FieldBytes, Width: 32, MaxBytes: 32, Required: true},
		{Name: "material_id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "material_version", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "phase", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "policy_approved", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "resolved_ip_candidates", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "revoked", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "rx_applied", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "rx_clear_cursor", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "rx_clear_pending", Type: FieldBytes, Width: 0, MaxBytes: 32 << 10, Required: true},
		{Name: "rx_close_write", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "rx_decoded_length", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "rx_eof", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "rx_length_cipher", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "rx_length_cursor", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "rx_length_opened", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "rx_nonce", Type: FieldNonce12, Width: 12, MaxBytes: 0, Required: true},
		{Name: "rx_nonce_exhausted", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "rx_payload_cipher", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "rx_payload_cursor", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "rx_payload_opened", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "rx_salt", Type: FieldSalt32, Width: 32, MaxBytes: 0, Required: true},
		{Name: "rx_salt_cursor", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "rx_subkey", Type: FieldBytes, Width: 32, MaxBytes: 32, Required: true},
		{Name: "target_address", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "target_address_cursor", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "target_atyp", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "target_cursor", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "target_host", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "target_host_cursor", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "target_host_length", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "target_parser_phase", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "target_port", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "target_port_cursor", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "tx_applied", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "tx_cipher_pending", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "tx_cipher_write_cursor", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "tx_close_write", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "tx_eof", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "tx_length_cipher", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "tx_length_sealed", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "tx_nonce", Type: FieldNonce12, Width: 12, MaxBytes: 0, Required: true},
		{Name: "tx_nonce_exhausted", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "tx_payload_cipher", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "tx_payload_sealed", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "tx_plain_pending", Type: FieldBytes, Width: 0, MaxBytes: 32 << 10, Required: true},
		{Name: "tx_salt", Type: FieldSalt32, Width: 32, MaxBytes: 0, Required: true},
		{Name: "tx_salt_reserved", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "tx_salt_wire_cursor", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "tx_subkey", Type: FieldBytes, Width: 32, MaxBytes: 32, Required: true},
		{Name: "upstream_dev", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "upstream_ino", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "upstream_raw_read_error", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "upstream_raw_read_pending", Type: FieldBytes, Width: 0, MaxBytes: 32 << 10, Required: true},
		{Name: "upstream_ref", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "upstream_tuple", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "upstream_type", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
	}},
	{Kind: KindSSServer, Fields: []FieldDescriptor{
		{Name: "allow_private", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "applied_revision", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "authority_phase", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "binding_ids", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "cleanup_pending", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "conn_ids", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "dispatch_sequence", Type: FieldNonnegativeI64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "epoch", Type: FieldNonnegativeI64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "generation", Type: FieldNonnegativeI64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "keys", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "keyset_digest", Type: FieldBytes, Width: 32, MaxBytes: 32, Required: true},
		{Name: "last_applied_sequence", Type: FieldNonnegativeI64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "listener_dev", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "listener_ino", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "listener_ref", Type: FieldUTF8, Width: 0, MaxBytes: 4096, Required: true},
		{Name: "listener_tuple", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "listener_type", Type: FieldU64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "material_ids", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
		{Name: "revoked", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "timer_ids", Type: FieldBytes, Width: 0, MaxBytes: MaxStateBytes, Required: true},
	}},
	{Kind: KindTimer, Fields: []FieldDescriptor{
		{Name: "cleanup_pending", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "deadline_boot_ns", Type: FieldNonnegativeI64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "fail_close_deadline_boot_ns", Type: FieldNonnegativeI64, Width: 8, MaxBytes: 0, Required: true},
		{Name: "fired", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
		{Name: "id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "owner_id", Type: FieldID16, Width: 16, MaxBytes: 0, Required: true},
		{Name: "revoked", Type: FieldBool, Width: 1, MaxBytes: 0, Required: true},
	}},
}
