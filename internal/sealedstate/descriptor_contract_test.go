package sealedstate

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// Names are binary field IDs. This explicit vector is a contract receipt, not
// a generated runtime inventory or a claim that P9's codec has already landed.
// origin: hot-update S5 plan §3 / §4.1 / §4.4 / §6.6 / §12.1; I1 reconnect
// state adds seven required endpoint fields (257 -> 264), keeping credentials,
// route/pins and the original retry request in the declared sealed schema.
func TestPrivateCutDescriptorsKeepRequiredState(t *testing.T) {
	kinds := []string{KindAuthority, KindBinding, KindEndpoint, KindFlow, KindMaterial, KindPermit, KindReceipt, KindRetirement, KindSaltRing, KindSSActor, KindSSServer, KindTimer}
	wantKinds := []string{"authority", "binding", "endpoint", "flow", "material", "permit", "receipt", "retirement", "salt-ring", "ss-actor", "ss-server", "timer"}
	if !reflect.DeepEqual(kinds, wantKinds) || len(RecordDescriptors) != len(kinds) {
		t.Fatal("private record kind set changed")
	}
	want := []string{
		"allocation_epoch/2/8/0/true allocation_nid/5/0/4096/true allocation_port/1/8/0/true allocation_sid/5/0/4096/true allocation_token_hash/5/0/4096/true applied_revision/1/8/0/true deadline_boot_ns/2/8/0/true dispatch_sequence/2/8/0/true enabled/3/1/0/true epoch/2/8/0/true generation/2/8/0/true id/6/16/0/true keys/4/0/33554432/true keyset_digest/4/32/32/true last_applied_sequence/2/8/0/true phase/1/8/0/true revoked/3/1/0/true",
		"conn_id/6/16/0/true consumed/3/1/0/true deadline_boot_ns/2/8/0/true flow_id/6/16/0/true id/6/16/0/true tuple/4/0/33554432/true",
		"allocation_epoch/2/8/0/true allocation_nid/5/0/4096/true allocation_port/1/8/0/true allocation_sid/5/0/4096/true allocation_token_hash/5/0/4096/true boot_id/5/0/4096/true broker_addr/5/0/4096/true carrier_gen/1/8/0/true cleanup_pending/3/1/0/true closed_allocation/3/1/0/true default_broker_addr/5/0/4096/true fence_allocation_gen/1/8/0/true fence_port_gen/1/8/0/true fence_session_gen/1/8/0/true first_detach_deadline_boot_ns/2/8/0/true flow_ids/4/0/33554432/true forgotten/3/1/0/true id/6/16/0/true instance_id/5/0/4096/true listener_ref/5/0/4096/true owner_gen/1/8/0/true pending_raw_refs/4/0/33554432/true pending_resume/4/0/32768/true permit_id/6/16/0/true phase/1/8/0/true pin_current/5/0/4096/true pin_previous/5/0/4096/true pin_valid_until_unix_ns/1/8/0/true ready_latch/3/1/0/true ready_latch_deadline_boot_ns/2/8/0/true receipt_ids/4/0/33554432/true register_token/5/0/4096/true state_ref/5/0/4096/true target_port/1/8/0/true ticket/4/0/512/true",
		"bind_nonce/6/16/0/true carrier_gen/1/8/0/true cleanup_pending/3/1/0/true close_mode/1/8/0/true close_reason/1/8/0/true close_receipt/3/1/0/true connect_deadline_boot_ns/2/8/0/true connect_so_error/1/8/0/true connect_so_error_consumed/3/1/0/true connect_state/1/8/0/true endpoint_id/6/16/0/true fin_flags/1/8/0/true final_applied/4/0/33554432/true final_end/4/0/33554432/true id/6/16/0/true intent/4/32/32/true open_reason/1/8/0/true open_seq/1/8/0/true open_status/1/8/0/true owner_gen/1/8/0/true phase/1/8/0/true raw_dev/1/8/0/true raw_ino/1/8/0/true raw_read_error/5/0/4096/true raw_read_pending/4/0/32768/true raw_ref/5/0/4096/true raw_tuple/4/0/33554432/true raw_type/1/8/0/true replay_ref/5/0/4096/true revoked/3/1/0/true rx_applied/1/8/0/true rx_close_write/3/1/0/true rx_fin/1/8/0/true rx_fin_receipt/3/1/0/true rx_pending/4/0/32768/true rx_received/1/8/0/true target_port/1/8/0/true tx_base/1/8/0/true tx_end/1/8/0/true tx_fin/1/8/0/true tx_fin_receipt/3/1/0/true tx_pending/4/0/32768/true tx_pending_error/5/0/4096/true tx_source_ended/3/1/0/true writer_error/5/0/4096/true",
		"active/3/1/0/true fingerprint/4/32/32/true id/6/16/0/true key_ids/4/0/33554432/true live_rx_salts/4/0/33554432/true live_tx_salts/4/0/33554432/true master/4/0/33554432/true version/1/8/0/true",
		"consumed/3/1/0/true cut_id/6/16/0/true deadline_boot_ns/2/8/0/true endpoint_id/6/16/0/true id/6/16/0/true image_digest/4/32/32/true next_owner_gen/1/8/0/true owner_gen/1/8/0/true purpose/1/8/0/true request_id/6/16/0/true resources_digest/4/32/32/true revoked/3/1/0/true ticket/4/0/512/true",
		"carrier_gen/1/8/0/true close_mode/1/8/0/true cut_id/6/16/0/true endpoint_id/6/16/0/true fin_flags/1/8/0/true final_applied/4/0/33554432/true final_end/4/0/33554432/true flow_id/6/16/0/true grant_digest/4/32/32/true id/6/16/0/true image_digest/4/32/32/true intent/4/32/32/true next_owner_gen/1/8/0/true open_status/1/8/0/true owner_gen/1/8/0/true reason/1/8/0/true request_id/6/16/0/true revoked/3/1/0/true ticket/4/0/512/true verdict/1/8/0/true",
		"endpoint_id/6/16/0/true holes/4/0/8192/true id/6/16/0/true next_open_seq/1/8/0/true retired_through/1/8/0/true terminal_metadata/4/0/33554432/true",
		"count/1/8/0/true fingerprint/4/32/32/true head/1/8/0/true id/6/16/0/true material_id/6/16/0/true material_version/1/8/0/true salts/4/0/262144/true",
		"authority_deadline_boot_ns/2/8/0/true authority_phase/1/8/0/true authorized_key_id/5/0/4096/true candidate_index/1/8/0/true cleanup_pending/3/1/0/true conn_id/6/16/0/true connect_deadline_boot_ns/2/8/0/true connect_so_error/1/8/0/true connect_so_error_consumed/3/1/0/true connect_state/1/8/0/true dns_decided/3/1/0/true dns_error/5/0/4096/true fired/3/1/0/true first_payload_suffix/4/0/33554432/true flow_id/6/16/0/true handshake_deadline_boot_ns/2/8/0/true id/6/16/0/true loopback_dev/1/8/0/true loopback_ino/1/8/0/true loopback_ref/5/0/4096/true loopback_tuple/4/0/33554432/true loopback_type/1/8/0/true material_fingerprint/4/32/32/true material_id/6/16/0/true material_version/1/8/0/true phase/1/8/0/true policy_approved/3/1/0/true resolved_ip_candidates/4/0/33554432/true revoked/3/1/0/true rx_applied/1/8/0/true rx_clear_cursor/1/8/0/true rx_clear_pending/4/0/32768/true rx_close_write/3/1/0/true rx_decoded_length/1/8/0/true rx_eof/3/1/0/true rx_length_cipher/4/0/33554432/true rx_length_cursor/1/8/0/true rx_length_opened/3/1/0/true rx_nonce/7/12/0/true rx_nonce_exhausted/3/1/0/true rx_payload_cipher/4/0/33554432/true rx_payload_cursor/1/8/0/true rx_payload_opened/3/1/0/true rx_salt/8/32/0/true rx_salt_cursor/1/8/0/true rx_subkey/4/32/32/true target_address/4/0/33554432/true target_address_cursor/1/8/0/true target_atyp/1/8/0/true target_cursor/1/8/0/true target_host/4/0/33554432/true target_host_cursor/1/8/0/true target_host_length/1/8/0/true target_parser_phase/1/8/0/true target_port/4/0/33554432/true target_port_cursor/1/8/0/true tx_applied/1/8/0/true tx_cipher_pending/4/0/33554432/true tx_cipher_write_cursor/1/8/0/true tx_close_write/3/1/0/true tx_eof/3/1/0/true tx_length_cipher/4/0/33554432/true tx_length_sealed/3/1/0/true tx_nonce/7/12/0/true tx_nonce_exhausted/3/1/0/true tx_payload_cipher/4/0/33554432/true tx_payload_sealed/3/1/0/true tx_plain_pending/4/0/32768/true tx_salt/8/32/0/true tx_salt_reserved/3/1/0/true tx_salt_wire_cursor/1/8/0/true tx_subkey/4/32/32/true upstream_dev/1/8/0/true upstream_ino/1/8/0/true upstream_raw_read_error/5/0/4096/true upstream_raw_read_pending/4/0/32768/true upstream_ref/5/0/4096/true upstream_tuple/4/0/33554432/true upstream_type/1/8/0/true",
		"allow_private/3/1/0/true applied_revision/1/8/0/true authority_phase/1/8/0/true binding_ids/4/0/33554432/true cleanup_pending/3/1/0/true conn_ids/4/0/33554432/true dispatch_sequence/2/8/0/true epoch/2/8/0/true generation/2/8/0/true id/6/16/0/true keys/4/0/33554432/true keyset_digest/4/32/32/true last_applied_sequence/2/8/0/true listener_dev/1/8/0/true listener_ino/1/8/0/true listener_ref/5/0/4096/true listener_tuple/4/0/33554432/true listener_type/1/8/0/true material_ids/4/0/33554432/true revoked/3/1/0/true timer_ids/4/0/33554432/true",
		"cleanup_pending/3/1/0/true deadline_boot_ns/2/8/0/true fail_close_deadline_boot_ns/2/8/0/true fired/3/1/0/true id/6/16/0/true owner_id/6/16/0/true revoked/3/1/0/true",
	}
	count := 0
	for i, record := range RecordDescriptors {
		if record.Kind != kinds[i] {
			t.Fatalf("record %d=%s want=%s", i, record.Kind, kinds[i])
		}
		var got []string
		previous := ""
		haveID := false
		for _, field := range record.Fields {
			if field.Name <= previous || field.Name != strings.ToLower(field.Name) {
				t.Fatalf("%s field names not unique ASCII IDs", record.Kind)
			}
			previous = field.Name
			if field.Name == "id" {
				haveID = field.Type == FieldID16 && field.Required
			}
			if !field.Required || field.Type < FieldU64 || field.Type > FieldSalt32 {
				t.Fatalf("%s omits a required decision or uses an unknown type", record.Kind)
			}
			got = append(got, fmt.Sprintf("%s/%d/%d/%d/%t", field.Name, field.Type, field.Width, field.MaxBytes, field.Required))
			count++
		}
		if !haveID {
			t.Fatalf("%s has no unique required ID", record.Kind)
		}
		if strings.Join(got, " ") != want[i] {
			t.Errorf("%s private descriptor fields drifted", record.Kind)
		}
	}
	if count != 264 {
		t.Fatalf("private cut fields=%d want=264", count)
	}
}
