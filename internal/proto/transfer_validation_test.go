package proto

import "testing"

// origin: hot-update S10 plan §0 / LINT-L2 forcetypeassert
func TestTransferWireRejectsMismatchedAuthorityFieldTypes(t *testing.T) {
	value := "unexpected-private-value"
	for name, input := range map[string]any{
		"identity": struct {
			CallID   string `json:"call_id"`
			Identity string
		}{CallID: "call", Identity: value},
		"origin": struct {
			CallID string `json:"call_id"`
			Origin string
		}{CallID: "call", Origin: value},
		"continuation": struct {
			CallID       string
			TransferID   string
			Continuation *string
		}{CallID: "call", TransferID: "transfer", Continuation: &value},
		"continuation_value": struct {
			Continuation string
		}{Continuation: value},
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("wire validator panicked on a mismatched authority field: %v", recovered)
				}
			}()
			if err := ValidateTransferWire(input); err == nil {
				t.Fatal("mismatched authority field was accepted")
			}
		})
	}
}
