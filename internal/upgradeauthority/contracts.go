// Package upgradeauthority identifies the creation of a session independently
// of its reusable SID. Comparisons belong inside the actual writer transaction
// or leader planner; an earlier handler lookup cannot prevent recreation races.
// origin: hot-update S8 plan §2.2 / §6.2 / P00
package upgradeauthority

import "database/sql"

// Capability is vocabulary only; P13/P17 may advertise it only after the real
// guarded writers and decoders are wired. P00 never advertises capabilities.
const Capability = "authority-incarnation-v1"

type Guard struct {
	SID           string `json:"sid"`
	Incarnation   string `json:"incarnation"`
	InstanceID    string `json:"instance_id"`
	ConfiguredNID string `json:"configured_nid"`
}

// Creation is local SQL comparison material, not a wire or image DTO. In
// particular the encoded hash may only bind SQL parameters, never JSON or logs.
// The digest uses the parsed creation salt, not the re-encodable hash text.
type Creation struct {
	SID            string `json:"-"`
	Owner          string `json:"-"`
	CreatedAtText  string `json:"-"`
	EncodedPINHash string `json:"-"`
}

// database/sql's argument list is the sole intentional any boundary. Business
// proofs keep concrete types; readers must be the current transaction/planner.
type RowReader interface {
	QueryRow(query string, args ...any) *sql.Row
}

type DeriveFunc = func(sid, owner string, createdAt int64, salt []byte) (string, error)
type MatchFunc = func(expected, actual string) error
type ReadCreationFunc = func(reader RowReader, guard Guard) (Creation, error)
type CreationPredicateFunc = func(creation Creation) (string, []any, error)
