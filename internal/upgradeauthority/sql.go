package upgradeauthority

import (
	"database/sql"
	"errors"
	"strings"
	"time"
)

// ReadCreation compares inside the caller's actual transaction or serialized
// leader planner. It never opens another pool. CAST preserves the timestamp's
// exact stored text for the subsequent predicate instead of scanning time.Time
// and accidentally changing its zone, precision or monotonic suffix.
//
// This is the creation check only: the writer adds its ACTIVE/DELETING and node
// instance/configured-NID checks at the same mutation boundary. In particular,
// final session cleanup must be able to compare a DELETING session's identity.
// origin: hot-update S8 plan §6.2 / P08
func ReadCreation(reader RowReader, guard Guard) (Creation, error) {
	if reader == nil || !creationText(guard.SID, 32) {
		return Creation{}, creationUnavailable()
	}
	if guard.Incarnation != "" && !incarnationValid(guard.Incarnation) {
		return Creation{}, incarnationMismatch()
	}
	row := reader.QueryRow(`SELECT sid, owner_pubkey_fp, CAST(created_at AS TEXT), pin_hash FROM sessions WHERE sid=?`, guard.SID)
	if row == nil {
		return Creation{}, creationUnavailable()
	}
	var creation Creation
	if err := row.Scan(&creation.SID, &creation.Owner, &creation.CreatedAtText, &creation.EncodedPINHash); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Creation{}, incarnationMismatch()
		}
		return Creation{}, creationUnavailable()
	}
	// A RowReader supplied by an adapter still has to return the requested SID;
	// otherwise its valid, but foreign, creation could authorize the wrong row.
	if creation.SID != guard.SID {
		return Creation{}, incarnationMismatch()
	}
	actual, err := creationIncarnation(creation)
	if err != nil {
		return Creation{}, err
	}
	if err := Match(guard.Incarnation, actual); err != nil {
		return Creation{}, err
	}
	return creation, nil
}

// CreationPredicate binds the exact row that ReadCreation already compared.
// Salt-derived hashing cannot be repeated by SQLite (there is no SHA function)
// and comparing SID alone would let delete/recreate races authorize stale work.
// Only these local arguments contain the encoded hash; never serialize or log
// them. Cluster planners render each through the existing literal renderer and
// bake the predicate into their existing op, rather than adding Statement.Args.
// origin: hot-update S8 plan §6.2 / P08
func CreationPredicate(creation Creation) (string, []any, error) {
	if _, err := creationIncarnation(creation); err != nil {
		return "", nil, err
	}
	return `EXISTS (SELECT 1 FROM sessions AS upgrade_authority WHERE upgrade_authority.sid=? AND upgrade_authority.owner_pubkey_fp=? AND CAST(upgrade_authority.created_at AS TEXT)=? AND upgrade_authority.pin_hash=?)`,
		[]any{creation.SID, creation.Owner, creation.CreatedAtText, creation.EncodedPINHash}, nil
}

// Incarnation projects an already compared creation without duplicating the PHC
// or timestamp parser in an authority writer. It never exposes creation material.
// origin: hot-update S8 plan §6.2 / P16
func Incarnation(creation Creation) (string, error) { return creationIncarnation(creation) }

// Capture learns only at admission. Callers retain the returned guard and never
// call this with empty expected authority while performing delayed cleanup.
// origin: hot-update S8 plan §6.2 / P16
func Capture(reader RowReader, guard Guard) (Guard, error) {
	creation, err := ReadCreation(reader, guard)
	if err != nil {
		return guard, err
	}
	guard.Incarnation, err = Incarnation(creation)
	return guard, err
}

// PredicateSQL retains the parameter-bound creation predicate in a replicated
// statement. The caller supplies its existing literal renderer; splitting before
// insertion keeps question marks in creation material from becoming placeholders.
// origin: hot-update S8 plan §6.2 / P16
func PredicateSQL(creation Creation, literal func(string) (string, error)) (string, error) {
	predicate, args, err := CreationPredicate(creation)
	if err != nil {
		return "", err
	}
	parts := strings.Split(predicate, "?")
	for i, arg := range args {
		// Replicated predicates accept only the text operands the contract emits.
		// origin: hot-update S8 plan §6.2; S10 LINT-L4
		text, ok := arg.(string)
		if !ok {
			return "", creationUnavailable()
		}
		value, err := literal(text)
		if err != nil {
			return "", creationUnavailable()
		}
		parts[i] += value
	}
	return strings.Join(parts, ""), nil
}

// GuardPredicate is used only inside a caller's write boundary or in an atomic
// UPDATE that retains this exact predicate. It cannot refresh an expected guard.
// origin: hot-update S8 plan §6.2 / P16
func GuardPredicate(reader RowReader, guard Guard, literal func(string) (string, error)) (string, error) {
	creation, err := ReadCreation(reader, guard)
	if err != nil {
		return "", err
	}
	return PredicateSQL(creation, literal)
}

func creationIncarnation(creation Creation) (string, error) {
	createdAt, err := creationTimestamp(creation.CreatedAtText)
	if err != nil {
		return "", err
	}
	salt, err := creationSalt(creation.EncodedPINHash)
	if err != nil {
		return "", err
	}
	return Derive(creation.SID, creation.Owner, createdAt, salt)
}

// Both live session.Create and the leader's LitTime retain time.Time.String(),
// including local zones and monotonic readings. SQLite's DEFAULT uses a plain
// UTC timestamp; imports may contain RFC3339. Hash the instant in all cases,
// while the SQL predicate above deliberately retains the original text.
func creationTimestamp(text string) (int64, error) {
	if !creationText(text, 128) {
		return 0, creationUnavailable()
	}
	wall, monotonic, hasMonotonic := strings.Cut(text, " m=")
	if hasMonotonic {
		if len(monotonic) < 2 || (monotonic[0] != '+' && monotonic[0] != '-') {
			return 0, creationUnavailable()
		}
		seconds, fraction, ok := strings.Cut(monotonic[1:], ".")
		if !ok || seconds == "" || len(fraction) != 9 {
			return 0, creationUnavailable()
		}
		for _, digit := range seconds + fraction {
			if digit < '0' || digit > '9' {
				return 0, creationUnavailable()
			}
		}
	}
	// Unnamed fixed zones are rendered twice by time.Time.String, for example
	// "+0530 +0530". The MST layout cannot read that numeric zone name. Remove
	// only an identical duplicate, so conflicting offsets remain unprovable.
	fields := strings.Fields(wall)
	if len(fields) == 4 && len(fields[3]) == 5 && (fields[3][0] == '+' || fields[3][0] == '-') {
		if fields[2] != fields[3] {
			return 0, creationUnavailable()
		}
		wall = strings.TrimSuffix(wall, " "+fields[3])
	}
	for _, layout := range []string{
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999999 -0700",
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02 15:04",
		"2006-01-02T15:04",
		"2006-01-02",
	} {
		instant, err := time.Parse(layout, wall)
		if err != nil {
			continue
		}
		// UnixNano silently wraps outside its representable interval. A wrapped
		// timestamp must not give an invalid row the identity of a valid one.
		nanos := instant.UnixNano()
		if nanos <= 0 || !time.Unix(0, nanos).Equal(instant) {
			return 0, creationUnavailable()
		}
		return nanos, nil
	}
	return 0, creationUnavailable()
}
