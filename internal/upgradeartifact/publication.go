// Package upgradeartifact publishes a grant-addressed immutable receipt beside
// the registered domain declaration. This is a stage output, never a daemon
// journal or mutable selector. Only Store accepts it with the original grant.
// origin: S8 internal review rv-txn F06
package upgradeartifact

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

func publicationName(grant upgradetx.Grant) string {
	data, _ := json.Marshal(grant)
	return "publication-" + digest(data) + ".json"
}

func publishReceipt(ctx context.Context, receipt Receipt, syncFile func(*os.File) error) (err error) {
	reg, err := upgradelock.RegistrationFor(receipt.Grant.Handle.Key)
	if err != nil {
		return err
	}
	if reg.CanonicalPath != receipt.CanonicalPath {
		return artifactError(upgradetx.CodeRejected, "publication canonical registration differs")
	}
	root, err := os.OpenRoot(reg.ControlDir)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	if err = publishObject(ctx, root, publicationName(receipt.Grant), data, syncFile); err != nil {
		return err
	}
	got, err := ReadPublication(reg.ControlDir, receipt.Grant)
	if err != nil {
		return err
	}
	if got != receipt {
		return artifactError(upgradetx.CodeRejected, "publication receipt conflicts")
	}
	return nil
}

// ReadPublication is deterministic and exact-grant bound; no directory search
// or running-executable inference is allowed. Expiry is checked by Start, while
// accepted cold resolution validates immutable evidence independently of TTL.
func ReadPublication(directory string, grant upgradetx.Grant) (receipt Receipt, err error) {
	path := filepath.Join(directory, publicationName(grant))
	if _, err := os.Lstat(path); err != nil {
		return Receipt{}, err
	}
	f, err := openImmutable(path, grant.Handle.Key, 0444)
	if err != nil {
		return Receipt{}, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	data, err := io.ReadAll(io.LimitReader(f, maxManifestBytes+1))
	if err != nil || len(data) > maxManifestBytes {
		return Receipt{}, artifactError(upgradetx.CodeStateCorrupt, "publication receipt size invalid")
	}
	var r Receipt
	if err = json.Unmarshal(data, &r); err != nil {
		return Receipt{}, artifactError(upgradetx.CodeStateCorrupt, "publication receipt invalid")
	}
	encoded, _ := json.Marshal(r)
	if !bytes.Equal(data, encoded) {
		return Receipt{}, artifactError(upgradetx.CodeStateCorrupt, "publication receipt encoding invalid")
	}
	if err = verifyReceipt(r, grant); err != nil {
		return Receipt{}, err
	}
	return r, nil
}
