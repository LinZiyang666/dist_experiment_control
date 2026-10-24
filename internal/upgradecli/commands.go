// Package upgradecli owns upgrade option validation and synchronous command
// execution. Command registration and transport adapters remain in cmd/tether.
// origin: hot-update S8 plan §5.2 / §7.1 / §11.2 P18
package upgradecli

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// UpgradeOptions preserves the existing node and broker first-call flags.
// A resume consumes the original grant instead of accepting a replacement URL,
// SHA or release. Key/NID parsing stays with the command that owns its namespace.
type UpgradeOptions struct {
	URL, SHA256, ToVersion, ExpectSHA256 string
	TxID, GrantOut, ArtifactReceipt      string
	PrepareOnly, Interrupting            bool
}

func ValidateUpgrade(component upgradetx.Component, opts UpgradeOptions) error {
	if component != upgradetx.ComponentAgent && component != upgradetx.ComponentBroker {
		return &upgradetx.Error{Code: upgradetx.CodeUnsupportedComponent, Detail: "S8 supports agent and broker upgrades"}
	}
	if opts.TxID != "" {
		if !hexValue(opts.TxID, 32) || opts.URL != "" || opts.SHA256 != "" || opts.ToVersion != "" || opts.ExpectSHA256 != "" || opts.PrepareOnly || opts.GrantOut != "" || opts.Interrupting {
			return rejected("resume requires only the original transaction and optional artifact receipt")
		}
		return nil
	}
	if opts.ArtifactReceipt != "" || opts.PrepareOnly != (opts.GrantOut != "") || opts.PrepareOnly && opts.Interrupting {
		return rejected("prepare-only requires grant-out and excludes interrupting; artifact receipt requires tx-id")
	}
	if component == upgradetx.ComponentAgent {
		if opts.URL == "" || !hexValue(opts.SHA256, 64) || opts.ToVersion != "" || opts.ExpectSHA256 != "" {
			return rejected("agent upgrade requires url and lowercase sha256")
		}
	} else if opts.ToVersion == "" || !hexValue(opts.ExpectSHA256, 64) || opts.URL != "" || opts.SHA256 != "" || opts.Interrupting {
		return rejected("broker reload requires to-version and lowercase expect-sha256")
	}
	return nil
}

type StageOptions struct {
	Grant, Source, Candidate, ReceiptOut string
}

// ParseStage is the leaf parser for the explicit root publication command.
// It neither starts a daemon nor writes a transaction journal.
func ParseStage(args []string) (StageOptions, error) {
	var opts StageOptions
	flags := flag.NewFlagSet("upgrade stage", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&opts.Grant, "grant", "", "target-issued grant file")
	flags.StringVar(&opts.Source, "source", "", "running source image")
	flags.StringVar(&opts.Candidate, "candidate", "", "candidate image")
	flags.StringVar(&opts.ReceiptOut, "receipt-out", "", "artifact receipt file")
	if flags.Parse(args) != nil || flags.NArg() != 0 || opts.Grant == "" || opts.Source == "" || opts.Candidate == "" || opts.ReceiptOut == "" {
		return StageOptions{}, rejected("stage requires grant, source, candidate and receipt-out files")
	}
	return opts, nil
}

// Stage delegates privileged issuer/path/manifest checks to P05's publisher.
// Passing that frozen function explicitly keeps an unwired command fail-closed;
// no global callback table or fallback publication path can mint a receipt.
func Stage(ctx context.Context, opts StageOptions, publish upgradeartifact.PublishFunc) (upgradeartifact.Receipt, error) {
	var empty upgradeartifact.Receipt
	if publish == nil {
		return empty, &upgradetx.Error{Code: upgradetx.CodeNotWired, Detail: "artifact publisher is required"}
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	for _, path := range []string{opts.Grant, opts.Source, opts.Candidate, opts.ReceiptOut} {
		if path == "" {
			return empty, rejected("stage requires four file paths")
		}
	}
	var grant upgradetx.Grant
	if err := readJSON(opts.Grant, &grant); err != nil {
		return empty, rejected("invalid stage grant file")
	}
	// origin: hot-update S8 plan §5.2 / P18 — prevent receipt output from
	// replacing an input or adopting another transaction with identical SHAs.
	if err := checkReceiptPath(opts, grant); err != nil {
		return empty, err
	}
	receipt, err := publishImages(ctx, opts, grant, publish)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(receipt.Grant, grant) || receipt.SourceSHA != grant.SourceSHA || receipt.TargetSHA != grant.TargetSHA || receipt.CanonicalPath == "" || receipt.SourcePath == "" || receipt.TargetPath == "" || !hexValue(receipt.ManifestDigest, 64) {
		return empty, unsettled("publisher returned an unbound artifact receipt")
	}
	if err := writeJSONFile(opts.ReceiptOut, receipt); err != nil {
		return empty, unsettled("artifact receipt persistence is uncertain; query the original grant")
	}
	return receipt, nil
}

// Close both borrowed image readers before persisting a success receipt. A
// publication error keeps its classification; failed cleanup cannot sign success.
// origin: hot-update S8 plan §5.2 / LINT-s8
func publishImages(ctx context.Context, opts StageOptions, grant upgradetx.Grant, publish upgradeartifact.PublishFunc) (receipt upgradeartifact.Receipt, err error) {
	source, err := os.Open(opts.Source)
	if err != nil {
		return receipt, rejected("cannot open stage source image")
	}
	defer func() {
		if closeErr := source.Close(); closeErr != nil && err == nil {
			err = unsettled("stage source image cleanup failed")
		}
	}()
	target, err := os.Open(opts.Candidate)
	if err != nil {
		return receipt, rejected("cannot open stage candidate image")
	}
	defer func() {
		if closeErr := target.Close(); closeErr != nil && err == nil {
			err = unsettled("stage candidate image cleanup failed")
		}
	}()
	return publish(ctx, grant, source, target)
}

// Client performs read-only queries and waits on the caller goroutine. Completed
// is P01's proof validator; the CLI never replaces it with a phase/SHA heuristic.
type Client struct {
	Endpoint     upgradeport.Endpoint
	Completed    upgradetx.CompletedFunc
	PollInterval time.Duration
}

func (c Client) Query(ctx context.Context, key upgradetx.Key, txID string) (upgradetx.View, error) {
	if !hexValue(txID, 32) {
		return upgradetx.View{}, rejected("query requires a transaction ID")
	}
	if c.Endpoint == nil {
		return upgradetx.View{}, &upgradetx.Error{Code: upgradetx.CodeNotWired, Detail: "upgrade endpoint is required"}
	}
	if err := ctx.Err(); err != nil {
		return upgradetx.View{}, err
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return upgradetx.View{}, unsettled("cannot generate query nonce")
	}
	nonce := hex.EncodeToString(random[:])
	view, err := c.Endpoint.Query(ctx, key, txID, nonce)
	if err != nil {
		return view, err
	}
	// origin: hot-update S8 plan §7.1 / M15 — a cached completed reply is
	// history, never a fresh observation. Unknown ownership remains queryable.
	if view.Observation.Nonce != nonce || view.Observation.At <= 0 || view.Historical.Key != key || view.Historical.Grant.Handle.Key != key || view.Historical.Grant.Handle.TxID != txID {
		return view, unsettled("query response does not match its transaction and fresh nonce")
	}
	if err := ctx.Err(); err != nil {
		return view, err
	}
	return view, nil
}

// Wait's timeout is absolute and includes transport time. rollbackGoal changes
// only the requested result: an automatic rollback misses the upgrade goal.
func (c Client) Wait(ctx context.Context, key upgradetx.Key, txID string, timeout time.Duration, rollbackGoal bool) (upgradetx.View, error) {
	return c.wait(ctx, key, txID, timeout, func(upgradetx.View) bool { return rollbackGoal })
}

// WaitRecorded is Wait whose goal is read from each fresh view of the record
// itself (moved verbatim from verbs.go reverseGoal):
// A standalone reverse record (manual rollback of a completed execution) has
// no forward attempt; its goal is the reverse target. An automatic reverse
// inside a forward record still misses the requested upgrade target.
// Reading it inside the wait keeps one absolute deadline and the same
// transient-refusal retry for the very first query, which reverseGoal's
// separate first query lacked.
// origin: hot-update S8 plan §7.1 / §8.1; S8 FIX-DESCRIBE (real-cut finding)
func (c Client) WaitRecorded(ctx context.Context, key upgradetx.Key, txID string, timeout time.Duration) (upgradetx.View, error) {
	return c.wait(ctx, key, txID, timeout, func(view upgradetx.View) bool {
		return view.Historical.Forward.Handle.TxID == "" && view.Historical.Reverse.Handle.TxID == txID
	})
}

// transientQuery is a query refused before it reached any effect: the route
// had no responder, or its reply was lost (deferred, never accepted). During
// a cut the target is between its exec and its new subscriptions, so the
// first queries after an accepted Start meet exactly this. A query has no
// effect, so it may be asked again with a fresh nonce.
func transientQuery(err error) bool {
	var typed *upgradetx.Error
	return errors.As(err, &typed) && typed.Code == upgradetx.CodeDeferred && !typed.Accepted
}

func (c Client) wait(ctx context.Context, key upgradetx.Key, txID string, timeout time.Duration, rollbackGoal func(upgradetx.View) bool) (upgradetx.View, error) {
	if timeout <= 0 || c.PollInterval < 0 {
		return upgradetx.View{}, rejected("wait requires a positive timeout and nonnegative poll interval")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	interval := c.PollInterval
	if interval == 0 {
		interval = 250 * time.Millisecond
	}
	var last upgradetx.View
	for {
		view, err := c.Query(ctx, key, txID)
		last = view
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return last, unsettled("wait ended before the transaction was settled")
			}
			// origin: S8 FIX-DESCRIBE (real-cut finding). Wait is the verb that
			// retries a transient refusal; only its absolute deadline ends it,
			// as unknown (74). Ending at the first lost reply turned every
			// prompt Start of a live agent cut into a false 75.
			if !transientQuery(err) {
				return last, err
			}
		} else if done, err := c.waitResult(view, key, txID, rollbackGoal(view)); done {
			return view, err
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return last, unsettled("wait ended before the transaction was settled")
		case <-timer.C:
		}
	}
}

func (c Client) waitResult(view upgradetx.View, key upgradetx.Key, txID string, rollbackGoal bool) (bool, error) {
	current := view.Observation.Current
	if !view.Observation.OwnerKnown || current.Handle.Key != key || current.Handle.TxID != txID || current.Outcome == upgradetx.OutcomeUnknown {
		return true, unsettled("current transaction or owner is unknown")
	}
	if view.Code != "" {
		return true, &upgradetx.Error{Code: view.Code, Detail: view.Detail, Accepted: current.CutStage != upgradetx.CutNone}
	}
	// A legacy observation has no causal S8 completion proof, even if a
	// compatibility projection calls its historical execution completed.
	if view.Historical.Grant.Mode == upgradetx.ModeLegacyInterrupting || current.Outcome == upgradetx.OutcomeLegacyInterrupting {
		return true, unsettled("legacy interrupting execution has no hot completion proof")
	}
	switch current.Phase {
	case upgradetx.PhaseRecoveryRequired, upgradetx.PhaseRollbackBlocked, upgradetx.PhaseAbandoned:
		return true, unsettled("transaction requires recovery; preserve current business and query its state")
	case upgradetx.PhaseDeferred:
		return true, &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: view.Detail, Accepted: current.CutStage != upgradetx.CutNone}
	case upgradetx.PhaseRejected, upgradetx.PhaseCancelled:
		if current.Phase == upgradetx.PhaseRejected && current.CutStage != upgradetx.CutNone {
			return true, unsettled("rejected transaction has an unsettled cut")
		}
		return true, rejected("requested upgrade target was not reached")
	case upgradetx.PhaseCompleted, upgradetx.PhaseRolledBack:
		attempt := current
		if current.Phase == upgradetx.PhaseRolledBack {
			attempt = view.Historical.Reverse
			if attempt.TargetSHA != view.Historical.RecordedPreviousSHA || attempt.Direction != upgradetx.DirectionReverse || attempt.Handle != current.Handle {
				return true, unsettled("rollback lacks a matching current reverse attempt")
			}
		}
		if c.Completed == nil {
			return true, &upgradetx.Error{Code: upgradetx.CodeNotWired, Detail: "completion proof validator is required", Accepted: true}
		}
		if err := c.Completed(attempt, view.Observation); err != nil {
			return true, unsettled("current completion proof is incomplete")
		}
		reversed := attempt.Direction == upgradetx.DirectionReverse
		if rollbackGoal != reversed {
			return true, rejected("requested target was not reached; current attempt is settled")
		}
		return true, nil
	case upgradetx.PhasePrepared, upgradetx.PhaseStaged, upgradetx.PhaseReady, upgradetx.PhaseFreezing, upgradetx.PhaseActivating, upgradetx.PhaseServingNew, upgradetx.PhaseMigrating, upgradetx.PhaseRollbackPending:
		return false, nil
	default:
		return true, unsettled("transaction phase is unknown")
	}
}

func hexValue(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, c := range value {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func rejected(detail string) error {
	return &upgradetx.Error{Code: upgradetx.CodeRejected, Detail: detail}
}
func unsettled(detail string) error {
	return &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: detail}
}

func readJSON(path string, value any) (err error) {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()
	data, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return rejected("authorization file exceeds its size limit")
	}
	// JSON's usual last-key-wins rule would hide a conflicting grant identity.
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := uniqueJSON(decoder, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return rejected("authorization file has trailing data")
	}
	decoder = json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(value)
}

func uniqueJSON(decoder *json.Decoder, depth int) error {
	if depth > 64 {
		return rejected("authorization file nesting exceeds its limit")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	seen := make(map[string]bool)
	for decoder.More() {
		if delim == '{' {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			// Struct decoding folds case; an uppercase spelling could otherwise
			// override the same snake_case grant field without looking duplicated.
			if !ok || name != strings.ToLower(name) || seen[name] {
				return rejected("authorization file has duplicate fields")
			}
			seen[name] = true
		}
		if err := uniqueJSON(decoder, depth+1); err != nil {
			return err
		}
	}
	_, err = decoder.Token()
	return err
}

func checkReceiptPath(opts StageOptions, grant upgradetx.Grant) error {
	output, err := filepath.Abs(opts.ReceiptOut)
	if err != nil {
		return rejected("invalid receipt output path")
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(output))
	if err != nil {
		return rejected("receipt output directory must exist")
	}
	output = filepath.Join(parent, filepath.Base(output))
	outInfo, outErr := os.Lstat(output)
	if outErr != nil && !errors.Is(outErr, os.ErrNotExist) || outErr == nil && !outInfo.Mode().IsRegular() {
		return rejected("receipt output must be a regular file")
	}
	for _, path := range []string{opts.Grant, opts.Source, opts.Candidate} {
		input, err := filepath.Abs(path)
		if err != nil {
			return rejected("invalid stage input path")
		}
		// Resolve the parent only: /proc/PID/exe can name an open executable
		// whose old pathname has been unlinked. Stat still identifies its inode.
		parent, err := filepath.EvalSymlinks(filepath.Dir(input))
		if err != nil {
			return rejected("stage input directory must exist")
		}
		input = filepath.Join(parent, filepath.Base(input))
		if input == output {
			return rejected("receipt output must not replace a stage input")
		}
		info, err := os.Stat(input)
		if err != nil || outErr == nil && os.SameFile(outInfo, info) {
			return rejected("receipt output aliases a stage input")
		}
	}
	if outErr == nil {
		var old upgradeartifact.Receipt
		if readJSON(output, &old) != nil || !reflect.DeepEqual(old.Grant, grant) {
			return rejected("receipt output belongs to an unknown or different grant")
		}
	}
	return nil
}

// writeJSONFile is the atomic, synced writer shared by artifact receipts and
// inert grants: temp file, fsync, rename, parent-directory fsync.
func writeJSONFile(path string, value any) (err error) {
	file, err := os.CreateTemp(filepath.Dir(path), ".upgrade-receipt-*")
	if err != nil {
		return err
	}
	// Rename consumes the temporary path. Cleanup must ignore that absence,
	// while checking real removal failures and never closing the FD twice.
	defer func() {
		if removeErr := os.Remove(file.Name()); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, removeErr)
		}
	}()
	closed := false
	defer func() {
		if !closed {
			if closeErr := file.Close(); closeErr != nil {
				err = errors.Join(err, closeErr)
			}
		}
	}()
	if err := json.NewEncoder(file).Encode(value); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	closed = true
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := dir.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	return dir.Sync()
}
