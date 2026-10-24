// declarations.go owns the `tether version` reader-declaration grammar. Callers
// bind the result to the probed executable bytes; parsing never infers an
// adjacent epoch, a feature or a domain that the candidate did not declare.
// origin: hot-update S4 plan §5.2 / T46; hot-update S8 plan §6.1 / §11.2 P17
// (moved from agentimage.ParseCapabilities, S8 review rv-n1 F5)
package upgradecompat

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Declarations are the reader lines of one version output. ReadEpochs nil means
// an older reader that never declared a set: it reads only its own epoch.
type Declarations struct {
	Release    string
	ABI        int
	Domains    []string
	Features   []string
	ReadEpochs []uint64
}

// EpochMismatchError reports a first line from another wire epoch. The caller
// owns the operator wording; an epoch change goes through reinstall.
type EpochMismatchError struct{ Got, Want string }

func (e *EpochMismatchError) Error() string {
	return "candidate reports " + strconv.Quote(e.Got) + ", this binary is " + strconv.Quote(e.Want)
}

// A reader set is a short explicit list. A bound keeps a hostile probe from
// spending unbounded validation work inside the 64 KiB capture.
const maxReadEpochs = 16

// ParseDeclarations checks the frozen version shape and ordered declarations.
// The first line must name subjectToken; every image declaration precedes the
// unique final ABI line, so a later line cannot hide an earlier invalid one.
func ParseDeclarations(output, subjectToken string) (Declarations, error) {
	var d Declarations
	domainsSeen, featuresSeen, epochsSeen, abiSeen := false, false, false, false
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if d.Release == "" {
			fields := strings.Fields(line)
			// Frozen shape: exactly `tether <release> (proto v<N>)`.
			if len(fields) != 4 || fields[0] != "tether" || fields[2] != "(proto" || !strings.HasSuffix(fields[3], ")") {
				return Declarations{}, fmt.Errorf("staged binary's version line %q is not `tether <release> (proto vN)`", line)
			}
			if epoch := strings.TrimSuffix(fields[3], ")"); epoch != subjectToken {
				return Declarations{}, &EpochMismatchError{Got: epoch, Want: subjectToken}
			}
			d.Release = fields[1]
			continue
		}
		// ABI is last: a later declaration cannot hide an earlier invalid one.
		// origin: hot-update S4 plan §5.2 / T46.
		if abiSeen {
			return Declarations{}, errors.New("candidate image ABI line must be last and unique")
		}
		var err error
		switch {
		case strings.HasPrefix(line, "image-abi"):
			suffix, ok := strings.CutPrefix(line, "image-abi v")
			n, convErr := strconv.Atoi(suffix)
			if !ok || convErr != nil || n <= 0 || strconv.Itoa(n) != suffix {
				return Declarations{}, fmt.Errorf("invalid candidate image ABI line %q", line)
			}
			d.ABI, abiSeen = n, true
		case strings.HasPrefix(line, "image-domains"):
			d.Domains, err = declaredTokens(line, "image-domains", &domainsSeen)
		case strings.HasPrefix(line, "image-features"):
			// Features name callable S8 adapters, never a new image domain.
			d.Features, err = declaredTokens(line, "image-features", &featuresSeen)
		case strings.HasPrefix(line, "image-read-epochs"):
			d.ReadEpochs, err = declaredEpochs(line, &epochsSeen)
		}
		if err != nil {
			return Declarations{}, err
		}
	}
	if d.Release == "" {
		return Declarations{}, errors.New("staged binary printed no version line")
	}
	if (domainsSeen || featuresSeen || epochsSeen) && !abiSeen {
		return Declarations{}, errors.New("candidate image declarations require an image ABI")
	}
	if !domainsSeen && d.ABI > 0 {
		d.Domains = []string{"exec-v1"}
	}
	return d, nil
}

// Unknown tokens describe the other reader; a caller never claims them.
func declaredTokens(line, name string, seenLine *bool) ([]string, error) {
	fields := strings.Fields(line)
	if *seenLine || len(fields) < 2 || fields[0] != name {
		return nil, errors.New("invalid or duplicate candidate " + name + " line")
	}
	*seenLine = true
	seen := make(map[string]bool)
	var tokens []string
	for _, token := range fields[1:] {
		if !validToken(token) || seen[token] {
			return nil, errors.New("invalid or duplicate candidate " + name + " token")
		}
		seen[token] = true
		tokens = append(tokens, token)
	}
	return tokens, nil
}

// Zero is never a wildcard and a repeated epoch is ambiguous evidence. The set
// is kept in declared order; adjacency is CheckEpochs' decision, not parsing's.
// origin: hot-update S8 plan §6.1 / S8 review rv-n1 F5
func declaredEpochs(line string, seenLine *bool) ([]uint64, error) {
	fields := strings.Fields(line)
	if *seenLine || len(fields) < 2 || len(fields) > maxReadEpochs+1 || fields[0] != "image-read-epochs" {
		return nil, errors.New("invalid or duplicate candidate image-read-epochs line")
	}
	*seenLine = true
	seen := make(map[uint64]bool)
	epochs := make([]uint64, 0, len(fields)-1)
	for _, field := range fields[1:] {
		epoch, err := strconv.ParseUint(field, 10, 64)
		if err != nil || epoch == 0 || strconv.FormatUint(epoch, 10) != field || seen[epoch] {
			return nil, errors.New("invalid or duplicate candidate image read epoch")
		}
		seen[epoch] = true
		epochs = append(epochs, epoch)
	}
	return epochs, nil
}

func validToken(token string) bool {
	if token == "" || token[0] < 'a' || token[0] > 'z' {
		return false
	}
	for _, c := range token {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			return false
		}
	}
	return true
}
