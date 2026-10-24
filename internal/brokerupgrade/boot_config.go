// boot_config.go reads the broker configuration named by its sealed argument
// vector. The same pathname is the broker Key's ConfigPath, so a pristine
// redispatch, a cold canonical boot and the transaction key all hash the file
// Cobra consumes. Moved from brokerimage.BootConfigBytes (S8 P17 per-file budget).
// origin: hot-update S6 plan §4.1 / T03; hot-update S8 plan §5.2 / §11.2 P17
package brokerupgrade

import (
	"errors"
	"io"
	"os"
	"strings"
)

// BootConfigBytes reads only the sealed config pathname. Flag parsing and
// semantic validation remain with serve's pure preflight dispatcher. A second
// spelling is rejected to avoid hashing a different file than Cobra consumes.
// Every refusal is the caller's own sentinel, so its exit class is unchanged.
func BootConfigBytes(args []string, limit int64, refusal error) ([]byte, error) {
	var path string
	seen := false
	for i := 2; i < len(args); i++ {
		name, value, inline := strings.Cut(args[i], "=")
		if name != "--config" {
			continue
		}
		if seen {
			return nil, refusal
		}
		seen = true
		if !inline {
			i++
			if i == len(args) {
				return nil, refusal
			}
			value = args[i]
		}
		path = value
	}
	if path == "" {
		return nil, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, refusal
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	err = errors.Join(err, file.Close())
	if err != nil || int64(len(data)) > limit {
		return nil, refusal
	}
	return data, nil
}
