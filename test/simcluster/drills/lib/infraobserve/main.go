// Command infraobserve exposes the existing P02 physical reader to deployment
// instruments. It has no repair, signal, service-start or product-status verb.
// origin: hot-update S9 plan §2.3 / §9.1 / LQ1; S9 INSTRUMENTS-3.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
)

type request struct {
	Operation string                `json:"operation"`
	PID       int32                 `json:"pid"`
	Unit      string                `json:"unit"`
	Scope     infrahost.CensusScope `json:"scope"`
	Path      string                `json:"path"`
	Probe     probe                 `json:"probe"`
}

type probe struct {
	Address    string `json:"address"`
	Protocol   string `json:"protocol"`
	ServerName string `json:"server_name"`
	CAFile     string `json:"ca_file"`
}

type unitReceipt struct {
	Process c.Process `json:"process"`
	User    string    `json:"user"`
	Restart string    `json:"restart"`
	Managed bool      `json:"managed"`
	Census  c.Census  `json:"census"`
	Serving bool      `json:"serving"`
}

func decode(raw []byte) (request, error) {
	var r request
	if len(raw) == 0 || len(raw) > c.MaxRequestBytes {
		return r, c.ErrIncomplete
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&r); err != nil {
		return r, c.ErrIncomplete
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return r, c.ErrIncomplete
	}
	return r, nil
}

func unit(r request, deadline c.Deadline) (_ unitReceipt, resultErr error) {
	var result unitReceipt
	// Unit names are identifiers; a deployment cannot turn an observation
	// into a systemctl option, arbitrary command, or host path.
	if r.Unit == "" || strings.ContainsAny(r.Unit, "/\x00\n\r \t") || strings.HasPrefix(r.Unit, "-") {
		return result, c.ErrIncomplete
	}
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return result, err
	}
	// ctx-root: one bounded read-only systemd observation in this tool.
	ctx, cancel := context.WithTimeout(context.Background(), remaining)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/usr/bin/systemctl", "show", r.Unit,
		"--property=MainPID,InvocationID,ControlGroup,User,Restart,LoadState")
	raw, err := cmd.Output()
	if err != nil || len(raw) > 16384 {
		return result, c.ErrIncomplete
	}
	values := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return result, c.ErrIncomplete
		}
		if _, exists := values[key]; exists {
			return result, c.ErrIncomplete
		}
		values[key] = value
	}
	pid, err := strconv.ParseInt(values["MainPID"], 10, 32)
	if err != nil || pid <= 0 || values["LoadState"] != "loaded" {
		return result, c.ErrIncomplete
	}
	result.Process, err = infrahost.ReadProcess(int32(pid), deadline)
	if err != nil || result.Process.InvocationID != values["InvocationID"] || result.Process.Cgroup != values["ControlGroup"] {
		return result, c.ErrIncomplete
	}
	result.User, result.Restart = values["User"], values["Restart"]
	if result.User == "" {
		result.User = "root"
	}
	result.Managed = result.User == "tether" && result.Restart == "always"
	if r.Operation == "unit-process" {
		return result, nil
	}
	// The deployment supplies the complete old/main/temporary scope. A single
	// MainPID or a Status census is deliberately insufficient for retirement.
	if len(r.Scope.Before) == 0 {
		r.Scope.Before = []c.Process{result.Process}
	}
	// Keep a real protocol connection open throughout the P02 census. An
	// accepted socket belonging to shadow cannot certify that main is serving.
	var held net.Conn
	if r.Probe.Address != "" {
		held, err = serviceProbe(ctx, r.Probe, deadline)
		if err != nil {
			return result, err
		}
		defer func() { resultErr = errors.Join(resultErr, held.Close()) }()
	}
	result.Census, err = infrahost.ReadCensus(r.Scope, deadline)
	if err == nil && held != nil {
		for _, socket := range result.Census.Sockets {
			if socket.Owner == result.Process && socket.Local == held.RemoteAddr().String() && socket.Remote == held.LocalAddr().String() {
				result.Serving = true
			}
		}
	}
	return result, err
}

func observe(r request) (_ any, resultErr error) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		return nil, c.ErrUnsupported
	}
	deadline, err := controlbudget.After(8 * time.Second)
	if err != nil {
		return nil, err
	}
	switch r.Operation {
	case "file":
		// Only length and digest leave target custody, including on failure.
		if !filepath.IsAbs(r.Path) || strings.Contains(r.Path, "/.tether/") {
			return nil, c.ErrIncomplete
		}
		f, err := os.Open(r.Path)
		if err != nil {
			return nil, c.ErrIncomplete
		}
		defer func() { resultErr = errors.Join(resultErr, f.Close()) }()
		info, err := f.Stat()
		if err != nil || !info.Mode().IsRegular() || info.Size() > 512<<20 {
			return nil, c.ErrIncomplete
		}
		hash := sha256.New()
		count, err := io.Copy(hash, io.LimitReader(f, info.Size()))
		if err != nil || count != info.Size() {
			return nil, c.ErrIncomplete
		}
		if _, err := controlbudget.Remaining(deadline); err != nil {
			return nil, err
		}
		return map[string]any{"length": count, "sha256": fmt.Sprintf("%x", hash.Sum(nil))}, nil
	case "process":
		return infrahost.ReadProcess(r.PID, deadline)
	case "census":
		return infrahost.ReadCensus(r.Scope, deadline)
	case "unit", "unit-process":
		return unit(r, deadline)
	default:
		return nil, c.ErrUnsupported
	}
}

func run(args []string, out io.Writer) int {
	if len(args) != 2 || args[0] != "--request" {
		return 2
	}
	r, err := decode([]byte(args[1]))
	if err != nil {
		return 2
	}
	value, err := observe(r)
	if err != nil {
		if errors.Is(err, c.ErrUnsupported) {
			fmt.Fprintln(os.Stderr, "NOT_RUN physical: unsupported")
		} else {
			fmt.Fprintln(os.Stderr, "NOT_RUN physical: kernel_or_unit_custody_unavailable")
		}
		return 2
	}
	if json.NewEncoder(out).Encode(value) != nil {
		return 2
	}
	return 0
}

func main() { os.Exit(run(os.Args[1:], os.Stdout)) }
