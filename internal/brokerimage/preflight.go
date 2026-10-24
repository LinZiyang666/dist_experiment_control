package brokerimage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/spawnsafe"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// PreflightProbeCommand is an offline stdin/stdout dispatcher implemented by
// P22 before daemon startup. The candidate checks its own embedded migration
// bytes and parses config; the parent never opens SQLite, Bolt or a lock.
// origin: hot-update S6 plan §3.1 / T06
const PreflightProbeCommand = "broker-image-preflight"
const maxProbeBytes = 64 << 10

type ProbeRequest struct {
	Config          []byte              `json:"config"`
	AllowedArgs     []string            `json:"allowed_args"`
	Schema          storage.SchemaStamp `json:"schema"`
	RequiredDomains []string            `json:"required_domains"`
}
type ProbeReply struct {
	Platform     string                    `json:"platform"`
	Proof        ReaderProof               `json:"proof"`
	ConfigDigest string                    `json:"config_digest"`
	ArgsDigest   string                    `json:"args_digest"`
	Supported    []storage.MigrationDigest `json:"supported"`
}

// ReaderProbe is the pure half of the command. The command owner supplies the
// parsed config result and its actual embedded migration set, with no database
// dependency. Callers must not substitute exporter-supported rows for compiled
// migration bytes. Config errors are scrubbed and no credential text is emitted.
// origin: hot-update S6 plan §3.1 / T06 / T07
func ReaderProbe(request ProbeRequest, supported []storage.MigrationDigest, validateConfig func([]byte, []string) error) (ProbeReply, error) {
	var reply ProbeReply
	if runtime.GOOS != "linux" || validateConfig == nil || len(request.Config) > MaxBundleBytes ||
		validateSchema(request.Schema) != nil || !canonicalStrings(request.RequiredDomains, true) || !contains(request.RequiredDomains, Domain) ||
		!reflect.DeepEqual(supported, request.Schema.Supported) || len(request.AllowedArgs) == 0 {
		return reply, ErrImage
	}
	if err := validateConfig(request.Config, request.AllowedArgs); err != nil {
		return reply, ErrImage
	}
	domains := strings.Fields(proto.ImageDomainsLine())[1:]
	sort.Strings(domains)
	for _, domain := range request.RequiredDomains {
		if !contains(domains, domain) {
			return reply, ErrImage
		}
	}
	configDigest := sha256.Sum256(request.Config)
	args, _ := json.Marshal(request.AllowedArgs)
	argsDigest := sha256.Sum256(args)
	mappedSHA, err := hashArtifact("/proc/self/exe")
	if err != nil {
		return reply, ErrImage
	}
	reply = ProbeReply{Platform: runtime.GOOS + "/" + runtime.GOARCH, Proof: ReaderProof{SHA256: mappedSHA, ABI: proto.ImageABIVersion, Domains: domains, MigrationDigest: migrationDigest(supported)}, ConfigDigest: hex.EncodeToString(configDigest[:]), ArgsDigest: hex.EncodeToString(argsDigest[:]), Supported: supported}
	return reply, nil
}

// Preflight binds both probes to stable artifact bytes and one absolute boot
// deadline. A previous file may be the currently mapped exporter or candidate;
// an arbitrary third SHA cannot become an admissible rollback reader.
func Preflight(request PreflightRequest) (candidate ReaderProof, previous ReaderProof, err error) {
	if runtime.GOOS != "linux" {
		return candidate, previous, rawsocket.ErrUnsupported
	}
	if request.Deadline <= 0 || len(request.Config) > MaxBundleBytes || validateSchema(request.Stamp.Schema) != nil ||
		!canonicalStrings(request.RequiredDomains, true) || !contains(request.RequiredDomains, Domain) || len(request.AllowedArgs) == 0 {
		return candidate, previous, ErrImage
	}
	if !filepath.IsAbs(request.CandidatePath) || !filepath.IsAbs(request.PreviousPath) {
		return candidate, previous, ErrImage
	}
	running, err := RunningArtifactSHA256()
	if err != nil {
		return candidate, previous, ErrImage
	}
	probe := ProbeRequest{Config: request.Config, AllowedArgs: request.AllowedArgs, Schema: request.Stamp.Schema, RequiredDomains: request.RequiredDomains}
	data, err := json.Marshal(probe)
	if err != nil || len(data) > MaxBundleBytes {
		return candidate, previous, ErrImage
	}
	// The readers are independent offline children under the same absolute
	// budget. Join both supervisors even on refusal: returning with a pending
	// reader would let its subprocess outlive cut admission.
	// origin: hot-update S6 plan §3.1 / T06 / T37
	type result struct {
		proof ReaderProof
		err   error
	}
	candidateResult, previousResult := make(chan result, 1), make(chan result, 1)
	go func() {
		proof, err := probeReader(request.CandidatePath, data, probe, int64(request.Deadline))
		candidateResult <- result{proof, err}
	}()
	go func() {
		proof, err := probeReader(request.PreviousPath, data, probe, int64(request.Deadline))
		previousResult <- result{proof, err}
	}()
	c, p := <-candidateResult, <-previousResult
	candidate, previous = c.proof, p.proof
	if err = errors.Join(c.err, p.err); err != nil {
		return candidate, previous, err
	}
	if previous.SHA256 != running && previous.SHA256 != candidate.SHA256 {
		return candidate, previous, ErrImage
	}
	return candidate, previous, nil
}
func probeReader(path string, input []byte, request ProbeRequest, deadline int64) (ReaderProof, error) {
	before, err := hashArtifact(path)
	if err != nil {
		return ReaderProof{}, ErrImage
	}
	// The pure command is itself the capability smoke: its proof comes from
	// compiled ABI/domains and migrations, with the mapped executable SHA. A
	// separate version process repeats the same capabilities and consumes the
	// sole-voter admission budget without testing another reader property.
	// origin: hot-update S6 plan §3.1 / T06 / T37
	output, err := runReaderProbe(path, []string{PreflightProbeCommand}, input, deadline)
	if err != nil {
		return ReaderProof{}, err
	}
	var reply ProbeReply
	if decodeCanonical(output, &reply) != nil || reply.Platform != runtime.GOOS+"/"+runtime.GOARCH ||
		reply.Proof.ABI != proto.ImageABIVersion || !canonicalStrings(reply.Proof.Domains, true) ||
		!reflect.DeepEqual(reply.Supported, request.Schema.Supported) || reply.Proof.MigrationDigest != request.Schema.Digest {
		return ReaderProof{}, ErrImage
	}
	for _, domain := range request.RequiredDomains {
		if !contains(reply.Proof.Domains, domain) {
			return ReaderProof{}, ErrImage
		}
	}
	config := sha256.Sum256(request.Config)
	args, _ := json.Marshal(request.AllowedArgs)
	argsSHA := sha256.Sum256(args)
	if reply.ConfigDigest != hex.EncodeToString(config[:]) || reply.ArgsDigest != hex.EncodeToString(argsSHA[:]) || reply.Proof.SHA256 != before {
		return ReaderProof{}, ErrImage
	}
	after, err := hashArtifact(path)
	if err != nil || after != before {
		return ReaderProof{}, ErrImage
	}
	reply.Proof.SHA256 = before
	return reply.Proof, nil
}
func runReaderProbe(path string, args []string, input []byte, deadline int64) ([]byte, error) {
	now, err := rawsocket.BootNow()
	if err != nil || deadline <= now {
		return nil, ErrImage
	}
	duration := min(time.Duration(deadline-now), upgradebudget.RequestLatency)
	// ctx-root: each isolated offline probe owns its bounded subprocess lifetime.
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, args...)
	output := &probeOutput{}
	cmd.Stdin = bytes.NewReader(input)
	cmd.Stdout = output
	cmd.Stderr = io.Discard
	cmd.WaitDelay = 2 * time.Second
	// The shared supervisor kills and reaps the whole probe group on every path,
	// including a candidate that exits after leaving a pipe-less background child.
	if spawnsafe.RunVersionProbe(cmd) != nil || output.overflow {
		return nil, ErrImage
	}
	now, err = rawsocket.BootNow()
	if err != nil || now >= deadline {
		return nil, ErrImage
	}
	return []byte(strings.TrimSuffix(output.String(), "\n")), nil
}
func hashArtifact(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", ErrImage
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return "", ErrImage
	}
	digest := sha256.New()
	if _, err = io.Copy(digest, file); err != nil {
		return "", ErrImage
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

// DecodeProbeRequest bounds input before decoding and refuses ambiguous JSON.
// This helper is safe to call before any daemon ownership or SQL initialization.
func DecodeProbeRequest(input io.Reader) (ProbeRequest, error) {
	var request ProbeRequest
	data, err := io.ReadAll(io.LimitReader(input, MaxBundleBytes+1))
	if err != nil || len(data) > MaxBundleBytes || decodeCanonical(data, &request) != nil {
		return request, ErrImage
	}
	return request, nil
}

// A positive deadline from another boot or an already exhausted cut is not
// authority to transfer files or start a potentially side-effecting constructor.
// origin: hot-update S6 plan §2.1 / §4.2
func deadlineAlive(deadline controlbudget.Deadline) bool {
	now, err := rawsocket.BootNow()
	return err == nil && deadline > 0 && int64(deadline) > now
}

// A reader's declarations must fit the same bounded capture as its input.
// Returning the original write count lets the supervisor drain hostile output
// without retaining it or leaving a child blocked on a full stdout pipe.
// origin: hot-update S6 plan §3.1 / T06
type probeOutput struct {
	bytes.Buffer
	overflow bool
}

func (o *probeOutput) Write(p []byte) (int, error) {
	n := len(p)
	room := maxProbeBytes - o.Len()
	if n > room {
		o.overflow = true
		p = p[:room]
	}
	_, _ = o.Buffer.Write(p)
	return n, nil
}
