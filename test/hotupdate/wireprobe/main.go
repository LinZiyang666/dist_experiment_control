// upgrade-wire-probe sends the existing kill wire using the selected ctl's
// identity and protocol source. It supplies no upgrade or replacement CLI.
// origin: hot-update S8 plan §7.3 / P24
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

func main() { os.Exit(probe()) }

// Return through cancellation before the process exits.
// origin: hot-update S10 lint L3a / gocritic exitAfterDefer
func probe() int {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // ctx-root: bounded wire probe process
	defer cancel()
	return run(ctx, os.Args[1:], os.Stdout, os.Stderr)
}

func run(ctx context.Context, args []string, out, diagnostic io.Writer) int {
	flags := flag.NewFlagSet("upgrade-wire-probe", flag.ContinueOnError)
	flags.SetOutput(diagnostic)
	home := flags.String("home", "", "explicit fixture ctl home")
	url := flags.String("nats-url", "", "explicit fixture NATS URL")
	verb := flags.String("verb", "", "existing wire verb (kill)")
	nid := flags.String("nid", "", "target node")
	pid := flags.String("pid", "", "original run process ID")
	signal := flags.Int("signal", 15, "SIGTERM (15)")
	if err := flags.Parse(args); err != nil {
		return 64
	}
	// Never resolve the user's default home, mint an identity or accept an
	// anonymous development connection: provenance belongs to the original ctl.
	// origin: hot-update S8 plan §7.3 / P24
	if flags.NArg() != 0 || !filepath.IsAbs(*home) || *url == "" || *verb != "kill" ||
		!subjectToken(*nid) || !subjectToken(*pid) || *signal != 15 || os.Getenv(cli.DevNoAuthEnv) != "" {
		_, _ = fmt.Fprintln(diagnostic, "wire probe requires explicit home, URL and original kill identity")
		return 64
	}
	id, err := cli.ReadIdentityIfExists(*home)
	if err != nil || id == nil {
		_, _ = fmt.Fprintln(diagnostic, "wire probe ctl identity is unavailable")
		return 77
	}
	// Read only this home. TETHER_SESSION must not route another cell's key.
	data, err := os.ReadFile(filepath.Join(*home, "current_session"))
	sid := strings.TrimSpace(string(data))
	if err != nil || !subjectToken(sid) {
		_, _ = fmt.Fprintln(diagnostic, "wire probe fixture session is unavailable")
		return 64
	}
	nc, err := cli.ConnectNATSWithNkey(*url, id, nats.Name("tether-ctl:"+sid), nats.MaxReconnects(0), nats.Timeout(2*time.Second))
	if err != nil {
		_, _ = fmt.Fprintln(diagnostic, "wire probe connection failed")
		return 74
	}
	defer nc.Close()
	body, err := json.Marshal(proto.KillReq{PID: *pid, Signal: *signal})
	if err != nil {
		return 70
	}
	msg, err := nc.RequestWithContext(ctx, proto.SubjCmdBy(sid, id.PublicKey, *nid, "kill"), body)
	if err != nil {
		_, _ = fmt.Fprintln(diagnostic, "wire probe kill result is unknown")
		return 74
	}
	var reply proto.KillResp
	if err := json.Unmarshal(msg.Data, &reply); err != nil || !reply.OK {
		_, _ = fmt.Fprintln(diagnostic, "wire probe kill was refused or malformed")
		return 74
	}
	if err := json.NewEncoder(out).Encode(reply); err != nil {
		return 74
	}
	return 0
}

func subjectToken(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' && r != '_' {
			return false
		}
	}
	return true
}
