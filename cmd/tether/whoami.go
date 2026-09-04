package main

import (
	"encoding/json"
	"fmt"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/spf13/cobra"
)

// whoami.go — print this machine's ctl identity.
//
// origin: prerelease audit increment 2 internal review, reported by five lanes
// (admission-product/L8-F1, admission-enforcement/L9-F5, ops-upgrade/L16-F4,
// test-blast-radius/F6, empirical n1-interop/F3).
//
// THE COMMAND THE HELP TEXT ALREADY PROMISED. `tether admin session-allow --help` told
// the operator "a user finds their own fingerprint with 'tether whoami'", and there was
// no such command — in a release whose headline change is that a fingerprint must be
// admitted before it can create a session. Every fresh deployment therefore began with
// an instruction that does not work.
//
// The fingerprint is what `session create` is keyed on and what the operator types into
// `tether admin session-allow`, so it is the FIRST line of the output. The public key is
// printed too because that is what auth_callout binds a connection to, and an operator
// correlating a refusal in the broker log against a user's claim needs both.
//
// Deliberately OFFLINE: it reads the local identity file and derives from it. It must
// work before the user has been admitted to anything — that is the whole point — so it
// must not need a broker, a session, or a network.
func newWhoamiCmd() *cobra.Command {
	var home string
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "whoami",
		Short: "Print this machine's ctl identity (fingerprint + public key)",
		Long: `Print the identity this machine authenticates as.

The FINGERPRINT is what an operator admits with 'tether admin session-allow' on
the broker host before you can run 'tether session create'. Give them this line.

Runs entirely offline: it reads the local identity, so it works before you have
been admitted to anything.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			fp, err := auth.FingerprintFromActor(id.PublicKey)
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			if asJSON {
				return json.NewEncoder(out).Encode(struct {
					Fingerprint string `json:"fingerprint"`
					PublicKey   string `json:"public_key"`
				}{fp, id.PublicKey})
			}
			_, _ = fmt.Fprintf(out, "fingerprint: %s\npublic key:  %s\n", fp, id.PublicKey)
			return nil
		},
	}
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	cmd.Flags().BoolVar(&asJSON, "json", false, "emit JSON")
	return cmd
}
