package upgradecli

import (
	"fmt"

	"github.com/LinZiyang666/tether/internal/serveconf"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/spf13/cobra"
)

// StagePorts are the cmd-owned edges of the root product stage: exit
// classification, the registered control directory of the grant's component
// (resolved from that component's own configuration, never from the grant
// file's paths) and the isolated version probe.
// origin: hot-update S8 plan §5.2 / LQ1 / §11.2 P19
type StagePorts struct {
	Exit       func(verb string, err error) error
	ControlDir func(upgradetx.Key) (string, error)
	Probe      ProbeImage
}

// NewUpgradeCommand is the explicit `tether upgrade stage` entry (LQ1). It
// publishes the two authorized images and writes the artifact receipt. It
// never writes a daemon journal, never calls Start and never starts a service:
// the original actor starts the same transaction with the receipt afterwards.
func NewUpgradeCommand(p StagePorts) *cobra.Command {
	root := &cobra.Command{Use: "upgrade", Short: "Root-owned upgrade artifact operations"}
	var opts StageOptions
	stage := &cobra.Command{Use: "stage", Short: "Publish the grant's source and candidate images and write an artifact receipt", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return p.Exit("upgrade stage", runStage(cmd, p, opts))
		}}
	stage.Flags().StringVar(&opts.Grant, "grant", "", "target-issued grant file from --prepare-only")
	stage.Flags().StringVar(&opts.Source, "source", "", "running source image (may be /proc/PID/exe)")
	stage.Flags().StringVar(&opts.Candidate, "candidate", "", "candidate image the grant names")
	stage.Flags().StringVar(&opts.ReceiptOut, "receipt-out", "", "artifact receipt output file")
	root.AddCommand(stage)
	return root
}

// ControlDirFor resolves the grant component's registered control directory
// from that component's own configuration: an agent's per-account home, or the
// broker data directory named by its config file. Paths inside the grant file
// are never consulted, so a forged grant cannot point stage at another tree.
// origin: hot-update S8 plan §5.2 / LQ1 / §11.2 P19
func ControlDirFor(key upgradetx.Key, ownHome string) (string, error) {
	switch key.Component {
	case upgradetx.ComponentAgent:
		home, err := UserHome(key.UID, ownHome)
		return AgentBootRole(home, key.SID, key.ConfiguredNID, key.UID).ControlDir, err
	case upgradetx.ComponentBroker:
		cfg, err := serveconf.Load(key.ConfigPath)
		if err != nil {
			return "", rejected("broker configuration is unreadable")
		}
		dataDir := BrokerDataDir(cfg.Broker.Cluster.DataDir, cfg.Broker.Storage.DB)
		if dataDir == "" {
			return "", rejected("broker configuration names no absolute data directory")
		}
		return BrokerBootRole(dataDir, key.ConfigPath, key.ConfiguredNID, key.UID).ControlDir, nil
	case upgradetx.ComponentCaddy, upgradetx.ComponentNATS:
		return "", &upgradetx.Error{Code: upgradetx.CodeUnsupportedComponent, Detail: "S8 stages agent and broker images only"}
	default:
		return "", &upgradetx.Error{Code: upgradetx.CodeUnsupportedComponent, Detail: "S8 stages agent and broker images only"}
	}
}

func runStage(cmd *cobra.Command, p StagePorts, opts StageOptions) error {
	if opts.Grant == "" || opts.Source == "" || opts.Candidate == "" || opts.ReceiptOut == "" {
		return rejected("stage requires --grant, --source, --candidate and --receipt-out")
	}
	var grant upgradetx.Grant
	if err := readJSON(opts.Grant, &grant); err != nil {
		return rejected("invalid stage grant file")
	}
	if p.ControlDir == nil || p.Probe == nil {
		return &upgradetx.Error{Code: upgradetx.CodeNotWired, Detail: "stage authority ports are unavailable"}
	}
	control, err := p.ControlDir(grant.Handle.Key)
	if err != nil {
		return err
	}
	ctx := upgradeartifact.WithPublicationPort(cmd.Context(), &StagePort{ControlDir: control, Probe: p.Probe})
	receipt, err := Stage(ctx, opts, upgradeartifact.Publish)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(cmd.OutOrStdout(), "staged tx_id=%s source=%s target=%s receipt=%s\n",
		receipt.Grant.Handle.TxID, receipt.SourceSHA, receipt.TargetSHA, opts.ReceiptOut)
	return err
}
