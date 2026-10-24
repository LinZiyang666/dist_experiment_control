// tether-infra-launch replaces itself with an authorized stock infrastructure
// executable. No business bootstrap or Go launcher parent survives that exec.
// origin: hot-update S9 plan §4.1 / §6.1 / T14
package main

import (
	"fmt"
	"os"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infralaunch"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

func main() { os.Exit(run(os.Args[1:])) }

func parseComponent(args []string) (infracontract.Component, error) {
	if len(args) != 1 {
		return 0, infralaunch.ErrLaunchRejected
	}
	switch args[0] {
	case "--component=nats":
		return infracontract.NATS, nil
	case "--component=caddy":
		return infracontract.Caddy, nil
	default:
		return 0, infralaunch.ErrLaunchRejected
	}
}

func run(args []string) int {
	component, err := parseComponent(args)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "infra_launch_rejected")
		return 64
	}
	// The root's original permit deadline may only shorten this admission bound.
	deadline, err := controlbudget.After(upgradebudget.RequestLatency)
	if err == nil {
		err = infralaunch.Run(component, deadline, nil)
	}
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		return 75
	}
	return 74 // Successful exec cannot return to this launcher image.
}
