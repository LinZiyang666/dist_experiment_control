package main

import (
	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
)

// origin: hot-update S5 plan §10.2 / V-18. Socket IO and command selection stay
// here; the imported diagnostic translates the authoritative report.
func clusterDoctorOnline(rep *adminsock.ClusterStatusReport) []clusteroffline.DoctorCheck {
	return cli.ClusterDoctorOnline(rep)
}
