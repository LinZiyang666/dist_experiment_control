package handoff

import "github.com/LinZiyang666/tether/internal/observationexec"

// The exact inheritance authority lives below both the observer and handoff;
// neither side may install a replaceable callback or infer custody from argv.
// origin: S8 internal review activation-and-route-migration F1
func observationExecDescriptors() ([]int, error)       { return observationexec.Descriptors() }
func observationExecEnvironment(env []string) []string { return observationexec.Environment(env) }
