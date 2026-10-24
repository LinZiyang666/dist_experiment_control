package main

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/infracontract"
)

// origin: hot-update S9 plan §6.1 / T15
func TestInfraLaunchArgumentsSelectOnlyFixedComponents(t *testing.T) {
	for _, test := range []struct {
		args []string
		want infracontract.Component
	}{
		{[]string{"--component=nats"}, infracontract.NATS},
		{[]string{"--component=caddy"}, infracontract.Caddy},
		{nil, 0}, {[]string{"--component=host"}, 0}, {[]string{"--component=nats", "--config=/tmp/other"}, 0},
		{[]string{"/bin/sh"}, 0}, {[]string{"--component=caddy;id"}, 0},
	} {
		got, err := parseComponent(test.args)
		if got != test.want || (err != nil) != (test.want == 0) {
			t.Fatalf("arguments %q: got %v %v", test.args, got, err)
		}
	}
}
