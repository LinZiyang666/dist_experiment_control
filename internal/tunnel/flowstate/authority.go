package flowstate

import (
	"fmt"
	"io"
)

func (a Authority) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, "[redacted authority]") }

func (k AuthorizedKey) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, "[redacted key]") }
