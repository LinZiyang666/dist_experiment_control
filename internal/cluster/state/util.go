package state

import "strconv"

// applied_index / applied_term are stored as decimal TEXT in cluster_meta
// (key/value are both TEXT). These helpers are the single encode/decode point.

func FormatUint(n uint64) string { return strconv.FormatUint(n, 10) }

func ParseUint(s string) (uint64, error) { return strconv.ParseUint(s, 10, 64) }
