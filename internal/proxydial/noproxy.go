package proxydial

import (
	"net"
	"strings"
)

// bypassProxy reports whether the TARGET host should be dialed directly instead
// of through the proxy. The built-in loopback bypass (localhost / 127.0.0.0/8 /
// ::1) always applies — even if NO_PROXY does not list it — so the dev broker
// nats://127.0.0.1:4222 is never sent to the proxy. noProxy is the raw NO_PROXY
// value. This is evaluated ONLY against the target, never the proxy hop.
func bypassProxy(host, noProxy string) bool {
	host = strings.TrimSuffix(host, ".") // tolerate an FQDN trailing dot
	if isLoopbackHost(host) {
		return true
	}
	for _, entry := range strings.Split(noProxy, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if entry == "*" {
			return true
		}
		if noProxyEntryMatches(entry, host) {
			return true
		}
	}
	return false
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

// noProxyEntryMatches matches one NO_PROXY entry against host. Entries may be:
//   - a CIDR (only matches when host is a literal IP — under SkipHostLookup the
//     dialer usually sees a hostname, so CIDR entries only bite an IP-literal
//     broker URL).
//   - an IP literal.
//   - a hostname. A leading dot (".example.com") matches subdomains only; a bare
//     form ("example.com") matches the domain AND its subdomains (Go
//     x/net/http/httpproxy semantics). An optional :port on the entry is ignored.
func noProxyEntryMatches(entry, host string) bool {
	if h, _, err := net.SplitHostPort(entry); err == nil {
		entry = h
	}
	if _, ipnet, err := net.ParseCIDR(entry); err == nil {
		if ip := net.ParseIP(host); ip != nil {
			return ipnet.Contains(ip)
		}
		return false
	}
	if eip := net.ParseIP(entry); eip != nil {
		if hip := net.ParseIP(host); hip != nil {
			return eip.Equal(hip)
		}
		return false
	}
	host = strings.ToLower(host)
	if strings.HasPrefix(entry, ".") {
		// Subdomain-only.
		entry = strings.ToLower(strings.TrimPrefix(entry, "."))
		return strings.HasSuffix(host, "."+entry)
	}
	entry = strings.ToLower(entry)
	return host == entry || strings.HasSuffix(host, "."+entry)
}
