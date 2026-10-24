package caddyconfig

import (
	"strings"

	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// validateRoutes never sorts routes. A catch-all terminal handler preceding
// /sub or discovery would preserve JSON yet silently steal their requests.
// origin: hot-update S9 plan §5.1 / T18
func validateRoutes(value any, host string, depth int) error {
	routes, ok := value.([]any)
	if !ok || len(routes) == 0 || depth > 32 {
		return ErrConfig
	}
	caught := false
	for _, value := range routes {
		route, ok := value.(map[string]any)
		if !ok || !fields(route, "match", "handle", "group", "terminal") {
			return c.ErrUnsupported
		}
		specific, err := validateMatchers(route["match"], host)
		if err != nil {
			return err
		}
		if caught {
			return ErrConfig
		}
		handlers, ok := route["handle"].([]any)
		if !ok || len(handlers) == 0 {
			return ErrConfig
		}
		terminal := false
		for _, value := range handlers {
			handler, ok := value.(map[string]any)
			if !ok {
				return ErrConfig
			}
			switch handler["handler"] {
			case "subroute":
				if !fields(handler, "handler", "routes") {
					return c.ErrUnsupported
				}
				if err := validateRoutes(handler["routes"], host, depth+1); err != nil {
					return err
				}
				// Stock Caddyfile handle directives wrap their handlers in a
				// subroute. A catch-all wrapper must not conceal a terminal proxy
				// from this ordering guard; ambiguous fallthrough is deferred.
				terminal = true
			case "reverse_proxy":
				if !fields(handler, "handler", "upstreams", "headers", "transport") {
					return c.ErrUnsupported
				}
				upstreams, ok := handler["upstreams"].([]any)
				if !ok || len(upstreams) == 0 {
					return ErrConfig
				}
				for _, value := range upstreams {
					upstream, ok := value.(map[string]any)
					if !ok || !fields(upstream, "dial") || !validUpstream(stringValue(upstream["dial"])) {
						return c.ErrUnsupported
					}
				}
				if h, ok := handler["headers"]; ok {
					if err := validateProxyHeaders(h); err != nil {
						return err
					}
				}
				if t, ok := handler["transport"]; ok {
					transport, ok := t.(map[string]any)
					if !ok || !fields(transport, "protocol", "dial_timeout", "response_header_timeout") || transport["protocol"] != "http" {
						return c.ErrUnsupported
					}
				}
				terminal = true
			case "static_response":
				if !fields(handler, "handler", "body", "status_code", "headers") {
					return c.ErrUnsupported
				}
				if h, ok := handler["headers"]; ok {
					if err := validateHeaderMap(h); err != nil {
						return err
					}
				}
				terminal = true
			case "rewrite":
				if !fields(handler, "handler", "uri", "strip_path_prefix", "strip_path_suffix") {
					return c.ErrUnsupported
				}
			default:
				return c.ErrUnsupported
			}
		}
		// Caddy's proxy/static handlers finish the chain even without the route
		// terminal flag. Nested subroutes are checked at their own ordering level.
		if !specific && (terminal || boolValue(route["terminal"])) {
			caught = true
		}
	}
	return nil
}

func validateMatchers(value any, host string) (bool, error) {
	if value == nil {
		return false, nil
	}
	sets, ok := value.([]any)
	if !ok || len(sets) == 0 {
		return false, ErrConfig
	}
	allSpecific := true
	for _, value := range sets {
		match, ok := value.(map[string]any)
		if !ok || !fields(match, "host", "path", "method", "header") {
			return false, c.ErrUnsupported
		}
		if h, ok := match["host"]; ok {
			hosts, ok := h.([]any)
			if !ok || len(hosts) != 1 || hosts[0] != host {
				return false, c.ErrUnsupported
			}
		}
		paths, ok := match["path"].([]any)
		if !ok || len(paths) == 0 {
			allSpecific = false
		} else {
			for _, value := range paths {
				path, ok := value.(string)
				if !ok || !strings.HasPrefix(path, "/") && path != "*" {
					return false, ErrConfig
				}
				if path == "*" || path == "/*" {
					allSpecific = false
				}
			}
		}
		if methods, ok := match["method"]; ok {
			values, ok := methods.([]any)
			if !ok || len(values) == 0 {
				return false, ErrConfig
			}
			for _, v := range values {
				if stringValue(v) == "" {
					return false, ErrConfig
				}
			}
		}
		if h, ok := match["header"]; ok {
			if err := validateHeaderMap(h); err != nil {
				return false, err
			}
		}
	}
	return allSpecific, nil
}

func validateHeaderMap(value any) error {
	headers, ok := value.(map[string]any)
	if !ok {
		return ErrConfig
	}
	for name, value := range headers {
		if name == "" || strings.ContainsAny(name, "\r\n\x00") {
			return ErrConfig
		}
		values, ok := value.([]any)
		if !ok {
			return ErrConfig
		}
		for _, value := range values {
			s, ok := value.(string)
			if !ok || strings.ContainsAny(s, "\r\n\x00") {
				return ErrConfig
			}
		}
	}
	return nil
}

func validateProxyHeaders(value any) error {
	directions, ok := value.(map[string]any)
	if !ok || !fields(directions, "request", "response") {
		return c.ErrUnsupported
	}
	for _, value := range directions {
		operations, ok := value.(map[string]any)
		if !ok || !fields(operations, "set", "add") {
			return c.ErrUnsupported
		}
		for _, value := range operations {
			if err := validateHeaderMap(value); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateTLS(value, policies any, host string) error {
	if policies != nil {
		list, ok := policies.([]any)
		if !ok || len(list) != 1 {
			return c.ErrUnsupported
		}
		p, ok := list[0].(map[string]any)
		// Client auth, custom selectors and connection policy changes require
		// a richer compatibility proof. Never silently drop them in the shadow.
		if !ok || len(p) != 0 {
			return c.ErrUnsupported
		}
	}
	if value == nil {
		return nil
	}
	tlsApp, ok := value.(map[string]any)
	if !ok || !fields(tlsApp, "certificates", "automation") {
		return c.ErrUnsupported
	}
	if certs, ok := tlsApp["certificates"]; ok {
		loaders, ok := certs.(map[string]any)
		if !ok || !fields(loaders, "automate", "load_files") {
			return c.ErrUnsupported
		}
		if automate, ok := loaders["automate"]; ok {
			names, ok := automate.([]any)
			if !ok || len(names) == 0 {
				return ErrConfig
			}
			for _, name := range names {
				if !validHost(stringValue(name)) {
					return ErrConfig
				}
			}
		}
		if files, ok := loaders["load_files"]; ok {
			list, ok := files.([]any)
			if !ok || len(list) == 0 {
				return ErrConfig
			}
			for _, value := range list {
				file, ok := value.(map[string]any)
				if !ok || !fields(file, "certificate", "key") || !privatePath(stringValue(file["certificate"])) || !privatePath(stringValue(file["key"])) {
					return c.ErrUnsupported
				}
				if err := validateCertificate(Options{Host: host, Certificate: stringValue(file["certificate"]), Key: stringValue(file["key"])}); err != nil {
					return err
				}
			}
		}
	}
	if automation, ok := tlsApp["automation"]; ok {
		a, ok := automation.(map[string]any)
		if !ok || !fields(a, "policies") {
			return c.ErrUnsupported
		}
		policies, ok := a["policies"].([]any)
		if !ok {
			return ErrConfig
		}
		for _, value := range policies {
			p, ok := value.(map[string]any)
			if !ok || !fields(p, "issuers", "subjects") {
				return c.ErrUnsupported
			}
			if subjects, ok := p["subjects"]; ok {
				list, ok := subjects.([]any)
				if !ok {
					return ErrConfig
				}
				for _, s := range list {
					if !validHost(stringValue(s)) {
						return ErrConfig
					}
				}
			}
			issuers, ok := p["issuers"].([]any)
			if !ok || len(issuers) == 0 {
				return ErrConfig
			}
			for _, value := range issuers {
				issuer, ok := value.(map[string]any)
				// Stock adaptation of the installer's global email uses both
				// default issuers. The shadow removes them together; the main
				// retains the original policy and its current account metadata.
				if !ok || !fields(issuer, "module", "email", "ca") || issuer["module"] != "acme" && issuer["module"] != "zerossl" {
					return c.ErrUnsupported
				}
			}
		}
	}
	return nil
}
