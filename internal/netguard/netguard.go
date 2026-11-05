// Package netguard keeps requests to customer endpoints off internal networks
// (anti-SSRF). The check runs at dial time on the resolved IP, so DNS
// rebinding and alternate IP spellings cannot slip past a check made on the URL.
package netguard

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"syscall"
	"time"
)

// ErrBlocked is returned when a connection would reach a non-public address.
var ErrBlocked = errors.New("netguard: blocked address")

// blockedPrefixes covers what the netip helpers in blocked do not.
var blockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),     // "this network"; 0.x can reach the local host
	netip.MustParsePrefix("100.64.0.0/10"), // carrier-grade NAT, internal to the provider
	netip.MustParsePrefix("192.0.0.0/24"),  // IETF protocol assignments
	netip.MustParsePrefix("198.18.0.0/15"), // benchmarking
	netip.MustParsePrefix("240.0.0.0/4"),   // reserved, includes 255.255.255.255
	netip.MustParsePrefix("64:ff9b::/96"),  // NAT64 can wrap an internal IPv4 address
	netip.MustParsePrefix("2002::/16"),     // 6to4 can wrap an internal IPv4 address too
}

// blocked reports whether ip is anything but a public unicast address.
func blocked(ip netip.Addr) bool {
	ip = ip.Unmap()
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() || ip.IsMulticast() {
		return true
	}
	return slices.ContainsFunc(blockedPrefixes, func(p netip.Prefix) bool {
		return p.Contains(ip)
	})
}

// control runs after DNS resolution and before connect, once per address the
// dialer tries, so the IP checked is the IP used. Anything it cannot parse is
// refused (fail closed).
func control(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("%w: unparsable address %q", ErrBlocked, address)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return fmt.Errorf("%w: unparsable address %q", ErrBlocked, address)
	}
	if blocked(ip) {
		return fmt.Errorf("%w: %s", ErrBlocked, ip)
	}
	return nil
}

// NewTransport returns a transport whose every connection passes the
// address check. Proxies are disabled: through a proxy the dialer would check
// the proxy's address, not the endpoint's.
func NewTransport() *http.Transport {
	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
		Control:   control,
	}
	return &http.Transport{
		Proxy:               nil,
		DialContext:         dialer.DialContext,
		ForceAttemptHTTP2:   true,
		TLSHandshakeTimeout: 10 * time.Second,
		IdleConnTimeout:     90 * time.Second,
	}
}
