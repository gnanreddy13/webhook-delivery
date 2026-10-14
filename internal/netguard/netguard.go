// Package netguard blocks outbound webhook requests to private, loopback,
// link-local and other non-public addresses (SSRF protection).
//
// The check runs in the dialer, on the IP actually being connected to, so it
// also covers hostnames that resolve (or later re-resolve, "DNS rebinding")
// to internal addresses. CheckURL is only an early, friendlier rejection at
// registration time.
package netguard

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
)

// Ranges that IsPrivate/IsLoopback/etc. don't cover but that must never be
// reachable from a webhook sender.
var reserved = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),       // "this network"
	netip.MustParsePrefix("100.64.0.0/10"),   // carrier-grade NAT
	netip.MustParsePrefix("192.0.0.0/24"),    // IETF protocol assignments
	netip.MustParsePrefix("192.0.2.0/24"),    // TEST-NET-1
	netip.MustParsePrefix("198.18.0.0/15"),   // benchmarking
	netip.MustParsePrefix("198.51.100.0/24"), // TEST-NET-2
	netip.MustParsePrefix("203.0.113.0/24"),  // TEST-NET-3
	netip.MustParsePrefix("240.0.0.0/4"),     // reserved, incl. broadcast
	netip.MustParsePrefix("64:ff9b::/96"),    // NAT64: could map to internal IPv4
	netip.MustParsePrefix("64:ff9b:1::/48"),  // local-use NAT64
	netip.MustParsePrefix("2001:db8::/32"),   // documentation
	netip.MustParsePrefix("fec0::/10"),       // deprecated site-local
}

// IsBlocked reports whether ip is not a public unicast address.
// Cloud metadata endpoints (169.254.169.254, fd00:ec2::254) are covered by
// the link-local and private (ULA) checks.
func IsBlocked(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || !ip.IsGlobalUnicast() || ip.IsPrivate() {
		return true // loopback, link-local, multicast, unspecified, RFC 1918, ULA
	}
	for _, p := range reserved {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// BlockedError is returned when a connection to a blocked address is refused.
type BlockedError struct{ Addr string }

func (e *BlockedError) Error() string {
	return fmt.Sprintf("destination %s is not allowed (private, loopback or reserved address)", e.Addr)
}

// Control is a net.Dialer Control function that refuses blocked addresses.
func Control(network, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return &BlockedError{Addr: address}
	}
	if IsBlocked(ip) {
		return &BlockedError{Addr: ip.String()}
	}
	return nil
}

// CheckURL rejects URLs that obviously point at internal hosts: IP literals
// in blocked ranges and localhost names. Hostnames are still checked again
// at connect time by Control.
func CheckURL(u *url.URL) error {
	if u.User != nil {
		return errors.New("url must not contain credentials")
	}
	host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return errors.New("url must not point to localhost")
	}
	if ip, err := netip.ParseAddr(host); err == nil && IsBlocked(ip) {
		return errors.New("url must not point to a private, loopback or reserved address")
	}
	return nil
}
