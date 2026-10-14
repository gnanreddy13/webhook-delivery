package netguard

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"testing"
	"time"
)

func TestIsBlocked(t *testing.T) {
	blocked := []string{
		"127.0.0.1", "127.255.0.9", "::1", "0.0.0.0", "::",
		"10.1.2.3", "172.16.0.1", "192.168.1.1", // RFC 1918
		"169.254.169.254", "fe80::1", // link-local, incl. cloud metadata
		"fd00:ec2::254", "fc00::1", // ULA, incl. AWS IPv6 metadata
		"100.64.0.1", "198.18.0.1", "240.0.0.1", "255.255.255.255",
		"224.0.0.1", "ff02::1", // multicast
		"::ffff:127.0.0.1", "::ffff:10.0.0.1", // IPv4-mapped
		"64:ff9b::a00:1", // NAT64 of 10.0.0.1
		"192.0.2.1", "2001:db8::1",
	}
	for _, s := range blocked {
		if !IsBlocked(netip.MustParseAddr(s)) {
			t.Errorf("%s should be blocked", s)
		}
	}
	allowed := []string{"1.1.1.1", "8.8.8.8", "93.184.215.14", "2606:4700:4700::1111"}
	for _, s := range allowed {
		if IsBlocked(netip.MustParseAddr(s)) {
			t.Errorf("%s should be allowed", s)
		}
	}
}

func TestCheckURL(t *testing.T) {
	bad := []string{
		"http://localhost:8080/x", "http://LOCALHOST./x", "http://api.localhost/x",
		"http://127.0.0.1/x", "http://[::1]/x", "http://169.254.169.254/latest/meta-data",
		"http://10.0.0.5/x", "https://user:pass@example.com/x",
	}
	for _, s := range bad {
		u, _ := url.Parse(s)
		if CheckURL(u) == nil {
			t.Errorf("%s should be rejected", s)
		}
	}
	for _, s := range []string{"https://example.com/hook", "http://8.8.8.8:8080/x"} {
		u, _ := url.Parse(s)
		if err := CheckURL(u); err != nil {
			t.Errorf("%s should be accepted: %v", s, err)
		}
	}
}

// The dial-time check must stop a request to a loopback server, including
// one reached through a hostname that resolves to it.
func TestControlBlocksLoopbackDial(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits++ }))
	defer srv.Close()
	_, port, _ := net.SplitHostPort(srv.Listener.Addr().String())

	client := &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{Control: Control}).DialContext,
		},
	}
	for _, target := range []string{srv.URL, "http://localhost:" + port} {
		_, err := client.Get(target)
		var be *BlockedError
		if !errors.As(err, &be) {
			t.Errorf("GET %s: want BlockedError, got %v", target, err)
		}
	}
	if hits != 0 {
		t.Errorf("blocked requests reached the server %d times", hits)
	}
}
