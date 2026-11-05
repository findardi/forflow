package netguard

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync/atomic"
	"testing"
)

func TestBlocked(t *testing.T) {
	tests := []struct {
		ip   string
		want bool
	}{
		{"127.0.0.1", true},
		{"127.255.255.254", true},
		{"::1", true},
		{"::ffff:127.0.0.1", true}, // IPv4-mapped loopback
		{"10.0.0.5", true},
		{"172.16.0.1", true},
		{"192.168.1.1", true},
		{"fd00::1", true},
		{"169.254.169.254", true}, // cloud metadata
		{"fe80::1", true},
		{"0.0.0.0", true},
		{"::", true},
		{"0.1.2.3", true},
		{"100.64.0.1", true},
		{"192.0.0.8", true},
		{"198.18.0.1", true},
		{"198.19.255.255", true},
		{"240.0.0.1", true},
		{"255.255.255.255", true},
		{"224.0.0.1", true},
		{"ff02::1", true},
		// Prefix.Contains does not match an IPv4-mapped address against an IPv4
		// prefix, so these need the Unmap in blocked.
		{"::ffff:100.64.0.1", true},
		{"::ffff:198.18.0.1", true},
		{"64:ff9b::7f00:1", true}, // NAT64 wrapping 127.0.0.1
		{"2002:7f00:1::", true},   // 6to4 wrapping 127.0.0.1

		{"1.1.1.1", false},
		{"8.8.8.8", false},
		{"108.18.0.1", false}, // public; once blocked by a typo of 198.18.0.0/15
		{"198.17.255.255", false},
		{"198.20.0.0", false},
		{"100.63.255.255", false},
		{"172.32.0.1", false},
		{"2606:4700:4700::1111", false},
		{"::ffff:8.8.8.8", false},
	}
	for _, tt := range tests {
		t.Run(tt.ip, func(t *testing.T) {
			if got := blocked(netip.MustParseAddr(tt.ip)); got != tt.want {
				t.Errorf("blocked(%s) = %v, want %v", tt.ip, got, tt.want)
			}
		})
	}
}

func TestControlRefusesWhatItCannotParse(t *testing.T) {
	for _, addr := range []string{"", "127.0.0.1", "example.com:443", "[::1"} {
		if err := control("tcp", addr, nil); !errors.Is(err, ErrBlocked) {
			t.Errorf("control(%q) = %v, want ErrBlocked", addr, err)
		}
	}
	if err := control("tcp", "8.8.8.8:443", nil); err != nil {
		t.Errorf("control(public) = %v, want nil", err)
	}
}

// End to end through http.Client: httptest listens on loopback, so the dial
// must be refused and the error must still match ErrBlocked after net/http
// wraps it in *url.Error and *net.OpError.
func TestTransportRefusesLoopback(t *testing.T) {
	var hit atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hit.Store(true) }))
	defer srv.Close()

	client := &http.Client{Transport: NewTransport()}
	resp, err := client.Get(srv.URL)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("request to loopback succeeded, want ErrBlocked")
	}
	if !errors.Is(err, ErrBlocked) {
		t.Errorf("error = %v, want ErrBlocked", err)
	}
	if hit.Load() {
		t.Error("server received the request")
	}
}

// localhost resolves to loopback; the name must not help.
func TestTransportRefusesLocalhostName(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	_, port, err := net.SplitHostPort(srv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("split listener address: %v", err)
	}
	client := &http.Client{Transport: NewTransport()}
	resp, err := client.Get("http://localhost:" + port)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("request to localhost succeeded, want ErrBlocked")
	}
	if !errors.Is(err, ErrBlocked) {
		t.Errorf("error = %v, want ErrBlocked", err)
	}
}

func TestTransportHasNoProxy(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://8.8.8.8:3128")
	t.Setenv("HTTPS_PROXY", "http://8.8.8.8:3128")
	if NewTransport().Proxy != nil {
		t.Error("Proxy is set; through a proxy the dial check sees the proxy's address")
	}
}
