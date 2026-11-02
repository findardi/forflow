package delivery

import (
	"errors"
	"net"
	"net/url"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestNextRetry(t *testing.T) {
	tests := []struct {
		attemptsBefore int32
		wantWait       time.Duration
		wantOK         bool
	}{
		{attemptsBefore: 0, wantWait: 5 * time.Second, wantOK: true},
		{attemptsBefore: 1, wantWait: 5 * time.Minute, wantOK: true},
		{attemptsBefore: 2, wantWait: 30 * time.Minute, wantOK: true},
		{attemptsBefore: 3, wantWait: 2 * time.Hour, wantOK: true},
		{attemptsBefore: 4, wantWait: 5 * time.Hour, wantOK: true},
		{attemptsBefore: 5, wantWait: 10 * time.Hour, wantOK: true},
		{attemptsBefore: 6, wantWait: 10 * time.Hour, wantOK: true},
		{attemptsBefore: 7, wantOK: false}, // eighth attempt was the last
		{attemptsBefore: 100, wantOK: false},
		{attemptsBefore: -1, wantOK: false},
	}
	for _, tt := range tests {
		wait, ok := nextRetry(tt.attemptsBefore)
		if ok != tt.wantOK || wait != tt.wantWait {
			t.Errorf("nextRetry(%d) = (%v, %v), want (%v, %v)",
				tt.attemptsBefore, wait, ok, tt.wantWait, tt.wantOK)
		}
	}
}

func TestTenPercentJitterStaysInBounds(t *testing.T) {
	for _, d := range retrySchedule {
		for range 1000 {
			got := tenPercentJitter(d)
			if got < d || got >= d+d/10 {
				t.Fatalf("tenPercentJitter(%v) = %v, want in [%v, %v)", d, got, d, d+d/10)
			}
		}
	}
	if got := tenPercentJitter(0); got != 0 {
		t.Errorf("tenPercentJitter(0) = %v, want 0", got)
	}
}

func TestErrorKind(t *testing.T) {
	// Shapes mirror what http.Client.Do returns: *url.Error wrapping the cause.
	wrap := func(err error) error { return &url.Error{Op: "Post", URL: "http://example.test", Err: err} }

	tests := []struct {
		name string
		err  error
		want string
	}{
		{"dns", wrap(&net.OpError{Op: "dial", Err: &net.DNSError{Err: "no such host", Name: "x.test"}}), "dns"},
		{"dns timeout stays dns", wrap(&net.DNSError{Err: "timeout", IsTimeout: true}), "dns"},
		{"deadline", wrap(os.ErrDeadlineExceeded), "timeout"},
		{"refused", wrap(&net.OpError{Op: "dial", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}), "conn_refused"},
		{"other", wrap(errors.New("boom")), "other"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := errorKind(tt.err); got != tt.want {
				t.Errorf("errorKind = %q, want %q", got, tt.want)
			}
		})
	}
}
