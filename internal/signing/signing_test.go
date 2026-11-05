package signing_test

import (
	"bytes"
	"cmp"
	"encoding/base64"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	standardwebhooks "github.com/standard-webhooks/standard-webhooks/libraries/go"

	"forflow/internal/signing"
)

// Test vector published by the Standard Webhooks reference library
// (libraries/go/webhook_test.go, TestWebhookSign).
const (
	vectorSecret    = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
	vectorMsgID     = "msg_p5jXN8AQM9LWM0D4loKWxJek"
	vectorTimestamp = 1614265330
	vectorPayload   = `{"test": 2432232314}`
	vectorSignature = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="
)

const tolerance = 5 * time.Minute

func vectorKey(t *testing.T) []byte {
	t.Helper()
	key, err := signing.DecodeSecret(vectorSecret)
	if err != nil {
		t.Fatalf("DecodeSecret(vector) error = %v", err)
	}
	return key
}

func TestSignMatchesSpecVector(t *testing.T) {
	got := signing.Sign(vectorKey(t), vectorMsgID, time.Unix(vectorTimestamp, 0), []byte(vectorPayload))
	if got != vectorSignature {
		t.Errorf("Sign() = %q, want %q", got, vectorSignature)
	}
}

// Cross-checking both directions proves consumers using the official library
// can verify us, not just that Sign and Verify agree with each other.
func TestCompatibleWithOfficialLibrary(t *testing.T) {
	secret := signing.GenerateSecret()
	ts := time.Unix(vectorTimestamp, 0)
	body := []byte(`{"order":1}`)

	lib, err := standardwebhooks.NewWebhookRaw(secret)
	if err != nil {
		t.Fatalf("NewWebhookRaw() error = %v", err)
	}

	t.Run("library verifies our signature", func(t *testing.T) {
		h := http.Header{}
		h.Set(standardwebhooks.HeaderWebhookID, vectorMsgID)
		h.Set(standardwebhooks.HeaderWebhookTimestamp, strconv.FormatInt(ts.Unix(), 10))
		h.Set(standardwebhooks.HeaderWebhookSignature, signing.Sign(secret, vectorMsgID, ts, body))

		if err := lib.VerifyIgnoringTimestamp(body, h); err != nil {
			t.Errorf("library Verify() error = %v", err)
		}
	})

	t.Run("we verify library signature", func(t *testing.T) {
		sig, err := lib.Sign(vectorMsgID, ts, body)
		if err != nil {
			t.Fatalf("library Sign() error = %v", err)
		}
		tsHeader := strconv.FormatInt(ts.Unix(), 10)

		if err := signing.Verify(secret, vectorMsgID, tsHeader, sig, body, ts, tolerance); err != nil {
			t.Errorf("Verify() error = %v", err)
		}
	})

	t.Run("encoded secret is accepted by library", func(t *testing.T) {
		if _, err := standardwebhooks.NewWebhook(signing.EncodeSecret(secret)); err != nil {
			t.Errorf("library NewWebhook(EncodeSecret()) error = %v", err)
		}
	})
}

func TestVerify(t *testing.T) {
	key := vectorKey(t)
	ts := time.Unix(vectorTimestamp, 0)
	tsHeader := strconv.Itoa(vectorTimestamp)
	body := []byte(vectorPayload)
	otherKey := bytes.Repeat([]byte{7}, 32)
	otherSig := signing.Sign(otherKey, vectorMsgID, ts, body)
	badB64 := "v1," + base64.StdEncoding.EncodeToString([]byte("not the mac"))

	tests := []struct {
		name     string
		msgID    string
		tsHeader string
		sig      string
		body     []byte
		now      time.Time
		want     error
	}{
		{name: "valid", sig: vectorSignature},
		{name: "valid at past tolerance edge", sig: vectorSignature, now: ts.Add(tolerance)},
		{name: "valid at future tolerance edge", sig: vectorSignature, now: ts.Add(-tolerance)},
		{name: "rotation: matching signature second", sig: otherSig + " " + vectorSignature},
		{name: "rotation: matching signature first", sig: vectorSignature + " " + otherSig},
		{name: "extra whitespace between signatures", sig: "  " + otherSig + "   " + vectorSignature + "  "},
		{name: "malformed parts are skipped", sig: ",,, v1,not,base64  v1, " + vectorSignature},

		{name: "body changed by one byte", sig: vectorSignature, body: []byte(`{"test": 2432232315}`), want: signing.ErrNotMatchSignature},
		{name: "different msg id", sig: vectorSignature, msgID: "msg_other", want: signing.ErrNotMatchSignature},
		{name: "signed with other secret", sig: otherSig, want: signing.ErrNotMatchSignature},
		{name: "timestamp header not as signed", sig: vectorSignature, tsHeader: "0" + tsHeader, want: signing.ErrNotMatchSignature},
		{name: "unknown version only", sig: strings.Replace(vectorSignature, "v1,", "v2,", 1), want: signing.ErrNotMatchSignature},
		{name: "empty signature header", sig: "", want: signing.ErrNotMatchSignature},
		{name: "version without value", sig: "v1,", want: signing.ErrNotMatchSignature},
		{name: "valid base64 wrong mac", sig: badB64, want: signing.ErrNotMatchSignature},

		{name: "too old", sig: vectorSignature, now: ts.Add(tolerance + time.Second), want: signing.ErrTimestampOutOfRange},
		{name: "too far in future", sig: vectorSignature, now: ts.Add(-tolerance - time.Second), want: signing.ErrTimestampOutOfRange},
		{name: "milliseconds timestamp", sig: vectorSignature, tsHeader: strconv.FormatInt(ts.UnixMilli(), 10), want: signing.ErrTimestampOutOfRange},
		{name: "duration saturates far future", sig: vectorSignature, tsHeader: strconv.FormatInt(math.MaxInt64, 10), want: signing.ErrTimestampOutOfRange},
		{name: "duration saturates far past", sig: vectorSignature, tsHeader: strconv.FormatInt(math.MinInt64, 10), want: signing.ErrTimestampOutOfRange},

		{name: "timestamp not a number", sig: vectorSignature, tsHeader: "abc", want: signing.ErrInvalidTimestamp},
		{name: "timestamp empty", sig: vectorSignature, tsHeader: " ", want: signing.ErrInvalidTimestamp},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msgID := cmp.Or(tt.msgID, vectorMsgID)
			hdr := cmp.Or(tt.tsHeader, tsHeader)
			b := body
			if tt.body != nil {
				b = tt.body
			}
			now := ts
			if !tt.now.IsZero() {
				now = tt.now
			}

			err := signing.Verify(key, msgID, hdr, tt.sig, b, now, tolerance)
			if !errors.Is(err, tt.want) {
				t.Errorf("Verify() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestDecodeSecret(t *testing.T) {
	enc := func(n int) string {
		return "whsec_" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, n))
	}

	tests := []struct {
		name    string
		in      string
		wantLen int
		wantErr bool
	}{
		{name: "spec vector", in: vectorSecret, wantLen: 24},
		{name: "minimum 24 bytes", in: enc(24), wantLen: 24},
		{name: "maximum 64 bytes", in: enc(64), wantLen: 64},
		{name: "23 bytes too short", in: enc(23), wantErr: true},
		{name: "65 bytes too long", in: enc(65), wantErr: true},
		{name: "missing prefix", in: strings.TrimPrefix(vectorSecret, "whsec_"), wantErr: true},
		{name: "wrong prefix", in: "whesc_" + strings.TrimPrefix(vectorSecret, "whsec_"), wantErr: true},
		{name: "invalid base64", in: "whsec_!!!not-base64!!!", wantErr: true},
		{name: "prefix only", in: "whsec_", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := signing.DecodeSecret(tt.in)
			if tt.wantErr {
				if !errors.Is(err, signing.ErrInvalidSecret) {
					t.Errorf("DecodeSecret() error = %v, want ErrInvalidSecret", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("DecodeSecret() error = %v", err)
			}
			if len(got) != tt.wantLen {
				t.Errorf("len = %d, want %d", len(got), tt.wantLen)
			}
		})
	}
}

func TestGenerateSecret(t *testing.T) {
	a, b := signing.GenerateSecret(), signing.GenerateSecret()

	if len(a) < 24 || len(a) > 64 {
		t.Errorf("len = %d, want within spec range 24..64", len(a))
	}
	if bytes.Equal(a, b) {
		t.Error("two generated secrets are equal")
	}

	decoded, err := signing.DecodeSecret(signing.EncodeSecret(a))
	if err != nil {
		t.Fatalf("DecodeSecret(EncodeSecret()) error = %v", err)
	}
	if !bytes.Equal(decoded, a) {
		t.Error("EncodeSecret/DecodeSecret round trip changed the secret")
	}
}

func TestFormatTimestamp(t *testing.T) {
	ts := time.Unix(vectorTimestamp, 999_999_999)
	want := strconv.Itoa(vectorTimestamp)

	if got := signing.FormatTimestamp(ts); got != want {
		t.Errorf("FormatTimestamp() = %q, want %q (whole seconds, truncated)", got, want)
	}
	// The header is an instant, so the location must not change it.
	if got := signing.FormatTimestamp(ts.In(time.FixedZone("UTC+7", 7*3600))); got != want {
		t.Errorf("FormatTimestamp(non-UTC) = %q, want %q", got, want)
	}
}

// A receiver mid-rotation holds either the old or the new secret; the official
// library must accept the header with either one, and reject any other.
func TestSignAllVerifiesWithEitherSecret(t *testing.T) {
	current, previous := signing.GenerateSecret(), signing.GenerateSecret()
	ts := time.Unix(vectorTimestamp, 0)
	body := []byte(`{"order":1}`)

	sig := signing.SignAll([][]byte{current, previous}, vectorMsgID, ts, body)
	if n := len(strings.Fields(sig)); n != 2 {
		t.Fatalf("header has %d signatures, want 2: %q", n, sig)
	}

	h := http.Header{}
	h.Set(standardwebhooks.HeaderWebhookID, vectorMsgID)
	h.Set(standardwebhooks.HeaderWebhookTimestamp, strconv.FormatInt(ts.Unix(), 10))
	h.Set(standardwebhooks.HeaderWebhookSignature, sig)

	for name, secret := range map[string][]byte{"current": current, "previous": previous} {
		lib, err := standardwebhooks.NewWebhookRaw(secret)
		if err != nil {
			t.Fatalf("NewWebhookRaw() error = %v", err)
		}
		if err := lib.VerifyIgnoringTimestamp(body, h); err != nil {
			t.Errorf("library rejected the %s secret: %v", name, err)
		}
	}

	lib, err := standardwebhooks.NewWebhookRaw(signing.GenerateSecret())
	if err != nil {
		t.Fatalf("NewWebhookRaw() error = %v", err)
	}
	if err := lib.VerifyIgnoringTimestamp(body, h); err == nil {
		t.Error("library accepted an unrelated secret")
	}
}
