// Package signing implements Standard Webhooks v1 signatures (HMAC-SHA256).
package signing

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"
)

// Secret sizes follow the spec range of 24 to 64 bytes.
const (
	PREFIX     = "whsec_"
	SECRET_LEN = 32
	MIN_LEN    = 24
	MAX_LEN    = 64
	VERSION    = "v1"
)

// Sentinel errors, matched with errors.Is.
var (
	ErrInvalidSecret       = errors.New("invalid secret")
	ErrInvalidTimestamp    = errors.New("invalid timestamp")
	ErrTimestampOutOfRange = errors.New("timestamp out of range")
	ErrNotMatchSignature   = errors.New("signature not match")
)

// GenerateSecret returns a new random endpoint secret. The error from
// rand.Read is ignored because it is documented to never return one.
func GenerateSecret() []byte {
	buf := make([]byte, SECRET_LEN)
	_, _ = rand.Read(buf)

	return buf
}

// EncodeSecret formats raw as the whsec_ string shown to customers.
func EncodeSecret(raw []byte) string {
	return PREFIX + base64.StdEncoding.EncodeToString(raw)
}

// DecodeSecret parses a whsec_ string back to raw key bytes. It rejects
// lengths outside the spec range so a weak key never reaches Sign.
func DecodeSecret(s string) ([]byte, error) {
	if !strings.HasPrefix(s, PREFIX) {
		return nil, ErrInvalidSecret
	}

	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(s, PREFIX))
	if err != nil {
		return nil, ErrInvalidSecret
	}

	if len(raw) < MIN_LEN || len(raw) > MAX_LEN {
		return nil, ErrInvalidSecret
	}

	return raw, nil
}

// Sign returns the webhook-signature value for one attempt. Call it per
// attempt: ts changes on every retry, msgID never does.
func Sign(secret []byte, msgID string, ts time.Time, body []byte) string {
	tsStr := FormatTimestamp(ts)

	mac := writeHMAC(msgID, tsStr, secret, body)

	return VERSION + "," + base64.StdEncoding.EncodeToString(mac)
}

// SignAll returns one signature per secret, space separated, which is how the
// spec carries a key rotation: a consumer accepts the header if any one matches.
func SignAll(secrets [][]byte, msgID string, ts time.Time, body []byte) string {
	sigs := make([]string, len(secrets))
	for i, secret := range secrets {
		sigs[i] = Sign(secret, msgID, ts, body)
	}

	return strings.Join(sigs, " ")
}

// Verify checks a signature header the way a consumer would. tsHeader is
// signed as received, not reformatted, so it must match the sender byte for
// byte. Any one v1 signature matching is enough, which allows secret rotation.
func Verify(secret []byte, msgID, tsHeader string, sigHeader string, body []byte, now time.Time, tolerance time.Duration) error {
	ts, err := strconv.ParseInt(tsHeader, 10, 64)
	if err != nil {
		return ErrInvalidTimestamp
	}

	diff := now.Sub(time.Unix(ts, 0))

	if diff.Abs() > tolerance {
		return ErrTimestampOutOfRange
	}

	mac := writeHMAC(msgID, tsHeader, secret, body)

	for _, part := range strings.Fields(sigHeader) {
		version, value, ok := strings.Cut(part, ",")
		if !ok || version != VERSION {
			continue
		}

		got, err := base64.StdEncoding.DecodeString(value)
		if err == nil && hmac.Equal(got, mac) {
			return nil
		}
	}

	return ErrNotMatchSignature
}

// writeHMAC computes the raw MAC over "msgID.ts.body", writing the parts in
// sequence so the body is never copied.
func writeHMAC(msgID, tsStr string, secret, body []byte) []byte {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(msgID))
	mac.Write([]byte{'.'})
	mac.Write([]byte(tsStr))
	mac.Write([]byte{'.'})
	mac.Write(body)

	return mac.Sum(nil)
}

// FormatTimestamp renders ts as the webhook-timestamp header value. Sign uses
// it too, so the header and the signed content cannot drift apart.
func FormatTimestamp(ts time.Time) string {
	return strconv.FormatInt(ts.Unix(), 10)
}
