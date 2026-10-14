package signing

import (
	"strconv"
	"testing"
	"time"
)

func TestSignVerify(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	body := []byte(`{"id":"evt_1"}`)
	ts := strconv.FormatInt(now.Unix(), 10)
	sig := Sign("whsec_test", now.Unix(), body)

	if err := Verify("whsec_test", ts, sig, body, time.Minute, now); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
	cases := map[string]func() error{
		"wrong secret":   func() error { return Verify("other", ts, sig, body, time.Minute, now) },
		"tampered body":  func() error { return Verify("whsec_test", ts, sig, []byte(`{"id":"evt_2"}`), time.Minute, now) },
		"old timestamp":  func() error { return Verify("whsec_test", ts, sig, body, time.Minute, now.Add(2*time.Minute)) },
		"bad timestamp":  func() error { return Verify("whsec_test", "abc", sig, body, time.Minute, now) },
		"missing prefix": func() error { return Verify("whsec_test", ts, sig[3:], body, time.Minute, now) },
	}
	for name, f := range cases {
		if f() == nil {
			t.Errorf("%s: expected verification to fail", name)
		}
	}
}
